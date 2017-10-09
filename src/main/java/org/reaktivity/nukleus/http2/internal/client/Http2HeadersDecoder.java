/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.http2.internal.client;

import static org.reaktivity.nukleus.http2.internal.types.stream.HpackContext.TE;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackContext.TRAILERS;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderFieldFW.HeaderFieldType.UNKNOWN;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW.LiteralType.INCREMENTAL_INDEXING;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackContext;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderBlockFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHuffman;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackStringFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;
import org.reaktivity.nukleus.http2.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW.NameType;

class Http2HeadersDecoder
{
    private final ClientStreamFactory factory;

    private boolean expectDynamicTableSizeUpdate = true;
    private final HpackContext decodeHpackContext;
    private final int headerTableSize;

    Http2ErrorCode connectionError;
    Http2ErrorCode streamError;

    MutableDirectBuffer tmpNameBuffer = new UnsafeBuffer(new byte[4096]);
    MutableDirectBuffer tmpValueBuffer = new UnsafeBuffer(new byte[4096]);

    boolean regularHeaderFound;

    long contentLength = -1;
    boolean statusFound;

    Http2HeadersDecoder(ClientStreamFactory factory, int headerTableSize)
    {
        this.factory = factory;
        this.headerTableSize = headerTableSize;
        decodeHpackContext = new HpackContext(headerTableSize, false);
    }

    /**
     * Validates http2 headers and transforms them to http headers
     * @param blockRO - the block of http2 headers
     * @return the list of http headers in a HttpBeginExFW.
     */
    HttpBeginExFW decodeHeaders(HpackHeaderBlockFW blockRO)
    {
        reset();
        factory.httpBeginExRW.wrap(factory.scratch, 0, factory.scratch.capacity());
        blockRO.forEach(this::toHttpHeader);

        if (error())
        {
            return null;
        }

        if (! statusFound)
        {
            streamError = Http2ErrorCode.PROTOCOL_ERROR;
            return null;
        }
        return factory.httpBeginExRW.build();
    }

    /**
     * Validate one http2 header field and adds it to headers in factory.httpBeginExRW
     * @param hf The header field to extract
     */
    private void toHttpHeader(HpackHeaderFieldFW hf)
    {
        if (hf.type() == UNKNOWN)
        {
            connectionError = Http2ErrorCode.COMPRESSION_ERROR;
            return;
        }

        dynamicTableSizeUpdate(hf);
        if (error())
        {
            return;
        }

        decodeHeaderField(hf);
    }

    private void dynamicTableSizeUpdate(HpackHeaderFieldFW hf)
    {
        switch (hf.type())
        {
        case INDEXED:
        case LITERAL:
            expectDynamicTableSizeUpdate = false;
            break;
        case UPDATE:
            if (!expectDynamicTableSizeUpdate)
            {
                // dynamic table size update MUST occur at the beginning of the first header block
                connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                return;
            }
            int maxTableSize = hf.tableSize();
            if (maxTableSize > headerTableSize)
            {
                connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                return;
            }
            decodeHpackContext.updateSize(hf.tableSize());
            break;
        default:
            break;
        }
    }

    private void decodeHeaderField(HpackHeaderFieldFW hf)
    {
        int index;

        switch (hf.type())
        {
        case INDEXED:
            index = hf.index();
            if (!decodeHpackContext.valid(index))
            {
                connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                return;
            }
            DirectBuffer name = decodeHpackContext.nameBuffer(index);
            DirectBuffer value = decodeHpackContext.valueBuffer(index);
            mapToHttp(name, value);
            break;

        case LITERAL:
            decodeLiteralHeaderField(hf);
            break;

        default:
            break;
        }
    }

    private void decodeLiteralHeaderField(HpackHeaderFieldFW hf)
    {
        int index;
        HpackLiteralHeaderFieldFW literalRO = hf.literal();
        if (literalRO.error())
        {
            connectionError = Http2ErrorCode.COMPRESSION_ERROR;
            return;
        }

        DirectBuffer name;
        DirectBuffer value;
        if (literalRO.nameType() == NameType.INDEXED)
        {
            index = literalRO.nameIndex();
            name = decodeHpackContext.nameBuffer(index);

            HpackStringFW valueRO = literalRO.valueLiteral();
            value = huffmanDecode(valueRO, tmpValueBuffer);
            if (value == null)
            {
                return;
            }
        }
        else
        { // NameType.NEW
            HpackStringFW nameRO = literalRO.nameLiteral();
            name = huffmanDecode(nameRO, tmpNameBuffer);
            if (name == null)
            {
                return;
            }

            HpackStringFW valueRO = literalRO.valueLiteral();
            value = huffmanDecode(valueRO, tmpValueBuffer);
            if (value == null)
            {
                return;
            }
        }
        mapToHttp(name, value);

        if (literalRO.literalType() == INCREMENTAL_INDEXING)
        {
            // make a copy for name and value as they go into dynamic table (outlives current frame)
            MutableDirectBuffer nameCopy = new UnsafeBuffer(new byte[name.capacity()]);
            nameCopy.putBytes(0, name, 0, name.capacity());
            MutableDirectBuffer valueCopy = new UnsafeBuffer(new byte[value.capacity()]);
            valueCopy.putBytes(0, value, 0, value.capacity());
            decodeHpackContext.add(nameCopy, valueCopy);
        }
    }

    // Writes HPACK header field to http representation in a buffer,
    // including some validations
    private void mapToHttp(DirectBuffer name, DirectBuffer value)
    {
        // validate connectionHeaders
        if (name.equals(HpackContext.CONNECTION))
        {
            streamError = Http2ErrorCode.PROTOCOL_ERROR;
            return;
        }

        // validate uppercaseHeaders
        for(int i=0; i < name.capacity(); i++)
        {
            if (name.getByte(i) >= 'A' && name.getByte(i) <= 'Z')
            {
                streamError = Http2ErrorCode.PROTOCOL_ERROR;
                return;
            }
        }

        validatePseudoHeaders(name, value);
        if (error())
        {
            return;
        }

        // 8.1.2.2 TE header MUST NOT contain any value other than "trailers".
        if (name.equals(TE) && !value.equals(TRAILERS))
        {
            streamError = Http2ErrorCode.PROTOCOL_ERROR;
            return;
        }

        // contentLengthHeader
        if (name.equals(decodeHpackContext.nameBuffer(28)))
        {
            String contentLengthStr = value.getStringWithoutLengthUtf8(0, value.capacity());
            contentLength = Long.parseLong(contentLengthStr);
        }

        factory.httpBeginExRW.headersItem(item -> item.name(name, 0, name.capacity())
                                                      .value(value, 0, value.capacity()));
    }

    /**
     * Validates and marks the presence of pseudo headers
     * @param name Name of the header
     * @param value Value of the header
     */
    private void validatePseudoHeaders(DirectBuffer name, DirectBuffer value)
    {
        if (name.capacity() > 0 && name.getByte(0) == ':')
        {
            // All pseudo-header fields MUST appear in the header block before regular header fields
            if (regularHeaderFound)
            {
                streamError = Http2ErrorCode.PROTOCOL_ERROR;
                return;
            }
            // request pseudo-header fields MUST be one of :status (others ?)
            int index = decodeHpackContext.index(name);
            switch (index)
            {
                case 8:             // :method
                    statusFound = true;
                    break;
                default:
                    streamError = Http2ErrorCode.PROTOCOL_ERROR;
                    return;
            }
        }
        else
        {
            regularHeaderFound = true;
        }
    }

    private DirectBuffer huffmanDecode(HpackStringFW stringFw, MutableDirectBuffer tmpBuffer)
    {
        DirectBuffer value = stringFw.payload();
        if (stringFw.huffman())
        {
            int length = HpackHuffman.decode(value, tmpBuffer);
            if (length == -1)
            {
                connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                return null;
            }
            value = new UnsafeBuffer(tmpBuffer, 0, length);
        }
        return value;
    }

    void reset()
    {
        connectionError = null;
        streamError = null;

        statusFound = false;
        regularHeaderFound = false;
        contentLength = -1;
    }

    boolean error()
    {
        return streamError != null || connectionError != null;
    }
}
