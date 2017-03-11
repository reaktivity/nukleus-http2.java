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
package org.reaktivity.nukleus.http2.internal.types.stream;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderFieldFW.HeaderFieldType;

import javax.xml.bind.DatatypeConverter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW.LiteralType.INCREMENTAL_INDEXING;

public class HpackHeaderBlockFWTest
{

    // Test for decoding "C.3.  Request Examples without Huffman Coding"
    @Test
    public void decodeC3()
    {
        HpackContext context = new HpackContext();

        // First request
        decodeC31(context);

        // Second request
        decodeC32(context);

        // Third request
        decodeC33(context);
    }

    // Decoding "C.3.1.  First Request"
    private void decodeC31(HpackContext context)
    {
        byte[] bytes = DatatypeConverter.parseHexBinary(
                "00" +  // +00 to test offset
                        // Header list begin
                        "828684410f7777772e6578616d706c652e636f6d" +
                        // Header list end
                        "00");
        DirectBuffer buffer = new UnsafeBuffer(bytes);
        HpackHeaderBlockFW fw = new HpackHeaderBlockFW().wrap(buffer, 1, buffer.capacity()-1);
        assertEquals(21, fw.limit());

        Map<String, String> headers = new LinkedHashMap<>();
        fw.forEach(getHeaders(context, headers));

        assertEquals(4, headers.size());
        assertEquals("GET", headers.get(":method"));
        assertEquals("http", headers.get(":scheme"));
        assertEquals("/", headers.get(":path"));
        assertEquals("www.example.com", headers.get(":authority"));
    }

    // Decoding "C.3.2.  Second Request"
    private void decodeC32(HpackContext context)
    {
        byte[] bytes = DatatypeConverter.parseHexBinary(
                "00" +  // +00 to test offset
                        // Header list begin
                        "828684be58086e6f2d6361636865" +
                        // Header list end
                        "00");
        DirectBuffer buffer = new UnsafeBuffer(bytes);
        HpackHeaderBlockFW fw = new HpackHeaderBlockFW().wrap(buffer, 1, buffer.capacity()-1);
        assertEquals(15, fw.limit());

        Map<String, String> headers = new LinkedHashMap<>();
        fw.forEach(getHeaders(context, headers));

        assertEquals(5, headers.size());
        assertEquals("GET", headers.get(":method"));
        assertEquals("http", headers.get(":scheme"));
        assertEquals("/", headers.get(":path"));
        assertEquals("www.example.com", headers.get(":authority"));
        assertEquals("no-cache", headers.get("cache-control"));
    }

    // Decoding "C.3.3.  Third Request"
    private void decodeC33(HpackContext context)
    {
        byte[] bytes = DatatypeConverter.parseHexBinary(
                "00" +  // +00 to test offset
                        // Header list begin
                        "828785bf400a637573746f6d2d6b65790c637573746f6d2d76616c7565" +
                        // Header list end
                        "00");
        DirectBuffer buffer = new UnsafeBuffer(bytes);
        HpackHeaderBlockFW fw = new HpackHeaderBlockFW().wrap(buffer, 1, buffer.capacity()-1);
        assertEquals(30, fw.limit());

        Map<String, String> headers = new LinkedHashMap<>();
        fw.forEach(getHeaders(context, headers));

        assertEquals(5, headers.size());
        assertEquals("GET", headers.get(":method"));
        assertEquals("https", headers.get(":scheme"));
        assertEquals("/index.html", headers.get(":path"));
        assertEquals("www.example.com", headers.get(":authority"));
        assertEquals("custom-value", headers.get("custom-key"));
    }

    // Test for encoding "C.3.  Request Examples without Huffman Coding"
    @Test
    public void encodeC3()
    {
        HpackContext context = new HpackContext();

        // First request
        encodeC31(context);

        // Second request
        encodeC32(context);

        // Third request
        encodeC33(context);
    }

    // Encoding "C.3.1.  First Request"
    private void encodeC31(HpackContext context)
    {
        byte[] bytes = new byte[100];
        MutableDirectBuffer buf = new UnsafeBuffer(bytes);

        HpackHeaderBlockFW fw = new HpackHeaderBlockFW.Builder()
                .wrap(buf, 1, buf.capacity())
                .header(hf -> hf.indexed(2))      // :method: GET
                .header(hf -> hf.indexed(6))      // :scheme: http
                .header(hf -> hf.indexed(4))      // :path: /
                .header(hf -> hf.literal(l -> l.type(INCREMENTAL_INDEXING).name(1).value("www.example.com")))
                .build();

        assertEquals(21, fw.limit());

        Map<String, String> headers = new LinkedHashMap<>();
        fw.forEach(getHeaders(context, headers));

        assertEquals(4, headers.size());
        assertEquals("GET", headers.get(":method"));
        assertEquals("http", headers.get(":scheme"));
        assertEquals("/", headers.get(":path"));
        assertEquals("www.example.com", headers.get(":authority"));
    }

    // Encoding "C.3.2.  Second Request"
    private void encodeC32(HpackContext context)
    {
        byte[] bytes = new byte[100];
        MutableDirectBuffer buf = new UnsafeBuffer(bytes);

        HpackHeaderBlockFW fw = new HpackHeaderBlockFW.Builder()
                .wrap(buf, 1, buf.capacity())
                .header(hf -> hf.indexed(2))        // :method: GET
                .header(hf -> hf.indexed(6))        // :scheme: http
                .header(hf -> hf.indexed(4))        // :path: /
                .header(hf -> hf.indexed(62))       // :authority: www.example.com
                .header(hf -> hf.literal(l -> l.type(INCREMENTAL_INDEXING).name(24).value("no-cache")))
                .build();
        assertEquals(15, fw.limit());

        Map<String, String> headers = new LinkedHashMap<>();
        fw.forEach(getHeaders(context, headers));

        assertEquals(5, headers.size());
        assertEquals("GET", headers.get(":method"));
        assertEquals("http", headers.get(":scheme"));
        assertEquals("/", headers.get(":path"));
        assertEquals("www.example.com", headers.get(":authority"));
        assertEquals("no-cache", headers.get("cache-control"));
    }

    // Encoding "C.3.3.  Third Request"
    private void encodeC33(HpackContext context)
    {
        byte[] bytes = new byte[100];
        MutableDirectBuffer buf = new UnsafeBuffer(bytes);

        HpackHeaderBlockFW fw = new HpackHeaderBlockFW.Builder()
                .wrap(buf, 1, buf.capacity())
                .header(y -> y.indexed(2))    // :method: GET
                .header(y -> y.indexed(7))    // :scheme: https
                .header(y -> y.indexed(5))    // :path: /index.html
                .header(y -> y.indexed(63))   // :authority: www.example.com
                .header(y -> y.literal(z -> z.type(INCREMENTAL_INDEXING).name("custom-key").value("custom-value")))
                .build();
        assertEquals(30, fw.limit());

        Map<String, String> headers = new LinkedHashMap<>();
        fw.forEach(getHeaders(context, headers));

        assertEquals(5, headers.size());
        assertEquals("GET", headers.get(":method"));
        assertEquals("https", headers.get(":scheme"));
        assertEquals("/index.html", headers.get(":path"));
        assertEquals("www.example.com", headers.get(":authority"));
        assertEquals("custom-value", headers.get("custom-key"));
    }

    // Test for decoding "C.4.  Request Examples with Huffman Coding"
    @Test
    public void decodeC4()
    {
        HpackContext context = new HpackContext();

        // First request
        decodeC41(context);

        // Second request
        decodeC42(context);

        // Third request
        decodeC43(context);
    }

    // Decoding "C.4.1.  First Request"
    private void decodeC41(HpackContext context)
    {
        byte[] bytes = DatatypeConverter.parseHexBinary(
                "00" +  // +00 to test offset
                        // Header list begin
                        "828684418cf1e3c2e5f23a6ba0ab90f4ff" +
                        // Header list end
                        "00");
        DirectBuffer buffer = new UnsafeBuffer(bytes);
        HpackHeaderBlockFW fw = new HpackHeaderBlockFW().wrap(buffer, 1, buffer.capacity()-1);
        assertEquals(18, fw.limit());

        Map<String, String> headers = new LinkedHashMap<>();
        fw.forEach(getHeaders(context, headers));

        assertEquals(4, headers.size());
        assertEquals("GET", headers.get(":method"));
        assertEquals("http", headers.get(":scheme"));
        assertEquals("/", headers.get(":path"));
        assertEquals("www.example.com", headers.get(":authority"));
    }

    // Decoding "C.4.2.  Second Request"
    private void decodeC42(HpackContext context)
    {
        byte[] bytes = DatatypeConverter.parseHexBinary(
                "00" +  // +00 to test offset
                        // Header list begin
                        "828684be5886a8eb10649cbf" +
                        // Header list end
                        "00");
        DirectBuffer buffer = new UnsafeBuffer(bytes);
        HpackHeaderBlockFW fw = new HpackHeaderBlockFW().wrap(buffer, 1, buffer.capacity()-1);
        assertEquals(13, fw.limit());

        Map<String, String> headers = new LinkedHashMap<>();
        fw.forEach(getHeaders(context, headers));

        assertEquals(5, headers.size());
        assertEquals("GET", headers.get(":method"));
        assertEquals("http", headers.get(":scheme"));
        assertEquals("/", headers.get(":path"));
        assertEquals("www.example.com", headers.get(":authority"));
        assertEquals("no-cache", headers.get("cache-control"));
    }

    // Decoding "C.4.3.  Third Request"
    private void decodeC43(HpackContext context)
    {
        byte[] bytes = DatatypeConverter.parseHexBinary(
                "00" +  // +00 to test offset
                        // Header list begin
                        "828785bf408825a849e95ba97d7f8925a849e95bb8e8b4bf" +
                        // Header list end
                        "00");
        DirectBuffer buffer = new UnsafeBuffer(bytes);
        HpackHeaderBlockFW fw = new HpackHeaderBlockFW().wrap(buffer, 1, buffer.capacity()-1);
        assertEquals(25, fw.limit());

        Map<String, String> headers = new LinkedHashMap<>();
        fw.forEach(getHeaders(context, headers));

        assertEquals(5, headers.size());
        assertEquals("GET", headers.get(":method"));
        assertEquals("https", headers.get(":scheme"));
        assertEquals("/index.html", headers.get(":path"));
        assertEquals("www.example.com", headers.get(":authority"));
        assertEquals("custom-value", headers.get("custom-key"));
    }


    static Consumer<HpackHeaderFieldFW> getHeaders(HpackContext context, Map<String, String> headers)
    {
        return x -> {
            HeaderFieldType headerFieldType = x.type();
            String name = null;
            String value = null;
            switch (headerFieldType)
            {
                case INDEXED :
                    int index = x.index();
                    name = context.name(index);
                    value = context.value(index);
                    headers.put(name, value);
                    break;
                case LITERAL :
                    HpackLiteralHeaderFieldFW literalRO = x.literal();
                    switch (literalRO.nameType())
                    {
                        case INDEXED:
                        {
                            index = literalRO.nameIndex();
                            name = context.name(index);

                            HpackStringFW valueRO = literalRO.valueLiteral();
                            DirectBuffer valuePayload = valueRO.payload();
                            value = valueRO.huffman()
                                    ? HpackHuffman.decode(valuePayload)
                                    : valuePayload.getStringWithoutLengthUtf8(0, valuePayload.capacity());
                            headers.put(name, value);
                        }
                        break;
                        case NEW:
                        {
                            HpackStringFW nameRO = literalRO.nameLiteral();
                            DirectBuffer namePayload = nameRO.payload();
                            name = nameRO.huffman()
                                    ? HpackHuffman.decode(namePayload)
                                    : namePayload.getStringWithoutLengthUtf8(0, namePayload.capacity());

                            HpackStringFW valueRO = literalRO.valueLiteral();
                            DirectBuffer valuePayload = valueRO.payload();
                            value = valueRO.huffman()
                                    ? HpackHuffman.decode(valuePayload)
                                    : valuePayload.getStringWithoutLengthUtf8(0, valuePayload.capacity());
                            headers.put(name, value);
                        }
                        break;
                    }
                    if (literalRO.literalType() == INCREMENTAL_INDEXING)
                    {
                        context.add(name, value);
                    }
                    break;

                case UPDATE:
                    break;
            }
        };
    }

}
