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

package org.reaktivity.nukleus.http2.internal.routable.stream;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.http2.internal.routable.Route;
import org.reaktivity.nukleus.http2.internal.routable.Target;
import org.reaktivity.nukleus.http2.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackContext;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHuffman;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackStringFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2HeadersFW;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW.LiteralType.INCREMENTAL_INDEXING;

class Http2Stream
{

    enum State
    {
        IDLE,
        RESERVED_LOCAL,
        RESERVED_REMOTE,
        OPEN,
        HALF_CLOSED_LOCAL,
        HALF_CLOSED_REMOTE,
        CLOSED
    }

    private final SourceInputStreamFactory.SourceInputStream connection;
    private final int http2StreamId;
    private final long targetId;
    private Route route;
    private State state;

    Http2Stream(SourceInputStreamFactory.SourceInputStream connection, int http2StreamId, long targetId)
    {
        this.connection = connection;
        this.http2StreamId = http2StreamId;
        this.targetId = targetId;
        this.state = State.IDLE;
    }

    void decode(Http2FrameFW http2RO)
    {
        switch (state)
        {
            case IDLE:
                inIdle(http2RO);
                break;
            case RESERVED_LOCAL:
                inReservedLocal(http2RO);
                break;
            case RESERVED_REMOTE:
                inReservedRemote(http2RO);
                break;
            case OPEN:
                inOpen(http2RO);
                break;
            case HALF_CLOSED_LOCAL:
                inHalfClosedLocal(http2RO);
                break;
            case HALF_CLOSED_REMOTE:
                inHalfClosedRemote(http2RO);
                break;
            case CLOSED:
                inClosed(http2RO);
                break;
        }
    }

    private void inClosed(Http2FrameFW http2RO)
    {
    }

    private void inHalfClosedRemote(Http2FrameFW http2RO)
    {
        if (!(http2RO.type() == Http2FrameType.WINDOW_UPDATE || http2RO.type() == Http2FrameType.PRIORITY
                || http2RO.type() == Http2FrameType.RST_STREAM))
        {
            connection.error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        if (http2RO.type() == Http2FrameType.RST_STREAM)
        {
            state = State.CLOSED;
        }
    }

    private void inHalfClosedLocal(Http2FrameFW http2RO)
    {
        if (http2RO.endStream() || http2RO.type() == Http2FrameType.RST_STREAM)
        {
            state = State.CLOSED;
        }
    }

    private void inOpen(Http2FrameFW http2RO)
    {
        if (http2RO.type() == Http2FrameType.DATA)
        {
            doData(http2RO);
        }
        if (http2RO.endStream())
        {
            state = State.HALF_CLOSED_REMOTE;
        }
    }

    private void inReservedRemote(Http2FrameFW http2RO)
    {
        if (!(http2RO.type() == Http2FrameType.HEADERS || http2RO.type() == Http2FrameType.RST_STREAM
                || http2RO.type() == Http2FrameType.PRIORITY))
        {
            connection.error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        if (http2RO.type() == Http2FrameType.RST_STREAM)
        {
            state = State.CLOSED;
        }
        else if (http2RO.type() == Http2FrameType.HEADERS)
        {
            state = State.HALF_CLOSED_LOCAL;
        }
    }

    private void inReservedLocal(Http2FrameFW http2RO)
    {
        if (!(http2RO.type() == Http2FrameType.RST_STREAM || http2RO.type() == Http2FrameType.PRIORITY
                || http2RO.type() == Http2FrameType.WINDOW_UPDATE))
        {
            connection.error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        if (http2RO.type() == Http2FrameType.RST_STREAM)
        {
            state = State.CLOSED;
        }
    }

    private void inIdle(Http2FrameFW http2RO)
    {
        if (!(http2RO.type() == Http2FrameType.HEADERS || http2RO.type() == Http2FrameType.PRIORITY))
        {
            connection.error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        if (http2RO.type() == Http2FrameType.HEADERS)
        {
            state = State.OPEN;
            doHeaders(http2RO);
        }
        else if (http2RO.type() == Http2FrameType.PUSH_PROMISE)
        {
            state = State.RESERVED_REMOTE;
        }
    }

    private void doHeaders(Http2FrameFW http2RO)
    {
        Http2HeadersFW headersRO = connection.headersRO();
        headersRO.wrap(http2RO.buffer(), http2RO.offset(), http2RO.limit());

        // TODO avoid iterating over headers twice
        Map<String, String> headersMap = new HashMap<>();
        headersRO.forEach(hf -> decodeHeaderField(connection.hpackContext, headersMap, hf));
        final Optional<Route> optional = connection.resolveTarget(connection.sourceRef, headersMap);
        route = optional.get();
        Target newTarget = route.target();
        final long targetRef = route.targetRef();

        newTarget.doHttpBegin(targetId, targetRef, targetId,
                hs -> headersRO.forEach(hf -> decodeHeaderField(connection.hpackContext, hs, hf)));
        newTarget.addThrottle(targetId, connection::handleThrottle);

        connection.source().doWindow(connection.sourceId, http2RO.sizeof());
        connection.throttleState = connection::throttleSkipNextWindow;
        state = State.OPEN;
    }

    private void doRst(Http2FrameFW http2RO)
    {
        if (state == State.IDLE)
        {
            connection.error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
    }

    private void doWindow(Http2FrameFW http2RO)
    {
        if (state == State.IDLE)
        {
            connection.error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
    }

    private void doContinuation(Http2FrameFW http2RO)
    {
        if (state == State.IDLE)
        {
            connection.error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
    }

    private void doData(Http2FrameFW http2RO)
    {
        if (state == State.IDLE)
        {
            connection.error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        if (route == null)
        {
            connection.processUnexpected(connection.sourceId);
            return;
        }
        Target newTarget = route.target();
        Http2DataFW dataRO = connection.dataRO().wrap(http2RO.buffer(), http2RO.offset(), http2RO.limit());
        newTarget.doHttpData(targetId, dataRO.buffer(), dataRO.dataOffset(), dataRO.dataLength());

    }



    private void decodeHeaderField(HpackContext context, ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW> builder,
                                   HpackHeaderFieldFW hf)
    {

        HpackHeaderFieldFW.HeaderFieldType headerFieldType = hf.type();
        switch (headerFieldType)
        {
            case INDEXED : {
                int index = hf.index();
                DirectBuffer nameBuffer = context.nameBuffer(index);
                DirectBuffer valueBuffer = context.valueBuffer(index);
                builder.item(i -> i.representation((byte) 0)
                                   .name(nameBuffer, 0, nameBuffer.capacity())
                                   .value(valueBuffer, 0, valueBuffer.capacity()));
            }
            break;

            case LITERAL :
                HpackLiteralHeaderFieldFW literalRO = hf.literal();
                switch (literalRO.nameType())
                {
                    case INDEXED:
                    {
                        int index = literalRO.nameIndex();
                        DirectBuffer nameBuffer = context.nameBuffer(index);

                        HpackStringFW valueRO = literalRO.valueLiteral();
                        DirectBuffer valuePayload = valueRO.payload();
                        if (valueRO.huffman())
                        {
                            String value = HpackHuffman.decode(valuePayload);
                            valuePayload = new UnsafeBuffer(value.getBytes(UTF_8));
                        }
                        DirectBuffer valueBuffer = valuePayload;
                        builder.item(i -> i.representation((byte) 0)
                                           .name(nameBuffer, 0, nameBuffer.capacity())
                                           .value(valueBuffer, 0, valueBuffer.capacity()));
                    }
                    break;
                    case NEW:
                    {
                        HpackStringFW nameRO = literalRO.nameLiteral();
                        DirectBuffer namePayload = nameRO.payload();
                        if (nameRO.huffman())
                        {
                            String name = HpackHuffman.decode(namePayload);
                            namePayload = new UnsafeBuffer(name.getBytes(UTF_8));
                        }
                        DirectBuffer nameBuffer = namePayload;

                        HpackStringFW valueRO = literalRO.valueLiteral();
                        DirectBuffer valuePayload = valueRO.payload();
                        if (valueRO.huffman())
                        {
                            String value = HpackHuffman.decode(valuePayload);
                            valuePayload = new UnsafeBuffer(value.getBytes(UTF_8));
                        }
                        DirectBuffer valueBuffer = valuePayload;
                        builder.item(i -> i.representation((byte) 0)
                                           .name(nameBuffer, 0, nameBuffer.capacity())
                                           .value(valueBuffer, 0, valueBuffer.capacity()));

                        if (literalRO.literalType() == INCREMENTAL_INDEXING)
                        {
                            context.add(nameBuffer, valueBuffer);
                        }
                    }
                    break;
                }
                break;

            case UPDATE:
                break;
        }
    }

    private void decodeHeaderField(HpackContext context, Map<String, String> headersMap, HpackHeaderFieldFW hf)
    {
        HpackHeaderFieldFW.HeaderFieldType headerFieldType = hf.type();
        switch (headerFieldType)
        {
            case INDEXED :
            {
                int index = hf.index();
                headersMap.put(context.name(index), context.value(index));
            }
            break;

            case LITERAL :
                HpackLiteralHeaderFieldFW literalRO = hf.literal();
                switch (literalRO.nameType())
                {
                    case INDEXED:
                    {
                        int index = literalRO.nameIndex();
                        String name = context.name(index);

                        HpackStringFW valueRO = literalRO.valueLiteral();
                        DirectBuffer valuePayload = valueRO.payload();
                        String value = valueRO.huffman()
                                ? HpackHuffman.decode(valuePayload)
                                : valuePayload.getStringWithoutLengthUtf8(0, valuePayload.capacity());
                        headersMap.put(name, value);
                    }
                    break;
                    case NEW: {
                        HpackStringFW nameRO = literalRO.nameLiteral();
                        DirectBuffer namePayload = nameRO.payload();
                        String name = nameRO.huffman()
                                ? HpackHuffman.decode(namePayload)
                                : namePayload.getStringWithoutLengthUtf8(0, namePayload.capacity());

                        HpackStringFW valueRO = literalRO.valueLiteral();
                        DirectBuffer valuePayload = valueRO.payload();
                        String value = valueRO.huffman()
                                ? HpackHuffman.decode(valuePayload)
                                : valuePayload.getStringWithoutLengthUtf8(0, valuePayload.capacity());
                        headersMap.put(name, value);
                    }
                    break;
                }
                break;

            case UPDATE:
                break;
        }
    }
}
