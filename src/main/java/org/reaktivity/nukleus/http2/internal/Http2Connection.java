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
package org.reaktivity.nukleus.http2.internal;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http2.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.OctetsFW;
import org.reaktivity.nukleus.http2.internal.types.String16FW;
import org.reaktivity.nukleus.http2.internal.types.StringFW;
import org.reaktivity.nukleus.http2.internal.types.control.HttpRouteExFW;
import org.reaktivity.nukleus.http2.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http2.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http2.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackContext;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderBlockFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHuffman;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackStringFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataExFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2Flags;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsId;
import org.reaktivity.nukleus.http2.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http2.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http2.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http2.internal.Http2Connection.State.CLOSED;
import static org.reaktivity.nukleus.http2.internal.Http2Connection.State.HALF_CLOSED_REMOTE;
import static org.reaktivity.nukleus.http2.internal.Http2Connection.State.OPEN;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackContext.CONNECTION;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackContext.DEFAULT_ACCESS_CONTROL_ALLOW_ORIGIN;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackContext.KEEP_ALIVE;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackContext.PROXY_CONNECTION;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackContext.TE;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackContext.TRAILERS;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackContext.UPGRADE;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderFieldFW.HeaderFieldType.UNKNOWN;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW.LiteralType.INCREMENTAL_INDEXING;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW.LiteralType.WITHOUT_INDEXING;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2PrefaceFW.PRI_REQUEST;

final class Http2Connection
{
    private static final Map<String, String> EMPTY_HEADERS = Collections.emptyMap();

    ServerStreamFactory factory;
    private DecoderState decoderState;

    // slab to assemble a complete HTTP2 frame
    // no need for separate slab per HTTP2 stream as the frames are not fragmented
    private int frameSlotIndex = NO_SLOT;
    private int frameSlotPosition;

    // slab to assemble a complete HTTP2 headers frame(including its continuation frames)
    // no need for separate slab per HTTP2 stream as no interleaved frames of any other type
    // or from any other stream
    private int headersSlotIndex = NO_SLOT;
    private int headersSlotPosition;

    long sourceId;
    long authorization;
    int lastStreamId;
    long sourceRef;
    int networkReplyWindowBudget;
    int networkReplyWindowPadding;
    int outWindowThreshold = -1;

    final WriteScheduler writeScheduler;

    final long sourceOutputEstId;
    private final HpackContext decodeContext;
    private final HpackContext encodeContext;
    private final MessageFunction<RouteFW> wrapRoute;

    final long networkReplyGroupId;

    final Int2ObjectHashMap<Http2Stream> http2Streams;      // HTTP2 stream-id --> Http2Stream

    private int clientStreamCount;
    private int promisedStreamCount;
    private int maxClientStreamId;
    private int maxPushPromiseStreamId;

    private boolean goaway;
    Settings initialSettings;
    Settings localSettings;
    Settings remoteSettings;
    private boolean expectContinuation;
    private int expectContinuationStreamId;
    private boolean expectDynamicTableSizeUpdate = true;
    long http2OutWindow;
    long http2InWindow;

    private boolean prefaceAvailable;
    private boolean http2FrameAvailable;
    private final Consumer<HpackHeaderFieldFW> headerFieldConsumer;
    private final HeadersContext headersContext = new HeadersContext();
    private final EncodeHeadersContext encodeHeadersContext = new EncodeHeadersContext();
    final Http2Writer http2Writer;
    MessageConsumer networkConsumer;
    RouteManager router;
    String sourceName;

    Http2Connection(ServerStreamFactory factory, RouteManager router, long networkReplyId, MessageConsumer networkConsumer,
                    MessageFunction<RouteFW> wrapRoute)
    {
        this.factory = factory;
        this.router = router;
        this.wrapRoute = wrapRoute;
        sourceOutputEstId = networkReplyId;
        http2Streams = new Int2ObjectHashMap<>();
        localSettings = new Settings();
        remoteSettings = new Settings();
        decodeContext = new HpackContext(localSettings.headerTableSize, false);
        encodeContext = new HpackContext(remoteSettings.headerTableSize, true);
        http2Writer = factory.http2Writer;
        writeScheduler = new Http2WriteScheduler(this, networkConsumer, http2Writer, sourceOutputEstId);
        http2InWindow = localSettings.initialWindowSize;
        http2OutWindow = remoteSettings.initialWindowSize;
        this.networkConsumer = networkConsumer;
        this.networkReplyGroupId = factory.supplyGroupId.getAsLong();

        BiConsumer<DirectBuffer, DirectBuffer> nameValue =
                ((BiConsumer<DirectBuffer, DirectBuffer>)this::collectHeaders)
                        .andThen(this::mapToHttp)
                        .andThen(this::validatePseudoHeaders)
                        .andThen(this::uppercaseHeaders)
                        .andThen(this::connectionHeaders)
                        .andThen(this::contentLengthHeader)
                        .andThen(this::teHeader);

        Consumer<HpackHeaderFieldFW> consumer = this::validateHeaderFieldType;
        consumer = consumer.andThen(this::dynamicTableSizeUpdate);
        this.headerFieldConsumer = consumer.andThen(h -> decodeHeaderField(h, nameValue));
    }

    void processUnexpected(
            long streamId)
    {
        factory.doReset(networkConsumer, streamId);
        cleanConnection();
    }

    void cleanConnection()
    {
        releaseSlot();
        releaseHeadersSlot();
        for(Http2Stream http2Stream : http2Streams.values())
        {
            closeStream(http2Stream);
        }
        http2Streams.clear();
    }

    void handleBegin(BeginFW beginRO)
    {
        this.sourceId = beginRO.streamId();
        this.authorization = beginRO.authorization();
        this.sourceRef = beginRO.sourceRef();
        this.sourceName = beginRO.source().asString();
        this.decoderState = this::decodePreface;
        initialSettings = new Settings(factory.config.serverConcurrentStreams(), 0);
        writeScheduler.settings(initialSettings.maxConcurrentStreams, initialSettings.initialWindowSize);
    }

    void handleData(DataFW dataRO)
    {
        OctetsFW payload = dataRO.payload();
        int limit = payload.limit();

        int offset = payload.offset();
        while (offset < limit)
        {
            offset += decoderState.decode(dataRO.buffer(), offset, limit);
        }
    }

    void handleAbort()
    {
        http2Streams.forEach((i, s) -> s.onAbort());
        cleanConnection();
    }

    void handleReset(ResetFW reset)
    {
        http2Streams.forEach((i, s) -> s.onReset());
        cleanConnection();
    }

    void handleEnd(EndFW end)
    {
        decoderState = (b, o, l) -> o;

        http2Streams.forEach((i, s) -> s.onEnd());
        writeScheduler.doEnd();
        cleanConnection();
    }

    // Decodes client preface
    private int decodePreface(final DirectBuffer buffer, final int offset, final int limit)
    {
        int length = prefaceAvailable(buffer, offset, limit);
        if (!prefaceAvailable)
        {
            return length;
        }
        if (factory.prefaceRO.error())
        {
            processUnexpected(sourceId);
            return limit-offset;
        }
        this.decoderState = this::decodeHttp2Frame;
        return length;
    }

    private int http2FrameLength(DirectBuffer buffer, final int offset, int limit)
    {
        assert limit - offset >= 3;

        int length = (buffer.getByte(offset) & 0xFF) << 16;
        length += (buffer.getShort(offset + 1, BIG_ENDIAN) & 0xFF_FF);
        return length + 9;      // +3 for length, +1 type, +1 flags, +4 stream-id
    }

    /*
     * Assembles a complete HTTP2 client preface and the flyweight is wrapped with the
     * buffer (it could be given buffer or slab)
     *
     * @return no of bytes consumed
     */
    private int prefaceAvailable(DirectBuffer buffer, int offset, int limit)
    {
        int available = limit - offset;

        if (frameSlotPosition > 0 && frameSlotPosition + available >= PRI_REQUEST.length)
        {
            MutableDirectBuffer prefaceBuffer = factory.framePool.buffer(frameSlotIndex);
            int remainingLength = PRI_REQUEST.length - frameSlotPosition;
            prefaceBuffer.putBytes(frameSlotPosition, buffer, offset, remainingLength);
            factory.prefaceRO.wrap(prefaceBuffer, 0, PRI_REQUEST.length);
            releaseSlot();
            prefaceAvailable = true;
            return remainingLength;
        }
        else if (available >= PRI_REQUEST.length)
        {
            factory.prefaceRO.wrap(buffer, offset, offset + PRI_REQUEST.length);
            prefaceAvailable = true;
            return PRI_REQUEST.length;
        }

        assert frameSlotIndex == NO_SLOT;
        if (!acquireSlot())
        {
            prefaceAvailable = false;
            return available;               // assume everything is consumed
        }

        MutableDirectBuffer prefaceBuffer = factory.framePool.buffer(frameSlotIndex);
        prefaceBuffer.putBytes(frameSlotPosition, buffer, offset, available);
        frameSlotPosition += available;
        prefaceAvailable = false;
        return available;
    }

    /*
     * Assembles a complete HTTP2 frame and the flyweight is wrapped with the
     * buffer (it could be given buffer or slab)
     *
     * @return consumed octets
     *         -1 if the frame size is more than the max frame size
     */
    // TODO check slab capacity
    private int http2FrameAvailable(DirectBuffer buffer, int offset, int limit)
    {
        int available = limit - offset;

        if (frameSlotPosition > 0 && frameSlotPosition + available >= 3)
        {
            MutableDirectBuffer frameBuffer = factory.framePool.buffer(frameSlotIndex);
            if (frameSlotPosition < 3)
            {
                frameBuffer.putBytes(frameSlotPosition, buffer, offset, 3 - frameSlotPosition);
            }
            int frameLength = http2FrameLength(frameBuffer, 0, 3);
            if (frameLength > localSettings.maxFrameSize + 9)
            {
                return -1;
            }
            if (frameSlotPosition + available >= frameLength)
            {
                int remainingFrameLength = frameLength - frameSlotPosition;
                frameBuffer.putBytes(frameSlotPosition, buffer, offset, remainingFrameLength);
                factory.http2RO.wrap(frameBuffer, 0, frameLength);
                releaseSlot();
                http2FrameAvailable = true;
                return remainingFrameLength;
            }
        }
        else if (available >= 3)
        {
            int frameLength = http2FrameLength(buffer, offset, limit);
            if (frameLength > localSettings.maxFrameSize + 9)
            {
                return -1;
            }
            if (available >= frameLength)
            {
                factory.http2RO.wrap(buffer, offset, offset + frameLength);
                http2FrameAvailable = true;
                return frameLength;
            }
        }

        if (!acquireSlot())
        {
            http2FrameAvailable = false;
            return available;
        }

        MutableDirectBuffer frameBuffer = factory.framePool.buffer(frameSlotIndex);
        frameBuffer.putBytes(frameSlotPosition, buffer, offset, available);
        frameSlotPosition += available;
        http2FrameAvailable = false;
        return available;
    }

    private boolean acquireSlot()
    {
        if (frameSlotIndex == NO_SLOT)
        {
            assert frameSlotPosition == 0;

            frameSlotIndex = factory.framePool.acquire(sourceId);
            if (frameSlotIndex == NO_SLOT)
            {
                // all slots are in use, just reset the connection
                factory.doReset(networkConsumer, sourceId);
                handleAbort();
                http2FrameAvailable = false;
                return false;
            }
        }
        return true;
    }

    private void releaseSlot()
    {
        if (frameSlotIndex != NO_SLOT)
        {
            factory.framePool.release(frameSlotIndex);
            frameSlotIndex = NO_SLOT;
            frameSlotPosition = 0;
        }
    }

    private boolean acquireHeadersSlot()
    {
        if (headersSlotIndex == NO_SLOT)
        {
            assert headersSlotPosition == 0;

            headersSlotIndex = factory.headersPool.acquire(sourceId);
            if (headersSlotIndex == NO_SLOT)
            {
                // all slots are in use, just reset the connection
                factory.doReset(networkConsumer, sourceId);
                handleAbort();
                return false;
            }
        }
        return true;
    }

    private void releaseHeadersSlot()
    {
        if (headersSlotIndex != NO_SLOT)
        {
            factory.headersPool.release(headersSlotIndex);
            headersSlotIndex = NO_SLOT;
            headersSlotPosition = 0;
        }
    }

    /*
     * Assembles a complete HTTP2 headers (including any continuations) if any.
     *
     * @return true if a complete HTTP2 headers is assembled or any other frame
     *         false otherwise
     */
    private boolean http2HeadersAvailable()
    {
        if (expectContinuation)
        {
            if (factory.http2RO.type() != Http2FrameType.CONTINUATION || factory.http2RO.streamId() != expectContinuationStreamId)
            {
                error(Http2ErrorCode.PROTOCOL_ERROR);
                return false;
            }
        }
        else if (factory.http2RO.type() == Http2FrameType.CONTINUATION)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return false;
        }
        switch (factory.http2RO.type())
        {
            case HEADERS:
                int streamId = factory.http2RO.streamId();
                if (streamId == 0 || streamId % 2 != 1 || streamId <= maxClientStreamId)
                {
                    error(Http2ErrorCode.PROTOCOL_ERROR);
                    return false;
                }

                factory.headersRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());
                int parentStreamId = factory.headersRO.parentStream();
                if (parentStreamId == streamId)
                {
                    // 5.3.1 A stream cannot depend on itself
                    streamError(streamId, Http2ErrorCode.PROTOCOL_ERROR);
                    return false;
                }
                if (factory.headersRO.dataLength() < 0)
                {
                    error(Http2ErrorCode.PROTOCOL_ERROR);
                    return false;
                }

                return http2HeadersAvailable(factory.headersRO.buffer(), factory.headersRO.dataOffset(),
                        factory.headersRO.dataLength(), factory.headersRO.endHeaders());

            case CONTINUATION:
                factory.continationRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());
                DirectBuffer payload = factory.continationRO.payload();
                boolean endHeaders = factory.continationRO.endHeaders();

                return http2HeadersAvailable(payload, 0, payload.capacity(), endHeaders);
        }

        return true;
    }

    /*
     * Assembles a complete HTTP2 headers (including any continuations) and the
     * flyweight is wrapped with the buffer (it could be given buffer or slab)
     *
     * @return true if a complete HTTP2 headers is assembled
     *         false otherwise
     */
    private boolean http2HeadersAvailable(DirectBuffer buffer, int offset, int length, boolean endHeaders)
    {
        if (endHeaders)
        {
            if (headersSlotPosition > 0)
            {
                MutableDirectBuffer headersBuffer = factory.headersPool.buffer(headersSlotIndex);
                headersBuffer.putBytes(headersSlotPosition, buffer, offset, length);
                headersSlotPosition += length;
                buffer = headersBuffer;
                offset = 0;
                length = headersSlotPosition;
            }
            int maxLimit = offset + length;
            expectContinuation = false;
            releaseHeadersSlot();           // early release, but fine
            factory.blockRO.wrap(buffer, offset, maxLimit);
            return true;
        }
        else
        {
            if (!acquireHeadersSlot())
            {
                return false;
            }
            MutableDirectBuffer headersBuffer = factory.headersPool.buffer(headersSlotIndex);
            headersBuffer.putBytes(headersSlotPosition, buffer, offset, length);
            headersSlotPosition += length;
            expectContinuation = true;
            expectContinuationStreamId = factory.headersRO.streamId();
        }

        return false;
    }

    private int decodeHttp2Frame(final DirectBuffer buffer, final int offset, final int limit)
    {
        int length = http2FrameAvailable(buffer, offset, limit);
        if (length == -1)
        {
            error(Http2ErrorCode.FRAME_SIZE_ERROR);
            return limit - offset;
        }
        if (!http2FrameAvailable)
        {
            return length;
        }
        Http2FrameType http2FrameType = factory.http2RO.type();
        // Assembles HTTP2 HEADERS and its CONTINUATIONS frames, if any
        if (!http2HeadersAvailable())
        {
            return length;
        }
        switch (http2FrameType)
        {
            case DATA:
                doData();
                break;
            case HEADERS:   // fall-through
            case CONTINUATION:
                doHeaders();
                break;
            case PRIORITY:
                doPriority();
                break;
            case RST_STREAM:
                doRst();
                break;
            case SETTINGS:
                doSettings();
                break;
            case PUSH_PROMISE:
                doPushPromise();
                break;
            case PING:
                doPing();
                break;
            case GO_AWAY:
                doGoAway();
                break;
            case WINDOW_UPDATE:
                doWindow();
                break;
            default:
                // Ignore and discard unknown frame
        }

        return length;
    }

    private void doGoAway()
    {
        int streamId = factory.http2RO.streamId();
        if (goaway)
        {
            if (streamId != 0)
            {
                processUnexpected(sourceId);
            }
        }
        else
        {
            goaway = true;
            Http2ErrorCode errorCode = (streamId != 0) ? Http2ErrorCode.PROTOCOL_ERROR : Http2ErrorCode.NO_ERROR;
            remoteSettings.enablePush = false;      // no new streams
            error(errorCode);
        }
    }

    private void doPushPromise()
    {
        error(Http2ErrorCode.PROTOCOL_ERROR);
    }

    private void doPriority()
    {
        int streamId = factory.http2RO.streamId();
        if (streamId == 0)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        int payloadLength = factory.http2RO.payloadLength();
        if (payloadLength != 5)
        {
            streamError(streamId, Http2ErrorCode.FRAME_SIZE_ERROR);
            return;
        }
        factory.priorityRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());
        int parentStreamId = factory.priorityRO.parentStream();
        if (parentStreamId == streamId)
        {
            // 5.3.1 A stream cannot depend on itself
            streamError(streamId, Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
    }

    private void doHeaders()
    {
        int streamId = factory.http2RO.streamId();

        Http2Stream stream = http2Streams.get(streamId);
        if (stream != null)
        {
            // TODO trailers
        }
        if (streamId <= maxClientStreamId)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        maxClientStreamId = streamId;

        if (clientStreamCount + 1 > localSettings.maxConcurrentStreams)
        {
            streamError(streamId, stream, Http2ErrorCode.REFUSED_STREAM);
            return;
        }

        State state = factory.http2RO.endStream() ? HALF_CLOSED_REMOTE : OPEN;

        headersContext.reset();

        factory.httpBeginExRW.wrap(factory.scratch, 0, factory.scratch.capacity());

        factory.blockRO.forEach(headerFieldConsumer);
        // All HTTP/2 requests MUST include exactly one valid value for the
        // ":method", ":scheme", and ":path" pseudo-header fields, unless it is
        // a CONNECT request (Section 8.3).  An HTTP request that omits
        // mandatory pseudo-header fields is malformed
        if (!headersContext.error() && (headersContext.method != 1 || headersContext.scheme != 1 || headersContext.path != 1))
        {
            headersContext.streamError = Http2ErrorCode.PROTOCOL_ERROR;
        }
        if (headersContext.error())
        {
            if (headersContext.streamError != null)
            {
                streamError(streamId, stream, headersContext.streamError);
                return;
            }
            if (headersContext.connectionError != null)
            {
                error(headersContext.connectionError);
                return;
            }
        }

        RouteFW route = resolveTarget(sourceRef, sourceName, headersContext.headers);
        if (route == null)
        {
            noRoute(streamId);
        }
        else
        {
            followRoute(streamId, state, route);
        }
    }

    private void followRoute(int streamId, State state, RouteFW route)
    {
        final String applicationName = route.target().asString();
        final MessageConsumer applicationTarget = router.supplyTarget(applicationName);
        HttpWriter httpWriter = factory.httpWriter;
        Http2Stream stream = newStream(streamId, state, applicationTarget, httpWriter);
        final long targetRef = route.targetRef();

        stream.contentLength = headersContext.contentLength;

        HttpBeginExFW beginEx = factory.httpBeginExRW.build();
        httpWriter.doHttpBegin(applicationTarget, stream.targetId, targetRef, stream.correlationId,
                beginEx.buffer(), beginEx.offset(), beginEx.sizeof());
        router.setThrottle(applicationName, stream.targetId, stream::onThrottle);

        if (factory.headersRO.endStream())
        {
            httpWriter.doHttpEnd(applicationTarget, stream.targetId);  // TODO use HttpWriteScheduler
        }
    }

    // No route for the HTTP2 request, send 404 on the corresponding HTTP2 stream
    private void noRoute(int streamId)
    {
        ListFW<HttpHeaderFW> headers =
                factory.headersRW.wrap(factory.errorBuf, 0, factory.errorBuf.capacity())
                                 .item(b -> b.name(":status").value("404"))
                                 .build();

        writeScheduler.headers(streamId, Http2Flags.END_STREAM, headers);
    }

    private void doRst()
    {
        int streamId = factory.http2RO.streamId();
        if (streamId == 0)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        int payloadLength = factory.http2RO.payloadLength();
        if (payloadLength != 4)
        {
            error(Http2ErrorCode.FRAME_SIZE_ERROR);
            return;
        }
        Http2Stream stream = http2Streams.get(streamId);
        if (stream == null || stream.state == State.IDLE)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
        }
        else
        {
            stream.onReset();
            closeStream(stream);
        }
    }

    void closeStream(Http2Stream stream)
    {
        if (stream.state != CLOSED)
        {
            stream.state = CLOSED;

            if (stream.isClientInitiated())
            {
                clientStreamCount--;
            }
            else
            {
                promisedStreamCount--;
            }
            factory.correlations.remove(stream.targetId);
            http2Streams.remove(stream.http2StreamId);
            stream.close();
        }
    }

    private void doWindow()
    {
        int streamId = factory.http2RO.streamId();
        if (factory.http2RO.payloadLength() != 4)
        {
            if (streamId == 0)
            {
                error(Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
            else
            {
                streamError(streamId, Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
        }
        if (streamId != 0)
        {
            State state = state(streamId);
            if (state == State.IDLE)
            {
                error(Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
            Http2Stream stream = http2Streams.get(streamId);
            if (stream == null)
            {
                // A receiver could receive a WINDOW_UPDATE frame on a "half-closed (remote)" or "closed" stream.
                // A receiver MUST NOT treat this as an error
                return;
            }
        }
        factory.http2WindowRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());

        // 6.9 WINDOW_UPDATE - legal range for flow-control window increment is 1 to 2^31-1 octets.
        if (factory.http2WindowRO.size() < 1)
        {
            if (streamId == 0)
            {
                error(Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
            else
            {
                streamError(streamId, Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
        }

        // 6.9.1 A sender MUST NOT allow a flow-control window to exceed 2^31-1 octets.
        if (streamId == 0)
        {
            http2OutWindow += factory.http2WindowRO.size();
            if (http2OutWindow > Integer.MAX_VALUE)
            {
                error(Http2ErrorCode.FLOW_CONTROL_ERROR);
                return;
            }
            writeScheduler.onHttp2Window();
        }
        else
        {
            Http2Stream stream = http2Streams.get(streamId);
            stream.http2OutWindow += factory.http2WindowRO.size();
            if (stream.http2OutWindow > Integer.MAX_VALUE)
            {
                streamError(streamId, Http2ErrorCode.FLOW_CONTROL_ERROR);
                return;
            }
            writeScheduler.onHttp2Window(streamId);
        }

    }

    private void doData()
    {
        int streamId = factory.http2RO.streamId();
        Http2Stream stream = http2Streams.get(streamId);

        if (streamId == 0 || stream == null || stream.state == State.IDLE)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        if (stream.state == HALF_CLOSED_REMOTE)
        {
            error(Http2ErrorCode.STREAM_CLOSED);
            closeStream(stream);
            return;
        }
        Http2DataFW dataRO = factory.http2DataRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(),
                factory.http2RO.limit());
        if (dataRO.dataLength() < 0)        // because of invalid padding length
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            closeStream(stream);
            return;
        }

        //
        if (stream.http2InWindow < factory.http2RO.payloadLength() || http2InWindow < factory.http2RO.payloadLength())
        {
            streamError(streamId, stream, Http2ErrorCode.FLOW_CONTROL_ERROR);
            return;
        }
        http2InWindow -= factory.http2RO.payloadLength();
        stream.http2InWindow -= factory.http2RO.payloadLength();

        stream.totalData += factory.http2RO.payloadLength();

        if (dataRO.endStream())
        {
            // 8.1.2.6 A request is malformed if the value of a content-length header field does
            // not equal the sum of the DATA frame payload lengths
            if (stream.contentLength != -1 && stream.totalData != stream.contentLength)
            {
                streamError(streamId, stream, Http2ErrorCode.PROTOCOL_ERROR);
                //stream.httpWriteScheduler.doEnd(stream.targetId);
                return;
            }
            stream.state = State.HALF_CLOSED_REMOTE;
        }

        stream.onData();
    }

    private void doSettings()
    {
        if (factory.http2RO.streamId() != 0)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        if (factory.http2RO.payloadLength()%6 != 0)
        {
            error(Http2ErrorCode.FRAME_SIZE_ERROR);
            return;
        }

        factory.settingsRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());

        if (factory.settingsRO.ack() && factory.http2RO.payloadLength() != 0)
        {
            error(Http2ErrorCode.FRAME_SIZE_ERROR);
            return;
        }
        if (!factory.settingsRO.ack())
        {
            factory.settingsRO.accept(this::doSetting);
            writeScheduler.settingsAck();
        }
        else
        {
            int update =  initialSettings.initialWindowSize - localSettings.initialWindowSize;
            for(Http2Stream http2Stream: http2Streams.values())
            {
                http2Stream.http2InWindow += update;           // http2InWindow can become negative
            }

            // now that peer acked our initial settings, can use them as our local settings
            localSettings = initialSettings;
        }
    }

    private void doSetting(Http2SettingsId id, Long value)
    {
        switch (id)
        {
            case HEADER_TABLE_SIZE:
                remoteSettings.headerTableSize = value.intValue();
                break;
            case ENABLE_PUSH:
                if (!(value == 0L || value == 1L))
                {
                    error(Http2ErrorCode.PROTOCOL_ERROR);
                    return;
                }
                remoteSettings.enablePush = (value == 1L);
                break;
            case MAX_CONCURRENT_STREAMS:
                remoteSettings.maxConcurrentStreams = value.intValue();
                break;
            case INITIAL_WINDOW_SIZE:
                if (value > Integer.MAX_VALUE)
                {
                    error(Http2ErrorCode.FLOW_CONTROL_ERROR);
                    return;
                }
                int old = remoteSettings.initialWindowSize;
                remoteSettings.initialWindowSize = value.intValue();
                int update = value.intValue() - old;

                // 6.9.2. Initial Flow-Control Window Size
                // SETTINGS frame can alter the initial flow-control
                // window size for streams with active flow-control windows
                for(Http2Stream http2Stream: http2Streams.values())
                {
                    http2Stream.http2OutWindow += update;           // http2OutWindow can become negative
                    if (http2Stream.http2OutWindow > Integer.MAX_VALUE)
                    {
                        // 6.9.2. Initial Flow-Control Window Size
                        // An endpoint MUST treat a change to SETTINGS_INITIAL_WINDOW_SIZE that
                        // causes any flow-control window to exceed the maximum size as a
                        // connection error of type FLOW_CONTROL_ERROR.
                        error(Http2ErrorCode.FLOW_CONTROL_ERROR);
                        return;
                    }
                }
                break;
            case MAX_FRAME_SIZE:
                if (value < Math.pow(2, 14) || value > Math.pow(2, 24) -1)
                {
                    error(Http2ErrorCode.PROTOCOL_ERROR);
                    return;
                }
                remoteSettings.maxFrameSize = value.intValue();
                break;
            case MAX_HEADER_LIST_SIZE:
                remoteSettings.maxHeaderListSize = value.intValue();
                break;
            default:
                // Ignore the unkonwn setting
                break;
        }
    }

    private void doPing()
    {
        if (factory.http2RO.streamId() != 0)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        if (factory.http2RO.payloadLength() != 8)
        {
            error(Http2ErrorCode.FRAME_SIZE_ERROR);
            return;
        }
        factory.pingRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());

        if (!factory.pingRO.ack())
        {
            writeScheduler.pingAck(factory.pingRO.payload(), 0, factory.pingRO.payload().capacity());
        }
    }

    private State state(int streamId)
    {
        Http2Stream stream = http2Streams.get(streamId);
        if (stream != null)
        {
            return stream.state;
        }
        if (streamId%2 == 1)
        {
            if (streamId <= maxClientStreamId)
            {
                return State.CLOSED;
            }
        }
        else
        {
            if (streamId <= maxPushPromiseStreamId)
            {
                return State.CLOSED;
            }
        }
        return State.IDLE;
    }

    RouteFW resolveTarget(
            long sourceRef,
            String sourceName,
            Map<String, String> headers)
    {
        MessagePredicate filter = (t, b, o, l) ->
        {
            RouteFW route = factory.routeRO.wrap(b, o, l);
            OctetsFW extension = route.extension();
            if (sourceRef == route.sourceRef() && sourceName.equals(route.source().asString()))
            {
                Map<String, String> routeHeaders;
                if (extension.sizeof() == 0)
                {
                    routeHeaders = EMPTY_HEADERS;
                }
                else
                {
                    final HttpRouteExFW routeEx = extension.get(factory.httpRouteExRO::wrap);
                    routeHeaders = new LinkedHashMap<>();
                    routeEx.headers().forEach(h -> routeHeaders.put(h.name().asString(), h.value().asString()));
                }

                return headers.entrySet().containsAll(routeHeaders.entrySet());
            }
            return false;
        };

        return router.resolve(authorization, filter, wrapRoute);
    }

    void handleWindow(WindowFW windowRO)
    {

        writeScheduler.onWindow();
    }

    void error(Http2ErrorCode errorCode)
    {
        writeScheduler.goaway(lastStreamId, errorCode);
        writeScheduler.doEnd();
    }

    void streamError(int streamId, Http2ErrorCode errorCode)
    {
        Http2Stream stream = http2Streams.get(streamId);
        streamError(streamId, stream, errorCode);
    }

    void streamError(int streamId, Http2Stream stream, Http2ErrorCode errorCode)
    {
        if (stream != null)
        {
            doRstByUs(stream, errorCode);
        }
        else
        {
            writeScheduler.rst(streamId, errorCode);
        }
    }

    private int nextPromisedId()
    {
        maxPushPromiseStreamId += 2;
        return maxPushPromiseStreamId;
    }

    /*
     * @param streamId corresponding http2 stream-id on which service response
     *                 will be sent
     * @return a stream id on which PUSH_PROMISE can be sent
     *         -1 otherwise
     */
    private int findPushId(int streamId)
    {
        if (remoteSettings.enablePush && promisedStreamCount +1 < remoteSettings.maxConcurrentStreams)
        {
            // PUSH_PROMISE frames MUST only be sent on a peer-initiated stream
            if (streamId%2 == 0)
            {
                // Find a stream on which PUSH_PROMISE can be sent
                return http2Streams.entrySet()
                                   .stream()
                                   .map(Map.Entry::getValue)
                                   .filter(s -> (s.http2StreamId & 0x01) == 1)     // client-initiated stream
                                   .filter(s -> s.state == OPEN || s.state == HALF_CLOSED_REMOTE)
                                   .mapToInt(s -> s.http2StreamId)
                                   .findAny()
                                   .orElse(-1);
            }
            else
            {
                return streamId;        // client-initiated stream
            }
        }
        return -1;
    }

    private void doPromisedRequest(int http2StreamId, ListFW<HttpHeaderFW> headers)
    {
        Map<String, String> headersMap = new HashMap<>();
        headers.forEach(
                httpHeader -> headersMap.put(httpHeader.name().asString(), httpHeader.value().asString()));
        RouteFW route = resolveTarget(sourceRef, sourceName, headersMap);
        final String applicationName = route.target().asString();
        final MessageConsumer applicationTarget = router.supplyTarget(applicationName);
        HttpWriter httpWriter = factory.httpWriter;
        Http2Stream http2Stream = newStream(http2StreamId, HALF_CLOSED_REMOTE, applicationTarget, httpWriter);
        long targetId = http2Stream.targetId;
        long targetRef = route.targetRef();

        httpWriter.doHttpBegin(applicationTarget, targetId, targetRef, http2Stream.correlationId,
                hs -> headers.forEach(h -> hs.item(b -> b.name(h.name())
                                                         .value(h.value()))));
        router.setThrottle(applicationName, targetId, http2Stream::onThrottle);
        httpWriter.doHttpEnd(applicationTarget, targetId);
    }

    private Http2Stream newStream(int http2StreamId, State state, MessageConsumer applicationTarget, HttpWriter httpWriter)
    {
        assert http2StreamId != 0;

        Http2Stream http2Stream = new Http2Stream(factory, this, http2StreamId, state, applicationTarget, httpWriter);
        http2Streams.put(http2StreamId, http2Stream);

        Correlation correlation = new Correlation(http2Stream.correlationId, sourceOutputEstId, writeScheduler,
                this::doPromisedRequest, this, http2StreamId, encodeContext, this::nextPromisedId, this::findPushId);

        factory.correlations.put(http2Stream.correlationId, correlation);
        if (http2Stream.isClientInitiated())
        {
            clientStreamCount++;
        }
        else
        {
            promisedStreamCount++;
        }
        return http2Stream;
    }

    private void validateHeaderFieldType(HpackHeaderFieldFW hf)
    {
        if (!headersContext.error() && hf.type() == UNKNOWN)
        {
            headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
        }
    }

    private void dynamicTableSizeUpdate(HpackHeaderFieldFW hf)
    {
        if (!headersContext.error())
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
                        headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                        return;
                    }
                    int maxTableSize = hf.tableSize();
                    if (maxTableSize > localSettings.headerTableSize)
                    {
                        headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                        return;
                    }
                    decodeContext.updateSize(hf.tableSize());
                    break;
            }
        }
    }

    private void validatePseudoHeaders(DirectBuffer name, DirectBuffer value)
    {
        if (!headersContext.error())
        {
            if (name.capacity() > 0 && name.getByte(0) == ':')
            {
                // All pseudo-header fields MUST appear in the header block before regular header fields
                if (headersContext.regularHeader)
                {
                    headersContext.streamError = Http2ErrorCode.PROTOCOL_ERROR;
                    return;
                }
                // request pseudo-header fields MUST be one of :authority, :method, :path, :scheme,
                int index = decodeContext.index(name);
                switch (index)
                {
                    case 1:             // :authority
                        break;
                    case 2:             // :method
                        headersContext.method++;
                        break;
                    case 4:             // :path
                        if (value.capacity() > 0)       // :path MUST not be empty
                        {
                            headersContext.path++;
                        }
                        break;
                    case 6:             // :scheme
                        headersContext.scheme++;
                        break;
                    default:
                        headersContext.streamError = Http2ErrorCode.PROTOCOL_ERROR;
                        return;
                }
            }
            else
            {
                headersContext.regularHeader = true;
            }
        }
    }

    private void connectionHeaders(DirectBuffer name, DirectBuffer value)
    {

        if (!headersContext.error() && name.equals(HpackContext.CONNECTION))
        {
            headersContext.streamError = Http2ErrorCode.PROTOCOL_ERROR;
        }
    }

    private void contentLengthHeader(DirectBuffer name, DirectBuffer value)
    {
        if (!headersContext.error() && name.equals(decodeContext.nameBuffer(28)))
        {
            String contentLength = value.getStringWithoutLengthUtf8(0, value.capacity());
            headersContext.contentLength = Long.parseLong(contentLength);
        }
    }

    // 8.1.2.2 TE header MUST NOT contain any value other than "trailers".
    private void teHeader(DirectBuffer name, DirectBuffer value)
    {
        if (!headersContext.error() && name.equals(TE) && !value.equals(TRAILERS))
        {
            headersContext.streamError = Http2ErrorCode.PROTOCOL_ERROR;
        }
    }

    private void uppercaseHeaders(DirectBuffer name, DirectBuffer value)
    {
        if (!headersContext.error())
        {
            for(int i=0; i < name.capacity(); i++)
            {
                if (name.getByte(i) >= 'A' && name.getByte(i) <= 'Z')
                {
                    headersContext.streamError = Http2ErrorCode.PROTOCOL_ERROR;
                }
            }
        }
    }

    // Collect headers into map to resolve target
    // TODO avoid this
    private void collectHeaders(DirectBuffer name, DirectBuffer value)
    {
        if (!headersContext.error())
        {
            String nameStr = name.getStringWithoutLengthUtf8(0, name.capacity());
            String valueStr = value.getStringWithoutLengthUtf8(0, value.capacity());
            headersContext.headers.put(nameStr, valueStr);
        }
    }

    // Writes HPACK header field to http representation in a buffer
    private void mapToHttp(DirectBuffer name, DirectBuffer value)
    {
        if (!headersContext.error())
        {
            factory.httpBeginExRW.headersItem(item -> item.name(name, 0, name.capacity())
                                                          .value(value, 0, value.capacity()));
        }
    }

    private void decodeHeaderField(HpackHeaderFieldFW hf,
                                   BiConsumer<DirectBuffer, DirectBuffer> nameValue)
    {
        int index;
        DirectBuffer name = null;
        DirectBuffer value = null;

        switch (hf.type())
        {
            case INDEXED :
                index = hf.index();
                if (!decodeContext.valid(index))
                {
                    headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                    return;
                }
                name = decodeContext.nameBuffer(index);
                value = decodeContext.valueBuffer(index);
                nameValue.accept(name, value);
                break;

            case LITERAL :
                HpackLiteralHeaderFieldFW literalRO = hf.literal();
                if (literalRO.error())
                {
                    headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                    return;
                }
                switch (literalRO.nameType())
                {
                    case INDEXED:
                    {
                        index = literalRO.nameIndex();
                        name = decodeContext.nameBuffer(index);

                        HpackStringFW valueRO = literalRO.valueLiteral();
                        value = valueRO.payload();
                        if (valueRO.huffman())
                        {
                            MutableDirectBuffer dst = new UnsafeBuffer(new byte[4096]); // TODO
                            int length = HpackHuffman.decode(value, dst);
                            if (length == -1)
                            {
                                headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                                return;
                            }
                            value = new UnsafeBuffer(dst, 0, length);
                        }
                        nameValue.accept(name, value);
                    }
                    break;
                    case NEW:
                    {
                        HpackStringFW nameRO = literalRO.nameLiteral();
                        name = nameRO.payload();
                        if (nameRO.huffman())
                        {
                            MutableDirectBuffer dst = new UnsafeBuffer(new byte[4096]); // TODO
                            int length = HpackHuffman.decode(name, dst);
                            if (length == -1)
                            {
                                headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                                return;
                            }
                            name = new UnsafeBuffer(dst, 0, length);
                        }

                        HpackStringFW valueRO = literalRO.valueLiteral();
                        value = valueRO.payload();
                        if (valueRO.huffman())
                        {
                            MutableDirectBuffer dst = new UnsafeBuffer(new byte[4096]); // TODO
                            int length = HpackHuffman.decode(value, dst);
                            if (length == -1)
                            {
                                headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                                return;
                            }
                            value = new UnsafeBuffer(dst, 0, length);
                        }
                        nameValue.accept(name, value);
                    }
                    break;
                }
                if (literalRO.literalType() == INCREMENTAL_INDEXING)
                {
                    // make a copy for name and value as they go into dynamic table (outlives current frame)
                    MutableDirectBuffer nameCopy = new UnsafeBuffer(new byte[name.capacity()]);
                    nameCopy.putBytes(0, name, 0, name.capacity());
                    MutableDirectBuffer valueCopy = new UnsafeBuffer(new byte[value.capacity()]);
                    valueCopy.putBytes(0, value, 0, value.capacity());
                    decodeContext.add(nameCopy, valueCopy);
                }
                break;
        }
    }


    void mapPushPromise(ListFW<HttpHeaderFW> httpHeaders, HpackHeaderBlockFW.Builder builder)
    {
        httpHeaders.forEach(h -> builder.header(b -> mapHeader(h, b)));
    }

    void mapHeaders(ListFW<HttpHeaderFW> httpHeaders, HpackHeaderBlockFW.Builder builder)
    {
        encodeHeadersContext.reset();

        httpHeaders.forEach(this::status)                       // checks if there is :status
                   .forEach(this::accessControlAllowOrigin)     // checks if there is access-control-allow-origin
                   .forEach(this::connectionHeaders);           // collects all connection headers
        if (!encodeHeadersContext.status)
        {
            builder.header(b -> b.indexed(8));          // no mandatory :status header, add :status: 200
        }

        httpHeaders.forEach(h ->
        {
            if (validHeader(h))
            {
                builder.header(b -> mapHeader(h, b));
            }
        });

        if (factory.config.accessControlAllowOrigin() && !encodeHeadersContext.accessControlAllowOrigin)
        {
            builder.header(b -> b.literal(l -> l.type(WITHOUT_INDEXING).name(20).value(DEFAULT_ACCESS_CONTROL_ALLOW_ORIGIN)));
        }
    }

    private void status(HttpHeaderFW httpHeader)
    {
        if (!encodeHeadersContext.status)
        {
            StringFW name = httpHeader.name();
            String16FW value = httpHeader.value();
            factory.nameRO.wrap(name.buffer(), name.offset() + 1, name.sizeof() - 1); // +1, -1 for length-prefixed buffer
            factory.valueRO.wrap(value.buffer(), value.offset() + 2, value.sizeof() - 2);

            if (factory.nameRO.equals(encodeContext.nameBuffer(8)))
            {
                encodeHeadersContext.status = true;
            }
        }
    }

    // Checks if response has access-control-allow-origin header
    private void accessControlAllowOrigin(HttpHeaderFW httpHeader)
    {
        if (factory.config.accessControlAllowOrigin() && !encodeHeadersContext.accessControlAllowOrigin)
        {
            StringFW name = httpHeader.name();
            String16FW value = httpHeader.value();
            factory.nameRO.wrap(name.buffer(), name.offset() + 1, name.sizeof() - 1); // +1, -1 for length-prefixed buffer
            factory.valueRO.wrap(value.buffer(), value.offset() + 2, value.sizeof() - 2);

            if (factory.nameRO.equals(encodeContext.nameBuffer(20)))
            {
                encodeHeadersContext.accessControlAllowOrigin = true;
            }
        }
    }

    private void connectionHeaders(HttpHeaderFW httpHeader)
    {
        StringFW name = httpHeader.name();
        String16FW value = httpHeader.value();
        factory.nameRO.wrap(name.buffer(), name.offset() + 1, name.sizeof() - 1); // +1, -1 for length-prefixed buffer

        if (factory.nameRO.equals(CONNECTION))
        {
            String[] headers = value.asString().split(",");
            for (String header : headers)
            {
                encodeHeadersContext.connectionHeaders.add(header.trim());
            }
        }
    }

    private boolean validHeader(HttpHeaderFW httpHeader)
    {
        StringFW name = httpHeader.name();
        String16FW value = httpHeader.value();
        factory.nameRO.wrap(name.buffer(), name.offset() + 1, name.sizeof() - 1); // +1, -1 for length-prefixed buffer
        factory.valueRO.wrap(value.buffer(), value.offset() + 2, value.sizeof() - 2);

        // Removing 8.1.2.1 Pseudo-Header Fields
        // Not sending error as it will allow requests to loop back
        if (factory.nameRO.equals(encodeContext.nameBuffer(1)) ||          // :authority
                factory.nameRO.equals(encodeContext.nameBuffer(2)) ||      // :method
                factory.nameRO.equals(encodeContext.nameBuffer(4)) ||      // :path
                factory.nameRO.equals(encodeContext.nameBuffer(6)))        // :scheme
        {
            return false;
        }

        // Removing 8.1.2.2 connection-specific header fields from response
        if (factory.nameRO.equals(encodeContext.nameBuffer(57)) ||         // transfer-encoding
                factory.nameRO.equals(CONNECTION) ||
                factory.nameRO.equals(KEEP_ALIVE) ||
                factory.nameRO.equals(PROXY_CONNECTION) ||
                factory.nameRO.equals(UPGRADE))
        {
            return false;
        }

        // Removing any header that is nominated by Connection header field
        for(String connectionHeader: encodeHeadersContext.connectionHeaders)
        {
            if (name.asString().equals(connectionHeader))
            {
                return false;
            }
        }

        return true;
    }

    // Map http1.1 header to http2 header field in HEADERS, PUSH_PROMISE request
    private void mapHeader(HttpHeaderFW httpHeader, HpackHeaderFieldFW.Builder builder)
    {
        StringFW name = httpHeader.name();
        String16FW value = httpHeader.value();
        factory.nameRO.wrap(name.buffer(), name.offset() + 1, name.sizeof() - 1); // +1, -1 for length-prefixed buffer
        factory.valueRO.wrap(value.buffer(), value.offset() + 2, value.sizeof() - 2);

        int index = encodeContext.index(factory.nameRO, factory.valueRO);
        if (index != -1)
        {
            // Indexed
            builder.indexed(index);
        }
        else
        {
            // Literal
            builder.literal(literalBuilder -> buildLiteral(literalBuilder, encodeContext));
        }
    }

    // Building Literal representation of header field
    // TODO dynamic table, huffman, never indexed
    private void buildLiteral(
            HpackLiteralHeaderFieldFW.Builder builder,
            HpackContext hpackContext)
    {
        int nameIndex = hpackContext.index(factory.nameRO);
        builder.type(WITHOUT_INDEXING);
        if (nameIndex != -1)
        {
            builder.name(nameIndex);
        }
        else
        {
            builder.name(factory.nameRO, 0, factory.nameRO.capacity());
        }
        builder.value(factory.valueRO, 0, factory.valueRO.capacity());
    }


    void handleHttpBegin(BeginFW begin, MessageConsumer applicationReplyThrottle, long applicationReplyId,
                         Correlation correlation)
    {
        OctetsFW extension = begin.extension();
        Http2Stream stream = http2Streams.get(correlation.http2StreamId);
        if (stream == null)
        {
            factory.doReset(applicationReplyThrottle, applicationReplyId);
        }
        else
        {
            stream.applicationReplyThrottle = applicationReplyThrottle;
            stream.applicationReplyId = applicationReplyId;

            stream.sendHttpWindow();

            if (extension.sizeof() > 0)
            {
                HttpBeginExFW beginEx = extension.get(factory.beginExRO::wrap);
                writeScheduler.headers(correlation.http2StreamId, Http2Flags.NONE, beginEx.headers());
            }
        }
    }

    void handleHttpData(DataFW dataRO, Correlation correlation)
    {
        OctetsFW extension = dataRO.extension();
        OctetsFW payload = dataRO.payload();

        if (extension.sizeof() > 0)
        {

            int pushStreamId = correlation.pushStreamIds.applyAsInt(correlation.http2StreamId);
            if (pushStreamId != -1)
            {
                int promisedStreamId = correlation.promisedStreamIds.getAsInt();
                Http2DataExFW dataEx = extension.get(factory.dataExRO::wrap);
                writeScheduler.pushPromise(pushStreamId, promisedStreamId, dataEx.headers());
                correlation.pushHandler.accept(promisedStreamId, dataEx.headers());
            }
        }
        if (payload.sizeof() > 0)
        {
            Http2Stream stream = http2Streams.get(correlation.http2StreamId);
            if (stream != null)
            {
                stream.applicationReplyWindowBudget -= dataRO.length() + dataRO.padding();
                if (stream.applicationReplyWindowBudget < 0)
                {
                    doRstByUs(stream, Http2ErrorCode.INTERNAL_ERROR);
                    return;
                }
            }

            writeScheduler.data(correlation.http2StreamId, payload.buffer(), payload.offset(), payload.sizeof());
        }

    }

    void handleHttpEnd(EndFW data, Correlation correlation)
    {
        Http2Stream stream = http2Streams.get(correlation.http2StreamId);

        if (stream != null)
        {
            stream.onHttpEnd();
        }
    }

    void handleHttpAbort(AbortFW abort, Correlation correlation)
    {
        Http2Stream stream = http2Streams.get(correlation.http2StreamId);

        if (stream != null)
        {
            stream.onHttpAbort();

        }
    }

    void doRstByUs(Http2Stream stream, Http2ErrorCode errorCode)
    {
        stream.onReset();
        writeScheduler.rst(stream.http2StreamId, errorCode);
        closeStream(stream);
    }

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

    private static final class HeadersContext
    {
        Http2ErrorCode connectionError;
        Map<String, String> headers = new HashMap<>();
        int method;
        int scheme;
        int path;
        boolean regularHeader;
        Http2ErrorCode streamError;
        long contentLength = -1;

        void reset()
        {
            connectionError = null;
            headers.clear();
            method = 0;
            scheme = 0;
            path = 0;
            regularHeader = false;
            streamError = null;
            contentLength = -1;
        }

        boolean error()
        {
            return streamError != null || connectionError != null;
        }
    }

    private static final class EncodeHeadersContext
    {
        boolean status;
        boolean accessControlAllowOrigin;
        final List<String> connectionHeaders = new ArrayList<>();

        void reset()
        {
            status = false;
            accessControlAllowOrigin = false;
            connectionHeaders.clear();
        }

    }

    @FunctionalInterface
    private interface DecoderState
    {
        int decode(DirectBuffer buffer, int offset, int length);
    }

}
