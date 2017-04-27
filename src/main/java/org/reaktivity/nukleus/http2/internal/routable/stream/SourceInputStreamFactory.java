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
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.http2.internal.routable.Correlation;
import org.reaktivity.nukleus.http2.internal.routable.Route;
import org.reaktivity.nukleus.http2.internal.routable.Source;
import org.reaktivity.nukleus.http2.internal.routable.Target;
import org.reaktivity.nukleus.http2.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.OctetsFW;
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http2.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http2.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackContext;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderBlockFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHuffman;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackStringFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ContinuationFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2GoawayFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2HeadersFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PingFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PrefaceFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2RstStreamFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsId;
import org.reaktivity.nukleus.http2.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http2.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.http2.internal.util.function.LongObjectBiConsumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.reaktivity.nukleus.http2.internal.routable.Route.headersMatch;
import static org.reaktivity.nukleus.http2.internal.routable.stream.Slab.SLOT_NOT_AVAILABLE;
import static org.reaktivity.nukleus.http2.internal.routable.stream.SourceInputStreamFactory.State.HALF_CLOSED_REMOTE;
import static org.reaktivity.nukleus.http2.internal.routable.stream.SourceInputStreamFactory.State.OPEN;
import static org.reaktivity.nukleus.http2.internal.router.RouteKind.OUTPUT_ESTABLISHED;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderFieldFW.HeaderFieldType.UNKNOWN;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW.LiteralType.INCREMENTAL_INDEXING;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2PrefaceFW.PRI_REQUEST;

public final class SourceInputStreamFactory
{

    private final FrameFW frameRO = new FrameFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final Http2PrefaceFW prefaceRO = new Http2PrefaceFW();
    private final Http2FrameFW http2RO = new Http2FrameFW();
    private final Http2SettingsFW settingsRO = new Http2SettingsFW();
    private final Http2DataFW http2DataRO = new Http2DataFW();
    private final Http2HeadersFW headersRO = new Http2HeadersFW();
    private final Http2ContinuationFW continationRO = new Http2ContinuationFW();
    private final HpackHeaderBlockFW blockRO = new HpackHeaderBlockFW();

    private final Http2PingFW pingRO = new Http2PingFW();

    private final Http2SettingsFW.Builder settingsRW = new Http2SettingsFW.Builder();
    private final Http2PingFW.Builder pingRW = new Http2PingFW.Builder();
    private final Http2GoawayFW.Builder goawayRW = new Http2GoawayFW.Builder();
    private final Http2RstStreamFW.Builder resetRW = new Http2RstStreamFW.Builder();
    private final HeadersContext headersContext = new HeadersContext();

    private final Source source;
    private final LongFunction<List<Route>> supplyRoutes;
    private final LongSupplier supplyStreamId;
    private final Target replyTarget;
    private final LongObjectBiConsumer<Correlation> correlateNew;
    private final Slab frameSlab;
    private final Slab headersSlab;

    private final MutableDirectBuffer buffer = new UnsafeBuffer(new byte[2048]);

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

        void reset()
        {
            connectionError = null;
            headers.clear();
            method = 0;
            scheme = 0;
            path = 0;
            regularHeader = false;
            streamError = null;
        }

        boolean error()
        {
            return streamError != null || connectionError != null;
        }
    }

    public SourceInputStreamFactory(
        Source source,
        LongFunction<List<Route>> supplyRoutes,
        LongSupplier supplyStreamId,
        Target replyTarget,
        LongObjectBiConsumer<Correlation> correlateNew)
    {
        this.source = source;
        this.supplyRoutes = supplyRoutes;
        this.supplyStreamId = supplyStreamId;
        this.replyTarget = replyTarget;
        this.correlateNew = correlateNew;
        // Actually, we need only DEFAULT_MAX_FRAME_SIZE + 9 bytes for frame header, but
        // slab needs power of two
        int slotCapacity = 1 << -Integer.numberOfLeadingZeros(Settings.DEFAULT_MAX_FRAME_SIZE + 9 - 1);
        int totalCapacity = 128 * slotCapacity;     // TODO max concurrent connections
        this.frameSlab = new Slab(totalCapacity, slotCapacity);
        this.headersSlab = new Slab(totalCapacity, slotCapacity);
    }

    public MessageHandler newStream()
    {
        return new SourceInputStream()::handleStream;
    }

    private final class SourceInputStream
    {
        private MessageHandler streamState;
        private MessageHandler throttleState;
        private DecoderState decoderState;

        // slab to assemble a complete HTTP2 frame
        // no need for separate slab per HTTP2 stream as the frames are not fragmented
        private int frameSlotIndex = SLOT_NOT_AVAILABLE;
        private int frameSlotPosition;
        // slab to assemble a complete HTTP2 headers frame(including its continuation frames)
        // no need for separate slab per HTTP2 stream as no interleaved frames of any other type
        // or from any other stream
        private int headersSlotIndex = SLOT_NOT_AVAILABLE;
        private int headersSlotPosition;

        long sourceId;
        int lastStreamId;
        long sourceRef;
        private long correlationId;
        private int window;
        private int sourceUpdateDeferred;
        final long sourceOutputEstId;
        private final HpackContext decodeContext;
        private final HpackContext encodeContext;

        private final Int2ObjectHashMap<Http2Stream> http2Streams;
        private int lastPromisedStreamId;

        private int noClientStreams;
        private int noPromisedStreams;
        private int maxClientStreamId;
        private boolean goaway;
        private Settings localSettings;
        private Settings remoteSettings;
        private boolean expectContinuation;
        private int expectContinuationStreamId;
        private boolean expectDynamicTableSizeUpdate = true;

        @Override
        public String toString()
        {
            return String.format("%s[source=%s, sourceId=%016x, window=%d]",
                    getClass().getSimpleName(), source.routableName(), sourceId, window);
        }

        private SourceInputStream()
        {
            this.streamState = this::streamBeforeBegin;
            this.throttleState = this::throttleSkipNextWindow;
            sourceOutputEstId = supplyStreamId.getAsLong();
            http2Streams = new Int2ObjectHashMap<>();
            localSettings = new Settings();
            remoteSettings = new Settings();
            decodeContext = new HpackContext(localSettings.headerTableSize, false);
            encodeContext = new HpackContext(remoteSettings.headerTableSize, true);
        }

        private void handleStream(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            streamState.onMessage(msgTypeId, buffer, index, length);
        }

        private void streamBeforeBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                processBegin(buffer, index, length);
            }
            else
            {
                processUnexpected(buffer, index, length);
            }
        }

        private void streamAfterBeginOrData(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                processData(buffer, index, length);
                break;
            case EndFW.TYPE_ID:
                processEnd(buffer, index, length);
                break;
            default:
                processUnexpected(buffer, index, length);
                break;
            }
        }

        private void streamAfterEnd(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            processUnexpected(buffer, index, length);
        }

        private void streamAfterReplyOrReset(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == DataFW.TYPE_ID)
            {
                dataRO.wrap(buffer, index, index + length);
                final long streamId = dataRO.streamId();
                source.doWindow(streamId, length);
            }
            else if (msgTypeId == EndFW.TYPE_ID)
            {
                endRO.wrap(buffer, index, index + length);
                final long streamId = endRO.streamId();

                source.removeStream(streamId);

                this.streamState = this::streamAfterEnd;
            }
        }

        private void processUnexpected(
            DirectBuffer buffer,
            int index,
            int length)
        {
            frameRO.wrap(buffer, index, index + length);
            long streamId = frameRO.streamId();

            processUnexpected(streamId);
        }

        void processUnexpected(
            long streamId)
        {
            source.doReset(streamId);

            this.streamState = this::streamAfterReplyOrReset;
        }

        private void processInvalidRequest(
            int requestBytes,
            String payloadChars)
        {
            final Target target = replyTarget;
            //final long newTargetId = supplyStreamId.getAsLong();

            // TODO: replace with connection pool (start)
            target.doBegin(sourceOutputEstId, 0L, correlationId);
            target.addThrottle(sourceOutputEstId, this::handleThrottle);
            // TODO: replace with connection pool (end)

            // TODO: acquire slab for response if targetWindow requires partial write
            DirectBuffer payload = new UnsafeBuffer(payloadChars.getBytes(UTF_8));
            target.doData(sourceOutputEstId, payload, 0, payload.capacity());

            this.decoderState = this::decodePreface;
            this.streamState = this::streamAfterReplyOrReset;
            this.throttleState = this::throttleSkipNextWindow;
            this.sourceUpdateDeferred = requestBytes - payload.capacity();
        }

        private void processBegin(
            DirectBuffer buffer,
            int index,
            int length)
        {
            beginRO.wrap(buffer, index, index + length);

            this.sourceId = beginRO.streamId();
            this.sourceRef = beginRO.referenceId();
            this.correlationId = beginRO.correlationId();

            this.streamState = this::streamAfterBeginOrData;
            this.decoderState = this::decodePreface;

            // TODO: acquire slab for request decode of up to initial bytes
final int initial = 51200;
            this.window += initial;
            source.doWindow(sourceId, initial);
        }

        private void processData(
            DirectBuffer buffer,
            int index,
            int length)
        {
            dataRO.wrap(buffer, index, index + length);

            window -= dataRO.length();

            if (window < 0)
            {
                processUnexpected(buffer, index, length);
            }
            else
            {
                final OctetsFW payload = dataRO.payload();
                final int limit = payload.limit();

                int offset = payload.offset();
                while (offset < limit)
                {
                    offset = decoderState.decode(buffer, offset, limit);
                }
            }
        }

        private void processEnd(
            DirectBuffer buffer,
            int index,
            int length)
        {
            endRO.wrap(buffer, index, index + length);
            final long streamId = endRO.streamId();

            decoderState = (b, o, l) -> o;

            source.removeStream(streamId);
            //target.removeThrottle(targetId);
            if (frameSlotIndex != SLOT_NOT_AVAILABLE)
            {
                frameSlab.release(frameSlotIndex);
                frameSlotIndex = SLOT_NOT_AVAILABLE;
                frameSlotPosition = 0;
            }
            if (headersSlotIndex != SLOT_NOT_AVAILABLE)
            {
                headersSlab.release(headersSlotIndex);
                headersSlotIndex = SLOT_NOT_AVAILABLE;
                headersSlotPosition = 0;
            }
        }


        private int decodePreface(final DirectBuffer buffer, final int offset, final int limit)
        {
            if (!prefaceAvailable(buffer, offset, limit))
            {
                return limit;
            }
            if (!prefaceRO.matches())
            {
                processUnexpected(sourceId);
                return limit;
            }
            this.decoderState = this::decodeHttp2Frame;
            source.doWindow(sourceId, prefaceRO.sizeof());

            // TODO: replace with connection pool (start)
            replyTarget.doBegin(sourceOutputEstId, 0L, correlationId);
            replyTarget.addThrottle(sourceOutputEstId, this::handleThrottle);
            // TODO: replace with connection pool (end)

            MutableDirectBuffer payload = new UnsafeBuffer(new byte[2048]);
            Http2SettingsFW settings = settingsRW.wrap(payload, 0, 2048)
                                                 .maxConcurrentStreams(100)
                                                 .build();

            replyTarget.doData(sourceOutputEstId, settings.buffer(), settings.offset(), settings.sizeof());


            return prefaceRO.limit();
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
         * @return true if a complete HTTP2 frame is assembled
         *         false otherwise
         */
        private boolean prefaceAvailable(DirectBuffer buffer, int offset, int limit)
        {
            int available = limit - offset;

            if (frameSlotPosition > 0 && frameSlotPosition +available >= PRI_REQUEST.length)
            {
                MutableDirectBuffer prefaceBuffer = frameSlab.buffer(frameSlotIndex);
                prefaceBuffer.putBytes(frameSlotPosition, buffer, offset, PRI_REQUEST.length- frameSlotPosition);
                prefaceRO.wrap(prefaceBuffer, 0, PRI_REQUEST.length);
                if (frameSlotIndex != SLOT_NOT_AVAILABLE)
                {
                    frameSlab.release(frameSlotIndex);
                    frameSlotIndex = SLOT_NOT_AVAILABLE;
                    frameSlotPosition = 0;
                }
                return true;
            }
            else if (available >= PRI_REQUEST.length)
            {
                prefaceRO.wrap(buffer, offset, offset + PRI_REQUEST.length);
                return true;
            }

            assert frameSlotIndex == SLOT_NOT_AVAILABLE;
            frameSlotIndex = frameSlab.acquire(sourceId);
            if (frameSlotIndex == SLOT_NOT_AVAILABLE)
            {
                // all slots are in use, just reset the connection
                source.doReset(sourceId);
                return false;
            }
            frameSlotPosition = 0;
            MutableDirectBuffer prefaceBuffer = frameSlab.buffer(frameSlotIndex);
            prefaceBuffer.putBytes(frameSlotPosition, buffer, offset, available);
            frameSlotPosition += available;
            return false;
        }

        /*
         * Assembles a complete HTTP2 frame and the flyweight is wrapped with the
         * buffer (it could be given buffer or slab)
         *
         * @return true if a complete HTTP2 frame is assembled
         *         false otherwise
         */
        // TODO check slab capacity
        private boolean http2FrameAvailable(DirectBuffer buffer, int offset, int limit)
        {
            int available = limit - offset;

            if (frameSlotPosition > 0 && frameSlotPosition +available >= 3)
            {
                MutableDirectBuffer frameBuffer = SourceInputStreamFactory.this.frameSlab.buffer(frameSlotIndex);
                if (frameSlotPosition < 3)
                {
                    frameBuffer.putBytes(frameSlotPosition, buffer, offset, 3- frameSlotPosition);
                }
                int length = http2FrameLength(frameBuffer, 0, 3);
                if (frameSlotPosition + available >= length)
                {
                    frameBuffer.putBytes(frameSlotPosition, buffer, offset, length- frameSlotPosition);
                    http2RO.wrap(frameBuffer, 0, length);
                    if (frameSlotIndex != SLOT_NOT_AVAILABLE)
                    {
                        frameSlab.release(frameSlotIndex);
                        frameSlotIndex = SLOT_NOT_AVAILABLE;
                        frameSlotPosition = 0;
                    }
                    return true;
                }
            }
            else if (available >= 3)
            {
                int length = http2FrameLength(buffer, offset, limit);
                if (available >= length)
                {
                    http2RO.wrap(buffer, offset, offset + length);
                    return true;
                }
            }

            assert frameSlotIndex == SLOT_NOT_AVAILABLE;
            frameSlotIndex = frameSlab.acquire(sourceId);
            if (frameSlotIndex == SLOT_NOT_AVAILABLE)
            {
                // all slots are in use, just reset the connection
                source.doReset(sourceId);
                return false;
            }
            frameSlotPosition = 0;
            MutableDirectBuffer frameBuffer = frameSlab.buffer(frameSlotIndex);
            frameBuffer.putBytes(frameSlotPosition, buffer, offset, available);
            frameSlotPosition += available;
            return false;
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
                if (http2RO.type() != Http2FrameType.CONTINUATION || http2RO.streamId() != expectContinuationStreamId)
                {
                    error(Http2ErrorCode.PROTOCOL_ERROR);
                    return false;
                }
            }
            else if (http2RO.type() == Http2FrameType.CONTINUATION)
            {
                error(Http2ErrorCode.PROTOCOL_ERROR);
                return false;
            }
            switch (http2RO.type())
            {
                case HEADERS:
                    int streamId = http2RO.streamId();
                    if (streamId == 0 || streamId % 2 != 1 || streamId <= maxClientStreamId)
                    {
                        error(Http2ErrorCode.PROTOCOL_ERROR);
                        return false;
                    }

                    headersRO.wrap(http2RO.buffer(), http2RO.offset(), http2RO.limit());
                    if (headersRO.dataLength() < 0)
                    {
                        error(Http2ErrorCode.PROTOCOL_ERROR);
                        return false;
                    }
                    source.doWindow(sourceId, http2RO.sizeof());

                    return http2HeadersAvailable(headersRO.buffer(), headersRO.dataOffset(), headersRO.dataLength(),
                            headersRO.endHeaders());

                case CONTINUATION:
                    continationRO.wrap(http2RO.buffer(), http2RO.offset(), http2RO.limit());
                    DirectBuffer payload = continationRO.payload();
                    boolean endHeaders = continationRO.endHeaders();

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
                    MutableDirectBuffer headersBuffer = headersSlab.buffer(headersSlotIndex);
                    headersBuffer.putBytes(headersSlotPosition, buffer, offset, length);
                    headersSlotPosition += length;
                    buffer = headersBuffer;
                    offset = 0;
                    length = headersSlotPosition;
                }
                int maxLimit = offset + length;
                expectContinuation = false;
                if (headersSlotIndex != SLOT_NOT_AVAILABLE)
                {
                    headersSlab.release(headersSlotIndex);      // early release, but fine
                    headersSlotIndex = SLOT_NOT_AVAILABLE;
                    headersSlotPosition = 0;
                }

                blockRO.wrap(buffer, offset, maxLimit);
                return true;
            }
            else
            {
                if (headersSlotIndex == SLOT_NOT_AVAILABLE)
                {
                    headersSlotIndex = headersSlab.acquire(sourceId);
                    if (headersSlotIndex == SLOT_NOT_AVAILABLE)
                    {
                        // all slots are in use, just reset the connection
                        source.doReset(sourceId);
                        return false;
                    }
                    headersSlotPosition = 0;
                }
                MutableDirectBuffer headersBuffer = headersSlab.buffer(headersSlotIndex);
                headersBuffer.putBytes(headersSlotPosition, buffer, offset, length);
                headersSlotPosition += length;
                expectContinuation = true;
                expectContinuationStreamId = headersRO.streamId();
            }

            return false;
        }

        private int decodeHttp2Frame(final DirectBuffer buffer, final int offset, final int limit)
        {
            if (!http2FrameAvailable(buffer, offset, limit))
            {
                return limit;
            }
System.out.println("---> " + http2RO);
            Http2FrameType http2FrameType = http2RO.type();
            // Assembles HTTP2 HEADERS and its CONTINUATIONS frames, if any
            if (!http2HeadersAvailable())
            {
                return offset + http2RO.sizeof();
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

            return offset + http2RO.sizeof();
        }

        private void doGoAway()
        {
            int streamId = http2RO.streamId();
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
                error(errorCode);
            }
        }

        private void doPushPromise()
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
        }

        private void doPriority()
        {
            int streamId = http2RO.streamId();
            if (streamId == 0)
            {
                error(Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
            int payloadLength = http2RO.payloadLength();
            if (payloadLength != 5)
            {
                streamError(streamId, Http2ErrorCode.FRAME_SIZE_ERROR);
                return;
            }
        }

        private void doHeaders()
        {
            int streamId = http2RO.streamId();

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

            if (noClientStreams + 1 > localSettings.maxConcurrentStreams)
            {
                streamError(streamId, Http2ErrorCode.REFUSED_STREAM);
                return;
            }

            State state = http2RO.endStream() ? HALF_CLOSED_REMOTE : OPEN;
            stream = newStream(streamId, state);

            headersContext.reset();

            // TODO avoid iterating over headers twice
            BiConsumer<DirectBuffer, DirectBuffer> collectHeaders = this::collectHeaders;
            BiConsumer<DirectBuffer, DirectBuffer> validatePseudoHeaders = collectHeaders.andThen(this::validatePseudoHeaders);
            BiConsumer<DirectBuffer, DirectBuffer> uppercaseHeaders = validatePseudoHeaders.andThen(this::uppercaseHeaders);
            BiConsumer<DirectBuffer, DirectBuffer> nameValue = uppercaseHeaders.andThen(this::connectionHeaders);

            Consumer<HpackHeaderFieldFW> consumer = this::validateHeaderFieldType;
            consumer = consumer.andThen(this::dynamicTableSizeUpdate);
            consumer = consumer.andThen(h -> decodeHeaderField(h, true, nameValue));

            blockRO.forEach(consumer);
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
                    streamError(streamId, headersContext.streamError);
                    return;
                }
                if (headersContext.connectionError != null)
                {
                    error(headersContext.connectionError);
                    return;
                }
            }

            final Optional<Route> optional = resolveTarget(sourceRef, headersContext.headers);
            Route route = stream.route = optional.get();
            Target newTarget = route.target();
            final long targetRef = route.targetRef();

            newTarget.doHttpBegin(stream.targetId, targetRef, stream.targetId,
                    hs -> blockRO.forEach(hf -> decodeHeaderField(hf, hs)));
            newTarget.addThrottle(stream.targetId, this::handleThrottle);

            source.doWindow(sourceId, http2RO.sizeof());
            throttleState = this::throttleSkipNextWindow;

            if (headersRO.endStream())
            {
                newTarget.doHttpEnd(stream.targetId);
            }
        }

        private void doRst()
        {
            int streamId = http2RO.streamId();
            if (streamId == 0)
            {
                error(Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
            int payloadLength = http2RO.payloadLength();
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
                closeStream(stream);
            }
        }

        private void closeStream(Http2Stream stream)
        {
            if (stream.isClientInitiated())
            {
                noClientStreams--;
            }
            else
            {
                noPromisedStreams--;
            }
            http2Streams.remove(stream.http2StreamId);
        }

        private void doWindow()
        {
            int streamId = http2RO.streamId();
            if (streamId != 0)
            {
                Http2Stream stream = http2Streams.get(streamId);
                if (stream == null || stream.state == State.IDLE)
                {
                    error(Http2ErrorCode.PROTOCOL_ERROR);
                }
            }
        }

        private void doData()
        {
            int streamId = http2RO.streamId();
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
            Target newTarget = stream.route.target();
            Http2DataFW dataRO = http2DataRO.wrap(http2RO.buffer(), http2RO.offset(), http2RO.limit());
            if (dataRO.dataLength() < 0)        // because of invalid padding length
            {
                error(Http2ErrorCode.PROTOCOL_ERROR);
                closeStream(stream);
                return;
            }
            newTarget.doHttpData(stream.targetId, dataRO.buffer(), dataRO.dataOffset(), dataRO.dataLength());

            if (dataRO.endStream())
            {
                newTarget.doHttpEnd(stream.targetId);
            }
        }

        private void doSettings()
        {
            if (http2RO.streamId() != 0)
            {
                error(Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
            if (http2RO.payloadLength()%6 != 0)
            {
                error(Http2ErrorCode.FRAME_SIZE_ERROR);
                return;
            }

            settingsRO.wrap(http2RO.buffer(), http2RO.offset(), http2RO.limit());

            if (settingsRO.ack() && http2RO.payloadLength() != 0)
            {
                error(Http2ErrorCode.FRAME_SIZE_ERROR);
                return;
            }
            if (!settingsRO.ack())
            {
                settingsRO.accept(this::doSetting);

                Http2SettingsFW settings = settingsRW.wrap(buffer, 0, buffer.capacity())
                                                     .ack()
                                                     .build();
                replyTarget.doData(sourceOutputEstId,
                        settings.buffer(), settings.offset(), settings.limit());
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
                    remoteSettings.initialWindowSize = value.intValue();
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
            if (http2RO.streamId() != 0)
            {
                error(Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
            if (http2RO.payloadLength() != 8)
            {
                error(Http2ErrorCode.FRAME_SIZE_ERROR);
                return;
            }
            pingRO.wrap(http2RO.buffer(), http2RO.offset(), http2RO.limit());

            if (!pingRO.ack())
            {
                Http2PingFW ping = pingRW.wrap(buffer, 0, buffer.capacity())
                                         .ack()
                                         .payload(pingRO.payload())
                                         .build();
                replyTarget.doData(sourceOutputEstId,
                        ping.buffer(), ping.offset(), ping.sizeof());
            }
        }

        Optional<Route> resolveTarget(
            long sourceRef,
            Map<String, String> headers)
        {
            final List<Route> routes = supplyRoutes.apply(sourceRef);
            final Predicate<Route> predicate = headersMatch(headers);

            return routes.stream().filter(predicate).findFirst();
        }

        void handleThrottle(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            throttleState.onMessage(msgTypeId, buffer, index, length);
        }

        void throttleSkipNextWindow(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                processSkipNextWindow(buffer, index, length);
                break;
            case ResetFW.TYPE_ID:
                processReset(buffer, index, length);
                break;
            default:
                // ignore
                break;
            }
        }

        private void throttleNextWindow(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                processNextWindow(buffer, index, length);
                break;
            case ResetFW.TYPE_ID:
                processReset(buffer, index, length);
                break;
            default:
                // ignore
                break;
            }
        }

        private void processSkipNextWindow(
            DirectBuffer buffer,
            int index,
            int length)
        {
            windowRO.wrap(buffer, index, index + length);

            throttleState = this::throttleNextWindow;
        }

        private void processNextWindow(
            DirectBuffer buffer,
            int index,
            int length)
        {
            windowRO.wrap(buffer, index, index + length);

            int update = windowRO.update();

            if (sourceUpdateDeferred != 0)
            {
                update += sourceUpdateDeferred;
                sourceUpdateDeferred = 0;
            }

            window += update;
            source.doWindow(sourceId, update + framing(update));
        }


        private void processReset(
            DirectBuffer buffer,
            int index,
            int length)
        {
            resetRO.wrap(buffer, index, index + length);
            if (frameSlotIndex != SLOT_NOT_AVAILABLE)
            {
                frameSlab.release(frameSlotIndex);
                frameSlotIndex = SLOT_NOT_AVAILABLE;
                frameSlotPosition = 0;
            }
            if (headersSlotIndex != SLOT_NOT_AVAILABLE)
            {
                headersSlab.release(headersSlotIndex);
                headersSlotIndex = SLOT_NOT_AVAILABLE;
                headersSlotPosition = 0;
            }
            source.doReset(sourceId);
        }

        void error(Http2ErrorCode errorCode)
        {
            Http2GoawayFW goawayRO = goawayRW.wrap(buffer, 0, buffer.capacity())
                                             .lastStreamId(lastStreamId)
                                             .errorCode(errorCode)
                                             .build();
            replyTarget.doData(sourceOutputEstId,
                    goawayRO.buffer(), goawayRO.offset(), goawayRO.sizeof());

            replyTarget.doEnd(sourceOutputEstId);
        }

        void streamError(int streamId, Http2ErrorCode errorCode)
        {
            Http2RstStreamFW resetRO = resetRW.wrap(buffer, 0, buffer.capacity())
                                             .streamId(streamId)
                                             .errorCode(errorCode)
                                             .build();
            replyTarget.doData(sourceOutputEstId,
                    resetRO.buffer(), resetRO.offset(), resetRO.sizeof());
        }

        private int nextPromisedId()
        {
            lastPromisedStreamId += 2;
            return lastPromisedStreamId;
        }

        /*
         * @param streamId corresponding http2 stream-id on which service response
         *                 will be sent
         * @return a stream id on which PUSH_PROMISE can be sent
         *         -1 otherwise
         */
        private int findPushId(int streamId)
        {
            if (remoteSettings.enablePush && noPromisedStreams+1 < remoteSettings.maxConcurrentStreams)
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

            Http2Stream http2Stream = newStream(http2StreamId, HALF_CLOSED_REMOTE);
            http2Streams.put(http2StreamId, http2Stream);
            long targetId = http2Stream.targetId;

            Map<String, String> headersMap = new HashMap<>();
            headers.forEach(
                    httpHeader -> headersMap.put(httpHeader.name().asString(), httpHeader.value().asString()));
            Optional<Route> optional = resolveTarget(sourceRef, headersMap);
            Route route = optional.get();
            Target newTarget = route.target();
            long targetRef = route.targetRef();

            newTarget.doHttpBegin(targetId, targetRef, targetId,
                    hs -> headers.forEach(h -> hs.item(b -> b.representation((byte) 0)
                                                             .name(h.name())
                                                             .value(h.value()))));
            newTarget.doHttpEnd(targetId);
            Correlation correlation = new Correlation(correlationId, sourceOutputEstId, this::doPromisedRequest,
                    http2StreamId, encodeContext, this::nextPromisedId, this::findPushId,
                    source.routableName(), OUTPUT_ESTABLISHED);

            correlateNew.accept(targetId, correlation);
            newTarget.addThrottle(targetId, this::handleThrottle);
        }

        private Http2Stream newStream(int http2StreamId, State state)
        {
            assert http2StreamId != 0;

            Http2Stream http2Stream = new Http2Stream(this, http2StreamId, state);
            http2Streams.put(http2StreamId, http2Stream);

            Correlation correlation = new Correlation(correlationId, sourceOutputEstId, this::doPromisedRequest,
                    http2RO.streamId(), encodeContext, this::nextPromisedId, this::findPushId,
                    source.routableName(), OUTPUT_ESTABLISHED);

            correlateNew.accept(http2Stream.targetId, correlation);
            if (http2Stream.isClientInitiated())
            {
                noClientStreams++;
            }
            else
            {
                noPromisedStreams++;
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
                System.out.println("hf type = " + hf.type());

                switch (hf.type())
                {
                    case INDEXED:
                    case LITERAL:
                        expectDynamicTableSizeUpdate = false;
                        break;
                    case UPDATE:
                        System.out.println("Dynamic table Update = " + hf.tableSize());

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
            DirectBuffer connection = new UnsafeBuffer("connection".getBytes(UTF_8));
            if (!headersContext.error() && name.equals(connection))
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

//        // MUST not omit or duplicate ":method" pseudo-header field
//        private void validateMethod(DirectBuffer name, DirectBuffer value)
//        {
//            if (!headersContext.compressionError && name.equals(decodeContext.nameBuffer(2)))
//            {
//                headersContext.method++;
//            }
//        }
//
//        // MUST not omit or duplicate ":scheme" pseudo-header field
//        private void validateScheme(DirectBuffer name, DirectBuffer value)
//        {
//            if (!headersContext.compressionError && name.equals(decodeContext.nameBuffer(6)))
//            {
//                headersContext.scheme++;
//            }
//        }
//
//        // MUST not omit or duplicate ":path" pseudo-header field, and value cannot be empty
//        private void validatePath(DirectBuffer name, DirectBuffer value)
//        {
//            if (!headersContext.compressionError && name.equals(decodeContext.nameBuffer(4)) && value.capacity() > 0)
//            {
//                headersContext.path++;
//            }
//        }

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

        private void decodeHeaderField(HpackHeaderFieldFW hf,
                                       ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW> builder)
        {
            if (!headersContext.error())
            {
                decodeHeaderField(hf, false, (name, value) -> builder.item(i -> i.representation((byte) 0)
                                                                                 .name(name, 0, name.capacity())
                                                                                 .value(value, 0, value.capacity())));
            }

        }

        private void decodeHeaderField(HpackHeaderFieldFW hf,
                                       boolean incrementalIndexing,
                                       BiConsumer<DirectBuffer, DirectBuffer> nameValue)
        {
            switch (hf.type())
            {
                case INDEXED :
                {
                    int index = hf.index();
                    if (!decodeContext.valid(index))
                    {
                        headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                        return;
                    }
                    DirectBuffer nameBuffer = decodeContext.nameBuffer(index);
                    DirectBuffer valueBuffer = decodeContext.valueBuffer(index);
                    nameValue.accept(nameBuffer, valueBuffer);
                }
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
                            int index = literalRO.nameIndex();
                            DirectBuffer nameBuffer = decodeContext.nameBuffer(index);

                            HpackStringFW valueRO = literalRO.valueLiteral();
                            DirectBuffer valuePayload = valueRO.payload();
                            if (valueRO.huffman())
                            {
                                MutableDirectBuffer dst = new UnsafeBuffer(new byte[4096]); // TODO
                                int length = HpackHuffman.decode(valuePayload, dst);
                                if (length == -1)
                                {
                                    headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                                    return;
                                }
                                valuePayload = new UnsafeBuffer(dst, 0, length);
                            }
                            DirectBuffer valueBuffer = valuePayload;
                            nameValue.accept(nameBuffer, valueBuffer);
                        }
                        break;
                        case NEW:
                        {
                            HpackStringFW nameRO = literalRO.nameLiteral();
                            DirectBuffer namePayload = nameRO.payload();
                            if (nameRO.huffman())
                            {
                                MutableDirectBuffer dst = new UnsafeBuffer(new byte[4096]); // TODO
                                int length = HpackHuffman.decode(namePayload, dst);
                                if (length == -1)
                                {
                                    headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                                    return;
                                }
                                namePayload = new UnsafeBuffer(dst, 0, length);
                            }
                            DirectBuffer nameBuffer = namePayload;

                            HpackStringFW valueRO = literalRO.valueLiteral();
                            DirectBuffer valuePayload = valueRO.payload();
                            if (valueRO.huffman())
                            {
                                MutableDirectBuffer dst = new UnsafeBuffer(new byte[4096]); // TODO
                                int length = HpackHuffman.decode(valuePayload, dst);
                                if (length == -1)
                                {
                                    headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                                    return;
                                }
                                valuePayload = new UnsafeBuffer(dst, 0, length);
                            }
                            DirectBuffer valueBuffer = valuePayload;
                            nameValue.accept(nameBuffer, valueBuffer);

                            if (incrementalIndexing && literalRO.literalType() == INCREMENTAL_INDEXING)
                            {
                                // make a copy for name and value as they go into dynamic table (outlives current frame)
                                MutableDirectBuffer name = new UnsafeBuffer(new byte[namePayload.capacity()]);
                                name.putBytes(0, namePayload, 0, namePayload.capacity());
                                MutableDirectBuffer value = new UnsafeBuffer(new byte[valuePayload.capacity()]);
                                value.putBytes(0, valuePayload, 0, valuePayload.capacity());
                                decodeContext.add(name, value);
                            }
                        }
                        break;
                    }
                    break;
            }
        }

    }

    private static int framing(
        int payloadSize)
    {
        // TODO: consider chunks
        return 0;
    }

    @FunctionalInterface
    private interface DecoderState
    {
        int decode(DirectBuffer buffer, int offset, int length);
    }

    class Http2Stream
    {
        private final SourceInputStream connection;
        private final int http2StreamId;
        private final long targetId;
        private Route route;
        private State state;

        Http2Stream(SourceInputStream connection, int http2StreamId, State state)
        {
            this.connection = connection;
            this.http2StreamId = http2StreamId;
            this.targetId = supplyStreamId.getAsLong();
            this.state = state;
        }

        boolean isClientInitiated()
        {
            return http2StreamId%2 == 1;
        }

    }
}
