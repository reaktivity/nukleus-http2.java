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
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.http2.internal.routable.Correlation;
import org.reaktivity.nukleus.http2.internal.routable.Route;
import org.reaktivity.nukleus.http2.internal.routable.Source;
import org.reaktivity.nukleus.http2.internal.routable.Target;
import org.reaktivity.nukleus.http2.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.OctetsFW;
import org.reaktivity.nukleus.http2.internal.types.StringFW;
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http2.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http2.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackContext;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHuffman;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackStringFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2GoawayFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2HeadersFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PingFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PrefaceFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsFW;
import org.reaktivity.nukleus.http2.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http2.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.http2.internal.util.function.LongObjectBiConsumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.reaktivity.nukleus.http2.internal.routable.Route.headersMatch;
import static org.reaktivity.nukleus.http2.internal.router.RouteKind.OUTPUT_ESTABLISHED;
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
    private final Http2PingFW pingRO = new Http2PingFW();

    private final Http2SettingsFW.Builder settingsRW = new Http2SettingsFW.Builder();
    private final Http2PingFW.Builder pingRW = new Http2PingFW.Builder();
    private final Http2GoawayFW.Builder goawayRW = new Http2GoawayFW.Builder();

    private final ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW> headersRW =
            new ListFW.Builder<>(new HttpHeaderFW.Builder(), new HttpHeaderFW());

    private final Source source;
    private final LongFunction<List<Route>> supplyRoutes;
    private final LongSupplier supplyStreamId;
    private final Target replyTarget;
    private final LongObjectBiConsumer<Correlation> correlateNew;

    private final AtomicBuffer buffer = new UnsafeBuffer(new byte[2048]);

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
    }

    public MessageHandler newStream()
    {
        return new SourceInputStream()::handleStream;
    }

    private final class SourceInputStream
    {
        private MessageHandler streamState;
        MessageHandler throttleState;
        private DecoderState decoderState;

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
        private int lastPromisedStreamId = 0;

        private final AtomicBuffer slab = new UnsafeBuffer(new byte[4096]);
        private int slabLength = 0;

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
            decodeContext = new HpackContext();
            encodeContext = new HpackContext();
            http2Streams = new Int2ObjectHashMap<>();
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
            final int initial = 512;
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

            AtomicBuffer payload = new UnsafeBuffer(new byte[2048]);
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

            if (slabLength > 0 && slabLength+available >= PRI_REQUEST.length)
            {
                slab.putBytes(slabLength, buffer, offset, PRI_REQUEST.length-slabLength);
                prefaceRO.wrap(slab, 0, PRI_REQUEST.length);
                slabLength = 0;
                return true;
            }
            else if (available >= PRI_REQUEST.length)
            {
                prefaceRO.wrap(buffer, offset, offset + PRI_REQUEST.length);
                return true;
            }

            slab.putBytes(slabLength, buffer, offset, available);
            slabLength += available;
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

            if (slabLength > 0 && slabLength+available >= 3)
            {
                if (slabLength < 3)
                {
                    slab.putBytes(slabLength, buffer, offset, 3-slabLength);
                }
                int length = http2FrameLength(slab, 0, 3);
                if (slabLength+available >= length)
                {
                    slab.putBytes(slabLength, buffer, offset, length-slabLength);
                    http2RO.wrap(slab, 0, length);
                    slabLength = 0;
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

            slab.putBytes(slabLength, buffer, offset, available);
            slabLength += available;
            return false;
        }

        private int decodeHttp2Frame(final DirectBuffer buffer, final int offset, final int limit)
        {
            if (!http2FrameAvailable(buffer, offset, limit))
            {
                return limit;
            }

            int nextOffset = offset + http2RO.sizeof();

            Http2FrameType http2FrameType = http2RO.type();

            if (http2FrameType == null)
            {
                return nextOffset;               // Ignore and discard unknown frame
            }
            if (http2FrameType == Http2FrameType.SETTINGS)
            {
                doSettings(http2RO);
                return nextOffset;
            }
            else if (http2FrameType == Http2FrameType.PING)
            {
                doPing();
                return nextOffset;
            }
            int streamId = lastStreamId = http2RO.streamId();
            Http2Stream http2Stream = http2Streams.get(streamId);
            if (http2Stream == null)
            {
                long targetId = supplyStreamId.getAsLong();
                http2Stream = new Http2Stream(this, streamId, targetId, State.IDLE);
                http2Streams.put(streamId, http2Stream);

                final Correlation correlation = new Correlation(correlationId, sourceOutputEstId, this::doPromisedRequest,
                        http2RO.streamId(), encodeContext, this::nextPromisedId, source.routableName(), OUTPUT_ESTABLISHED);

                correlateNew.accept(targetId, correlation);
            }
            http2Stream.onFrame();
            return nextOffset;
        }

        private void doSettings(Http2FrameFW http2RO)
        {
            settingsRO.wrap(http2RO.buffer(), http2RO.offset(), http2RO.limit());
            if (!settingsRO.ack())
            {
                Http2SettingsFW settings = settingsRW.wrap(buffer, 0, buffer.capacity())
                                                     .ack()
                                                     .build();
                replyTarget.doData(sourceOutputEstId,
                        settings.buffer(), settings.offset(), settings.limit());
            }
            // TODO when ack flag is true
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

        private int nextPromisedId()
        {
            lastPromisedStreamId += 2;
            return lastPromisedStreamId;
        };

        private void doPromisedRequest(int http2StreamId, ListFW<HttpHeaderFW> headers)
        {
            Map<String, String> headersMap = new HashMap<>();
            headers.forEach(
                    httpHeader -> headersMap.put(httpHeader.name().asString(), httpHeader.value().asString()));
            Optional<Route> optional = resolveTarget(sourceRef, headersMap);
            Route route = optional.get();
            Target newTarget = route.target();
            long targetRef = route.targetRef();
            long targetId = supplyStreamId.getAsLong();
            newTarget.doHttpBegin(targetId, targetRef, targetId,
                    hs -> headers.forEach(h -> hs.item(builder -> mapHeader(h, builder))));
            Correlation correlation = new Correlation(correlationId, sourceOutputEstId, this::doPromisedRequest,
                    http2StreamId, encodeContext, this::nextPromisedId, source.routableName(), OUTPUT_ESTABLISHED);

            correlateNew.accept(targetId, correlation);
            newTarget.addThrottle(targetId, this::handleThrottle);

            Http2Stream http2Stream = new Http2Stream(this, http2StreamId, targetId, State.HALF_CLOSED_REMOTE);
            http2Streams.put(http2StreamId, http2Stream);
        }

        private void mapHeader(HttpHeaderFW h, HttpHeaderFW.Builder builder)
        {
            StringFW name = h.name();
            StringFW value = h.value();
            builder.representation((byte) 0)
                   .name(name.buffer(), name.offset()+1, name.sizeof()-1)
                   .value(value.buffer(), value.offset()+1, value.sizeof()-1);
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

        Http2Stream(SourceInputStream connection, int http2StreamId, long targetId, State state)
        {
            this.connection = connection;
            this.http2StreamId = http2StreamId;
            this.targetId = targetId;
            this.state = state;
        }

        void onFrame()
        {
            switch (state)
            {
                case IDLE:
                    inIdle();
                    break;
                case RESERVED_LOCAL:
                    inReservedLocal();
                    break;
                case RESERVED_REMOTE:
                    inReservedRemote();
                    break;
                case OPEN:
                    inOpen();
                    break;
                case HALF_CLOSED_LOCAL:
                    inHalfClosedLocal();
                    break;
                case HALF_CLOSED_REMOTE:
                    inHalfClosedRemote();
                    break;
                case CLOSED:
                    inClosed();
                    break;
            }
        }

        private void inClosed()
        {
        }

        private void inHalfClosedRemote()
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

        private void inHalfClosedLocal()
        {
            if (http2RO.endStream() || http2RO.type() == Http2FrameType.RST_STREAM)
            {
                state = State.CLOSED;
            }
        }

        private void inOpen()
        {
            if (http2RO.endStream())
            {
                state = State.HALF_CLOSED_REMOTE;
            }
            switch (http2RO.type())
            {
                case HEADERS:
                    doHeaders();
                    break;
                case DATA:
                    doData();
                    break;
                case RST_STREAM:
                    state = State.CLOSED;
                    connection.http2Streams.remove(http2StreamId);
                    break;
                default:
                    connection.error(Http2ErrorCode.PROTOCOL_ERROR);
            }
        }

        private void inReservedRemote()
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

        private void inReservedLocal()
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

        private void inIdle()
        {
            switch (http2RO.type())
            {
                case HEADERS:
                    state = State.OPEN;
                    doHeaders();
                    break;
                case PRIORITY:
                    break;
                default:
                    connection.error(Http2ErrorCode.PROTOCOL_ERROR);
            }
        }

        private void doHeaders()
        {
            headersRO.wrap(http2RO.buffer(), http2RO.offset(), http2RO.limit());

            // TODO avoid iterating over headers twice
            Map<String, String> headersMap = new HashMap<>();
            headersRO.forEach(h -> decodeHeaderField(connection.decodeContext, headersMap, h));
            final Optional<Route> optional = connection.resolveTarget(connection.sourceRef, headersMap);
            route = optional.get();
            Target newTarget = route.target();
            final long targetRef = route.targetRef();

            newTarget.doHttpBegin(targetId, targetRef, targetId,
                    hs -> headersRO.forEach(hf -> decodeHeaderField(connection.decodeContext, hs, hf)));
            newTarget.addThrottle(targetId, connection::handleThrottle);

            source.doWindow(connection.sourceId, http2RO.sizeof());
            connection.throttleState = connection::throttleSkipNextWindow;
            state = State.OPEN;
        }

        private void doRst()
        {
            if (state == State.IDLE)
            {
                connection.error(Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
        }

        private void doWindow()
        {
            if (state == State.IDLE)
            {
                connection.error(Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
        }

        private void doContinuation()
        {
            if (state == State.IDLE)
            {
                connection.error(Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
        }

        private void doData()
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
            Http2DataFW dataRO = http2DataRO.wrap(http2RO.buffer(), http2RO.offset(), http2RO.limit());
            newTarget.doHttpData(targetId, dataRO.buffer(), dataRO.dataOffset(), dataRO.dataLength());

        }

        private void decodeHeaderField(HpackContext context, ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW> builder,
                                       HpackHeaderFieldFW hf)
        {

            HpackHeaderFieldFW.HeaderFieldType headerFieldType = hf.type();
            switch (headerFieldType)
            {
                case INDEXED :
                {
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
}
