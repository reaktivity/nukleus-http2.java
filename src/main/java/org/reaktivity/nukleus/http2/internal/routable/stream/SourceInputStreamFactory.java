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
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http2.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http2.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackContext;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2HeadersFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PrefaceFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsFW;
import org.reaktivity.nukleus.http2.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http2.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.http2.internal.util.function.LongObjectBiConsumer;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.reaktivity.nukleus.http2.internal.routable.Route.headersMatch;
import static org.reaktivity.nukleus.http2.internal.router.RouteKind.OUTPUT_ESTABLISHED;

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

    private final Http2SettingsFW.Builder settingsRW = new Http2SettingsFW.Builder();

    private final ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW> headersRW =
            new ListFW.Builder<>(new HttpHeaderFW.Builder(), new HttpHeaderFW());

    private final Source source;
    private final LongFunction<List<Route>> supplyRoutes;
    private final LongSupplier supplyStreamId;
    private final Target replyTarget;
    private final LongObjectBiConsumer<Correlation> correlateNew;

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

    final class SourceInputStream
    {
        private MessageHandler streamState;
        MessageHandler throttleState;
        private DecoderState decoderState;

        long sourceId;

        private Target target;
        private long targetId;      // TODO multiple targetId since multiplexing
        long sourceRef;
        private long correlationId;
        private int window;
        private int contentRemaining;
        private int sourceUpdateDeferred;
        final long sourceOutputEstId;
        final HpackContext hpackContext;
        private final Int2ObjectHashMap<Http2Stream> http2Streams;

        @Override
        public String toString()
        {
            return String.format("%s[source=%s, sourceId=%016x, window=%d, targetId=%016x]",
                    getClass().getSimpleName(), source.routableName(), sourceId, window, targetId);
        }

        private SourceInputStream()
        {
            this.streamState = this::streamBeforeBegin;
            this.throttleState = this::throttleSkipNextWindow;
            sourceOutputEstId = supplyStreamId.getAsLong();
            hpackContext = new HpackContext();
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

        private void processUnexpected(
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
            prefaceRO.wrap(buffer, offset, limit);
            this.decoderState = this::decodeHttp2Frames;
            source.doWindow(sourceId, limit - offset);

            // TODO: replace with connection pool (start)
            replyTarget.doBegin(sourceOutputEstId, 0L, correlationId);
            replyTarget.addThrottle(sourceOutputEstId, this::handleThrottle);
            // TODO: replace with connection pool (end)

            AtomicBuffer payload = new UnsafeBuffer(new byte[2048]);
            Http2SettingsFW settings = settingsRW.wrap(payload, 0, 2048).maxConcurrentStreams(100).build();

            replyTarget.doData(sourceOutputEstId, settings.buffer(), settings.offset(), settings.limit());


            return prefaceRO.limit();
        }

        private int decodeHttp2Frames(final DirectBuffer buffer, final int offset, final int limit)
        {
            int nextOffset = offset;

            for (; nextOffset < limit; nextOffset = http2RO.limit())
            {
                assert limit - nextOffset >= 3;

                http2RO.wrap(buffer, nextOffset, limit);
                int streamId = http2RO.streamId();
                Http2Stream http2Stream = http2Streams.get(streamId);
                if (http2Stream == null)
                {
                    long targetId = supplyStreamId.getAsLong();
                    http2Stream = new Http2Stream(this, targetId);
                    http2Streams.put(streamId, http2Stream);

                    final Correlation correlation = new Correlation(correlationId, sourceOutputEstId,
                            http2RO.streamId(), source.routableName(), OUTPUT_ESTABLISHED);

                    correlateNew.accept(targetId, correlation);
                }
                http2Stream.decode(http2RO);
            }

            return nextOffset;
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

        Http2HeadersFW headersRO()
        {
            return headersRO;
        }

        Http2SettingsFW.Builder settingsRW()
        {
            return settingsRW;
        }

        Target replyTarget()
        {
            return replyTarget;
        }

        Source source()
        {
            return source;
        }

        Http2DataFW dataRO()
        {
            return http2DataRO;
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

}
