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
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.http2.internal.routable.Correlation;
import org.reaktivity.nukleus.http2.internal.routable.Source;
import org.reaktivity.nukleus.http2.internal.routable.Target;
import org.reaktivity.nukleus.http2.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.OctetsFW;
import org.reaktivity.nukleus.http2.internal.types.StringFW;
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http2.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http2.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackContext;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataExFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2HeadersFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PushPromiseFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http2.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http2.internal.types.stream.WindowFW;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;

import static org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW.LiteralType.WITHOUT_INDEXING;

public final class TargetOutputEstablishedStreamFactory
{

    private final FrameFW frameRO = new FrameFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final HttpBeginExFW beginExRO = new HttpBeginExFW();
    private final Http2DataExFW dataExRO = new Http2DataExFW();

    private final Http2DataFW.Builder dataRW = new Http2DataFW.Builder();
    private final Http2HeadersFW.Builder http2HeadersRW = new Http2HeadersFW.Builder();
    private final Http2PushPromiseFW.Builder pushPromiseRW = new Http2PushPromiseFW.Builder();

    private final DirectBuffer nameRO = new UnsafeBuffer(new byte[0]);
    private final DirectBuffer valueRO = new UnsafeBuffer(new byte[0]);


    private final Source source;
    private final Function<String, Target> supplyTarget;
    private final LongFunction<Correlation> correlateEstablished;

    public TargetOutputEstablishedStreamFactory(
        Source source,
        Function<String, Target> supplyTarget,
        LongSupplier supplyStreamId,
        LongFunction<Correlation> correlateEstablished)
    {
        this.source = source;
        this.supplyTarget = supplyTarget;
        this.correlateEstablished = correlateEstablished;
    }

    public MessageHandler newStream()
    {
        return new TargetOutputEstablishedStream()::handleStream;
    }

    private final class TargetOutputEstablishedStream
    {
        private MessageHandler streamState;

        private long sourceId;

        private Target target;
        private long sourceOutputEstId;
        private int http2StreamId;

        private int window;
        // TODO size ??
        private final MutableDirectBuffer writeBuffer = new UnsafeBuffer(new byte[4096]);
        private final HpackContext hpackContext = new HpackContext();
        private PushHandler pushHandler;
        private IntSupplier promisedStreamIds;

        @Override
        public String toString()
        {
            return String.format("%s[source=%s, sourceId=%016x, window=%d, sourceOutputEstId=%016x]",
                    getClass().getSimpleName(), source.routableName(), sourceId, window, sourceOutputEstId);
        }

        private TargetOutputEstablishedStream()
        {
            this.streamState = this::beforeBegin;
        }

        private void handleStream(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            streamState.onMessage(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
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

        private void afterBeginOrData(
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

        private void afterEnd(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            processUnexpected(buffer, index, length);
        }

        private void afterRejectOrReset(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == DataFW.TYPE_ID)
            {
                dataRO.wrap(buffer, index, index + length);
                long streamId = dataRO.streamId();

                source.doWindow(streamId, length);
            }
            else if (msgTypeId == EndFW.TYPE_ID)
            {
                endRO.wrap(buffer, index, index + length);
                long streamId = endRO.streamId();

                source.removeStream(streamId);

                this.streamState = this::afterEnd;
            }
        }

        private void processUnexpected(
            DirectBuffer buffer,
            int index,
            int length)
        {
            frameRO.wrap(buffer, index, index + length);

            long streamId = frameRO.streamId();

            source.doReset(streamId);

            this.streamState = this::afterRejectOrReset;
        }

        private void processBegin(
            DirectBuffer buffer,
            int index,
            int length)
        {
            beginRO.wrap(buffer, index, index + length);

            final long newSourceId = beginRO.streamId();
            final long sourceRef = beginRO.referenceId();
            final long targetCorrelationId = beginRO.correlationId();
            final OctetsFW extension = beginRO.extension();

            final Correlation correlation = correlateEstablished.apply(targetCorrelationId);

            if (sourceRef == 0L && correlation != null)
            {
                Target newTarget = supplyTarget.apply(correlation.source());
                sourceOutputEstId = correlation.getSourceOutputEstId();
                long sourceCorrelationId = correlation.id();
                promisedStreamIds = correlation.promisedStreamIds();

                this.sourceId = newSourceId;
                this.target = newTarget;
                this.http2StreamId = correlation.http2StreamId();
                this.pushHandler = correlation.pushHandler();

                newTarget.addThrottle(sourceOutputEstId, this::handleThrottle);
                HttpBeginExFW beginEx = extension.get(beginExRO::wrap);
                Http2HeadersFW http2HeadersRO = http2HeadersRW
                        .wrap(writeBuffer, 0, writeBuffer.capacity())
                        .streamId(http2StreamId)
                        .endHeaders()
                        .set(beginEx.headers(), mapHeader(hpackContext))
                        .build();

                target.doData(sourceOutputEstId, http2HeadersRO.buffer(), http2HeadersRO.offset(),
                        http2HeadersRO.limit());

                this.streamState = this::afterBeginOrData;
            }
            else
            {
                processUnexpected(buffer, index, length);
            }
        }

        private void processData(
            DirectBuffer buffer,
            int index,
            int length)
        {
            dataRO.wrap(buffer, index, index + length);

            window -= dataRO.length();

            OctetsFW extension = dataRO.extension();
            OctetsFW payload = dataRO.payload();

            if (extension.sizeof() > 0)
            {
                int promisedStreamId = promisedStreamIds.getAsInt();
                Http2DataExFW dataEx = extension.get(dataExRO::wrap);
                Http2PushPromiseFW pushPromise = pushPromiseRW
                        .wrap(writeBuffer, 0, writeBuffer.capacity())
                        .streamId(http2StreamId)
                        .promisedStreamId(promisedStreamId)
                        .endHeaders()
                        .set(dataEx.headers(), mapHeader(hpackContext))
                        .build();

                // TODO remove the following and throttle based on HTTP2_WINDOW update
                target.addThrottle(sourceOutputEstId, this::handleThrottle);
                target.doData(sourceOutputEstId, pushPromise.buffer(), pushPromise.offset(), pushPromise.limit());

                Map<String, String> promisedHeaders = new HashMap<>();
                dataEx.headers().forEach(
                        httpHeader -> promisedHeaders.put(httpHeader.name().asString(), httpHeader.value().asString()));
                pushHandler.doPromisedRequest(promisedStreamId, promisedHeaders);

            }
            if (payload.sizeof() > 0)
            {
                Http2DataFW http2Data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                              .streamId(http2StreamId)
                                              .endStream()      // TODO there may be multiple DATA frames
                                              .payload(payload.buffer(), payload.offset(), payload.sizeof())
                                              .build();
                // TODO remove the following and throttle based on HTTP2_WINDOW update
                target.addThrottle(sourceOutputEstId, this::handleThrottle);
                target.doData(sourceOutputEstId, http2Data.buffer(), http2Data.offset(), http2Data.limit());
            }
        }

        private void processEnd(
            DirectBuffer buffer,
            int index,
            int length)
        {
            endRO.wrap(buffer, index, index + length);

            target.removeThrottle(sourceOutputEstId);
            source.removeStream(sourceId);
        }

        private void handleThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
                case WindowFW.TYPE_ID:
                    processWindow(buffer, index, length);
                    break;
                case ResetFW.TYPE_ID:
                    processReset(buffer, index, length);
                    break;
                default:
                    // ignore
                    break;
            }
        }

        private void processWindow(
            DirectBuffer buffer,
            int index,
            int length)
        {
            windowRO.wrap(buffer, index, index + length);

            source.doWindow(sourceId, windowRO.update());
        }

        private void processReset(
            DirectBuffer buffer,
            int index,
            int length)
        {
            resetRO.wrap(buffer, index, index + length);

            source.doReset(sourceId);
        }
    }

    // Map http1.1 header to http2 header field in HEADERS, PUSH_PROMISE request
    private BiFunction<HttpHeaderFW, HpackHeaderFieldFW.Builder, HpackHeaderFieldFW> mapHeader(HpackContext hpackContext)
    {
        return (httpHeader, builder) ->
        {
            StringFW name = httpHeader.name();
            StringFW value = httpHeader.value();
            nameRO.wrap(name.buffer(), name.offset() + 1, name.sizeof() - 1); // +1, -1 for length-prefixed buffer
            valueRO.wrap(value.buffer(), value.offset() + 1, value.sizeof() - 1);

            int index = hpackContext.index(nameRO, valueRO);
            if (index != -1)
            {
                // Indexed
                builder.indexed(index);
            }
            else
            {
                // Literal
                builder.literal(literalBuilder -> buildLiteral(literalBuilder, hpackContext));
            }
            return builder.build();
        };
    }

    // Building Literal representation of header field
    // TODO dynamic table, huffman, never indexed
    private void buildLiteral(
        HpackLiteralHeaderFieldFW.Builder builder,
        HpackContext hpackContext)
    {
        int nameIndex = hpackContext.index(nameRO);
        builder.type(WITHOUT_INDEXING);
        if (nameIndex != -1)
        {
            builder.name(nameIndex);
        }
        else
        {
            builder.name(nameRO, 0, nameRO.capacity());
        }
        builder.value(valueRO, 0, valueRO.capacity());
    }
}
