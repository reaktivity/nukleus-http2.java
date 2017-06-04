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
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.OctetsFW;
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http2.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http2.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackContext;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataExFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http2.internal.util.function.IntObjectBiConsumer;

import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.IntUnaryOperator;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;

public final class TargetOutputEstablishedStreamFactory
{
    private final FrameFW frameRO = new FrameFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();

    private final HttpBeginExFW beginExRO = new HttpBeginExFW();
    private final Http2DataExFW dataExRO = new Http2DataExFW();

    private final DirectBuffer nameRO = new UnsafeBuffer(new byte[0]);
    private final DirectBuffer valueRO = new UnsafeBuffer(new byte[0]);

    private final Source source;
    private final LongFunction<Correlation> correlateEstablished;

    public TargetOutputEstablishedStreamFactory(
        Source source,
        Function<String, Target> supplyTarget,
        LongSupplier supplyStreamId,
        LongFunction<Correlation> correlateEstablished)
    {
        this.source = source;
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

        private long sourceOutputEstId;
        private int http2StreamId;

        private int window;

        private HpackContext encodeContext;
        private IntObjectBiConsumer<ListFW<HttpHeaderFW>> pushHandler;
        private IntSupplier promisedStreamIds;
        private IntUnaryOperator pushStreamIds;
        private WriteScheduler writeScheduler;

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
            final long sourceRef = beginRO.sourceRef();
            final long targetCorrelationId = beginRO.correlationId();
            final OctetsFW extension = beginRO.extension();

            final Correlation correlation = correlateEstablished.apply(targetCorrelationId);

            if (sourceRef == 0L && correlation != null)
            {
                sourceOutputEstId = correlation.getSourceOutputEstId();
                promisedStreamIds = correlation.promisedStreamIds();
                pushStreamIds = correlation.pushStreamIds();


                this.sourceId = newSourceId;
                this.http2StreamId = correlation.http2StreamId();
                this.writeScheduler = correlation.writeScheduler();
                this.pushHandler = correlation.pushHandler();
                this.encodeContext = correlation.encodeContext();

                if (extension.sizeof() > 0)
                {
                    HttpBeginExFW beginEx = extension.get(beginExRO::wrap);
                    writeScheduler.headers(http2StreamId, beginEx.headers());
                }

                this.streamState = this::afterBeginOrData;
                this.window = 8192;
                source.doWindow(sourceId, window);
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

                int pushStreamId = pushStreamIds.applyAsInt(http2StreamId);
                if (pushStreamId != -1)
                {
                    int promisedStreamId = promisedStreamIds.getAsInt();
                    Http2DataExFW dataEx = extension.get(dataExRO::wrap);
                    writeScheduler.pushPromise(pushStreamId, promisedStreamId, dataEx.headers(), this::sendWindow);
                    pushHandler.accept(promisedStreamId, dataEx.headers());
                }
            }
            if (payload.sizeof() > 0)
            {
                writeScheduler.data(http2StreamId, payload.buffer(), payload.offset(), payload.sizeof(), this::sendWindow);
            }
        }

        private void processEnd(
            DirectBuffer buffer,
            int index,
            int length)
        {
            endRO.wrap(buffer, index, index + length);
            writeScheduler.dataEos(http2StreamId);
            source.removeStream(sourceId);
        }

        private void sendWindow(int update)
        {
            window += update;
            source.doWindow(sourceId, update);
        }

    }

}
