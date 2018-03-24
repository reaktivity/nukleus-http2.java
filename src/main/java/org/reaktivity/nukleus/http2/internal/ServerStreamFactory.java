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

import static java.util.Objects.requireNonNull;

import java.util.function.IntUnaryOperator;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http2.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.control.HttpRouteExFW;
import org.reaktivity.nukleus.http2.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http2.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http2.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderBlockFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ContinuationFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataExFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2HeadersFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PingFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PrefaceFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PriorityFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2WindowUpdateFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http2.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http2.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class ServerStreamFactory implements StreamFactory
{

    private static final double OUTWINDOW_LOW_THRESHOLD = 0.5;      // TODO configuration
    private static final double INWINDOW_THRESHOLD = 0.5;

    final RouteFW routeRO = new RouteFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    final AbortFW abortRO = new AbortFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();

    final WindowFW windowRO = new WindowFW();
    final ResetFW resetRO = new ResetFW();

    final HttpRouteExFW httpRouteExRO = new HttpRouteExFW();
    final Http2PrefaceFW prefaceRO = new Http2PrefaceFW();
    final Http2FrameFW http2RO = new Http2FrameFW();
    final Http2SettingsFW settingsRO = new Http2SettingsFW();
    final Http2DataFW http2DataRO = new Http2DataFW();
    final Http2HeadersFW headersRO = new Http2HeadersFW();
    final Http2ContinuationFW continationRO = new Http2ContinuationFW();
    final HpackHeaderBlockFW blockRO = new HpackHeaderBlockFW();
    final Http2WindowUpdateFW http2WindowRO = new Http2WindowUpdateFW();
    final Http2PriorityFW priorityRO = new Http2PriorityFW();
    final UnsafeBuffer scratch = new UnsafeBuffer(new byte[8192]);  // TODO
    final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();
    final ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW> headersRW =
            new ListFW.Builder<>(new HttpHeaderFW.Builder(), new HttpHeaderFW());
    final DirectBuffer nameRO = new UnsafeBuffer(new byte[0]);
    final DirectBuffer valueRO = new UnsafeBuffer(new byte[0]);
    final HttpBeginExFW beginExRO = new HttpBeginExFW();
    final Http2DataExFW dataExRO = new Http2DataExFW();
    final HpackHeaderBlockFW.Builder blockRW = new HpackHeaderBlockFW.Builder();

    final Http2PingFW pingRO = new Http2PingFW();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    final Http2Configuration config;
    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    final BufferPool bufferPool;
    final BufferPool framePool;
    final BufferPool headersPool;
    final BufferPool httpWriterPool;
    final BufferPool http2ReplyPool;
    final LongSupplier supplyStreamId;
    final LongSupplier supplyTrace;
    final LongSupplier supplyCorrelationId;
    final HttpWriter httpWriter;
    final Http2Writer http2Writer;

    // Buf to build HTTP error status code header
    final MutableDirectBuffer errorBuf = new UnsafeBuffer(new byte[64]);


    final Long2ObjectHashMap<Correlation> correlations;
    private final MessageFunction<RouteFW> wrapRoute;
    final LongSupplier supplyGroupId;
    final LongFunction<IntUnaryOperator> groupBudgetClaimer;
    final LongFunction<IntUnaryOperator> groupBudgetReleaser;

    ServerStreamFactory(
            Http2Configuration config,
            RouteManager router,
            MutableDirectBuffer writeBuffer,
            BufferPool bufferPool,
            LongSupplier supplyStreamId,
            LongSupplier supplyCorrelationId,
            Long2ObjectHashMap<Correlation> correlations,
            LongSupplier supplyGroupId,
            LongSupplier supplyTrace,
            LongFunction<IntUnaryOperator> groupBudgetClaimer,
            LongFunction<IntUnaryOperator> groupBudgetReleaser)
    {
        this.config = config;
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.bufferPool = requireNonNull(bufferPool);
        this.framePool = bufferPool.duplicate();
        this.headersPool = bufferPool.duplicate();
        this.httpWriterPool = bufferPool.duplicate();
        this.http2ReplyPool = bufferPool.duplicate();
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.supplyCorrelationId = requireNonNull(supplyCorrelationId);
        this.correlations = requireNonNull(correlations);
        this.supplyGroupId = requireNonNull(supplyGroupId);
        this.supplyTrace = requireNonNull(supplyTrace);
        this.groupBudgetClaimer = requireNonNull(groupBudgetClaimer);
        this.groupBudgetReleaser = requireNonNull(groupBudgetReleaser);

        this.httpWriter = new HttpWriter(writeBuffer);
        this.http2Writer = new Http2Writer(writeBuffer);

        this.wrapRoute = this::wrapRoute;
    }

    @Override
    public MessageConsumer newStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length,
            MessageConsumer throttle)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long sourceRef = begin.sourceRef();

        MessageConsumer newStream;

        if (sourceRef == 0L)
        {
            newStream = newConnectReplyStream(begin, throttle);
        }
        else
        {
            newStream = newAcceptStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newAcceptStream(
            final BeginFW begin,
            final MessageConsumer networkThrottle)
    {
        final long networkRef = begin.sourceRef();
        final String acceptName = begin.source().asString();

        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, l);
            return networkRef == route.sourceRef() &&
                    acceptName.equals(route.source().asString());
        };

        final RouteFW route = router.resolve(begin.authorization(), filter, this::wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long networkId = begin.streamId();

            newStream = new ServerAcceptStream(networkThrottle, networkId)::handleStream;
        }

        return newStream;
    }

    private MessageConsumer newConnectReplyStream(
            final BeginFW begin,
            final MessageConsumer throttle)
    {
        final long throttleId = begin.streamId();

        return new ServerConnectReplyStream(throttle, throttleId)::handleStream;
    }

    private RouteFW wrapRoute(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }

    private final class ServerAcceptStream
    {
        private final MessageConsumer networkThrottle;
        private final long networkId;

        private long networkCorrelationId;
        private MessageConsumer networkReply;
        private long networkReplyId;

        private MessageConsumer streamState;
        private Http2Connection http2Connection;
        private int initialWindow = bufferPool.slotCapacity();
        private int window;

        private ServerAcceptStream(
                MessageConsumer networkThrottle,
                long networkId)
        {
            this.networkThrottle = networkThrottle;
            this.networkId = networkId;
            this.streamState = this::beforeBegin;
        }

        private void handleStream(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            streamState.accept(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
            }
            else
            {
                doReset(networkThrottle, networkId, 0);
            }
        }

        private void afterBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case DataFW.TYPE_ID:
                    final DataFW data = dataRO.wrap(buffer, index, index + length);
                    handleData(data);
                    break;
                case EndFW.TYPE_ID:
                    final EndFW end = endRO.wrap(buffer, index, index + length);
                    handleEnd(end);
                    break;
                case AbortFW.TYPE_ID:
                    final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                    handleAbort(abort);
                    break;
                default:
                    doReset(networkThrottle, networkId, 0);
                    break;
            }
        }

        private void handleBegin(
                BeginFW begin)
        {
            final String networkReplyName = begin.source().asString();
            networkCorrelationId = begin.correlationId();

            networkReply = router.supplyTarget(networkReplyName);
            networkReplyId = supplyStreamId.getAsLong();

            initialWindow = bufferPool.slotCapacity();
            doWindow(networkThrottle, networkId, initialWindow, 0, 0);
            window = initialWindow;

            doBegin(networkReply, networkReplyId, supplyTrace.getAsLong(), 0L, networkCorrelationId);
            router.setThrottle(networkReplyName, networkReplyId, this::handleThrottle);

            this.streamState = this::afterBegin;
            http2Connection = new Http2Connection(ServerStreamFactory.this, router, networkReplyId,
                    networkReply, wrapRoute);
            http2Connection.handleBegin(begin);
        }

        private void handleData(
                DataFW data)
        {
            window -= dataRO.length() + dataRO.padding();
            if (window < 0)
            {
                doReset(networkThrottle, networkId, 0);
                //http2Connection.handleReset();
            }
            else
            {
                if (window < initialWindow * INWINDOW_THRESHOLD)
                {
                    int windowPending = initialWindow - window;
                    window = initialWindow;
                    doWindow(networkThrottle, networkId, windowPending, 0, 0);
                }

                http2Connection.handleData(data);
            }
        }

        private void handleEnd(
                EndFW end)
        {
            http2Connection.handleEnd(end);
        }

        private void handleAbort(
                AbortFW abort)
        {
            correlations.remove(networkCorrelationId);

            // aborts reply stream
            doAbort(networkReply, networkReplyId);

            // aborts http request stream, resets http response stream
            http2Connection.handleAbort(abort.trace());
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
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    handleWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    handleReset(reset);
                    break;
                default:
                    // ignore
                    break;
            }
        }

        private void handleWindow(
                WindowFW window)
        {
            int credit = windowRO.credit();
            int padding = windowRO.padding();
            if (http2Connection.outWindowThreshold == -1)
            {
                http2Connection.outWindowThreshold = (int) (OUTWINDOW_LOW_THRESHOLD * credit);
            }
            http2Connection.networkReplyBudget += credit;
            http2Connection.networkReplyPadding = padding;
            http2Connection.handleWindow(window);
        }

        private void handleReset(
                ResetFW reset)
        {
            http2Connection.handleReset(reset);

            doReset(networkThrottle, networkId, 0);
        }
    }


    private final class ServerConnectReplyStream
    {
        private final MessageConsumer applicationReplyThrottle;
        private final long applicationReplyId;

        private MessageConsumer streamState;

        private Http2Connection http2Connection;
        private Correlation correlation;

        private ServerConnectReplyStream(
                MessageConsumer applicationReplyThrottle,
                long applicationReplyId)
        {
            this.applicationReplyThrottle = applicationReplyThrottle;
            this.applicationReplyId = applicationReplyId;
            this.streamState = this::beforeBegin;
        }

        private void handleStream(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            streamState.accept(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
            }
            else
            {
                doReset(applicationReplyThrottle, applicationReplyId, 0);
            }
        }

        private void afterBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case DataFW.TYPE_ID:
                    final DataFW data = dataRO.wrap(buffer, index, index + length);
                    handleData(data);
                    break;
                case EndFW.TYPE_ID:
                    final EndFW end = endRO.wrap(buffer, index, index + length);
                    handleEnd(end);
                    break;
                case AbortFW.TYPE_ID:
                    final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                    handleAbort(abort);
                    break;
                default:
                    doReset(applicationReplyThrottle, applicationReplyId, 0);
                    break;
            }
        }

        private void handleBegin(
                BeginFW begin)
        {
            final long sourceRef = begin.sourceRef();
            final long correlationId = begin.correlationId();
            correlation = sourceRef == 0L ? correlations.remove(correlationId) : null;
            if (correlation != null)
            {
                http2Connection = correlation.http2Connection;

                http2Connection.handleHttpBegin(begin, applicationReplyThrottle, applicationReplyId, correlation);

                this.streamState = this::afterBegin;
            }
            else
            {
                doReset(applicationReplyThrottle, applicationReplyId, 0);
            }
        }

        private void handleData(
                DataFW data)
        {
            http2Connection.handleHttpData(data, correlation);
        }

        private void handleEnd(
                EndFW end)
        {
            http2Connection.handleHttpEnd(end, correlation);
        }

        private void handleAbort(
                AbortFW abort)
        {
            http2Connection.handleHttpAbort(abort, correlation);
        }

    }

    private void doBegin(
            final MessageConsumer target,
            final long targetId,
            final long traceId,
            final long targetRef,
            final long correlationId)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .streamId(targetId)
                                     .trace(traceId)
                                     .source("http2")
                                     .sourceRef(targetRef)
                                     .correlationId(correlationId)
                                     .extension(e -> e.reset())
                                     .build();

        target.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doAbort(
            final MessageConsumer target,
            final long targetId)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .streamId(targetId)
                                     .extension(e -> e.reset())
                                     .build();

        target.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    void doWindow(
            final MessageConsumer throttle,
            final long throttleId,
            final int credit,
            final int padding,
            final long groupId)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                        .streamId(throttleId)
                                        .credit(credit)
                                        .padding(padding)
                                        .groupId(groupId)
                                        .build();

        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    void doReset(
            final MessageConsumer throttle,
            final long throttleId,
            final long traceId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .streamId(throttleId)
                                     .trace(traceId)
                                     .build();

        throttle.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }
}