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

import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.DirectBufferBuilder;
import org.reaktivity.nukleus.buffer.MemoryManager;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http2.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.control.HttpRouteExFW;
import org.reaktivity.nukleus.http2.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http2.internal.types.stream.AckFW;
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;
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
import org.reaktivity.nukleus.http2.internal.types.stream.TransferFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class ServerStreamFactory implements StreamFactory
{
    private static final int FIN = 0x01;
    private static final int RST = 0x02;

    final RouteFW routeRO = new RouteFW();

    private final BeginFW beginRO = new BeginFW();
    private final TransferFW writeRO = new TransferFW();
    private final AckFW ackRO = new AckFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final TransferFW.Builder writeRW = new TransferFW.Builder();
    private final AckFW.Builder ackRW = new AckFW.Builder();

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

    final Http2Configuration config;
    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    final LongSupplier supplyStreamId;
    final LongSupplier supplyCorrelationId;
    final HttpWriter httpWriter;
    final Http2Writer http2Writer;

    // Buf to build HTTP error status code header
    final MutableDirectBuffer errorBuf = new UnsafeBuffer(new byte[64]);


    final Long2ObjectHashMap<Correlation> correlations;
    private final MessageFunction<RouteFW> wrapRoute;
    private final MemoryManager memoryManager;
    private final Supplier<DirectBufferBuilder> supplyBufferBuilder;

    ServerStreamFactory(
            Http2Configuration config,
            RouteManager router,
            MutableDirectBuffer writeBuffer,
            LongSupplier supplyStreamId,
            LongSupplier supplyCorrelationId,
            Long2ObjectHashMap<Correlation> correlations,
            MemoryManager memoryManager,
            Supplier<DirectBufferBuilder> supplyBufferBuilder)
    {
        this.config = config;
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);

        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.supplyCorrelationId = requireNonNull(supplyCorrelationId);
        this.correlations = requireNonNull(correlations);
        this.memoryManager = requireNonNull(memoryManager);
        this.supplyBufferBuilder = requireNonNull(supplyBufferBuilder);

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
                doReset(networkThrottle, networkId);
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
                case TransferFW.TYPE_ID:
                    final TransferFW data = writeRO.wrap(buffer, index, index + length);
                    handleData(data);

                    if ((data.flags() & FIN) == FIN)
                    {
                        handleEnd(data);
                    }
                    if ((data.flags() & RST) == RST)
                    {
                        handleAbort(data);
                    }
                    break;
                default:
                    doReset(networkThrottle, networkId);
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

            doBegin(networkReply, networkReplyId, 0L, networkCorrelationId);
            router.setThrottle(networkReplyName, networkReplyId, this::handleThrottle);

            this.streamState = this::afterBegin;
            http2Connection = new Http2Connection(ServerStreamFactory.this, router, networkReplyId,
                    networkReply, wrapRoute);
            http2Connection.handleBegin(begin);
        }

        private void handleData(
            TransferFW data)
        {
            http2Connection.handleData(data);
        }

        private void handleEnd(
            TransferFW end)
        {
            http2Connection.handleEnd(end);
        }

        private void handleAbort(
            TransferFW abort)
        {
            correlations.remove(networkCorrelationId);

            // aborts reply stream
            doAbort(networkReply, networkReplyId);

            // aborts http request stream, resets http response stream
            http2Connection.handleAbort();
        }

        private void handleThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
                case AckFW.TYPE_ID:
                    final AckFW reset = ackRO.wrap(buffer, index, index + length);
                    http2Connection.handleWindow(reset);
                    // TODO
                    handleReset(reset);
                    break;
                default:
                    // ignore
                    break;
            }
        }

        private void handleReset(
            AckFW reset)
        {
            http2Connection.handleReset(reset);

            doReset(networkThrottle, networkId);
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
                doReset(applicationReplyThrottle, applicationReplyId);
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
                case TransferFW.TYPE_ID:
                    final TransferFW data = writeRO.wrap(buffer, index, index + length);
                    handleData(data);

                    if ((data.flags() & FIN) == FIN)
                    {
                        handleEnd(data);
                    }
                    if ((data.flags() & RST) == RST)
                    {
                        handleAbort(data);
                    }
                    break;
                default:
                    doReset(applicationReplyThrottle, applicationReplyId);
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
                doReset(applicationReplyThrottle, applicationReplyId);
            }
        }

        private void handleData(
            TransferFW data)
        {
            http2Connection.handleHttpData(data, correlation);
        }

        private void handleEnd(
            TransferFW end)
        {
            http2Connection.handleHttpEnd(end, correlation);
        }

        private void handleAbort(
            TransferFW abort)
        {
            http2Connection.handleHttpAbort(abort, correlation);
        }

    }

    private void doBegin(
        final MessageConsumer target,
        final long targetId,
        final long targetRef,
        final long correlationId)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .streamId(targetId)
                                     .source("http2")
                                     .sourceRef(targetRef)
                                     .correlationId(correlationId)
                                     .extension(e -> e.reset())
                                     .build();

        target.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    void doData(
        final MessageConsumer target,
        final long targetId,
        final long address,
        final int length)
    {
        final TransferFW abort = writeRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .streamId(targetId)
                                     .regionsItem(b -> b.address(address).length(length))
                                     .extension(e -> e.reset())
                                     .build();

        target.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    void doAbort(
        final MessageConsumer target,
        final long targetId)
    {
        final TransferFW abort = writeRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .streamId(targetId)
                                     .flags(RST)
                                     .extension(e -> e.reset())
                                     .build();

        target.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    void doAck(
        final MessageConsumer throttle,
        final long throttleId)
    {
        final AckFW window = ackRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                  .streamId(throttleId)
                                  .build();

        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    void doEnd(
        MessageConsumer target,
        long targetId)
    {
        TransferFW end = writeRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                             .streamId(targetId)
                             .flags(FIN)
                             .extension(e -> e.reset())
                             .build();

        target.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    void doReset(
        final MessageConsumer throttle,
        final long throttleId)
    {
        final AckFW reset = ackRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                 .streamId(throttleId)
                                 .flags(RST)
                                 .build();

        throttle.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }
}