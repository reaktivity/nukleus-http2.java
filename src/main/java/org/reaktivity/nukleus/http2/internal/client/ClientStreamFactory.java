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
package org.reaktivity.nukleus.http2.internal.client;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http2.internal.Http2Configuration;
import org.reaktivity.nukleus.http2.internal.Http2NukleusFactorySpi;
import org.reaktivity.nukleus.http2.internal.types.OctetsFW;
import org.reaktivity.nukleus.http2.internal.types.control.HttpRouteExFW;
import org.reaktivity.nukleus.http2.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http2.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http2.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderBlockFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ContinuationFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2HeadersFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http2.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http2.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

import java.util.function.LongSupplier;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class ClientStreamFactory implements StreamFactory
{
    static final DirectBuffer SOURCE_NAME_BUFFER = new UnsafeBuffer(Http2NukleusFactorySpi.NAME.getBytes(UTF_8));

    // read only
    private final RouteFW routeRO = new RouteFW();
    final BeginFW beginRO = new BeginFW();
    final DataFW dataRO = new DataFW();
    final EndFW endRO = new EndFW();
    final AbortFW abortRO = new AbortFW();
    final WindowFW windowRO = new WindowFW();
    final ResetFW resetRO = new ResetFW();
    final HttpRouteExFW httpRouteExRO = new HttpRouteExFW();
    final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();

    // http2 frames
    final Http2FrameFW http2RO = new Http2FrameFW();
    final Http2HeadersFW headersRO = new Http2HeadersFW();
    final Http2ContinuationFW continuationRO = new Http2ContinuationFW();
    final HpackHeaderBlockFW blockRO = new HpackHeaderBlockFW();
    final Http2SettingsFW settingsRO = new Http2SettingsFW();
    final Http2DataFW http2DataRO = new Http2DataFW();

    final RouteManager router;
    final MutableDirectBuffer writeBuffer;
    final BufferPool bufferPool;
    final LongSupplier supplyStreamId;
    final LongSupplier supplyCorrelationId;
    final Long2ObjectHashMap<ClientCorrelation> correlations;
    private final Http2ClientConnectionManager http2ConnectionManager;

    // builders
    final BeginFW.Builder beginRW = new BeginFW.Builder();
    final AbortFW.Builder abortRW = new AbortFW.Builder();
    final WindowFW.Builder windowRW = new WindowFW.Builder();
    final ResetFW.Builder resetRW = new ResetFW.Builder();
    final EndFW.Builder endRW = new EndFW.Builder();
    final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();
    final DataFW.Builder dataRW = new DataFW.Builder();

    final DirectBuffer nameRO = new UnsafeBuffer(new byte[0]);
    final DirectBuffer valueRO = new UnsafeBuffer(new byte[0]);
    final UnsafeBuffer scratch = new UnsafeBuffer(new byte[8192]);  // TODO

    public ClientStreamFactory(Http2Configuration config, RouteManager router, MutableDirectBuffer writeBuffer,
            BufferPool bufferPool, LongSupplier supplyStreamId, LongSupplier supplyCorrelationId,
            Long2ObjectHashMap<ClientCorrelation> correlations)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.bufferPool = requireNonNull(bufferPool);

        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.supplyCorrelationId = requireNonNull(supplyCorrelationId);
        this.correlations = requireNonNull(correlations);
        this.http2ConnectionManager = new Http2ClientConnectionManager(this);
}

    @Override
    public MessageConsumer newStream(int msgTypeId, DirectBuffer buffer, int index, int length, MessageConsumer throttle)
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
        final MessageConsumer acceptThrottle)
    {
        final long sourceRef = begin.sourceRef();
        final String acceptName = begin.source().asString();

        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, l);
            return sourceRef == route.sourceRef() &&
                    acceptName.equals(route.source().asString());
        };

        final RouteFW route = router.resolve(filter, (t, b, o, l) -> routeRO.wrap(b, o, o + l));

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long streamId = begin.streamId();
            newStream = new ClientAcceptStream(this, acceptThrottle, streamId, http2ConnectionManager)::handleStream;
        }

        return newStream;
    }

    private MessageConsumer newConnectReplyStream(BeginFW begin, MessageConsumer connectReplyThrottle)
    {
        final long connectReplyId = begin.streamId();
//        final String connectReplyName = begin.source().asString();
        return new ClientConnectReplyStream(this, connectReplyThrottle, connectReplyId, //, connectReplyName
                http2ConnectionManager)::handleStream;
    }

    // methods for sending frames on a stream
    void doBegin(
        final MessageConsumer target,
        final long targetId,
        final long targetRef,
        final long correlationId)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .source(SOURCE_NAME_BUFFER, 0, SOURCE_NAME_BUFFER.capacity())
                .sourceRef(targetRef)
                .correlationId(correlationId)
                .extension(e -> e.reset())
                .build();

        target.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    void doAbort(
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
        final int writableBytes,
        final int writableFrames)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleId)
                .update(writableBytes)
                .frames(writableFrames)
                .build();

        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    void doReset(
            final MessageConsumer throttle,
            final long throttleId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(throttleId)
                .build();

        throttle.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    void doEnd(
            final MessageConsumer target,
            final long targetId)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .build();

        target.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    RouteFW resolveTarget(
        long sourceRef,
        String sourceName,
        Map<String, String> headers)
    {
        MessagePredicate filter = (t, b, o, l) ->
        {
            RouteFW route = routeRO.wrap(b, o, l);
            OctetsFW extension = route.extension();
            if (extension.sizeof() > 0)
            {
                final HttpRouteExFW routeEx = extension.get(httpRouteExRO::wrap);
                return route.sourceRef() == sourceRef &&
                        routeEx.headers().anyMatch(h -> !Objects.equals(h.value(), headers.get(h.name())));
            }
            return route.sourceRef() == sourceRef;
        };

        return router.resolve(filter,
                    (msgTypeId, buffer, index, length) -> routeRO.wrap(buffer, index, index + length));
    }
}
