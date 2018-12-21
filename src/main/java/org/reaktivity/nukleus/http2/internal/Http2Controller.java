/**
 * Copyright 2016-2018 The Reaktivity Project
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

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.nativeOrder;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.ControllerSpi;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http2.internal.types.OctetsFW;
import org.reaktivity.nukleus.http2.internal.types.control.FreezeFW;
import org.reaktivity.nukleus.http2.internal.types.control.HttpRouteExFW;
import org.reaktivity.nukleus.http2.internal.types.control.Role;
import org.reaktivity.nukleus.http2.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http2.internal.types.control.UnrouteFW;

public final class Http2Controller implements Controller
{
    private static final int MAX_SEND_LENGTH = 1024; // TODO: Configuration and Context

    // TODO: thread-safe flyweights or command queue from public methods
    private final RouteFW.Builder routeRW = new RouteFW.Builder();
    private final UnrouteFW.Builder unrouteRW = new UnrouteFW.Builder();
    private final FreezeFW.Builder freezeRW = new FreezeFW.Builder();

    private final HttpRouteExFW.Builder routeExRW = new HttpRouteExFW.Builder();

    private final ControllerSpi controllerSpi;
    private final MutableDirectBuffer writeBuffer;

    public Http2Controller(ControllerSpi controllerSpi)
    {
        this.controllerSpi = controllerSpi;
        this.writeBuffer = new UnsafeBuffer(allocateDirect(MAX_SEND_LENGTH).order(nativeOrder()));
    }

    @Override
    public int process()
    {
        return controllerSpi.doProcess();
    }

    @Override
    public void close() throws Exception
    {
        controllerSpi.doClose();
    }

    @Override
    public Class<Http2Controller> kind()
    {
        return Http2Controller.class;
    }

    @Override
    public String name()
    {
        return "http2";
    }

    public <T> T supplyTarget(
            String target,
            BiFunction<ToIntFunction<MessageConsumer>, MessagePredicate, T> factory)
    {
        return controllerSpi.doSupplyTarget(target, factory);
    }

    public CompletableFuture<Long> routeServer(
        String localAddress,
        String remoteAddress,
        Map<String, String> headers)
    {
        long correlationId = controllerSpi.nextCorrelationId();

        RouteFW route = routeRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                               .correlationId(correlationId)
                               .nukleus(name())
                               .role(b -> b.set(Role.SERVER))
                               .localAddress(localAddress)
                               .remoteAddress(remoteAddress)
                               .extension(extension(headers))
                               .build();

        return controllerSpi.doRoute(route.typeId(), route.buffer(), route.offset(), route.sizeof());
    }

    public CompletableFuture<Long> routeClient(
        String localAddress,
        String remoteAddress,
        Map<String, String> headers)
    {
        long correlationId = controllerSpi.nextCorrelationId();

        RouteFW route = routeRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                               .correlationId(correlationId)
                               .nukleus(name())
                               .role(b -> b.set(Role.CLIENT))
                               .localAddress(localAddress)
                               .remoteAddress(remoteAddress)
                               .extension(extension(headers))
                               .build();

        return controllerSpi.doRoute(route.typeId(), route.buffer(), route.offset(), route.sizeof());
    }

    public CompletableFuture<Void> unroute(
        long routeId)
    {
        long correlationId = controllerSpi.nextCorrelationId();

        UnrouteFW unroute = unrouteRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .correlationId(correlationId)
                                     .nukleus(name())
                                     .routeId(routeId)
                                     .build();

        return controllerSpi.doUnroute(unroute.typeId(), unroute.buffer(), unroute.offset(), unroute.sizeof());
    }

    public CompletableFuture<Void> freeze()
    {
        long correlationId = controllerSpi.nextCorrelationId();

        FreezeFW freeze = freezeRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                  .correlationId(correlationId)
                                  .nukleus(name())
                                  .build();

        return controllerSpi.doFreeze(freeze.typeId(), freeze.buffer(), freeze.offset(), freeze.sizeof());
    }

    private Consumer<OctetsFW.Builder> extension(
        Map<String, String> headers)
    {
        if (headers != null)
        {
            return e -> e.set((buffer, offset, limit) ->
                    routeExRW.wrap(buffer, offset, limit)
                             .headers(hs ->
                             {
                                 headers.forEach((k, v) ->
                                 {
                                     hs.item(h -> h.name(k).value(v));
                                 });
                             })
                             .build()
                             .sizeof());
        }
        else
        {
            return e -> e.reset();
        }
    }

}
