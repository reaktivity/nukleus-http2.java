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
package org.reaktivity.nukleus.http2.internal.control;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.http2.internal.Http2Controller;
import org.reaktivity.reaktor.test.ControllerRule;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

public class ControllerIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/http2/control/route")
        .addScriptRoot("unroute", "org/reaktivity/specification/nukleus/http2/control/unroute");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ControllerRule controller = new ControllerRule(Http2Controller.class)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024);

    @Rule
    public final TestRule chain = outerRule(k3po).around(timeout).around(controller);

    @Test
    @Specification({
        "${route}/input/new/nukleus"
    })
    public void shouldRouteInputNew() throws Exception
    {
        long targetRef = new Random().nextLong();
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(":authority", "localhost:8080");

        k3po.start();

        controller.controller(Http2Controller.class)
                  .routeInputNew("source", 0L, "target", targetRef, headers)
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/nukleus"
    })
    public void shouldRouteOutputNew() throws Exception
    {
        long targetRef = new Random().nextLong();
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(":authority", "localhost:8080");

        k3po.start();

        controller.controller(Http2Controller.class)
                  .routeOutputNew("source", 0L, "target", targetRef, headers)
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/established/nukleus"
    })
    public void shouldRouteOutputEstablished() throws Exception
    {
        k3po.start();

        controller.controller(Http2Controller.class)
                  .routeOutputEstablished("target", 0L, "source", 0L, null)
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/input/established/nukleus"
    })
    public void shouldRouteInputEstablished() throws Exception
    {
        k3po.start();

        controller.controller(Http2Controller.class)
                  .routeInputEstablished("target", 0L, "source", 0L, null)
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/input/new/nukleus",
        "${unroute}/input/new/nukleus"
    })
    public void shouldUnrouteInputNew() throws Exception
    {
        long targetRef = new Random().nextLong();
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(":authority", "localhost:8080");

        k3po.start();

        long sourceRef = controller.controller(Http2Controller.class)
                  .routeInputNew("source", 0L, "target", targetRef, headers)
                  .get();

        k3po.notifyBarrier("ROUTED_INPUT");

        controller.controller(Http2Controller.class)
                  .unrouteInputNew("source", sourceRef, "target", targetRef, headers)
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/nukleus",
        "${unroute}/output/new/nukleus"
    })
    public void shouldUnrouteOutputNew() throws Exception
    {
        long targetRef = new Random().nextLong();
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(":authority", "localhost:8080");

        k3po.start();

        long sourceRef = controller.controller(Http2Controller.class)
                  .routeOutputNew("source", 0L, "target", targetRef, headers)
                  .get();

        k3po.notifyBarrier("ROUTED_OUTPUT");

        controller.controller(Http2Controller.class)
                  .unrouteOutputNew("source", sourceRef, "target", targetRef, headers)
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/established/nukleus",
        "${unroute}/output/established/nukleus"
    })
    public void shouldUnrouteOutputEstablished() throws Exception
    {
        k3po.start();

        long targetRef = controller.controller(Http2Controller.class)
                  .routeOutputEstablished("target", 0L, "source", 0L, null)
                  .get();

        k3po.notifyBarrier("ROUTED_OUTPUT");

        controller.controller(Http2Controller.class)
                  .unrouteOutputEstablished("target", targetRef, "source", 0L, null)
                  .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/input/established/nukleus",
        "${unroute}/input/established/nukleus"
    })
    public void shouldUnrouteInputEstablished() throws Exception
    {
        k3po.start();

        long targetRef  = controller.controller(Http2Controller.class)
                  .routeInputEstablished("target", 0L, "source", 0L, null)
                  .get();

        k3po.notifyBarrier("ROUTED_INPUT");

        controller.controller(Http2Controller.class)
                  .unrouteInputEstablished("target", targetRef, "source", 0L, null)
                  .get();

        k3po.finish();
    }
}
