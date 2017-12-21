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
package org.reaktivity.nukleus.http2.internal.streams.client.rfc7540;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Ignore;

public class MessageFormatIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/http2/control/route")
            .addScriptRoot("spec", "org/reaktivity/specification/http2/rfc7540/message.format.client")
            .addScriptRoot("nukleus", "org/reaktivity/specification/nukleus/http2/streams/rfc7540/message.format.client");

    private final TestRule timeout = new DisableOnDebug(new Timeout(3, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
            .directory("target/nukleus-itests")
            .commandBufferCapacity(1024)
            .responseBufferCapacity(1024)
            .counterValuesBufferCapacity(1024)
            .nukleus("http2"::equals)
            .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/continuation.frames/client",
            "${spec}/continuation.frames/server" })
    public void continuationFrames() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/dynamic.table.requests/client",
            "${spec}/dynamic.table.requests/server" })
    public void dynamicTableRequests() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/max.frame.size/client",
            "${spec}/max.frame.size/server" })
    public void maxFrameSize() throws Exception
    {
        k3po.finish();
    }


    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/max.nukleus.data.frame.size/client",
            "${spec}/max.nukleus.data.frame.size/server" })
    public void maxNukleusDataFrameSize() throws Exception
    {
        k3po.finish();
    }

    @Ignore("Push promise not yet implemented")
    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/stream.id.order/client",
            "${spec}/stream.id.order/server" })
    public void streamIdOrder() throws Exception
    {
        k3po.finish();
    }
}
