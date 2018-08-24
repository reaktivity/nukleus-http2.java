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
package org.reaktivity.nukleus.http2.internal.streams.server.rfc7540;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ConnectionManagementIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/http2/control/route")
            .addScriptRoot("spec", "org/reaktivity/specification/http2/rfc7540/connection.management")
            .addScriptRoot("nukleus", "org/reaktivity/specification/nukleus/http2/streams/rfc7540/connection.management");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
            .directory("target/nukleus-itests")
            .commandBufferCapacity(1024)
            .responseBufferCapacity(1024)
            .nukleus("http2"::equals)
            .configure("nukleus.http2.server.concurrent.streams", 100)
            .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Ignore("TODO: transport stream->nukleus->http stream")
    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/connection.established/client" })
    public void connectionEstablished() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/http.get.exchange/client",
            "${nukleus}/http.get.exchange/server" })
    public void httpGetExchange() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/http.unknown.authority/client" })
    public void httpUnknownAuthority() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/http.post.exchange/client",
            "${nukleus}/http.post.exchange/server" })
    public void httpPostExchange() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/connection.has.two.streams/client",
            "${nukleus}/connection.has.two.streams/server" })
    public void connectionHasTwoStreams() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/http.push.promise/client",
            "${nukleus}/http.push.promise/server" })
    public void pushResources() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/push.promise.on.different.stream/client",
            "${nukleus}/push.promise.on.different.stream/server" })
    public void pushPromiseOnDifferentStream() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/multiple.data.frames/client",
            "${nukleus}/multiple.data.frames/server" })
    public void multipleDataFrames() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/reset.http2.stream/client",
            "${nukleus}/reset.http2.stream/server" })
    public void resetHttp2Stream() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/client.sent.read.abort.on.open.request/client",
            "${nukleus}/client.sent.read.abort.on.open.request/server"
    })
    public void clientSentReadAbortOnOpenRequest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/client.sent.read.abort.on.closed.request/client",
            "${nukleus}/client.sent.read.abort.on.closed.request/server"
    })
    public void clientSentReadAbortOnClosedRequest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/client.sent.write.abort.on.open.request/client",
            "${nukleus}/client.sent.write.abort.on.open.request/server"
    })
    public void clientSentWriteAbortOnOpenRequest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/client.sent.write.abort.on.closed.request/client",
            "${nukleus}/client.sent.write.abort.on.closed.request/server"
    })
    public void clientSentWriteAbortOnClosedRequest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/client.sent.write.close/client",
            "${nukleus}/client.sent.write.close/server"
    })
    public void clientSentWriteClose() throws Exception
    {
        k3po.finish();
    }

    @Ignore("BEGIN vs RESET read order not yet guaranteed to match write order")
    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/server.sent.read.abort.on.open.request/client",
            "${nukleus}/server.sent.read.abort.on.open.request/server"
    })
    public void serverSentReadAbortOnOpenRequest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/rst.stream.last.frame/client",
            "${nukleus}/rst.stream.last.frame/server"
    })
    public void rstStreamLastFrame() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/server.sent.write.abort.on.open.request/client",
            "${nukleus}/server.sent.write.abort.on.open.request/server"
    })
    public void serverSentWriteAbortOnOpenRequest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/server.sent.write.abort.on.closed.request/client",
            "${nukleus}/server.sent.write.abort.on.closed.request/server"
    })
    public void serverSentWriteAbortOnClosedRequest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/server.sent.write.close/client",
            "${nukleus}/server.sent.write.close/server"
    })
    public void serverSentWriteClose() throws Exception
    {
        k3po.finish();
    }

}
