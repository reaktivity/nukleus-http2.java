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
            .counterValuesBufferCapacity(1024)
            .nukleus("http2"::equals)
            .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Ignore("Not valid in client mode. Nukleus will not initiate a connection without a request")
    @Test
    @Specification({
            "${route}/client/controller",
            "${spec}/connection.established/server" })
    public void connectionEstablished() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/http.get.exchange/client",
            "${spec}/http.get.exchange/server" })
    public void httpGetExchange() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/http.post.exchange/client",
            "${spec}/http.post.exchange/server" })
    public void httpPostExchange() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/connection.has.two.streams/client",
            "${spec}/connection.has.two.streams/server" })
    public void connectionHasTwoStreams() throws Exception
    {
        k3po.finish();
    }

    @Ignore("Push promise not implemented in current user story")
    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/http.push.promise/client",
            "${spec}/http.push.promise/server" })
    public void pushResources() throws Exception
    {
        k3po.finish();
    }

    @Ignore("We do not support push promises yet")
    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/push.promise.on.different.stream/client",
            "${spec}/push.promise.on.different.stream/server" })
    public void pushPromiseOnDifferentStream() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/multiple.data.frames/client",
            "${spec}/multiple.data.frames/server" })
    public void multipleDataFrames() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/reset.http2.stream/client",
            "${spec}/reset.http2.stream/server" })
    public void resetHttp2Stream() throws Exception
    {
        k3po.finish();
    }

    @Ignore("Not valid in client mode. If a http connection is aborted, the http2 connection is not aborted")
    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/client.sent.read.abort.on.open.request/client",
            "${spec}/client.sent.read.abort.on.open.request/server" })
    public void clientSentReadAbortOnOpenRequest() throws Exception
    {
        k3po.finish();
    }

    @Ignore("Not valid in client mode. If a http connection is aborted, the http2 connection is not aborted")
    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/client.sent.read.abort.on.closed.request/client",
            "${spec}/client.sent.read.abort.on.closed.request/server" })
    public void clientSentReadAbortOnClosedRequest() throws Exception
    {
        k3po.finish();
    }

    @Ignore("Not valid in client mode. If a http connection is aborted, the http2 connection is not aborted")
    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/client.sent.write.abort.on.open.request/client",
            "${spec}/client.sent.write.abort.on.open.request/server" })
    public void clientSentWriteAbortOnOpenRequest() throws Exception
    {
        k3po.finish();
    }

    @Ignore("Not valid in client mode. If a http connection is aborted, the http2 connection is not aborted")
    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/client.sent.write.abort.on.closed.request/client",
            "${spec}/client.sent.write.abort.on.closed.request/server" })
    public void clientSentWriteAbortOnClosedRequest() throws Exception
    {
        k3po.finish();
    }

    @Ignore("Not valid in client mode. If a http connection is aborted, the http2 connection is not aborted")
    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/client.sent.write.close/client",
            "${spec}/client.sent.write.close/server" })
    public void clientSentWriteClose() throws Exception
    {
        k3po.finish();
    }

    @Ignore("BEGIN vs RESET read order not yet guaranteed to match write order")
    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/server.sent.read.abort.on.open.request/client",
            "${spec}/server.sent.read.abort.on.open.request/server" })
    public void serverSentReadAbortOnOpenRequest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/server.sent.write.abort.on.open.request/client",
            "${spec}/server.sent.write.abort.on.open.request/server" })
    public void serverSentWriteAbortOnOpenRequest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/server.sent.write.abort.on.closed.request/client",
            "${spec}/server.sent.write.abort.on.closed.request/server" })
    public void serverSentWriteAbortOnClosedRequest() throws Exception
    {
        k3po.finish();
    }

    @Ignore("According to https://tools.ietf.org/html/rfc7540#section-8.1.2.6," +
            "data length not equal to content-length must be considered a PROTOCOL_ERROR, so we send an abort, not an end frame")
    @Test
    @Specification({
            "${route}/client/controller",
            "${nukleus}/server.sent.write.close/client",
            "${spec}/server.sent.write.close/server" })
    public void serverSentWriteClose() throws Exception
    {
        k3po.finish();
    }
}
