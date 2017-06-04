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
import org.reaktivity.reaktor.test.NukleusRule;

public class ConnectionManagementIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/http2/control/route")
            .addScriptRoot("streams", "org/reaktivity/specification/nukleus/http2/streams/rfc7540/connection.management");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final NukleusRule nukleus = new NukleusRule("http2")
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024);

    @Rule
    public final TestRule chain = outerRule(nukleus).around(k3po).around(timeout);

    @Ignore("TODO: transport stream->nukleus->http stream")
    @Test
    @Specification({
            "${route}/server/controller",
            "${streams}/connection.established/client" })
    public void connectionEstablished() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${streams}/http.get.exchange/client",
            "${streams}/http.get.exchange/server" })
    public void httpGetExchange() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${streams}/http.post.exchange/client",
            "${streams}/http.post.exchange/server" })
    public void httpPostExchange() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${streams}/connection.has.two.streams/client",
            "${streams}/connection.has.two.streams/server" })
    public void connectionHasTwoStreams() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${streams}/http.push.promise/client",
            "${streams}/http.push.promise/server" })
    public void pushResources() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${streams}/push.promise.on.different.stream/client",
            "${streams}/push.promise.on.different.stream/server" })
    public void pushPromiseOnDifferentStream() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${streams}/multiple.data.frames/client",
            "${streams}/multiple.data.frames/server" })
    public void multipleDataFrames() throws Exception
    {
        k3po.finish();
    }
}
