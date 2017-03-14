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
package org.reaktivity.nukleus.http2.internal.bench;

import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.http2.internal.Http2Controller;
import org.reaktivity.nukleus.tcp.internal.TcpController;
import org.reaktivity.reaktor.Reaktor;
import org.reaktivity.reaktor.matchers.ControllerMatcher;
import org.reaktivity.reaktor.matchers.NukleusMatcher;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.net.InetAddress.getByName;
import static org.reaktivity.nukleus.Configuration.DIRECTORY_PROPERTY_NAME;
import static org.reaktivity.nukleus.Configuration.STREAMS_BUFFER_CAPACITY_PROPERTY_NAME;

public class Http2Server
{

    public static void main(String... args) throws Exception
    {
        Properties properties = new Properties();
        properties.setProperty(DIRECTORY_PROPERTY_NAME, "target/nukleus-benchmarks");
        properties.setProperty(STREAMS_BUFFER_CAPACITY_PROPERTY_NAME, Long.toString(1024L * 1024L * 16L));

        final NukleusMatcher matchNukleus = n -> "tcp".equals(n) || "http2".equals(n);
        final Configuration configuration = new Configuration(properties);

        ControllerMatcher matchController =
                c -> Http2Controller.class.isAssignableFrom(c) || TcpController.class.isAssignableFrom(c);
        Reaktor reaktor = Reaktor.builder()
                                 .config(configuration)
                                 .discover(matchNukleus)
                                 .discover(matchController)
                                 .errorHandler(ex -> ex.printStackTrace(System.err))
                                 .build();

        TcpController tcpController = reaktor.controller(TcpController.class);
        Http2Controller http2Controller = reaktor.controller(Http2Controller.class);
        reaktor.start();
        Map<String, String> headers = new HashMap<>();
        headers.put(":authority", "localhost:8080");
        long http2In = http2Controller.routeInputNew("tcp", 0L, "http2", 0L, headers)
                                      .get();
        tcpController.routeInputNew("any", 8080, "http2", http2In, getByName("127.0.0.1"))
                     .get();

        Thread.sleep(10000000);
    }

}
