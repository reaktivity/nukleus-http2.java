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
package org.reaktivity.nukleus.http2.internal.bench;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.http2.internal.Http2Configuration;
import org.reaktivity.nukleus.http2.internal.Http2Controller;
import org.reaktivity.nukleus.tcp.internal.TcpController;
import org.reaktivity.reaktor.Reaktor;
import org.reaktivity.reaktor.internal.ReaktorConfiguration;

public class Http2Server
{

    public static void main(String... args) throws Exception
    {
        Properties properties = new Properties();
        properties.setProperty(ReaktorConfiguration.DIRECTORY_PROPERTY_NAME, "target/nukleus-benchmarks");
        properties.setProperty(ReaktorConfiguration.STREAMS_BUFFER_CAPACITY_PROPERTY_NAME, Long.toString(1024L * 1024L * 128L));
        properties.setProperty(ReaktorConfiguration.BUFFER_POOL_CAPACITY_PROPERTY, Long.toString(1024L * 1024L * 128L));
        properties.setProperty(ReaktorConfiguration.BUFFER_SLOT_CAPACITY_PROPERTY, Integer.toString(32768));
        properties.setProperty(Http2Configuration.HTTP2_SERVER_CONCURRENT_STREAMS, Integer.toString(50));

        Configuration configuration = new Configuration(properties);
        Reaktor reaktor = Reaktor.builder()
                                 .config(configuration)
                                 .nukleus(n -> "tcp".equals(n) || "http2".equals(n))
                                 .controller(c -> "tcp".equals(c) || "http2".equals(c))
                                 .errorHandler(Throwable::printStackTrace)
                                 .build();

        TcpController tcpController = reaktor.controller(TcpController.class);
        Http2Controller http2Controller = reaktor.controller(Http2Controller.class);
        reaktor.start();
        Map<String, String> headers = new HashMap<>();
        headers.put(":authority", "127.0.0.1:8080");
        long http2In = http2Controller.routeServer("tcp", 0L, "http2", 0L, headers)
                                      .get();
        tcpController.routeServer("127.0.0.1", 8080, "http2", http2In)
                     .get();

        Thread.sleep(10000000);
    }

}