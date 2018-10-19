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

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Configuration;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Http2Configuration extends Configuration
{
    public static final String HTTP2_SERVER_CONCURRENT_STREAMS = "nukleus.http2.server.concurrent.streams";
    public static final String HTTP2_ACCESS_CONTROL_ALLOW_ORIGIN = "nukleus.http2.server.access.control.allow.origin";
    public static final String HTTP2_SERVER_HEADER = "nukleus.http2.server.header";

    private static final int HTTP2_SERVER_CONCURRENT_STREAMS_DEFAULT = Integer.MAX_VALUE;
    private static final boolean HTTP2_ACCESS_CONTROL_ALLOW_ORIGIN_DEFALUT = false;
    private static final String HTTP2_SERVER_HEADER_DEFAULT = null;

    private final DirectBuffer serverHeader;

    public Http2Configuration(
        Configuration config)
    {
        super(config);
        String server = getProperty(HTTP2_SERVER_HEADER, HTTP2_SERVER_HEADER_DEFAULT);
        serverHeader = server != null ? new UnsafeBuffer(server.getBytes(UTF_8)) : null;
    }

    public int serverConcurrentStreams()
    {
        return getInteger(HTTP2_SERVER_CONCURRENT_STREAMS, HTTP2_SERVER_CONCURRENT_STREAMS_DEFAULT);
    }

    public boolean accessControlAllowOrigin()
    {
        return getBoolean(HTTP2_ACCESS_CONTROL_ALLOW_ORIGIN, HTTP2_ACCESS_CONTROL_ALLOW_ORIGIN_DEFALUT);
    }

    public DirectBuffer serverHeader()
    {
        return serverHeader;
    }

}
