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
package org.reaktivity.nukleus.http2.internal;

import org.reaktivity.nukleus.Configuration;

public class Http2Configuration extends Configuration
{
    private static final String HTTP2_SERVER_CONCURRENT_STREAMS = "nukleus.http2.server.concurrent.streams";
    private static final int HTTP2_SERVER_CONCURRENT_STREAMS_DEFAULT = 100;

    private static final String HTTP2_ACCESS_CONTROL_ALLOW_ORIGIN = "nukleus.http2.server.access.control.allow.origin";
    private static final boolean HTTP2_ACCESS_CONTROL_ALLOW_ORIGIN_DEFALUT = false;

    Http2Configuration(Configuration config)
    {
        super(config);
    }

    int serverConcurrentStreams()
    {
        return getInteger(HTTP2_SERVER_CONCURRENT_STREAMS, HTTP2_SERVER_CONCURRENT_STREAMS_DEFAULT);
    }

    boolean accessControlAllowOrigin()
    {
        return getBoolean(HTTP2_ACCESS_CONTROL_ALLOW_ORIGIN, HTTP2_ACCESS_CONTROL_ALLOW_ORIGIN_DEFALUT);
    }

}
