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

class Http2Configuration extends Configuration
{

    private static final String HTTP2_WINDOW_BYTES = "nukleus.http2.window.bytes";
    private static final String HTTP_WINDOW_BYTES = "nukleus.http2.window.bytes";

    private static final int HTTP2_WINDOW_BYTES_DEFAULT = 8192;
    private static final int HTTP_WINDOW_BYTES_DEFAULT = 8192;

    Http2Configuration(Configuration config)
    {
        super(config);
    }

    int http2Window()
    {
        return getInteger(HTTP2_WINDOW_BYTES, HTTP2_WINDOW_BYTES_DEFAULT);
    }

    int httpWindow()
    {
        return getInteger(HTTP_WINDOW_BYTES, HTTP_WINDOW_BYTES_DEFAULT);
    }

}
