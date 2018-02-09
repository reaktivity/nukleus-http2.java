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
package org.reaktivity.nukleus.http2.internal.types.stream;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.HEADERS;

public class Http2FrameHeaderFWTest
{

    @Test
    public void encode()
    {
        byte[] bytes = new byte[100];
        MutableDirectBuffer buf = new UnsafeBuffer(bytes);

        Http2FrameHeaderFW frameHeader = new Http2FrameHeaderFW.Builder()
                .wrap(buf, 1, buf.capacity())       // non-zero offset
                .streamId(3)
                .type(HEADERS)
                .payloadLength(34567)
                .build();

        assertEquals(34567, frameHeader.payloadLength());
        assertEquals(1, frameHeader.offset());
        assertEquals(1 + 9, frameHeader.limit());
        assertEquals(HEADERS, frameHeader.type());
        assertEquals(0, frameHeader.flags());
        assertEquals(3, frameHeader.streamId());
    }

}
