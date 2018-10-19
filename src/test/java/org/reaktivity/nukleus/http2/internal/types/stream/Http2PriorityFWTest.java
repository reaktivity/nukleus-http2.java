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
package org.reaktivity.nukleus.http2.internal.types.stream;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.PRIORITY;

public class Http2PriorityFWTest
{

    @Test
    public void encode()
    {
        byte[] bytes = new byte[100];
        MutableDirectBuffer buf = new UnsafeBuffer(bytes);

        Http2PriorityFW priority = new Http2PriorityFW.Builder()
                .wrap(buf, 1, buf.capacity())       // non-zero offset
                .streamId(3)
                .exclusive()
                .parentStream(1)
                .weight(256)
                .build();

        assertEquals(5, priority.payloadLength());
        assertEquals(1, priority.offset());
        assertEquals(15, priority.limit());
        assertEquals(PRIORITY, priority.type());
        assertEquals(3, priority.streamId());
        assertTrue(priority.exclusive());
        assertEquals(1, priority.parentStream());
        assertEquals(256, priority.weight());
    }

}
