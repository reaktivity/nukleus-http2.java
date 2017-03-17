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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.SETTINGS;

public class Http2SettingsFWTest
{

    @Test
    public void decode()
    {
        byte[] bytes = new byte[] {
                0x7f, 0x7f,
                // SETTINGS frame begin
                0x00, 0x00, 0x06, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, (byte) 0xff, (byte) 0xff,
                // SETTINGS frame end
                0x7f, 0x7f
        };

        DirectBuffer buffer = new UnsafeBuffer(bytes);
        Http2SettingsFW fw = new Http2SettingsFW().wrap(buffer, 2, buffer.capacity());
        assertEquals(17, fw.limit());
        assertEquals(65535L, fw.initialWindowSize());
    }

    @Test
    public void encode()
    {
        byte[] bytes = new byte[100];
        MutableDirectBuffer buf = new UnsafeBuffer(bytes);

        Http2SettingsFW fw = new Http2SettingsFW.Builder()
                .wrap(buf, 1, buf.capacity())
                .initialWindowSize(65535L)
                .maxHeaderListSize(4096L)
                .build();

        assertEquals(12, fw.payloadLength());
        assertEquals(1, fw.offset());
        assertEquals(22, fw.limit());
        assertEquals(SETTINGS, fw.type());
        assertEquals(0, fw.flags());
        assertEquals(0, fw.streamId());
        assertEquals(65535L, fw.initialWindowSize());
        assertEquals(4096L, fw.maxHeaderListSize());
    }

}
