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
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class Http2HeadersFWTest
{

    @Test
    public void decode()
    {
        byte[] bytes = new byte[]
        {
                0x7f, 0x7f,
                // HEADERS frame begin
                0x00, 0x00, 0x0f, 0x01, 0x05, 0x00, 0x00, 0x00, 0x01, (byte) 0x82, (byte) 0x86,
                (byte) 0x84, 0x41, (byte) 0x8a, 0x08, (byte) 0x9d, 0x5c, 0x0b, (byte) 0x81, 0x70,
                (byte) 0xdc, 0x78, 0x0f, 0x03,
                // HEADERS frame end
                0x7f, 0x7f
        };

        DirectBuffer buffer = new UnsafeBuffer(bytes);
        Http2HeadersFW fw = new Http2HeadersFW().wrap(buffer, 2, buffer.capacity());
        assertEquals(26, fw.limit());
        assertTrue(fw.endStream());
        assertTrue(fw.endHeaders());
        assertFalse(fw.priority());
        assertFalse(fw.padded());

        Map<String, String> headers = new LinkedHashMap<>();
        HpackContext hpackContext = new HpackContext();
        fw.forEach(HpackHeaderBlockFWTest.getHeaders(hpackContext, headers));

        assertEquals(4, headers.size());
        assertEquals("GET", headers.get(":method"));
        assertEquals("http", headers.get(":scheme"));
        assertEquals("/", headers.get(":path"));
        assertEquals("127.0.0.1:8080", headers.get(":authority"));
    }

}
