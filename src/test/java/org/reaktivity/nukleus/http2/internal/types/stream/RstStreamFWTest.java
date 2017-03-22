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
import static org.reaktivity.nukleus.http2.internal.types.stream.ErrorCode.PROTOCOL_ERROR;
import static org.reaktivity.nukleus.http2.internal.types.stream.FrameType.RST_STREAM;

public class RstStreamFWTest
{

    @Test
    public void encode()
    {
        byte[] bytes = new byte[100];
        MutableDirectBuffer buf = new UnsafeBuffer(bytes);

        RstStreamFW fw = new RstStreamFW.Builder()
                .wrap(buf, 1, buf.capacity())
                .streamId(3)
                .errorCode(PROTOCOL_ERROR)
                .build();

        assertEquals(4, fw.payloadLength());
        assertEquals(1, fw.offset());
        assertEquals(14, fw.limit());
        assertEquals(RST_STREAM, fw.type());
        assertEquals(0, fw.flags());
        assertEquals(3, fw.streamId());
        ErrorCode errorCode = ErrorCode.from(fw.errorCode());
        assertEquals(PROTOCOL_ERROR, errorCode);
    }

}