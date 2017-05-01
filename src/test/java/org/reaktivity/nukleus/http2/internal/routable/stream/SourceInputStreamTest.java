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
package org.reaktivity.nukleus.http2.internal.routable.stream;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SourceInputStreamTest
{

    @Test
    public void fragmentedReads()
    {
        SourceInputStreamFactory factory = new SourceInputStreamFactory(null, null, () -> 0, null, null);
        SourceInputStreamFactory.SourceInputStream ss = factory.new SourceInputStream();

        // Creating two frames (one after another) in a buffer
        int firsFrameLength = 1024;
        int secondFrameLength = 4096;
        int totalLength = firsFrameLength + secondFrameLength;
        MutableDirectBuffer buf = new UnsafeBuffer(new byte[firsFrameLength + secondFrameLength]);
        new Http2DataFW.Builder().wrap(buf, 0, buf.capacity())
                                 .endStream()
                                 .streamId(1)
                                 .payload(new UnsafeBuffer(new byte[firsFrameLength - 9]))
                                 .build();
        new Http2DataFW.Builder().wrap(buf, firsFrameLength, buf.capacity())
                                 .streamId(3)
                                 .payload(new UnsafeBuffer(new byte[secondFrameLength - 9]))
                                 .build();

        // Parse the HTTP2 frames for different fragment lengths
        for (int fragLength = 1; fragLength <= buf.capacity(); fragLength++)
        {
            int fragOffset = 0;
            while (fragOffset < buf.capacity())
            {
                int limit = fragOffset + fragLength > totalLength ? totalLength : fragOffset + fragLength;
                int consumed = ss.http2FrameAvailable(buf, fragOffset, limit);

                fragOffset += consumed;
                if (fragOffset == firsFrameLength)
                {
                    if (fragLength > firsFrameLength)
                    {
                        assertEquals(firsFrameLength, consumed);
                    }
                    else
                    {
                        int last = firsFrameLength%fragLength == 0 ? fragLength : firsFrameLength%fragLength;
                        assertEquals(last, consumed);
                    }
                    assertTrue(ss.http2FrameAvailable);
                    assertEquals(1, factory.http2RO.streamId());
                    assertTrue(factory.http2RO.endStream());
                }
                else if (fragOffset == totalLength)
                {
                    assertTrue(ss.http2FrameAvailable);
                    assertEquals(3, factory.http2RO.streamId());
                    assertFalse(factory.http2RO.endStream());
                }
                else
                {
                    assertEquals(consumed, fragLength);
                    assertFalse(ss.http2FrameAvailable);
                }
            }
        }
    }

}