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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class CircularDirectBuffer
{
    private final MutableDirectBuffer buffer;

    /*
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |   |x x x x x x x x x x|         |
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *       ^                   ^
     *       start               end
     *
     *
     *
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *  |x x x|     |x x x x x x x| |
     *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     *         ^     ^
     *       end     start
     */

    private int start;
    private int end;

    CircularDirectBuffer(MutableDirectBuffer buffer)
    {
        this.buffer = buffer;
    }

    boolean add(DirectBuffer data)
    {
        return add(data, 0, data.capacity());
    }

    /*
     * Copies the bytes from the given buffer into this buffer contiguously.
     *
     * @return true if the data is copied
     *         false if it is not copied (not enough contiguous space)
     */
    boolean add(DirectBuffer data, int offset, int length)
    {
        if (start == end)                   // empty
        {
            start = end = 0;
            if (length < buffer.capacity())
            {
                buffer.putBytes(0, data, offset, length);
                end = length;
                return true;
            }
        }
        else if (start < end)
        {
            if (end + length < buffer.capacity())
            {
                buffer.putBytes(end, data, offset, length);
                end += length;
                return true;
            }
            else if (length < start)
            {
                buffer.putBytes(0, data, offset, length);
                end = length;
                return true;
            }
        }
        else                        // wrapped
        {
            if (end + length < start)
            {
                buffer.putBytes(end, data, offset, length);
                end += length;
                return true;
            }
        }

        return false;
    }

    private int start(int length)
    {
        return start + length < buffer.capacity() ? start : 0;
    }

    void remove(int length)
    {
        start = start(length) + length;
    }

}
