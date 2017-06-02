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
    final MutableDirectBuffer buffer;

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

    /*
     * @return offset at which data of given length can be written
     *         -1 if there is no space to write
     */
    int writeOffset(int length)
    {
        int prevEnd = end;
        if (start == end)                   // empty
        {
            start = end = 0;
            if (length < buffer.capacity())
            {
                return 0;
            }
        }
        else if (start < end)
        {
            if (end + length < buffer.capacity())
            {
                return prevEnd;
            }
            else if (length < start)
            {
                return 0;
            }
        }
        else                        // wrapped
        {
            if (end + length < start)
            {
                return prevEnd;
            }
        }

        return -1;
    }

    void write(int offset, int length)
    {
        end = offset + length;
    }

    int write(DirectBuffer srcBuffer, int srcIndex, int length)
    {
        int index = writeOffset(length);
        if (index != -1)
        {
            buffer.putBytes(index, srcBuffer, srcIndex, length);
            end = index + length;
        }
        return index;
    }

    private int readOffset(int length)
    {
        return start + length < buffer.capacity() ? start : 0;
    }

    void read(int length)
    {
        start = readOffset(length) + length;
    }

}
