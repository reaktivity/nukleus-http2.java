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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class CircularEntryBuffer
{
    private final int capacity;
    /*
     * A circular buffer that keeps an entry together (without wrapping
     * across the boundary)
     *
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

    public CircularEntryBuffer(int capacity)
    {
        this.capacity = capacity;
    }

    /*
     * @return offset at which data of given length can be written
     *         -1 if there is no space to write
     */
    public int writeOffset(int length)
    {
        int prevEnd = end;
        if (start == end)                   // empty
        {
            start = end = 0;
            if (length < capacity)
            {
                return 0;
            }
        }
        else if (start < end)
        {
            if (end + length < capacity)
            {
                return prevEnd;
            }
            else if (length < start)
            {
                return 0;
            }
        }
        else                                // wrapped
        {
            if (end + length < start)
            {
                return prevEnd;
            }
        }

        return -1;
    }

    public void write(int offset, int length)
    {
        end = offset + length;
    }

    int write(MutableDirectBuffer dstBuffer, DirectBuffer srcBuffer, int srcIndex, int length)
    {
        int index = writeOffset(length);
        if (index != -1)
        {
            dstBuffer.putBytes(index, srcBuffer, srcIndex, length);
            end = index + length;
        }
        return index;
    }

    private int readOffset(int length)
    {
        return start + length < capacity ? start : 0;
    }

    void read(int length)
    {
        start = readOffset(length) + length;
    }

    public void read(int offset, int length)
    {
        start = offset + length;
    }

    public String toString()
    {
        return "capacity = " + capacity + " [start = " + start + " end = " + end + "]";
    }

}
