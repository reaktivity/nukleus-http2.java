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
package org.reaktivity.nukleus.http2.internal.client;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

class Http2FrameExtractor
{
    private int frameSlotIndex = NO_SLOT;
    private int frameSlotPosition;
    boolean isHttp2FrameAvailable;

    final ClientStreamFactory factory;

    Http2FrameExtractor(ClientStreamFactory factory)
    {
        this.factory = factory;
    }

    /**
     * Assembles a complete HTTP2 frame and the flyweight. The result will be found in factory.http2RO
     *
     * @return consumed bytes
     *         -1 if there is an error and the connection must be reset
     */
    public int http2FrameAvailable(DirectBuffer buffer, int offset, int limit, int localMaxFrameSize)
    {
        int available = limit - offset;

        if (frameSlotPosition > 0 && frameSlotPosition + available >= 3)
        {
            MutableDirectBuffer frameBuffer = factory.bufferPool.buffer(frameSlotIndex);
            if (frameSlotPosition < 3)
            {
                frameBuffer.putBytes(frameSlotPosition, buffer, offset, 3 - frameSlotPosition);
            }
            int frameLength = http2FrameLength(frameBuffer, 0, 3);
            if (frameLength > localMaxFrameSize + 9)
            {
                // return error - frame too big
                return -1;
            }
            if (frameSlotPosition + available >= frameLength)
            {
                int remainingFrameLength = frameLength - frameSlotPosition;
                frameBuffer.putBytes(frameSlotPosition, buffer, offset, remainingFrameLength);
                factory.http2RO.wrap(frameBuffer, 0, frameLength);
                releaseSlot();
                isHttp2FrameAvailable = true;
                return remainingFrameLength;
            }
        }
        else if (available >= 3)
        {
            int frameLength = http2FrameLength(buffer, offset, limit);
            if (frameLength > localMaxFrameSize + 9)
            {
                // return error - frame too big
                return -1;
            }
            if (available >= frameLength)
            {
                factory.http2RO.wrap(buffer, offset, offset + frameLength);
                isHttp2FrameAvailable = true;
                return frameLength;
            }
        }

        if (!acquireSlot())
        {
            // return error, no buffer available
            isHttp2FrameAvailable = false;
            return -1;
        }

        MutableDirectBuffer frameBuffer = factory.bufferPool.buffer(frameSlotIndex);
        frameBuffer.putBytes(frameSlotPosition, buffer, offset, available);
        frameSlotPosition += available;
        isHttp2FrameAvailable = false;
        return available;
    }

    private int http2FrameLength(DirectBuffer buffer, final int offset, int limit)
    {
        if (limit - offset < 3)
        {
            return -1;
        }

        int length = (buffer.getByte(offset) & 0xFF) << 16;
        length += (buffer.getShort(offset + 1, BIG_ENDIAN) & 0xFF_FF);
        return length + 9;      // +3 for length, +1 type, +1 flags, +4 stream-id
    }

    private boolean acquireSlot()
    {
        if (frameSlotIndex == NO_SLOT)
        {
            frameSlotIndex = factory.bufferPool.acquire(0);
            if (frameSlotIndex == NO_SLOT)
            {
                isHttp2FrameAvailable = false;
                return false;
            }
        }
        return true;
    }

    private void releaseSlot()
    {
        if (frameSlotIndex != NO_SLOT)
        {
            factory.bufferPool.release(frameSlotIndex);
            frameSlotIndex = NO_SLOT;
            frameSlotPosition = 0;
        }
    }
}
