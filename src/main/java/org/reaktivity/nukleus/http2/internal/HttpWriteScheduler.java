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

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;

import static org.reaktivity.nukleus.http2.internal.Slab.NO_SLOT;

class HttpWriteScheduler
{
    private final MutableDirectBuffer buffer = new UnsafeBuffer(new byte[0]);

    private final Slab slab;
    private final HttpWriter target;
    private final long targetId;
    private final MessageConsumer applicationTarget;

    private Http2Stream stream;
    private int slot = NO_SLOT;
    private CircularDirectBuffer targetBuffer;
    private boolean end;
    private boolean endSent;

    HttpWriteScheduler(Slab slab, MessageConsumer applicationTarget, HttpWriter target, long targetId, Http2Stream stream)
    {
        this.slab = slab;
        this.applicationTarget = applicationTarget;
        this.target = target;
        this.targetId = targetId;
        this.stream = stream;
    }

    /*
     * @return true if the data is written or stored
     *         false if there are no slots or no space in the buffer
     */
    boolean onData(Http2DataFW http2DataRO)
    {
        end = http2DataRO.endStream();

        if (targetBuffer == null)
        {
            int data = http2DataRO.dataLength();
            int toHttp = Math.min(data, stream.targetWindow);
            int toSlab = data - toHttp;

            // Send to http if there is available window
            if (toHttp > 0)
            {
                target.doHttpData(applicationTarget, targetId, http2DataRO.buffer(), http2DataRO.dataOffset(), toHttp);
                stream.sendHttp2Window(toHttp);
            }

            // Store the remaining to a buffer
            if (toSlab > 0)
            {
                MutableDirectBuffer dst = acquire();
                if (dst != null)
                {
                    boolean written = targetBuffer.write(dst, http2DataRO.buffer(), http2DataRO.dataOffset() + toHttp, toSlab);
                    assert written;
                    return written;
                }
                return false;                           // No slots
            }

            // since there is no data is pending, we can send END frame
            if (end && !endSent)
            {
                endSent = true;
                target.doHttpEnd(applicationTarget, targetId);
            }

            return true;
        }
        else
        {
            // Store the data in the existing buffer
            boolean written = targetBuffer.write(buffer, http2DataRO.buffer(), http2DataRO.dataOffset(),
                    http2DataRO.dataLength());
            assert written;
            return written;
        }
    }

    void onWindow()
    {
        if (targetBuffer != null)
        {
            int toHttp = Math.min(targetBuffer.size(), stream.targetWindow);

            // Send to http if there is available window
            if (toHttp > 0)
            {
                int offset1 = targetBuffer.readOffset();
                int read1 = targetBuffer.read(toHttp);
                target.doHttpData(applicationTarget, targetId, buffer, offset1, read1);

                // toHttp may span across the boundary, one more read may be required
                if (read1 != toHttp)
                {
                    int offset2 = targetBuffer.readOffset();
                    int read2 = targetBuffer.read(toHttp-read1);
                    assert read1 + read2 == toHttp;

                    target.doHttpData(applicationTarget, targetId, buffer, offset2, read2);
                }

                stream.sendHttp2Window(toHttp);
            }

            if (targetBuffer.size() == 0)
            {
                // since there is no data is pending in slab, we can send END frame right away
                if (end && !endSent)
                {
                    endSent = true;
                    target.doHttpEnd(applicationTarget, targetId);
                }

                release();
            }
        }
    }

    void onReset()
    {
        release();
    }

    /*
     * @return buffer if there is a slot, buffer is wrapped on that slot
     *         null if all slots are taken
     */
    private MutableDirectBuffer acquire()
    {
        if (slot == NO_SLOT)
        {
            slot = slab.acquire(targetId);
            if (slot != NO_SLOT)
            {
                int capacity = slab.buffer(slot, this::buffer).capacity();
                targetBuffer = new CircularDirectBuffer(capacity);
            }
        }
        return slot != NO_SLOT ? buffer : null;
    }

    private void release()
    {
        if (slot != NO_SLOT)
        {
            slab.release(slot);
            slot = NO_SLOT;
            targetBuffer = null;
        }
    }

    private MutableDirectBuffer buffer(MutableDirectBuffer src)
    {
        buffer.wrap(src.addressOffset(), src.capacity());
        return buffer;
    }

}
