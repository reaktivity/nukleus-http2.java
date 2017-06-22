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
import org.reaktivity.nukleus.http2.internal.routable.stream.CircularDirectBuffer;
import org.reaktivity.nukleus.http2.internal.routable.stream.Slab;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;

import static org.reaktivity.nukleus.http2.internal.routable.stream.Slab.NO_SLOT;

public class HttpWriteScheduler2
{
    private final MutableDirectBuffer buffer = new UnsafeBuffer(new byte[0]);

    private final Slab slab;
    private final Target2 target;
    private final long targetId;

    private Http2Stream2 stream;
    private int slot = NO_SLOT;
    private CircularDirectBuffer targetBuffer;
    private boolean end;
    private boolean endSent;

    public HttpWriteScheduler2(Slab slab, Target2 target, long targetId, Http2Stream2 stream)
    {
        this.slab = slab;
        this.target = target;
        this.targetId = targetId;
        this.stream = stream;
    }

    /*
     * @return true if the data is written or stored
     *         false if there are no slots or no space in the buffer
     */
    public boolean onData(Http2DataFW http2DataRO)
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
                target.doHttpData(targetId, http2DataRO.buffer(), http2DataRO.dataOffset(), toHttp);
                stream.sendHttp2Window(toHttp);
            }

            // Store the remaining to a buffer
            if (toSlab > 0)
            {
                MutableDirectBuffer dst = acquire();
                if (dst != null)
                {
                    return targetBuffer.write(dst, http2DataRO.buffer(), http2DataRO.dataOffset() + toHttp, toSlab);
                }
                return false;                           // No slots
            }

            // since there is no data is pending, we can send END frame
            if (end && !endSent)
            {
                endSent = true;
                target.doHttpEnd(targetId);
            }

            return true;
        }
        else
        {
            // Store the data in the existing buffer
            return targetBuffer.write(buffer, http2DataRO.buffer(), http2DataRO.dataOffset(), http2DataRO.dataLength());
        }
    }

    public void onWindow()
    {
        if (targetBuffer != null)
        {
            int toHttp = Math.min(targetBuffer.size(), stream.targetWindow);

            // Send to http if there is available window
            if (toHttp > 0)
            {
                int offset1 = targetBuffer.readOffset();
                int read1 = targetBuffer.read(toHttp);
                target.doHttpData(targetId, buffer, offset1, read1);

                // toHttp may span across the boundary, one more read may be required
                if (read1 != toHttp)
                {
                    int offset2 = targetBuffer.readOffset();
                    int read2 = targetBuffer.read(toHttp-read1);
                    assert read1 + read2 == toHttp;

                    target.doHttpData(targetId, buffer, offset2, read2);
                }

                stream.sendHttp2Window(toHttp);
            }

            if (targetBuffer.size() == 0)
            {
                // since there is no data is pending in slab, we can send END frame right away
                if (end && !endSent)
                {
                    endSent = true;
                    target.doHttpEnd(targetId);
                }

                release();
            }
        }
    }

    public void onReset()
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
