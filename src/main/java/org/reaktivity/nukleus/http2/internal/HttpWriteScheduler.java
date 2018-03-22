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
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;

import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;

class HttpWriteScheduler
{
    private final BufferPool httpWriterPool;
    private final HttpWriter target;
    private final long targetId;
    private final MessageConsumer applicationTarget;

    private Http2Stream stream;
    private int slot = NO_SLOT;
    private CircularDirectBuffer targetBuffer;
    private boolean end;
    private boolean endSent;
    private int applicationBudget;
    private int applicationPadding;
    private long applicationGroupId;

    private int totalRead;
    private int totalWritten;
    private long traceId;

    HttpWriteScheduler(BufferPool httpWriterPool, MessageConsumer applicationTarget, HttpWriter target, long targetId,
                       Http2Stream stream)
    {
        this.httpWriterPool = httpWriterPool;
        this.applicationTarget = applicationTarget;
        this.target = target;
        this.targetId = targetId;
        this.stream = stream;
    }

    /*
     * @return true if the data is written or stored
     *         false if there are no slots or no space in the buffer
     */
    boolean onData(long traceId, Http2DataFW http2DataRO)
    {
        // keep traceId of the only first data frame and we don't write traceId
        // for subsequent data frames until the buffer is drained. This ok since
        // we don't expect data to be buffered (except for initial request)
        if (this.traceId == 0 && targetBuffer == null)
        {
            this.traceId = traceId;
        }
        totalRead += http2DataRO.dataLength();
        end = http2DataRO.endStream();

        if (targetBuffer == null)
        {
            int toSlab = http2DataRO.dataLength();
            int toHttp = 0;
            int part;
            while((part = getPart(toSlab)) > 0)
            {
                toHttp(http2DataRO.buffer(), http2DataRO.dataOffset() + toHttp, part);
                toHttp += part;
                toSlab -= part;
            }

            // Store the remaining to a buffer
            if (toSlab > 0)
            {
                MutableDirectBuffer dst = acquire();
                if (dst != null)
                {
                    boolean written = targetBuffer.write(dst, http2DataRO.buffer(), http2DataRO.dataOffset() + toHttp, toSlab);
//                    assert written;
//                    assert totalRead == totalWritten + targetBuffer.size();

                    return written;
                }
                return false;                           // No slots
            }

            // since there is no data is pending, we can send END frame
            if (end && !endSent)
            {
                endSent = true;
                target.doHttpEnd(applicationTarget, targetId, traceId);
            }

            return true;
        }
        else
        {
            // Store the data in the existing buffer
            MutableDirectBuffer buffer = acquire();
            boolean written = targetBuffer.write(buffer, http2DataRO.buffer(), http2DataRO.dataOffset(),
                    http2DataRO.dataLength());
//            assert written;
//            assert totalRead == totalWritten + targetBuffer.size();

            return written;
        }
    }

    void onWindow(int credit, int padding, long groupId)
    {
        applicationBudget += credit;
        applicationPadding = padding;
        applicationGroupId = groupId;

        if (targetBuffer != null)
        {
            int toHttp;
            while ((toHttp = getPart(targetBuffer.size())) > 0)
            {
                // cannot read all toHttp from circular buffer in one go
                MutableDirectBuffer buffer = acquire();
                int offset = targetBuffer.readOffset();
                int part = targetBuffer.read(toHttp);
                toHttp(buffer, offset, part);
            }
            assert totalRead == totalWritten + targetBuffer.size();

            if (targetBuffer.size() == 0)
            {
                // since there is no data is pending in slab, we can send END frame right away
                if (end && !endSent)
                {
                    endSent = true;
                    target.doHttpEnd(applicationTarget, targetId, traceId);
                }

                release();
            }
        }

        sendHttp2Window();
    }

    private int getPart(int remaining)
    {
        int toHttp = Math.min(remaining, applicationBudget - applicationPadding);
        return Math.min(toHttp, 65535);
//        int claimed = stream.connection.factory.groupBudgetClaimer.apply(applicationGroupId)
//                                                                  .applyAsInt(toHttp + applicationWindowPadding);
//        toHttp = claimed - applicationWindowPadding;
//        if (toHttp <= 0)
//        {
//            stream.connection.factory.groupBudgetReleaser.apply(applicationGroupId)
//                                                         .applyAsInt(toHttp);
//        }
//        return toHttp;
    }

    private void toHttp(DirectBuffer buffer, int offset, int length)
    {
        assert length <= 65535;

        applicationBudget -= length + applicationPadding;
        target.doHttpData(applicationTarget, targetId, traceId, applicationPadding, buffer, offset, length);
        totalWritten += length;
        traceId = 0;
    }

    void onReset()
    {
        release();
    }

    void doAbort()
    {
        target.doHttpAbort(applicationTarget, targetId);
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
            slot = httpWriterPool.acquire(targetId);
            if (slot != NO_SLOT)
            {
                int capacity = httpWriterPool.buffer(slot).capacity();
                targetBuffer = new CircularDirectBuffer(capacity);
            }
        }
        return slot != NO_SLOT ? httpWriterPool.buffer(slot) : null;
    }

    private void release()
    {
        if (slot != NO_SLOT)
        {
            httpWriterPool.release(slot);
            slot = NO_SLOT;
            targetBuffer = null;
        }
    }

    private void sendHttp2Window()
    {
        // buffer may already have some data, so can only send window for remaining
        int buffered = targetBuffer == null ? 0 : targetBuffer.size();
        long applicationCredit = Math.min(
                applicationBudget - Math.max(stream.http2InWindow, 0),    // http2InWindow can be -ve
                httpWriterPool.slotCapacity() - buffered);
        if (applicationCredit > 0)
        {
            stream.http2InWindow += applicationCredit;
            stream.connection.http2InWindow += applicationCredit;

            // HTTP2 connection-level flow-control
            stream.connection.writeScheduler.windowUpdate(0, (int) applicationCredit);

            // HTTP2 stream-level flow-control
            stream.connection.writeScheduler.windowUpdate(stream.http2StreamId, (int) applicationCredit);
        }
    }

}
