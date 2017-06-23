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
import org.reaktivity.nukleus.http2.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http2.internal.types.stream.WindowFW;

import java.util.Deque;
import java.util.LinkedList;
import java.util.function.UnaryOperator;

import static org.reaktivity.nukleus.http2.internal.Slab.NO_SLOT;

class Http2Stream
{
    private final Http2Connection connection;
    private final HttpWriteScheduler httpWriteScheduler;
    final int http2StreamId;
    final long targetId;
    final long correlationId;
    Http2Connection.State state;
    long http2OutWindow;
    long http2InWindow;

    long contentLength;
    long totalData;
    int targetWindow;

    private int replySlot = NO_SLOT;
    CircularDirectBuffer replyBuffer;
    Deque replyQueue;
    public boolean endStream;
    long totalOutData;
    private ServerStreamFactory factory;

    Http2Stream(ServerStreamFactory factory, Http2Connection connection, int http2StreamId, Http2Connection.State state,
                Target httpTarget)
    {
        this.factory = factory;
        this.connection = connection;
        this.http2StreamId = http2StreamId;
        this.targetId = factory.supplyStreamId.getAsLong();
        this.correlationId = factory.supplyCorrelationId.getAsLong();
        this.http2InWindow = connection.localSettings.initialWindowSize;
        this.http2OutWindow = connection.remoteSettings.initialWindowSize;
        this.state = state;
        this.httpWriteScheduler = new HttpWriteScheduler(factory.frameSlab, httpTarget, targetId, this);
    }

    boolean isClientInitiated()
    {
        return http2StreamId%2 == 1;
    }

    void onData()
    {
        httpWriteScheduler.onData(factory.http2DataRO);
    }

    void onThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        switch (msgTypeId)
        {
            case WindowFW.TYPE_ID:
                factory.windowRO.wrap(buffer, index, index + length);
                int update = factory.windowRO.update();
                targetWindow += update;
                httpWriteScheduler.onWindow();
                break;
            case ResetFW.TYPE_ID:
                doReset(buffer, index, length);
                break;
            default:
                // ignore
                break;
        }
    }

    /*
     * @return true if there is a buffer
     *         false if all slots are taken
     */
    MutableDirectBuffer acquireReplyBuffer(UnaryOperator<MutableDirectBuffer> change)
    {
        if (replySlot == NO_SLOT)
        {
            replySlot = factory.frameSlab.acquire(connection.sourceOutputEstId);
            if (replySlot != NO_SLOT)
            {
                int capacity = factory.frameSlab.buffer(replySlot).capacity();
                replyBuffer = new CircularDirectBuffer(capacity);
                replyQueue = new LinkedList();
            }
        }
        return replySlot != NO_SLOT ? factory.frameSlab.buffer(replySlot, change) : null;
    }

    void releaseReplyBuffer()
    {
        if (replySlot != NO_SLOT)
        {
            factory.frameSlab.release(replySlot);
            replySlot = NO_SLOT;
            replyBuffer = null;
            replyQueue = null;
        }
    }

    private void doReset(
            DirectBuffer buffer,
            int index,
            int length)
    {
        factory.resetRO.wrap(buffer, index, index + length);
        httpWriteScheduler.onReset();
        releaseReplyBuffer();
        //source.doReset(sourceId);
    }

    void sendHttp2Window(int update)
    {
        targetWindow -= update;

        http2InWindow += update;
        connection.http2InWindow += update;

        // connection-level flow-control
        connection.writeScheduler.windowUpdate(0, update);

        // stream-level flow-control
        connection.writeScheduler.windowUpdate(http2StreamId, update);
    }

}
