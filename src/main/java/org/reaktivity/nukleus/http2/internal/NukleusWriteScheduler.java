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
import org.reaktivity.nukleus.buffer.MemoryManager;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http2.internal.types.Flyweight;
import org.reaktivity.nukleus.http2.internal.types.stream.TransferFW;

import java.io.Closeable;

class NukleusWriteScheduler implements Closeable
{
    private static final int REGION_SIZE = 65536;

    private final Http2Connection connection;
    private final Http2Writer http2Writer;
    private final long targetId;
    private final MessageConsumer networkConsumer;
    private final MutableDirectBuffer writeBuffer;
    private final TransferFW.Builder transfer;
    private final MemoryManager memoryManager;

    private int accumulatedLength;
    private final MutableDirectBuffer regionBuffer;
    private final long regionAddress;
    private int regionWriteOffset;
    private int regionAckOffset;

    NukleusWriteScheduler(
        MemoryManager memoryManager,
        Http2Connection connection,
        MessageConsumer networkConsumer,
        Http2Writer http2Writer,
        long targetId)
    {
        this.connection = connection;
        this.networkConsumer = networkConsumer;
        this.http2Writer = http2Writer;
        this.targetId = targetId;
        this.writeBuffer = http2Writer.writeBuffer;
        this.transfer = new TransferFW.Builder();
        this.memoryManager = memoryManager;
        regionAddress = memoryManager.acquire(REGION_SIZE);
        regionBuffer = new UnsafeBuffer(new byte[0]);
        System.out.printf("Resolved address %x\n", memoryManager.resolve(regionAddress));
        regionBuffer.wrap(memoryManager.resolve(regionAddress), REGION_SIZE);
    }

    int queueHttp2Frame(
        int lengthGuess,
        Flyweight.Builder.Visitor visitor)
    {
        int length = visitor.visit(regionBuffer, regionWriteOffset, lengthGuess);
        transfer.regionsItem(b -> b.address(regionAddress + regionWriteOffset).length(length).streamId(targetId));
        regionWriteOffset += length;
        accumulatedLength += length;

        return length;
    }

    void queueHttp2Data(
        long address,
        int length,
        long regionStreamId)
    {
        transfer.regionsItem(b -> b.address(address).length(length).streamId(regionStreamId));
    }

    void doEnd()
    {
        connection.factory.doEnd(networkConsumer, targetId);
    }

    void ack(
        long address,
        int length)
    {
        regionAckOffset += length;
        if (regionAckOffset == regionWriteOffset)
        {
            regionWriteOffset = regionAckOffset = 0;
        }
    }

    void flushBegin()
    {
        transfer.wrap(writeBuffer, 0, writeBuffer.capacity()).streamId(targetId);
    }

    void flushEnd()
    {
        if (accumulatedLength > 0)
        {
            TransferFW t = transfer.build();
            http2Writer.doTransfer(networkConsumer, t);
            accumulatedLength = 0;
        }
    }

    boolean fits(
        int sizeof)
    {
        return sizeof <= remaining();
    }

    int remaining()
    {
        return regionBuffer.capacity() - regionWriteOffset;
    }

    @Override
    public void close()
    {
        memoryManager.release(regionAddress, REGION_SIZE);
    }

}
