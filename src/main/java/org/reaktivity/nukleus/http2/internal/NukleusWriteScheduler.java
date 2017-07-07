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
import org.reaktivity.nukleus.http2.internal.types.Flyweight;

import static org.reaktivity.nukleus.http2.internal.Slab.NO_SLOT;

class NukleusWriteScheduler
{
    private final MutableDirectBuffer accumulated = new UnsafeBuffer(new byte[0]);
    private int accumulatedSlot = NO_SLOT;

    private final Http2Connection connection;
    private final Http2Writer http2Writer;
    private final long targetId;
    private final long sourceOutputEstId;
    private final MessageConsumer networkConsumer;

    private Slab slab;
    private int accumulatedOffset;

    NukleusWriteScheduler(
            Http2Connection connection,
            long sourceOutputEstId,
            Slab slab,
            MessageConsumer networkConsumer,
            Http2Writer http2Writer,
            long targetId)
    {
        this.connection = connection;
        this.sourceOutputEstId = sourceOutputEstId;
        this.slab = slab;
        this.networkConsumer = networkConsumer;
        this.http2Writer = http2Writer;
        this.targetId = targetId;
    }

    int http2Frame(
            int lengthGuess,

            Flyweight.Builder.Visitor visitor)
    {
        if (accumulatedSlot == NO_SLOT)
        {
            accumulatedSlot = slab.acquire(sourceOutputEstId);
        }

        if (accumulatedSlot == NO_SLOT)
        {
            connection.cleanConnection();
            return -1;
        }
        slab.buffer(accumulatedSlot, this::accumulated);
        int length = visitor.visit(accumulated, accumulatedOffset, lengthGuess);
        accumulatedOffset += length;

        assert accumulatedOffset < 65536;       // DataFW's length is 2 bytes
        return length;
    }

    void doEnd()
    {
        http2Writer.doEnd(networkConsumer, targetId);
    }

    void flush()
    {
        if (accumulatedOffset > 0)
        {
            http2Writer.doData(networkConsumer, targetId, accumulated, 0, accumulatedOffset);
            accumulatedOffset = 0;
        }

        if (accumulatedSlot != NO_SLOT)
        {
            slab.release(accumulatedSlot);
            accumulatedSlot = NO_SLOT;
        }
    }

    private MutableDirectBuffer accumulated(MutableDirectBuffer buffer)
    {
        accumulated.wrap(buffer.addressOffset(), buffer.capacity());
        return accumulated;
    }

}
