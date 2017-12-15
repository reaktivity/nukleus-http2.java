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
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http2.internal.types.Flyweight;
import org.reaktivity.nukleus.http2.internal.types.stream.DataFW;

class NukleusWriteScheduler
{
    private final Http2Connection connection;
    private final Http2Writer http2Writer;
    private final long targetId;
    private final MessageConsumer networkConsumer;
    private final MutableDirectBuffer writeBuffer;

    private int accumulatedLength;

    NukleusWriteScheduler(
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
    }

    int http2Frame(
            int lengthGuess,
            Flyweight.Builder.Visitor visitor)
    {
        int length = visitor.visit(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD + accumulatedLength, lengthGuess);
        accumulatedLength += length;

        return length;
    }

    void doEnd()
    {
        http2Writer.doEnd(networkConsumer, targetId);
    }

    void flush()
    {
        if (accumulatedLength > 0)
        {
            toNetwork(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, accumulatedLength);
            int adjustment = nukleusWindowBudgetAdjustment(accumulatedLength);

            connection.networkReplyWindowBudget -= accumulatedLength + adjustment;
            assert connection.networkReplyWindowBudget >= 0;

            accumulatedLength = 0;
        }

        assert accumulatedLength == 0;
    }

    boolean fits(int sizeof)
    {
        int candidateSizeof = accumulatedLength + sizeof;
        int adjustment = nukleusWindowBudgetAdjustment(candidateSizeof);

        return candidateSizeof + adjustment <= connection.networkReplyWindowBudget;
    }

    int remaining()
    {
        int adjustment = nukleusWindowBudgetAdjustment(accumulatedLength);
        int sizeof = connection.networkReplyWindowBudget - (accumulatedLength + adjustment);
        int remaining = fits(sizeof) ? sizeof : sizeof - connection.networkReplyWindowPadding;
        return Math.max(remaining, 0);
    }

    int nukleusWindowBudgetAdjustment(int sizeof)
    {
        int nukleusFrameCount = (int) Math.ceil((double)sizeof/65535);

        // Every nukleus DATA frame incurs padding overhead
        return nukleusFrameCount * connection.networkReplyWindowPadding;
    }

    private void toNetwork(MutableDirectBuffer buffer, int offset, int length)
    {
        while (length > 0)
        {
            int chunk = Math.min(length, 65535);     // limit by nukleus DATA frame length (2 bytes)
            http2Writer.doData(networkConsumer, targetId, connection.networkReplyWindowPadding, buffer, offset, chunk);
            offset += chunk;
            length -= chunk;
        }
    }

}
