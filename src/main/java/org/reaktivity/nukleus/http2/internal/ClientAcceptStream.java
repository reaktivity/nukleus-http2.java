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
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http2.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http2.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http2.internal.types.stream.WindowFW;

class ClientAcceptStream
{
    private MessageConsumer streamState;

    private ClientStreamFactory factory;
    private MessageConsumer acceptThrottle;
    private long acceptId;

    private long acceptCorrelationId;
    private MessageConsumer acceptReply;
    private long acceptReplyId;

    private final int initialFrameWindow = factory.bufferPool.slotCapacity();
    private int acceptFrameWindow;
    private int acceptReplyFrameWindow;

    ClientAcceptStream(ClientStreamFactory factory, MessageConsumer acceptThrottle, long streamId)
    {
        this.factory = factory;
        this.acceptThrottle = acceptThrottle;
        this.acceptId = streamId;
        this.streamState = this::streamBeforeBegin;
    }

    void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        streamState.accept(msgTypeId, buffer, index, length);
    }

    // start stream handlers

    // new stream, we have not received begin frame yet
    private void streamBeforeBegin(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        if (msgTypeId == BeginFW.TYPE_ID)
        {
            // init accept reply stream
            final BeginFW begin = factory.beginRO.wrap(buffer, index, index + length);

            final String acceptReplyName = begin.source().asString();
            acceptCorrelationId = begin.correlationId();

            acceptReply = factory.router.supplyTarget(acceptReplyName);
            acceptReplyId = factory.supplyStreamId.getAsLong();

            factory.doWindow(acceptThrottle, acceptId, initialFrameWindow, initialFrameWindow);
            acceptFrameWindow = initialFrameWindow;

            factory.doBegin(acceptReply, acceptReplyId, 0L, acceptCorrelationId);
            factory.router.setThrottle(acceptReplyName, acceptReplyId, this::handleAcceptReplyThrottle);

            this.streamState = this::streamAfterBegin;
        }
        else
        {
            resetAcceptStream();
        }
    }

    // we have already received begin, connection is initiated
    private void streamAfterBegin(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            processData(buffer, index, length);
            break;
        case EndFW.TYPE_ID:
            factory.doEnd(acceptReply, acceptReplyId);
            break;
        default:
            resetAcceptStream();
            break;
        }
    }

    // we have already received a reset frame
    private void streamAfterReset(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        if (msgTypeId == DataFW.TYPE_ID)
        {
            DataFW data = this.factory.dataRO.wrap(buffer, index, index + length);
            final long streamId = data.streamId();
            factory.doWindow(acceptThrottle, streamId, data.length(), data.length());
            acceptFrameWindow += data.length();
        }
    }
    // end stream handlers

    // resets the accept stream
    private void resetAcceptStream()
    {
        factory.doReset(acceptThrottle, acceptId);
        this.streamState = this::streamAfterReset;
    }

    // handles the throttle frames for accept reply stream
    private void handleAcceptReplyThrottle(
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

                acceptReplyFrameWindow += update;
                break;
            case ResetFW.TYPE_ID:
                resetAcceptStream();
                break;
            default:
                // ignore
                break;
        }
    }

    private void processData(DirectBuffer buffer, int index, int length)
    {
        // TODO Auto-generated method stub
    }
}
