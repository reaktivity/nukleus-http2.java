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

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http2.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.EndFW;

class ClientAcceptStream
{
    private MessageConsumer streamState;

    private ClientStreamFactory factory;
    private MessageConsumer acceptThrottle;
    private long acceptStreamId;
    Http2ClientConnectionManager http2ConnectionManager;

    Http2ClientStreamId targetStreamId;

    private final int initialFrameWindow;
    private int acceptFrameWindow;

    ClientAcceptStream(ClientStreamFactory factory, MessageConsumer acceptThrottle, long streamId,
            Http2ClientConnectionManager http2ConnectionManager)
    {
        this.factory = factory;
        this.acceptThrottle = acceptThrottle;
        this.acceptStreamId = streamId;
        this.http2ConnectionManager = http2ConnectionManager;

        this.streamState = this::streamBeforeBegin;
        initialFrameWindow = factory.bufferPool.slotCapacity();
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

            factory.doWindow(acceptThrottle, acceptStreamId, initialFrameWindow, initialFrameWindow);
            acceptFrameWindow = initialFrameWindow;

            this.streamState = this::streamAfterBegin;

            // now create connect stream
            targetStreamId = http2ConnectionManager.newStream(begin);
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
            Http2ClientConnection connection = http2ConnectionManager.getConnection(targetStreamId.nukleusStreamId);
            connection.endStream(targetStreamId.http2StreamId);
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
        factory.doReset(acceptThrottle, acceptStreamId);
        this.streamState = this::streamAfterReset;
    }

    private void processData(DirectBuffer buffer, int index, int length)
    {
        final DataFW data = factory.dataRO.wrap(buffer, index, index + length);
        Http2ClientConnection connection = http2ConnectionManager.getConnection(targetStreamId.nukleusStreamId);
        connection.doHttp2Data(targetStreamId.http2StreamId, data.payload());

        factory.doWindow(acceptThrottle, acceptStreamId, length, length);
        acceptFrameWindow += length;
    }
}
