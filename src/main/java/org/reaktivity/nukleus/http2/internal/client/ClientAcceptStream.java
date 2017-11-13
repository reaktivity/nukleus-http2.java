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
import org.reaktivity.nukleus.http2.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http2.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HttpBeginExFW;

class ClientAcceptStream
{
    private MessageConsumer streamState;

    private ClientStreamFactory factory;
    private MessageConsumer acceptThrottle;
    private long acceptStreamId;

    Http2ClientStreamId targetStreamId;

    private int contentLength = -1;
    private int totalData = 0;

    ClientAcceptStream(ClientStreamFactory factory, MessageConsumer acceptThrottle, long streamId)
    {
        this.factory = factory;
        this.acceptThrottle = acceptThrottle;
        this.acceptStreamId = streamId;

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


            this.streamState = this::streamAfterBegin;

            // now create connect stream
            targetStreamId = factory.http2ConnectionManager.newStream(begin, acceptThrottle);
            extractContentLength(begin);
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
            Http2ClientConnection connection = factory.http2ConnectionManager.getConnection(targetStreamId.nukleusStreamId);
            connection.endHttp2ConnectStream(targetStreamId.http2StreamId);
            break;
        case AbortFW.TYPE_ID:
            connection = factory.http2ConnectionManager.getConnection(targetStreamId.nukleusStreamId);
            connection.resetStream(targetStreamId.http2StreamId);
            break;
        default:
            resetAcceptStream();
            break;
        }
    }
    // end stream handlers

    // resets the accept stream
    void resetAcceptStream()
    {
        factory.doReset(acceptThrottle, acceptStreamId);
    }

    private void processData(DirectBuffer buffer, int index, int length)
    {
        final DataFW data = factory.dataRO.wrap(buffer, index, index + length);
        Http2ClientConnection connection = factory.http2ConnectionManager.getConnection(targetStreamId.nukleusStreamId);
        totalData += data.payload().sizeof();
        connection.doHttp2Data(targetStreamId.http2StreamId, data.payload(), contentLength > 0 && totalData == contentLength);
    }

    private void extractContentLength(BeginFW begin)
    {
        HttpBeginExFW beginEx = begin.extension().get(factory.httpBeginExRO::wrap);
        beginEx.headers().forEach(h ->
        {
            if ("content-length".equalsIgnoreCase(h.name().asString()))
            {
                contentLength = Integer.valueOf(h.value().asString());
                return;
            }
        });
    }
}
