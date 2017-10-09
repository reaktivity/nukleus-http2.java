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

import static org.reaktivity.nukleus.http2.internal.Http2ConnectionState.HALF_CLOSED_REMOTE;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http2.internal.Http2ConnectionState;
import org.reaktivity.nukleus.http2.internal.types.OctetsFW;
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http2.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType;
import org.reaktivity.nukleus.http2.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http2.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http2.internal.types.stream.WindowFW;

class ClientConnectReplyStream
{
    private MessageConsumer streamState;

    private final ClientStreamFactory factory;
    private final MessageConsumer connectReplyThrottle;
    private final long connectReplyId;
    Http2ClientConnectionManager http2ConnectionManager;

    private ClientCorrelation correlation;
    private Http2ClientConnection http2ClientConnection;

    private final int initialFrameWindow;
    private int connectReplyFrameWindow;
    private int connectFrameWindow;
    private int acceptReplyFrameWindow;

    private final Http2FrameExtractor frameExtractor;
    private final Http2FrameDecoder frameDecoder;

    ClientConnectReplyStream(ClientStreamFactory factory, MessageConsumer connectReplyThrottle,
            long connectReplyId, Http2ClientConnectionManager http2ConnectionManager)
    {
        this.factory = factory;
        this.connectReplyId = connectReplyId;
        this.connectReplyThrottle = connectReplyThrottle;
        this.http2ConnectionManager = http2ConnectionManager;

        this.streamState = this::streamBeforeBegin;
        initialFrameWindow = factory.bufferPool.slotCapacity();
        frameExtractor = new Http2FrameExtractor(factory);
        frameDecoder = new Http2FrameDecoder(factory);
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
            factory.doWindow(connectReplyThrottle, connectReplyId, initialFrameWindow, initialFrameWindow);
            streamState = this::streamAfterBegin;
            BeginFW begin = this.factory.beginRO.wrap(buffer, index, index + length);
            final long correlationId = begin.correlationId();
            correlation = begin.sourceRef() == 0L ? factory.correlations.remove(correlationId) : null;
            http2ClientConnection = correlation.http2ClientConnection;
        }
        else
        {
            resetStream();
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
            // TODO what needs to be done ?
            break;
        default:
            resetStream();
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
            factory.doWindow(connectReplyThrottle, streamId, data.length(), data.length());
            connectReplyFrameWindow += data.length();
        }
    }
    // end stream handlers

    // resets the stream
    private void resetStream()
    {
        factory.doReset(connectReplyThrottle, connectReplyId);
        this.streamState = this::streamAfterReset;

        // TODO reset also the accept and accept reply streams

    }

    private void processData(DirectBuffer buffer, int index, int length)
    {
        DataFW data = factory.dataRO.wrap(buffer, index, index + length);
        OctetsFW payload = data.payload();

        int limit = payload.limit();
        int offset = payload.offset();

        while (offset < limit)
        {
            int processedbytes = frameExtractor.http2FrameAvailable(buffer, offset, limit,
                    http2ClientConnection.localSettings.maxFrameSize);
            if (processedbytes > 0)
            {
                offset += processedbytes;
                if (frameExtractor.isHttp2FrameAvailable)
                {
                    decodeHttp2Frame();
                }
            }
            else
            { // we have a decoding error close entire connection
                resetStream();
            }
        }

        factory.doWindow(connectReplyThrottle, data.streamId(), data.length(), data.length());
    }

    private void decodeHttp2Frame()
    {
        Http2FrameType frameType = factory.http2RO.type();

        if (frameDecoder.expectContinuation)
        {
            if (frameType != Http2FrameType.CONTINUATION || factory.http2RO.streamId() != frameDecoder.expectContinuationStreamId)
            {
                http2ClientConnection.doHttp2ConnectionError(Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
        }
        else if (frameType == Http2FrameType.CONTINUATION)
        {
            http2ClientConnection.doHttp2ConnectionError(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }

        switch (factory.http2RO.type())
        {
            case HEADERS:
                frameDecoder.headerStateReset();
                handleHttp2Headers();
                break;
            case CONTINUATION:
                handleHttp2Headers();
                break;
            case DATA:
                handleHttp2Data();
                break;
            case PRIORITY:
//                doPriority();
                break;
            case RST_STREAM:
//                doRst();
                break;
            case SETTINGS:
                http2ClientConnection.handleSettings(factory.http2RO);
                break;
            case PUSH_PROMISE:
//                doPushPromise();
                break;
            case PING:
//                doPing();
                break;
            case GO_AWAY:
//                doGoAway();
                break;
            case WINDOW_UPDATE:
//                doWindow();
                break;
            default:
                // Ignore and discard unknown frame
        }
    }

    // will handle the error codes available in Http2FrameDecoder
    private void handleDecodeError()
    {
        if (frameDecoder.connectionError != null)
        {
            http2ClientConnection.doHttp2ConnectionError(frameDecoder.connectionError);
        }
        else if (frameDecoder.streamError != null)
        {
            http2ClientConnection.doHttp2StreamError(factory.http2RO.streamId(), frameDecoder.streamError);
        }
    }

    private void handleHttp2Headers()
    {
        if (! frameDecoder.decodeHttp2Headers())
        {
            // headers are not available
            if (frameDecoder.hasErrors())
            {
                handleDecodeError();
            }
            return;
        }

        HttpBeginExFW beginEx = http2ClientConnection.decodeHttp2Headers(factory.blockRO, factory.http2RO.streamId());
        if (beginEx == null)
        { // we have a decoding error
            return;
        }

        // open the accept reply stream
        correlation.acceptReply = factory.router.supplyTarget(correlation.acceptReplyName);
        correlation.acceptReplyStreamId = factory.supplyStreamId.getAsLong();

        BeginFW begin = factory.beginRW.wrap(factory.writeBuffer, 0, factory.writeBuffer.capacity())
                .streamId(correlation.acceptReplyStreamId)
                .source(ClientStreamFactory.SOURCE_NAME_BUFFER, 0, ClientStreamFactory.SOURCE_NAME_BUFFER.capacity())
                .sourceRef(0L)
                .correlationId(correlation.acceptCorrelationId)
                .extension(beginEx.buffer(), beginEx.offset(), beginEx.sizeof())
                .build();

        correlation.acceptReply.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
        factory.router.setThrottle(correlation.acceptReplyName, correlation.acceptReplyStreamId, this::handleAcceptReplyThrottle);
    }

    // handles a http2 data frame
    private void handleHttp2Data()
    {
        int streamId = factory.http2RO.streamId();
        Http2ClientStream stream = http2ClientConnection.http2Streams.get(streamId);

        if (streamId == 0 || stream == null)
        {
            http2ClientConnection.doHttp2StreamError(streamId, Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }

        if (stream.state == HALF_CLOSED_REMOTE)
        {
            http2ClientConnection.doHttp2StreamError(streamId, Http2ErrorCode.STREAM_CLOSED);
            return;
        }

        if (stream.state == Http2ConnectionState.CLOSED)
        {
            http2ClientConnection.doHttp2ConnectionError(Http2ErrorCode.STREAM_CLOSED);
            return;
        }

        Http2DataFW http2Data = factory.http2DataRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(),
                factory.http2RO.limit());
        int dataLength = http2Data.dataLength();

        if (dataLength < 0)        // because of invalid padding length
        {
            http2ClientConnection.doHttp2StreamError(streamId, Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }

        // TODO validate and update windows

        stream.totalData += dataLength;

        int offset = http2Data.dataOffset();
        while (dataLength > 0)
        {
            int chunk = Math.min(dataLength, 65535);     // limit by nukleus DATA frame length (2 bytes)

            DataFW data = factory.dataRW.wrap(factory.writeBuffer, 0, factory.writeBuffer.capacity())
                    .streamId(correlation.acceptReplyStreamId)
                    .payload(http2Data.buffer(), offset, dataLength)
                    .build();

            correlation.acceptReply.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
            offset += chunk;
            dataLength -= chunk;
        }

        if (http2Data.endStream())
        {
            processEndStream(stream, correlation);
        }
    }

    private void processEndStream(Http2ClientStream stream, ClientCorrelation correlation)
    {
        // 8.1.2.6 A request is malformed if the value of a content-length header field does
        // not equal the sum of the DATA frame payload lengths
        if (stream.contentLength != -1 && stream.totalData != stream.contentLength)
        {
            http2ClientConnection.doHttp2StreamError(stream.http2StreamId, Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        stream.state = Http2ConnectionState.HALF_CLOSED_REMOTE;

        factory.doEnd(correlation.acceptReply, correlation.acceptReplyStreamId);
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
                // TODO not sure what needs to be done
                break;
            default:
                // ignore
                break;
        }
    }
}
