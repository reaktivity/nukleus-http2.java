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
import org.reaktivity.nukleus.http2.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http2.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2WindowUpdateFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http2.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http2.internal.types.stream.WindowFW;

class ClientConnectReplyStream
{
    private MessageConsumer streamState;

    private final ClientStreamFactory factory;
    private final MessageConsumer connectReplyThrottle;
    private final long connectReplyId;

    private Http2ClientConnection http2ClientConnection;

    private final int initialFrameWindow;
    private int connectReplyFrameWindow;
    private static final double INWINDOW_THRESHOLD = 0.5;

    private final Http2FrameExtractor frameExtractor;
    private final Http2HeaderFrameAssembler frameDecoder;

    ClientConnectReplyStream(ClientStreamFactory factory, MessageConsumer connectReplyThrottle,
            long connectReplyId)
    {
        this.factory = factory;
        this.connectReplyId = connectReplyId;
        this.connectReplyThrottle = connectReplyThrottle;

        this.streamState = this::streamBeforeBegin;
        initialFrameWindow = factory.bufferPool.slotCapacity();
        frameExtractor = new Http2FrameExtractor(factory);
        frameDecoder = new Http2HeaderFrameAssembler(factory);
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
            factory.doWindow(connectReplyThrottle, connectReplyId, initialFrameWindow, 0);
            connectReplyFrameWindow = initialFrameWindow;
            streamState = this::streamAfterBegin;
            BeginFW begin = this.factory.beginRO.wrap(buffer, index, index + length);
            final long correlationId = begin.correlationId();
            http2ClientConnection = begin.sourceRef() == 0L ? factory.correlations.remove(correlationId) : null;
        }
        else
        {
            resetConnectReplyStream();
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
        case AbortFW.TYPE_ID:
            endConnection();
            break;
        default:
            resetConnectReplyStream();
            break;
        }
    }

    private void endConnection()
    {
        // send abort on all http connections
        for (Http2ClientStream stream : http2ClientConnection.http2Streams.values())
        {
            factory.doAbort(http2ClientConnection.acceptReply, stream.acceptReplyStreamId);
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
            factory.doWindow(connectReplyThrottle, streamId, data.length(), 0);
            connectReplyFrameWindow += data.length();
        }
    }
    // end stream handlers

    // resets the stream
    private void resetConnectReplyStream()
    {
        factory.doReset(connectReplyThrottle, connectReplyId);
        this.streamState = this::streamAfterReset;

        // TODO should we reset also the accept and accept reply streams ?

    }

    private void processData(DirectBuffer buffer, int index, int length)
    {
        DataFW data = factory.dataRO.wrap(buffer, index, index + length);

        // handle nukleus window
        connectReplyFrameWindow -= data.length();
        if (connectReplyFrameWindow < 0)
        {
            factory.doReset(connectReplyThrottle, data.streamId());
            return;
        }

        if (connectReplyFrameWindow < initialFrameWindow * INWINDOW_THRESHOLD)
        {
            factory.doWindow(connectReplyThrottle, data.streamId(), initialFrameWindow - connectReplyFrameWindow, 0);
            connectReplyFrameWindow = initialFrameWindow;
        }
        // end handle nukleus window

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
            { // we have a decoding error, close entire connection
                resetConnectReplyStream();
                return;
            }
        }
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

        if (factory.http2RO.streamId() > 0 && factory.http2RO.streamId() % 2 == 0)
        { // push promise streams - ignore for now
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
                handleResetStream();
                break;
            case SETTINGS:
                http2ClientConnection.handleSettings(factory.http2RO);
                break;
            case PUSH_PROMISE:
                handlePushPromise();
                break;
            case PING:
                handlePing();
                break;
            case GO_AWAY:
                http2ClientConnection.cleanupRequests();
                break;
            case WINDOW_UPDATE:
                Http2WindowUpdateFW windowUpdate = factory.http2WindowUpdateRO.wrap(
                        factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());
                if (factory.http2RO.streamId() > 0)
                {
                    http2ClientConnection.updateAcceptStreamWindow(windowUpdate.streamId(), windowUpdate.size());
                }
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
        if (! frameDecoder.assembleHttp2Headers())
        {
            // headers are not available
            if (frameDecoder.hasErrors())
            {
                handleDecodeError();
            }
            return;
        }

        HttpBeginExFW beginEx = http2ClientConnection.decodeHttp2Headers(factory.blockRO, factory.http2RO.streamId());

        //we read the headers block, we can release it
        frameDecoder.headerStateReset();
        if (beginEx == null)
        { // we have a decoding error
            return;
        }

        Http2ClientStream http2Stream = http2ClientConnection.http2Streams.get(factory.http2RO.streamId());

        // open the accept reply stream
        http2ClientConnection.acceptReply = factory.router.supplyTarget(http2ClientConnection.acceptReplyName);
        long acceptReplyStreamId = factory.supplyStreamId.getAsLong();

        System.out.println("AR BEGIN id:" + acceptReplyStreamId + " corel:" + http2Stream.acceptCorrelationId);
        BeginFW begin = factory.beginRW.wrap(factory.writeBuffer, 0, factory.writeBuffer.capacity())
                .streamId(acceptReplyStreamId)
                .source(ClientStreamFactory.SOURCE_NAME_BUFFER, 0, ClientStreamFactory.SOURCE_NAME_BUFFER.capacity())
                .sourceRef(0L)
                .correlationId(http2Stream.acceptCorrelationId)
                .extension(beginEx.buffer(), beginEx.offset(), beginEx.sizeof())
                .build();

        http2ClientConnection.acceptReply.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
        factory.router.setThrottle(http2ClientConnection.acceptReplyName, acceptReplyStreamId, this::handleAcceptReplyThrottle);

        // save acceptReplyStreamId into the stream
        http2Stream.acceptReplyStreamId = acceptReplyStreamId;
        http2ClientConnection.http2StreamsByAcceptReplyIds.put(acceptReplyStreamId, http2Stream);
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

        if (dataLength < 0) // because of invalid padding length
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
                    .streamId(stream.acceptReplyStreamId)
                    .payload(http2Data.buffer(), offset, dataLength)
                    .build();

            http2ClientConnection.acceptReply.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
            offset += chunk;
            dataLength -= chunk;
        }

        if (http2Data.endStream())
        {
            processEndStream(stream);
        }
    }

    private void handlePushPromise()
    {
        // not implemented so far
    }

    private void handleResetStream()
    {
        int streamId = factory.http2RO.streamId();
        Http2ClientStream stream = http2ClientConnection.http2Streams.get(streamId);

        http2ClientConnection.closeStream(streamId, true);

        if (http2ClientConnection.acceptReply != null)
        {
            factory.doAbort(http2ClientConnection.acceptReply, stream.acceptReplyStreamId);
        }
    }

    private void handlePing()
    {
        if (factory.http2RO.streamId() != 0)
        {
            http2ClientConnection.doHttp2ConnectionError(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        if (factory.http2RO.payloadLength() != 8)
        {
            http2ClientConnection.doHttp2ConnectionError(Http2ErrorCode.FRAME_SIZE_ERROR);
            return;
        }
        factory.http2PingRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());

        if (!factory.http2PingRO.ack())
        {
            http2ClientConnection.doPingAck(factory.http2PingRO.payload(), 0, factory.http2PingRO.payload().capacity());
        }
    }

    private void processEndStream(Http2ClientStream stream)
    {
        // 8.1.2.6 A request is malformed if the value of a content-length header field does
        // not equal the sum of the DATA frame payload lengths
        if (stream.contentLength != -1 && stream.totalData != stream.contentLength)
        {
            http2ClientConnection.doHttp2StreamError(stream.http2StreamId, Http2ErrorCode.PROTOCOL_ERROR);
            stream.state = Http2ConnectionState.CLOSED;
        }
        else
        {
            if (stream.endSent)
            {
                http2ClientConnection.closeStream(stream.http2StreamId, false);
            }
            else
            {
                stream.state = Http2ConnectionState.HALF_CLOSED_REMOTE;
            }
        }

        factory.doEnd(http2ClientConnection.acceptReply, stream.acceptReplyStreamId);
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
                int update = factory.windowRO.credit();
                int padding = factory.windowRO.padding();

                Http2ClientStream stream = http2ClientConnection.http2StreamsByAcceptReplyIds.get(factory.windowRO.streamId());
                if (stream == null)
                { // if http2 stream not found, abort stream
                    factory.doAbort(http2ClientConnection.acceptReply, factory.windowRO.streamId());
                    return;
                }

                // we send http2 window update
                http2ClientConnection.handleAcceptReplyThrottle(stream, update, padding);

                // and then we update the nukleus window
                connectReplyFrameWindow += update;
                factory.doWindow(connectReplyThrottle, connectReplyId, update, padding);
                break;
            case ResetFW.TYPE_ID:
                factory.resetRO.wrap(buffer, index, index + length);
                break;
            default:
                // ignore
                break;
        }
    }
}
