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
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http2.internal.CircularDirectBuffer;
import org.reaktivity.nukleus.http2.internal.types.Flyweight;
import org.reaktivity.nukleus.http2.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderBlockFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW.Builder;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2Flags;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2GoawayFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2HeadersFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PingFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PrefaceFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2RstStreamFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2WindowUpdateFW;

import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;

import java.util.function.BiConsumer;

class Http2Writer
{
    private final DataFW.Builder dataRW = new DataFW.Builder();

    private final Http2SettingsFW.Builder settingsRW = new Http2SettingsFW.Builder();
    private final Http2RstStreamFW.Builder http2ResetRW = new Http2RstStreamFW.Builder();
    private final Http2GoawayFW.Builder goawayRW = new Http2GoawayFW.Builder();
    private final Http2PingFW.Builder pingRW = new Http2PingFW.Builder();
    private final Http2WindowUpdateFW.Builder http2WindowRW = new Http2WindowUpdateFW.Builder();
    private final Http2DataFW.Builder http2DataRW = new Http2DataFW.Builder();
    private final Http2HeadersFW.Builder http2HeadersRW = new Http2HeadersFW.Builder();
    private final Http2PrefaceFW.Builder prefaceRW = new Http2PrefaceFW.Builder();

    private final MessageConsumer target;
    private final Http2ClientConnection clientConnection;

    // buffer received as parameter where the data frames will be created before sending them to the target
    private final MutableDirectBuffer writeBuffer;

    // temporary buffer used to compose the http2 frames before copying them to the connection buffer
    private final int tmpBufferSlot;
    private final BufferPool tmpBufferPool;

    // Buffer to be used for all http2 frames except DATA frames, in case there is no nukleus window.
    // It contains fully formed http2 frames
    private int bufferSlot = NO_SLOT;
    CircularDirectBuffer circularBuffer;
    private final BufferPool connectionBufferPool;

    private int nukleusWindow;
    private int nukleusPadding;
    private int connectionWindow;

    private static final int HTTP2_FRAME_HEADER_SIZE = 9;
    private static final int NUKLEUS_MAX_FRAME_SIZE = 65535;

    Http2Writer(MutableDirectBuffer writeBuffer, MessageConsumer target,
                Http2ClientConnection clientConnection, BufferPool bufferPool)
    {
        this.writeBuffer = writeBuffer;
        this.target = target;
        this.clientConnection = clientConnection;
        this.connectionBufferPool = bufferPool.duplicate();
        this.tmpBufferPool = bufferPool.duplicate();

        connectionWindow = clientConnection.remoteSettings.initialWindowSize;
        tmpBufferSlot = tmpBufferPool.acquire(0);
    }

    void updateNukleusWindow(int update, int padding)
    {
        nukleusWindow += update;
        nukleusPadding = padding;
        flush();
    }

    // methods used to write http2 frames
    int doSettings(int maxConcurrentStreams, int initialWindowSize)
    {
        Flyweight.Builder.Visitor visitor = visitSettings(maxConcurrentStreams, initialWindowSize);
        return http2Frame(visitor);
    }

    int doSettingsAck()
    {
        return http2Frame(visitSettingsAck());
    }

    int doReset(int http2StreamId, Http2ErrorCode errorCode)
    {
        return http2Frame(visitRst(http2StreamId, errorCode));
    }

    int doPreface()
    {
        return http2Frame(visitPreface());
    }

    int doGoaway(int lastStreamId, Http2ErrorCode errorCode)
    {
        return http2Frame(visitGoaway(lastStreamId, errorCode));
    }

    int doHeaders(
        int http2StreamId,
        byte flags,
        ListFW<HttpHeaderFW> headers,
        BiConsumer<ListFW<HttpHeaderFW>, HpackHeaderBlockFW.Builder> builder)
    {
        return http2Frame(visitHeaders(http2StreamId, flags, headers, builder));
    }

    int doDataEos(int http2StreamId)
    {
        return http2Frame(visitDataEos(http2StreamId));
    }

    int doPingAck(DirectBuffer payloadBuffer, int payloadOffset, int payloadLength)
    {
        return http2Frame(visitPingAck(payloadBuffer, payloadOffset, payloadLength));
    }

    int doWindowUpdate(int http2StreamId, int update)
    {
        return http2Frame(visitWindowUpdate(http2StreamId, update));
    }

    int doData(
        int http2StreamId,
        DirectBuffer payloadBuffer,
        int payloadOffset,
        int payloadLength,
        boolean endStream)
    {
        Http2ClientStream stream = clientConnection.http2Streams.get(http2StreamId);

        if (stream.circularBuffer != null || payloadLength != calculateAvailableHttp2DataWindow(stream, payloadLength))
        { // put data in the buffer
            stream.circularBuffer.write(stream.acquireBuffer(), payloadBuffer, payloadOffset, payloadLength);
        }
        else
        { // write directly as it is only one frame and there is enough window
            return http2Frame(
                visitData(http2StreamId, payloadBuffer, payloadOffset, payloadLength, endStream))
                - HTTP2_FRAME_HEADER_SIZE;
        }

        return -1;
    }
    // end - methods used to write http2 frames

    // composes a http2 frame and writes it to the connectionBuffer.
    // This method should not be used directly for http2 DATA frames as it does not use the http2 windows/buffers.
    private int http2Frame(Flyweight.Builder.Visitor visitor)
    {
        MutableDirectBuffer connectionBuffer = acquireConnectionBuffer();
        MutableDirectBuffer tmpBuffer = acquireTmpBuffer();

        int length = visitor.visit(tmpBuffer, 0, tmpBuffer.capacity());
        if (circularBuffer.write(connectionBuffer, tmpBuffer, 0, length))
        {
            return length;
        }
        return -1;
    }

    void flush()
    {
        // write from the connectionBuffer
        if (circularBuffer != null)
        {
            while (nukleusWindow > nukleusPadding && circularBuffer.size() > 0)
            {
                // minimum between nukleus window (without padding), nukleus frame size and data length
                int toSend = circularBuffer.size() > nukleusWindow - nukleusPadding ?
                        nukleusWindow - nukleusPadding : circularBuffer.size();
                toSend = toSend > NUKLEUS_MAX_FRAME_SIZE ? NUKLEUS_MAX_FRAME_SIZE : toSend;

                int offset = circularBuffer.readOffset();
                toSend = circularBuffer.read(toSend); // limit the size by the contiguous block available

                MutableDirectBuffer buffer = acquireConnectionBuffer();
                DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                        .streamId(clientConnection.nukleusStreamId)
                        .payload(buffer, offset, toSend)
                        .build();
                target.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
                nukleusWindow -= toSend + nukleusPadding;
            }
        }

        // continue with the data buffers from the streams
        while (nukleusWindow > nukleusPadding && connectionWindow > 0)
        { // instead of random, just send one data frame for each stream until there is no more window
            boolean nothingWritten = true;
            for (Http2ClientStream stream : clientConnection.http2Streams.values())
            {
                if (stream.http2Window > 0 && stream.circularBuffer != null)
                {
                    int toSend = calculateAvailableHttp2DataWindow(stream, stream.circularBuffer.size());

                    int offset = stream.circularBuffer.readOffset();
                    toSend = stream.circularBuffer.read(toSend); // limit the size by the contiguous block available
                    int[] length = new int[1];
                    length[0] = toSend + HTTP2_FRAME_HEADER_SIZE;

                    MutableDirectBuffer buffer = stream.acquireBuffer();
                    DataFW data = dataRW.wrap(writeBuffer, 0, length[0])
                            .streamId(clientConnection.nukleusStreamId)
                            .payload(p -> p.set((b, o, l) -> (length[0])))
                            .build();
                    http2DataRW.wrap(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD, length[0])
                            .streamId(stream.http2StreamId)
                            .payload(buffer, offset, DataFW.FIELD_OFFSET_PAYLOAD + length[0])
                            .build();
                    target.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
                    connectionWindow -= toSend;
                    stream.http2Window -= toSend;
                    nukleusWindow -= toSend + HTTP2_FRAME_HEADER_SIZE + nukleusPadding;
                    nothingWritten = false;
                }

                if (nukleusWindow <= nukleusPadding || connectionWindow == 0)
                { // we have reached the end of one of the windows
                    break;
                }
            }
            if (nothingWritten)
            { // no buffered data in any of the connections, just exit
                break;
            }
        }
    }

    // calculates minimum between nukleus frame size(minus http2 header), nukleus window, connection window,
    //                 stream window, http2 frame size and data length
    private int calculateAvailableHttp2DataWindow(Http2ClientStream stream, int length)
    {
        int availableNukleusWindow = nukleusWindow - nukleusPadding - HTTP2_FRAME_HEADER_SIZE;
        int toSend = length > NUKLEUS_MAX_FRAME_SIZE - HTTP2_FRAME_HEADER_SIZE ?
                NUKLEUS_MAX_FRAME_SIZE - HTTP2_FRAME_HEADER_SIZE : length;
        int toSend2 = connectionWindow > stream.http2Window ? stream.http2Window : connectionWindow;
        int toSend3 = clientConnection.remoteSettings.maxFrameSize > availableNukleusWindow ?
                availableNukleusWindow : clientConnection.remoteSettings.maxFrameSize;
        toSend = toSend > toSend2 ? toSend2 : toSend;
        toSend = toSend > toSend3 ? toSend3 : toSend;
        return toSend;
    }

    private MutableDirectBuffer acquireConnectionBuffer()
    {
        if (bufferSlot == NO_SLOT)
        {
            bufferSlot = connectionBufferPool.acquire(0);
            if (bufferSlot != NO_SLOT)
            {
                circularBuffer = new CircularDirectBuffer(connectionBufferPool.slotCapacity());
            }
        }
        return bufferSlot != NO_SLOT ? connectionBufferPool.buffer(bufferSlot) : null;
    }

    void releaseConnectionBuffer()
    {
        if (bufferSlot != NO_SLOT)
        {
            connectionBufferPool.release(bufferSlot);
            bufferSlot = NO_SLOT;
            circularBuffer = null;
        }
    }

    private MutableDirectBuffer acquireTmpBuffer()
    {
        return tmpBufferPool.buffer(tmpBufferSlot);
    }

    // method that will release all used buffers, should be called when connection is ended
    void doCleanup()
    {
        releaseConnectionBuffer();
        tmpBufferPool.release(tmpBufferSlot);

        for (Http2ClientStream stream : clientConnection.http2Streams.values())
        {
            stream.releaseBuffer();
        }
    }

    // visitors
    private Flyweight.Builder.Visitor visitSettings(
            int maxConcurrentStreams,
            int initialWindowSize)
    {
        return (buffer, offset, limit) ->
                settingsRW.wrap(buffer, offset, limit)
                          .maxConcurrentStreams(maxConcurrentStreams)
                          .initialWindowSize(initialWindowSize)
                          .build()
                          .sizeof();
    }

    private Flyweight.Builder.Visitor visitSettingsAck()
    {
        return (buffer, offset, limit) ->
                settingsRW.wrap(buffer, offset, limit)
                          .ack()
                          .build()
                          .sizeof();
    }

    private Flyweight.Builder.Visitor visitRst(
            int http2StreamId,
            Http2ErrorCode errorCode)
    {
        return (buffer, offset, limit) ->
                http2ResetRW.wrap(buffer, offset, limit)
                       .streamId(http2StreamId)
                       .errorCode(errorCode)
                       .build()
                       .sizeof();
    }

    private Flyweight.Builder.Visitor visitGoaway(
            int lastStreamId,
            Http2ErrorCode errorCode)
    {
        return (buffer, offset, limit) ->
                goawayRW.wrap(buffer, offset, limit)
                        .lastStreamId(lastStreamId)
                        .errorCode(errorCode)
                        .build()
                        .sizeof();
    }

    private Flyweight.Builder.Visitor visitPingAck(
            DirectBuffer payloadBuffer, int payloadOffset, int payloadLength)
    {
        return (buffer, offset, limit) ->
                pingRW.wrap(buffer, offset, limit)
                      .ack()
                      .payload(payloadBuffer, payloadOffset, payloadLength)
                      .build()
                      .sizeof();
    }

    private Flyweight.Builder.Visitor visitWindowUpdate(
            int http2StreamId,
            int update)
    {
        return (buffer, offset, limit) ->
                http2WindowRW.wrap(buffer, offset, limit)
                        .streamId(http2StreamId)
                        .size(update)
                        .build()
                        .sizeof();
    }

    private Flyweight.Builder.Visitor visitData(
            int http2StreamId,
            DirectBuffer payloadBuffer,
            int payloadOffset,
            int payloadLength,
            boolean endStream)
    {
        return (buffer, offset, limit) ->
            {
                Builder builder = http2DataRW.wrap(buffer, offset, limit)
                           .streamId(http2StreamId)
                           .payload(payloadBuffer, payloadOffset, payloadLength);
                if (endStream)
                {
                    builder.endStream();
                }
                return builder.build().sizeof();
            };
    }

    private Flyweight.Builder.Visitor visitDataEos(int http2StreamId)
    {
        assert http2StreamId != 0;

        return (buffer, offset, limit) ->
                http2DataRW.wrap(buffer, offset, limit)
                           .streamId(http2StreamId)
                           .endStream()
                           .build()
                           .sizeof();
    }

    private Flyweight.Builder.Visitor visitHeaders(
            int http2StreamId,
            byte flags,
            ListFW<HttpHeaderFW> headers,
            BiConsumer<ListFW<HttpHeaderFW>, HpackHeaderBlockFW.Builder> builder)
    {
        byte headersFlags = (byte) (flags | Http2Flags.END_HEADERS);

        return (buffer, offset, limit) ->
                http2HeadersRW.wrap(buffer, offset, limit)
                              .streamId(http2StreamId)
                              .flags(headersFlags)
                              .headers(b -> builder.accept(headers, b))
                              .build()
                              .sizeof();
    }

    private Flyweight.Builder.Visitor visitPreface()
    {
        return (buffer, offset, limit) ->
                prefaceRW.wrap(buffer, offset, limit)
                          .build()
                          .sizeof();
    }
}
