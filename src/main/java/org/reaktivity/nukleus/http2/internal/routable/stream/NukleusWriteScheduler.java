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
package org.reaktivity.nukleus.http2.internal.routable.stream;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.http2.internal.routable.Target;
import org.reaktivity.nukleus.http2.internal.types.Flyweight;
import org.reaktivity.nukleus.http2.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType;

import java.util.function.BiFunction;

import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.DATA;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.GO_AWAY;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.HEADERS;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.PING;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.PUSH_PROMISE;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.RST_STREAM;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.SETTINGS;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.WINDOW_UPDATE;

class NukleusWriteScheduler implements WriteScheduler
{
    private final SourceInputStreamFactory.SourceInputStream connection;
    private final Target target;
    private final long targetId;

    NukleusWriteScheduler(
            SourceInputStreamFactory.SourceInputStream connection,
            Target target,
            long targetId)
    {
        this.connection = connection;
        this.target = target;
        this.targetId = targetId;
    }

    public boolean http2(
            int streamId,
            int lengthGuess,
            boolean direct,
            Http2FrameType type,
            Flyweight.Builder.Visitor visitor)
    {
        if (direct)
        {
            int actualLength = target.doHttp2(targetId, visitor);
            connection.outWindow -= actualLength;
            System.out.printf("NukleusWriter direct length = %d nuklei-window = %d con.http2-out-window = %d\n",
                    actualLength, connection.outWindow, connection.http2OutWindow);

            return true;
        }
        else
        {
            connection.acquireReplyBuffer(targetId);
            CircularDirectBuffer cb = connection.replyBuffer;

            int offset = cb.writeOffset(lengthGuess);
            int length = visitor.visit(cb.buffer, offset, lengthGuess);
            cb.write(offset, length);
            ConnectionEntry entry = new ConnectionEntry(cb.buffer, offset, length, lengthGuess, type);
            connection.replyQueue.add(entry);

            return offset != -1;
        }

    }

    @Override
    public boolean windowUpdate(int streamId, int update)
    {
        Flyweight.Builder.Visitor window = target.visitWindowUpdate(streamId, update);
        int sizeof = 9 + 4;             // +9 for HTTP2 framing, +4 window size increment
        boolean direct = connection.replyBuffer == null && sizeof <= connection.outWindow;
        return http2(streamId, sizeof, direct, WINDOW_UPDATE, window);
    }

    @Override
    public boolean pingAck(DirectBuffer buffer, int offset, int length)
    {
        int sizeof = 9 + 8;             // +9 for HTTP2 framing, +8 for a ping
        boolean direct = connection.replyBuffer == null && sizeof <= connection.outWindow;
        Flyweight.Builder.Visitor ping = target.visitPingAck(buffer, offset, length);
        return http2(0, sizeof, direct, PING, ping);
    }

    @Override
    public boolean goaway(int lastStreamId, Http2ErrorCode errorCode)
    {
        int sizeof = 9 + 8;             // +9 for HTTP2 framing, +8 for goaway payload
        boolean direct = connection.replyBuffer == null && sizeof <= connection.outWindow;

        Flyweight.Builder.Visitor goaway = target.visitGoaway(lastStreamId, errorCode);
        return http2(0, sizeof, direct, GO_AWAY, goaway);
    }

    @Override
    public boolean rst(int streamId, Http2ErrorCode errorCode)
    {
        int sizeof = 9 + 4;             // +9 for HTTP2 framing, +4 for RST_STREAM payload
        boolean direct = connection.replyBuffer == null && sizeof <= connection.outWindow;

        Flyweight.Builder.Visitor rst = target.visitRst(streamId, errorCode);
        return http2(streamId, sizeof, direct, RST_STREAM, rst);
    }

    @Override
    public boolean settings(int maxConcurrentStreams)
    {
        int sizeof = 9 + 6;             // +9 for HTTP2 framing, +6 for a setting
        boolean direct = connection.replyBuffer == null && sizeof <= connection.outWindow;
        Flyweight.Builder.Visitor settings = target.visitSettings(maxConcurrentStreams);
        return http2(0, sizeof, direct, SETTINGS, settings);
    }

    @Override
    public boolean settingsAck()
    {
        int sizeof = 9;                 // +9 for HTTP2 framing
        boolean direct = connection.replyBuffer == null && sizeof <= connection.outWindow;
        Flyweight.Builder.Visitor settings = target.visitSettingsAck();
        return http2(0, sizeof, direct, SETTINGS, settings);
    }

    @Override
    public boolean headers(int streamId, ListFW<HttpHeaderFW> headers,
                           BiFunction<HttpHeaderFW, HpackHeaderFieldFW.Builder, HpackHeaderFieldFW> mapper)
    {
        int sizeof = 9 + headersLength(headers);    // +9 for HTTP2 framing
        boolean direct = connection.replyBuffer == null && sizeof <= connection.outWindow;
        Flyweight.Builder.Visitor data = target.visitHeaders(streamId, headers, mapper);
        return http2(streamId, sizeof, direct, HEADERS, data);
    }

    @Override
    public boolean pushPromise(int streamId, int promisedStreamId, ListFW<HttpHeaderFW> headers,
                               BiFunction<HttpHeaderFW, HpackHeaderFieldFW.Builder, HpackHeaderFieldFW> mapper)
    {
        int sizeof = 9 + 4 + headersLength(headers);    // +9 for HTTP2 framing, +4 for promised stream id
        boolean direct = connection.replyBuffer == null && sizeof <= connection.outWindow;
        Flyweight.Builder.Visitor data = target.visitPushPromise(streamId, promisedStreamId, headers, mapper);
        return http2(streamId, sizeof, direct, PUSH_PROMISE, data);
    }

    @Override
    public boolean data(int streamId, DirectBuffer buffer, int offset, int length)
    {
        int sizeof = 9 + length;    // +9 for HTTP2 framing
        boolean direct = connection.replyBuffer == null && sizeof <= connection.outWindow;
        Flyweight.Builder.Visitor data = target.visitData(streamId, buffer, offset, length);
        return http2(streamId, sizeof, direct, DATA, data);
    }

    public void doEnd()
    {
        target.doEnd(targetId);
    }

    // Since it is not encoding, this gives an approximate length of header block
    private int headersLength(ListFW<HttpHeaderFW> headers)
    {
        int[] length = new int[1];
        headers.forEach(x -> length[0] += x.name().sizeof() + x.value().sizeof() + 4);
        return length[0];
    }

    @Override
    public boolean dataEos(int streamId)
    {
        int sizeof = 9;    // +9 for HTTP2 framing
        boolean direct = connection.replyBuffer == null && sizeof <= connection.outWindow;
        Flyweight.Builder.Visitor data = target.visitDataEos(streamId);
        return http2(streamId, sizeof, direct, DATA, data);
    }

    @Override
    public void onWindow()
    {
        ConnectionEntry entry;

        while ((entry = pop()) != null)
        {
            target.doData(targetId, connection.replyBuffer.buffer, entry.offset, entry.length);
        }
        if (connection.replyQueue != null && connection.replyQueue.isEmpty())
        {
            connection.releaseReplyBuffer();
        }
    }

    private ConnectionEntry pop()
    {
        if (connection.replyQueue != null)
        {
            ConnectionEntry entry = (ConnectionEntry) connection.replyQueue.peek();
            if (entry != null && entry.fits())
            {
                entry = (ConnectionEntry) connection.replyQueue.poll();
                connection.replyBuffer.read(entry.lengthGuess);
                entry.adjustWindows();
                return entry;
            }
        }

        return null;
    }

    @Override
    public void onHttp2Window()
    {
        throw new IllegalStateException();
    }

    @Override
    public void onHttp2Window(int streamId)
    {
        throw new IllegalStateException();
    }

    private class ConnectionEntry
    {
        final DirectBuffer buffer;
        final int offset;
        final int length;
        final int lengthGuess;
        final Http2FrameType type;

        ConnectionEntry(
                DirectBuffer buffer,
                int offset,
                int length,
                int lengthGuess,
                Http2FrameType type)
        {
            this.buffer = buffer;
            this.offset = offset;
            this.length = length;
            this.lengthGuess = lengthGuess;
            this.type = type;
        }

        boolean fits()
        {
            // TODO split DATA so that it fits
            return length <= connection.outWindow;
        }

        void adjustWindows()
        {
            connection.outWindow -= length;
            System.out.printf("adjust length = %d nuklei-window = %d con.http2-out-window = %d\n", length, connection.outWindow,
                    connection.http2OutWindow);
        }

    }

}
