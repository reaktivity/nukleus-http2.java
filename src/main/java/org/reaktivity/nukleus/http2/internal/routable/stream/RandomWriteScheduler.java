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

public class RandomWriteScheduler implements WriteScheduler
{
    private final long targetId;
    private final SourceInputStreamFactory.SourceInputStream connection;
    private final NukleusWriter writer;
    private final Target target;

    private boolean eos;
    private int noEntries;

    RandomWriteScheduler(
            SourceInputStreamFactory.SourceInputStream connection,
            Target target,
            long targetId)
    {
        this.connection = connection;
        this.target = target;
        this.targetId = targetId;
        this.writer = new NukleusWriter(connection, target, targetId);
    }

    public boolean http2(int streamId, int sizeof, Http2FrameType type, Flyweight.Builder.Visitor visitor)
    {
        assert !eos;

        SourceInputStreamFactory.Http2Stream stream = connection.http2Streams.get(streamId);
        stream.acquireReplyBuffer();
        CircularDirectBuffer cb = stream.replyBuffer;
        int offset = cb.writeOffset(sizeof);
        if (offset != -1)
        {
            int actualLength = visitor.visit(cb.buffer, offset, sizeof);
            cb.write(offset, actualLength);
            StreamEntry entry = new StreamEntry(stream, cb.buffer, offset, actualLength, type);
            stream.replyQueue.add(entry);
        }

        return offset != -1;
    }

    @Override
    public boolean windowUpdate(int streamId, int update)
    {
        return writer.windowUpdate(streamId, update);
    }

    @Override
    public boolean pingAck(DirectBuffer buffer, int offset, int length)
    {
        return writer.pingAck(buffer, offset, length);
    }

    @Override
    public boolean goaway(int lastStreamId, Http2ErrorCode errorCode)
    {
        return writer.goaway(lastStreamId, errorCode);
    }

    @Override
    public boolean rst(int streamId, Http2ErrorCode errorCode)
    {
        return writer.rst(streamId, errorCode);
    }

    @Override
    public boolean settings(int maxConcurrentStreams)
    {
        return writer.settings(maxConcurrentStreams);
    }

    @Override
    public boolean settingsAck()
    {
        return writer.settingsAck();
    }

    @Override
    public boolean headers(int streamId, ListFW<HttpHeaderFW> headers,
                           BiFunction<HttpHeaderFW, HpackHeaderFieldFW.Builder, HpackHeaderFieldFW> mapper)
    {
        return writer.headers(streamId, headers, mapper);
    }

    @Override
    public boolean pushPromise(int streamId, int promisedStreamId, ListFW<HttpHeaderFW> headers,
                               BiFunction<HttpHeaderFW, HpackHeaderFieldFW.Builder, HpackHeaderFieldFW> mapper)
    {
        return writer.pushPromise(streamId, promisedStreamId, headers, mapper);
    }

    @Override
    public boolean data(int streamId, DirectBuffer buffer, int offset, int length)
    {
        SourceInputStreamFactory.Http2Stream stream = connection.http2Streams.get(streamId);
        boolean direct = stream.replyBuffer == null && length <= connection.http2OutWindow &&
                length <= stream.http2OutWindow;
        if (direct)
        {
            stream.http2OutWindow -= length;
            return writer.data(streamId, buffer, offset, length);
        }
        else
        {
            Flyweight.Builder.Visitor data = target.visitData(streamId, buffer, offset, length);
            return http2(streamId, length + 9, DATA, data);
        }
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
        SourceInputStreamFactory.Http2Stream stream = connection.http2Streams.get(streamId);
        boolean direct = stream.replyBuffer == null && 0 <= connection.http2OutWindow &&
                0 <= stream.http2OutWindow;

        if (direct)
        {
            return writer.dataEos(streamId);
        }
        else
        {
            Flyweight.Builder.Visitor data = target.visitDataEos(streamId);
            return http2(streamId, 9, DATA, data);
        }
    }

    public void doEnd()
    {
        eos = true;
        if (noEntries == 0)
        {
            writer.doEnd();
        }
    }

    public void onHttp2Window()
    {
        boolean found = false;
        StreamEntry entry;

        while((entry = pop()) != null)
        {
            writer.data(entry.stream.http2StreamId, entry.buffer, entry.offset, entry.length);
            found = true;
        }
        if (found)
        {
            writer.onWindow();
        }
    }

    public void onHttp2Window(int streamId)
    {
        boolean found = false;
        StreamEntry entry;

        SourceInputStreamFactory.Http2Stream stream = connection.http2Streams.get(streamId);
        while ((entry = pop(stream)) != null)
        {
            writer.data(streamId, entry.buffer, entry.offset, entry.length);
            found = true;
        }

        if (found)
        {
            writer.onWindow();
        }
    }

    public void onWindow()
    {
        writer.onWindow();
    }

    private StreamEntry pop()
    {
        // TODO Map#values may not iterate randomly, randomly pick a stream ??
        // Select a frame on a HTTP2 stream that can be written
        for(SourceInputStreamFactory.Http2Stream stream : connection.http2Streams.values())
        {
            StreamEntry entry = pop(stream);
            if (entry != null)
            {
                return entry;
            }
        }

        return null;
    }

    private StreamEntry pop(SourceInputStreamFactory.Http2Stream stream)
    {
        if (stream.replyQueue != null)
        {
            StreamEntry entry = (StreamEntry) stream.replyQueue.peek();
            if (entry != null && entry.fits())
            {
                entry = (StreamEntry) stream.replyQueue.poll();
                stream.replyBuffer.read(entry.length);
                entry.adjustWindows();
                noEntries--;
                if (stream.replyQueue.isEmpty())
                {
                    stream.releaseReplyBuffer();
                }
                return entry;
            }
        }

        return null;
    }

    private class StreamEntry
    {
        final SourceInputStreamFactory.Http2Stream stream;
        final DirectBuffer buffer;
        final int offset;
        final int length;
        final Http2FrameType type;

        StreamEntry(
                SourceInputStreamFactory.Http2Stream stream,
                DirectBuffer buffer,
                int offset,
                int length,
                Http2FrameType type)
        {
            this.stream = stream;
            this.buffer = buffer;
            this.offset = offset;
            this.length = length;
            this.type = type;
            noEntries++;
        }

        boolean fits()
        {
            assert type == DATA;
            // TODO split DATA so that it fits
            return length <= connection.http2OutWindow && length <= stream.http2OutWindow;
        }

        void adjustWindows()
        {
System.out.printf("adjust length = %d nuklei-window = %d http2-out-window = %d\n", length, connection.outWindow,
        connection.http2OutWindow);
            assert type == DATA;

            connection.http2OutWindow -= (length - 9);
            stream.http2OutWindow -= (length - 9);
        }

    }

    class NukleusWriter implements WriteScheduler
    {
        private final SourceInputStreamFactory.SourceInputStream connection;
        private Target target;
        private final long targetId;

        NukleusWriter(
                SourceInputStreamFactory.SourceInputStream connection,
                Target target,
                long targetId)
        {
            this.connection = connection;
            this.target = target;
            this.targetId = targetId;
        }

        public boolean http2(int streamId, int lengthGuess, boolean direct, Http2FrameType type, Flyweight.Builder.Visitor visitor)
        {

            assert !eos;

            if (direct)
            {
                int actualLength = target.doHttp2(targetId, visitor);
                connection.outWindow -= actualLength;
                System.out.printf("NukleusWriter direct length = %d nuklei-window = %d http2-out-window = %d\n",
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
            throw new UnsupportedOperationException("TODO");
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
                    noEntries--;
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
            noEntries++;
        }

        boolean fits()
        {
            // TODO split DATA so that it fits
            return length <= connection.outWindow;
        }

        void adjustWindows()
        {
            connection.outWindow -= length;
            System.out.printf("adjust length = %d nuklei-window = %d http2-out-window = %d\n", length, connection.outWindow,
                    connection.http2OutWindow);
        }

    }

}
