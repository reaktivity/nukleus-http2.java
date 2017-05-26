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
import org.agrona.MutableDirectBuffer;
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

public class SimpleWriteScheduler implements WriteScheduler
{
    private final long targetId;
    private final SourceInputStreamFactory.SourceInputStream connection;

    private Target target;
    private boolean eos;
    private int noEntries;

    SimpleWriteScheduler(
            SourceInputStreamFactory.SourceInputStream connection,
            Target target,
            long targetId)
    {
        this.connection = connection;
        this.target = target;
        this.targetId = targetId;
    }

    public boolean http2(int streamId, boolean direct, Http2FrameType type, Flyweight.Builder.Visitor visitor)
    {
        assert !eos;

        if (direct)
        {
            int actualLength = target.doHttp2(targetId, visitor);
            connection.outWindow -= actualLength;
System.out.printf("direct length = %d nuklei-window = %d http2-out-window = %d\n", actualLength, connection.outWindow,
        connection.http2OutWindow);

            return true;
        }
        else
        {
            MutableDirectBuffer buffer;
            int offset;
            int actualLength;

            if (streamId == 0)
            {
                connection.acquireReplyBuffer(targetId);
                buffer = connection.replyBuffer;
                offset = connection.replySlotPosition;
                actualLength = visitor.visit(buffer, offset, buffer.capacity());
                connection.replySlotPosition += actualLength;
                WriteEntry entry = new WriteEntry(null, buffer, offset, actualLength, type);
                connection.replyQueue.add(entry);
            }
            else
            {
                SourceInputStreamFactory.Http2Stream stream = connection.http2Streams.get(streamId);
                stream.acquireReplyBuffer();
                buffer = stream.replyBuffer;
                offset = stream.replySlotPosition;
                actualLength = visitor.visit(buffer, offset, buffer.capacity());
                stream.replySlotPosition += actualLength;
                WriteEntry entry = new WriteEntry(stream, buffer, offset, actualLength, type);
                stream.replyQueue.add(entry);
            }
            return buffer != null;
        }
    }

    @Override
    public boolean windowUpdate(int streamId, int update)
    {
        Flyweight.Builder.Visitor window = target.visitWindowUpdate(streamId, update);
        int sizeof = 9 + 4;             // +9 for HTTP2 framing, +4 window size increment
        boolean direct = noEntries == 0 && sizeof <= connection.outWindow;
        return http2(streamId, direct, WINDOW_UPDATE, window);
    }

    @Override
    public boolean pingAck(DirectBuffer buffer, int offset, int length)
    {
        int sizeof = 9 + 8;             // +9 for HTTP2 framing, +8 for a ping
        boolean direct = noEntries == 0 && sizeof <= connection.outWindow;
        Flyweight.Builder.Visitor ping = target.visitPingAck(buffer, offset, length);
        return http2(0, direct, PING, ping);
    }

    @Override
    public boolean goaway(int lastStreamId, Http2ErrorCode errorCode)
    {
        int sizeof = 9 + 8;             // +9 for HTTP2 framing, +8 for goaway payload
        boolean direct = noEntries == 0 && sizeof <= connection.outWindow;

        Flyweight.Builder.Visitor goaway = target.visitGoaway(lastStreamId, errorCode);
        return http2(0, direct, GO_AWAY, goaway);
    }

    @Override
    public boolean rst(int streamId, Http2ErrorCode errorCode)
    {
        int sizeof = 9 + 4;             // +9 for HTTP2 framing, +4 for RST_STREAM payload
        boolean direct = noEntries == 0 && sizeof <= connection.outWindow;

        Flyweight.Builder.Visitor rst = target.visitRst(streamId, errorCode);
        return http2(streamId, direct, RST_STREAM, rst);
    }

    @Override
    public boolean settings(int maxConcurrentStreams)
    {
        int sizeof = 9 + 6;             // +9 for HTTP2 framing, +6 for a setting
        boolean direct = noEntries == 0 && sizeof <= connection.outWindow;
        Flyweight.Builder.Visitor settings = target.visitSettings(maxConcurrentStreams);
        return http2(0, direct, SETTINGS, settings);
    }

    @Override
    public boolean settingsAck()
    {
        int sizeof = 9;                 // +9 for HTTP2 framing
        boolean direct = noEntries == 0 && sizeof <= connection.outWindow;
        Flyweight.Builder.Visitor settings = target.visitSettingsAck();
        return http2(0, direct, SETTINGS, settings);
    }

    @Override
    public boolean headers(int streamId, ListFW<HttpHeaderFW> headers,
                           BiFunction<HttpHeaderFW, HpackHeaderFieldFW.Builder, HpackHeaderFieldFW> mapper)
    {
        int sizeof = 9 + headersLength(headers);    // +9 for HTTP2 framing
        boolean direct = noEntries == 0 && sizeof <= connection.outWindow;
        Flyweight.Builder.Visitor data = target.visitHeaders(streamId, headers, mapper);
        return http2(streamId, direct, HEADERS, data);
    }

    @Override
    public boolean pushPromise(int streamId, int promisedStreamId, ListFW<HttpHeaderFW> headers,
                               BiFunction<HttpHeaderFW, HpackHeaderFieldFW.Builder, HpackHeaderFieldFW> mapper)
    {
        int sizeof = 9 + 4 + headersLength(headers);    // +9 for HTTP2 framing, +4 for promised stream id
        boolean direct = noEntries == 0 && sizeof <= connection.outWindow;
        Flyweight.Builder.Visitor data = target.visitPushPromise(streamId, promisedStreamId, headers, mapper);
        return http2(streamId, direct, PUSH_PROMISE, data);
    }

    @Override
    public boolean data(int streamId, DirectBuffer buffer, int offset, int length)
    {
        int sizeof = 9 + length;    // +9 for HTTP2 framing
        SourceInputStreamFactory.Http2Stream stream = connection.http2Streams.get(streamId);

        boolean direct = noEntries == 0 && sizeof <= connection.outWindow && length <= connection.http2OutWindow &&
                length <= stream.http2OutWindow;
        if (direct)
        {
            stream.http2OutWindow -= length;
        }

        Flyweight.Builder.Visitor data = target.visitData(streamId, buffer, offset, length);
        return http2(streamId, direct, DATA, data);
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
        SourceInputStreamFactory.Http2Stream stream = connection.http2Streams.get(streamId);

        boolean direct = noEntries == 0 && sizeof <= connection.outWindow && 0 <= connection.http2OutWindow &&
                0 <= stream.http2OutWindow;

        Flyweight.Builder.Visitor data = target.visitDataEos(streamId);
        return http2(streamId, direct, DATA, data);
    }

    public void doEnd()
    {
        eos = true;
        if (noEntries == 0)
        {
            target.doEnd(targetId);
        }
    }

    public void flush()
    {
        WriteEntry entry;

        while((entry = pop()) != null)
        {
            noEntries--;
            target.doData(targetId, entry.buffer, entry.offset, entry.length);
            entry.adjustWindows();
        }
        if (noEntries == 0 && eos)
        {
            target.doEnd(targetId);
        }
    }

    private WriteEntry pop()
    {
        // First, select a frame (like SETTINGS, PING, etc) on connection
        if (connection.replyQueue != null)
        {
            WriteEntry entry = (WriteEntry) connection.replyQueue.peek();
            if (entry != null && entry.fits())
            {
                entry = (WriteEntry) connection.replyQueue.poll();
                if (connection.replyQueue.isEmpty())
                {
                    connection.releaseReplyBuffer();
                }
                return entry;
            }
        }

        // TODO randomly pick a stream
        // Select a frame on a HTTP2 stream that can be written
        for(SourceInputStreamFactory.Http2Stream stream : connection.http2Streams.values())
        {
            if (stream.replyQueue != null)
            {
                WriteEntry entry = (WriteEntry) stream.replyQueue.peek();
                if (entry != null && entry.fits())
                {
                    entry = (WriteEntry) stream.replyQueue.poll();
                    if (stream.replyQueue.isEmpty())
                    {
                        stream.releaseReplyBuffer();
                    }
                    return entry;
                }
            }
        }

        return null;
    }

    private class WriteEntry
    {
        final SourceInputStreamFactory.Http2Stream stream;
        final DirectBuffer buffer;
        final int offset;
        final int length;
        final Http2FrameType type;

        WriteEntry(
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
            // TODO split DATA so that it fits
            boolean fits = length <= connection.outWindow;
            if (fits && type == DATA)
            {
                fits = length <= connection.http2OutWindow && length <= stream.http2OutWindow;
            }
            return fits;
        }

        void adjustWindows()
        {
            connection.outWindow -= length;
System.out.printf("adjust length = %d nuklei-window = %d http2-out-window = %d\n", length, connection.outWindow,
        connection.http2OutWindow);

            if (type == DATA)
            {
                connection.http2OutWindow -= (length - 9);
                stream.http2OutWindow -= (length - 9);
            }
        }

    }

}
