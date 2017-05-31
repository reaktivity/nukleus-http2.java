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

public class Http2WriteScheduler implements WriteScheduler
{
    private final SourceInputStreamFactory.SourceInputStream connection;
    private final NukleusWriteScheduler writer;
    private final Target target;

    private boolean eos;
    private int noEntries;

    Http2WriteScheduler(
            SourceInputStreamFactory.SourceInputStream connection,
            Target target,
            long targetId)
    {
        this.connection = connection;
        this.target = target;
        this.writer = new NukleusWriteScheduler(connection, target, targetId);
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
        if (stream == null) {
            return true;
        }
        boolean direct = stream.replyBuffer == null && length <= connection.http2OutWindow &&
                length <= stream.http2OutWindow;
        if (direct)
        {
            connection.http2OutWindow -= length;
            stream.http2OutWindow -= length;
            return writer.data(streamId, buffer, offset, length);
        }
        else
        {
            Flyweight.Builder.Visitor data = target.visitData(streamId, buffer, offset, length);
            return http2(streamId, length + 9, DATA, data);
        }
    }

    @Override
    public boolean dataEos(int streamId)
    {
        SourceInputStreamFactory.Http2Stream stream = connection.http2Streams.get(streamId);
        if (stream == null) {
            return true;
        }
        boolean direct = stream.replyBuffer == null &&
                0 <= connection.http2OutWindow &&
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
System.out.printf("adjust length = %d nuklei-window = %d con.http2-out-window = %d stream.http2-out-window=%d\n",
        length, connection.outWindow, connection.http2OutWindow, stream.http2OutWindow);
            assert type == DATA;

            connection.http2OutWindow -= (length - 9);
            stream.http2OutWindow -= (length - 9);
        }

    }

}
