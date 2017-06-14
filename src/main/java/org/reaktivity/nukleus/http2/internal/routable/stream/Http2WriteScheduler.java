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
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.http2.internal.routable.Target;
import org.reaktivity.nukleus.http2.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;

import java.util.function.Consumer;

public class Http2WriteScheduler implements WriteScheduler
{
    private static final DirectBuffer EMPTY = new UnsafeBuffer(new byte[0]);

    private final MutableDirectBuffer read = new UnsafeBuffer(new byte[0]);
    private final SourceInputStreamFactory.SourceInputStream connection;
    private final NukleusWriteScheduler writer;

    private boolean end;
    private boolean endSent;
    private int entryCount;

    Http2WriteScheduler(
            SourceInputStreamFactory.SourceInputStream connection,
            long sourceOutputEstId,
            Slab slab,
            Target target,
            long targetId)
    {
        this.connection = connection;
        this.writer = new NukleusWriteScheduler(connection, sourceOutputEstId, slab, target, targetId);
    }

    public boolean http2(int streamId, DirectBuffer buffer, int offset, int length, Consumer<Integer> progress)
    {
        SourceInputStreamFactory.Http2Stream stream = connection.http2Streams.get(streamId);
        MutableDirectBuffer dstBuffer = stream.acquireReplyBuffer(this::read);
        CircularDirectBuffer cb = stream.replyBuffer;
        boolean written = cb.write(dstBuffer, buffer, offset, length);
        if (written)
        {
            StreamEntry entry = new StreamEntry(stream, length, progress);
            stream.replyQueue.add(entry);
            onHttp2Window(streamId);                // Make progress with partial write
        }

        return written;
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
    public boolean headers(int streamId, ListFW<HttpHeaderFW> headers)
    {
        return writer.headers(streamId, headers);
    }

    @Override
    public boolean pushPromise(int streamId, int promisedStreamId, ListFW<HttpHeaderFW> headers,
                               Consumer<Integer> progress)
    {
        return writer.pushPromise(streamId, promisedStreamId, headers, progress);
    }

    @Override
    public boolean data(int streamId, DirectBuffer buffer, int offset, int length, Consumer<Integer> progress)
    {
        SourceInputStreamFactory.Http2Stream stream = connection.http2Streams.get(streamId);
        if (stream == null)
        {
            return true;
        }

        if (!buffered(stream) && length <= connection.http2OutWindow && length <= stream.http2OutWindow)
        {
            connection.http2OutWindow -= length;
            stream.http2OutWindow -= length;
            stream.totalOutData += length;
            return writer.data(streamId, buffer, offset, length, progress);
        }
        else
        {
            return http2(streamId, buffer, offset, length, progress);
        }
    }

    @Override
    public boolean dataEos(int streamId)
    {
        SourceInputStreamFactory.Http2Stream stream = connection.http2Streams.get(streamId);
        if (stream == null)
        {
            return true;
        }
        boolean direct = !buffered(stream) && 0 <= connection.http2OutWindow && 0 <= stream.http2OutWindow;
        if (direct)
        {
            return writer.dataEos(streamId);
        }
        stream.endStream = true;
        return true;
    }

    @Override
    public void doEnd()
    {
        end = true;
        if (entryCount == 0 && !endSent)
        {
            endSent = true;
            writer.doEnd();
        }
    }

    @Override
    public void onHttp2Window()
    {
        StreamEntry entry;
        while((entry = pop()) != null)
        {
            write(entry);

            if (!buffered(entry.stream))
            {
                entry.stream.releaseReplyBuffer();

                if (entry.stream.endStream)
                {
                    entry.stream.endStream = false;
                    writer.dataEos(entry.stream.http2StreamId);
                }
            }
        }

        if (entryCount == 0 && end && !endSent)
        {
            endSent = true;
            writer.doEnd();
        }
    }

    @Override
    public void onHttp2Window(int streamId)
    {
        SourceInputStreamFactory.Http2Stream stream = connection.http2Streams.get(streamId);

        StreamEntry entry;
        while ((entry = pop(stream)) != null)
        {
            write(entry);
        }

        if (!buffered(stream))
        {
            stream.releaseReplyBuffer();

            if (stream.endStream)
            {
                stream.endStream = false;
                writer.dataEos(streamId);
            }
        }

        if (entryCount == 0 && end && !endSent)
        {
            endSent = true;
            writer.doEnd();
        }
    }

    private void write(StreamEntry entry)
    {
        // ring buffer may have split the entry into two parts
        DirectBuffer read = entry.stream.acquireReplyBuffer(this::read);
        int offset1 = entry.stream.replyBuffer.readOffset();
        int read1 = entry.stream.replyBuffer.read(entry.length);
        writer.data(entry.stream.http2StreamId, read, offset1, read1, entry.progress);

        if (read1 != entry.length)
        {
            int offset2 = entry.stream.replyBuffer.readOffset();
            int read2 = entry.stream.replyBuffer.read(entry.length - read1);
            assert read1 + read2 == entry.length;
            writer.data(entry.stream.http2StreamId, read, offset2, read2, entry.progress);
        }

        entry.adjustWindows();
    }

    @Override
    public void onWindow()
    {
        writer.onWindow();
    }

    private boolean buffered(SourceInputStreamFactory.Http2Stream stream)
    {
        return stream.replyQueue != null && !stream.replyQueue.isEmpty();
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
        if (buffered(stream))
        {
            StreamEntry entry = (StreamEntry) stream.replyQueue.peek();
            if (entry.fits())
            {
                entryCount--;
                return (StreamEntry) stream.replyQueue.poll();
            }
        }

        return null;
    }

    private MutableDirectBuffer read(MutableDirectBuffer buffer)
    {
        read.wrap(buffer.addressOffset(), buffer.capacity());
        return read;
    }

    private class StreamEntry
    {
        final SourceInputStreamFactory.Http2Stream stream;
        final int length;
        private final Consumer<Integer> progress;

        StreamEntry(
                SourceInputStreamFactory.Http2Stream stream,
                int length,
                Consumer<Integer> progress)
        {
            this.stream = stream;
            this.length = length;
            this.progress = progress;
            entryCount++;
        }

        boolean fits()
        {
            int min = Math.min(Math.min(length, (int) connection.http2OutWindow), (int) stream.http2OutWindow);
            if (min > 0)
            {
                int remaining = length - min;
                if (remaining > 0)
                {
                    entryCount--;
                    stream.replyQueue.poll();
                    StreamEntry entry1 = new StreamEntry(stream, min, progress);
                    StreamEntry entry2 = new StreamEntry(stream, remaining, progress);

                    stream.replyQueue.addFirst(entry2);
                    stream.replyQueue.addFirst(entry1);
                }
            }

            return min > 0;
        }

        void adjustWindows()
        {
            connection.http2OutWindow -= length;
            stream.http2OutWindow -= length;
            stream.totalOutData += length;
        }

        public String toString()
        {
            return String.format("length=%d", length);
        }

    }

}
