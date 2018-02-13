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
package org.reaktivity.nukleus.http2.internal;

import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.DATA;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.GO_AWAY;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.HEADERS;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.PING;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.PUSH_PROMISE;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.RST_STREAM;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.SETTINGS;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.WINDOW_UPDATE;

import java.util.Deque;
import java.util.LinkedList;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.MemoryManager;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http2.internal.types.Flyweight;
import org.reaktivity.nukleus.http2.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.stream.AckFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderBlockFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType;
import org.reaktivity.nukleus.http2.internal.types.stream.RegionFW;
import org.reaktivity.nukleus.http2.internal.types.stream.TransferFW;

public class Http2WriteScheduler implements WriteScheduler
{
    private final Http2Connection connection;
    private final Http2Writer http2Writer;
    private final NukleusWriteScheduler writer;
    private final Deque<WriteScheduler.Entry> replyQueue;
    private final MemoryManager memoryManager;
    private final long targetId;

    private boolean end;
    private boolean endSent;
    private int entryCount;

    Http2WriteScheduler(
            MemoryManager memoryManager,
            Http2Connection connection,
            MessageConsumer networkConsumer,
            Http2Writer http2Writer,
            long targetId)
    {
        this.memoryManager = memoryManager;
        this.connection = connection;
        this.http2Writer = http2Writer;
        this.targetId = targetId;
        this.writer = new NukleusWriteScheduler(memoryManager, connection, networkConsumer, http2Writer, targetId);
        this.replyQueue = new LinkedList<>();
    }

    @Override
    public boolean windowUpdate(int streamId, int update)
    {
        int length = 4;                     // 4 window size increment
        int sizeof = length + 9;            // +9 for HTTP2 framing
        Http2FrameType type = WINDOW_UPDATE;
        Http2Stream stream = stream(streamId);
        Flyweight.Builder.Visitor visitor = http2Writer.visitWindowUpdate(streamId, update);

        if (!buffered() && hasSpaceForFrames(length))
        {
            writeHttp2FrameAndFlush(stream, type, sizeof, visitor);
        }
        else
        {
            Entry entry = new Entry(stream, streamId, length, type, visitor);
            addEntry(entry);
        }
        return true;
    }

    @Override
    public boolean pingAck(DirectBuffer buffer, int offset, int length)
    {
        assert length == 8;

        int streamId = 0;
        int sizeof = 9 + length;             // +9 for HTTP2 framing, +8 for a ping
        Http2FrameType type = PING;

        if (!buffered() && hasSpaceForFrames(length))
        {
            Flyweight.Builder.Visitor visitor = http2Writer.visitPingAck(buffer, offset, length);
            writeHttp2FrameAndFlush(null, type, sizeof, visitor);
        }
        else
        {
            MutableDirectBuffer copy = new UnsafeBuffer(new byte[8]);
            copy.putBytes(0, buffer, offset, length);
            Flyweight.Builder.Visitor visitor = http2Writer.visitPingAck(copy, 0, length);
            Entry entry = new Entry(null, streamId, length, type, visitor);
            addEntry(entry);
        }

        return true;
    }

    @Override
    public boolean goaway(int lastStreamId, Http2ErrorCode errorCode)
    {
        int streamId = 0;
        int length = 8;                     // 8 for goaway payload
        int sizeof = length + 9;            // +9 for HTTP2 framing
        Flyweight.Builder.Visitor goaway = http2Writer.visitGoaway(lastStreamId, errorCode);
        Http2FrameType type = GO_AWAY;

        if (!buffered() && hasSpaceForFrames(length))
        {
            writeHttp2FrameAndFlush(null, type, sizeof, goaway);
        }
        else
        {
            Entry entry = new Entry(null, streamId, length, type, goaway);
            addEntry(entry);
        }

        return true;
    }

    @Override
    public boolean rst(int streamId, Http2ErrorCode errorCode)
    {
        int length = 4;                     // 4 for RST_STREAM payload
        int sizeof = length + 9;            // +9 for HTTP2 framing
        Flyweight.Builder.Visitor visitor = http2Writer.visitRst(streamId, errorCode);
        Http2Stream stream = stream(streamId);
        Http2FrameType type = RST_STREAM;

        if (!buffered() && hasSpaceForFrames(length))
        {
            writeHttp2FrameAndFlush(stream, type, sizeof, visitor);
        }
        else
        {
            Entry entry = new Entry(stream, streamId, length, type, visitor);
            addEntry(entry);
        }

        return true;
    }

    @Override
    public boolean settings(int maxConcurrentStreams)
    {
        int streamId = 0;
        int length = 6;                     // 6 for a setting
        int sizeof = length + 9;            // +9 for HTTP2 framing
        Flyweight.Builder.Visitor settings = http2Writer.visitSettings(maxConcurrentStreams);
        Http2FrameType type = SETTINGS;

        if (!buffered() && hasSpaceForFrames(length))
        {
            writeHttp2FrameAndFlush(null, type, sizeof, settings);
        }
        else
        {
            Entry entry = new Entry(null, streamId, length, type, settings);
            addEntry(entry);
        }

        return true;
    }

    @Override
    public boolean settingsAck()
    {
        int streamId = 0;
        int length = 0;
        int sizeof = length + 9;                 // +9 for HTTP2 framing
        Flyweight.Builder.Visitor visitor = http2Writer.visitSettingsAck();
        Http2FrameType type = SETTINGS;

        if (!buffered() && hasSpaceForFrames(length))
        {
            writeHttp2FrameAndFlush(null, type, sizeof, visitor);
        }
        else
        {
            Entry entry = new Entry(null, streamId, length, type, visitor);
            addEntry(entry);
        }

        return true;
    }

    @Override
    public boolean headers(int streamId, byte flags, ListFW<HttpHeaderFW> headers)
    {
        MutableDirectBuffer copy = null;
        int length = headersLength(headers);        // estimate only
        int sizeof = 9 + headersLength(headers);    // +9 for HTTP2 framing
        Http2FrameType type = HEADERS;
        Http2Stream stream = stream(streamId);

        if (buffered() || !hasSpaceForFrames(length))
        {
            copy = new UnsafeBuffer(new byte[8192]);
            connection.factory.blockRW.wrap(copy, 0, copy.capacity());
            connection.mapHeaders(headers, connection.factory.blockRW);
            HpackHeaderBlockFW block = connection.factory.blockRW.build();
            length = block.sizeof();
            sizeof = 9 + length;
        }

        if (buffered() || !hasSpaceForFrames(length))
        {
            Flyweight.Builder.Visitor visitor = http2Writer.visitHeaders(streamId, flags, copy, 0, length);
            Entry entry = new Entry(stream, streamId, length, type, visitor);
            addEntry(entry);
        }
        else
        {
            Flyweight.Builder.Visitor visitor = http2Writer.visitHeaders(streamId, flags, headers, connection::mapHeaders);
            writeHttp2FrameAndFlush(stream, type, sizeof, visitor);
        }

        return true;
    }

    @Override
    public boolean pushPromise(int streamId, int promisedStreamId, ListFW<HttpHeaderFW> headers)
    {
        MutableDirectBuffer copy = null;
        int length = headersLength(headers);            // estimate only
        int sizeof = 9 + 4 + length;                    // +9 for HTTP2 framing, +4 for promised stream id
        Http2FrameType type = PUSH_PROMISE;
        Http2Stream stream = stream(streamId);

        if (buffered() || !hasSpaceForFrames(length))
        {
            copy = new UnsafeBuffer(new byte[8192]);
            connection.factory.blockRW.wrap(copy, 0, copy.capacity());
            connection.mapPushPromise(headers, connection.factory.blockRW);
            HpackHeaderBlockFW block = connection.factory.blockRW.build();
            length = block.sizeof();
            sizeof = 9 + 4 + length;                    // +9 for HTTP2 framing, +4 for promised stream id
        }

        if (buffered() || !hasSpaceForFrames(length))
        {
            Flyweight.Builder.Visitor visitor =
                    http2Writer.visitPushPromise(streamId, promisedStreamId, copy, 0, length);

            Entry entry = new Entry(stream, streamId, length, type, visitor);
            addEntry(entry);
        }
        else
        {
            Flyweight.Builder.Visitor pushPromise =
                    http2Writer.visitPushPromise(streamId, promisedStreamId, headers, connection::mapPushPromise);
            writeHttp2FrameAndFlush(stream, type, sizeof, pushPromise);
        }

        return true;
    }

    @Override
    public boolean data(int streamId, TransferFW transfer)
    {
        assert streamId != 0;

        Http2Stream stream = stream(streamId);
        if (stream == null)
        {
            return true;
        }
        else
        {
            // TOD ACK the regions
        }

        transfer.regions().forEach(r -> dataRegion(streamId, r));
        return true;
    }

    public boolean dataRegion(int streamId, RegionFW region)
    {

        Http2FrameType type = DATA;
        Http2Stream stream = stream(streamId);

        if (!buffered() && !buffered(streamId) && hasSpaceForFrameHeaders(region.length())
                && region.length() <= connection.http2OutWindow
                && region.length() <= stream.http2OutWindow)
        {
            // Send multiple DATA frames (because of max frame size)
            int length = region.length();
            long address = region.address();
            writer.flushBegin();
            while (length > 0)
            {
                int chunk = Math.min(region.length(), connection.remoteSettings.maxFrameSize);
                Flyweight.Builder.Visitor visitor = http2Writer.visitDataHeader(streamId, chunk);
                writeHttp2Frame(stream, type, chunk, visitor);
                writeHttp2DataFrameWithoutHeader(stream, type, address, length, region.streamId());

                address += chunk;
                length -= chunk;
            }
            writer.flushEnd();
        }
        else
        {
            DataEntry entry = new DataEntry(stream, streamId, type, region.address(), region.length(), region.streamId());
            addEntry(entry);
            flush();
        }
        return true;
    }

    @Override
    public boolean dataEos(int streamId)
    {
        int length = 0;
        int sizeof = length + 9;    // +9 for HTTP2 framing
        Flyweight.Builder.Visitor data = http2Writer.visitDataEos(streamId);
        Http2FrameType type = DATA;

        Http2Stream stream = connection.http2Streams.get(streamId);
        if (stream == null)
        {
            return true;
        }
        stream.endStream = true;

        if (!buffered() && !buffered(streamId) && hasSpaceForFrames(length) && 0 <= connection.http2OutWindow &&
                0 <= stream.http2OutWindow)
        {
            writeHttp2FrameAndFlush(stream, type, sizeof, data);
            connection.closeStream(stream);
        }
        else
        {
            DataEosEntry entry = new DataEosEntry(stream, streamId, length, type, data);
            addEntry(entry);
        }

        return true;
    }

    private boolean hasSpaceForFrames(int length)
    {
        int frameCount = length == 0 ? 1 : (int) Math.ceil((double) length/connection.remoteSettings.maxFrameSize);
        int sizeof = length + frameCount * 9;
        return writer.fits(sizeof);
    }

    private boolean hasSpaceForFrameHeaders(int length)
    {
        int frameCount = length == 0 ? 1 : (int) Math.ceil((double) length/connection.remoteSettings.maxFrameSize);
        int sizeof = frameCount * 9;
        return writer.fits(sizeof);
    }

    private void addEntry(Entry entry)
    {
        if (entry.type == DATA)
        {
            Deque<WriteScheduler.Entry> queue = queue(entry.stream);
            if (queue != null)
            {
                queue.add(entry);
            }
        }
        else
        {
            replyQueue.add(entry);
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
    public void doEnd()
    {
        end = true;

        assert entryCount >= 0;
        if (entryCount == 0 && !endSent)
        {
            endSent = true;
            writer.doEnd();
        }
    }

    private void flush()
    {

        Entry entry;
        writer.flushBegin();
        while ((entry = pop()) != null)
        {
            entry.write();
        }
        writer.flushEnd();

        for(Http2Stream stream : connection.http2Streams.values())
        {
            if (stream.applicationReplyThrottle != null)
            {
                stream.sendHttpWindow();
            }
        }

        if (entryCount == 0 && end && !endSent)
        {
            endSent = true;
            writer.doEnd();
        }
    }

    @Override
    public void onHttp2Window()
    {
        flush();
    }

    @Override
    public void onHttp2Window(int streamId)
    {
        flush();
    }

    @Override
    public void onAck(AckFW ack)
    {
        ack.regions().forEach(r ->
        {
            if (r.streamId() == targetId)
            {
                writer.ack(r.address(), r.length());
            }
            else
            {
                connection.processTransportAck(r.address(), r.length(), r.streamId());
            }
        });

        flush();
    }

    private Entry pop()
    {
        if (buffered())
        {
            // There are entries on connection queue but cannot make progress, so
            // not make any progress on other streams
            return pop(null);
        }

        // TODO Map#values may not iterate randomly, randomly pick a stream ??
        // Select a frame on a HTTP2 stream that can be written
        for(Http2Stream stream : connection.http2Streams.values())
        {
            Entry entry = pop(stream);
            if (entry != null)
            {
                return entry;
            }
        }

        return null;
    }

    private Entry pop(Http2Stream stream)
    {
        if (buffered(stream))
        {
            Deque replyQueue = queue(stream);
            Entry entry = (Entry) replyQueue.peek();
            if (entry.fits())
            {
                entryCount--;
                entry =  (Entry) replyQueue.poll();
                return entry;
            }
        }

        return null;
    }

    private Deque<WriteScheduler.Entry> queue(Http2Stream stream)
    {
        return (stream == null) ? replyQueue : stream.replyQueue;
    }

    private boolean buffered(Http2Stream stream)
    {
        Deque queue = (stream == null) ? replyQueue : stream.replyQueue;
        return buffered(queue);
    }

    private boolean buffered(Deque queue)
    {
        return !(queue == null || queue.isEmpty());
    }

    private boolean buffered()
    {
        return !replyQueue.isEmpty();
    }

    private boolean buffered(int streamId)
    {
        assert streamId != 0;

        Http2Stream stream = connection.http2Streams.get(streamId);
        return !(stream == null || stream.replyQueue.isEmpty());
    }

    Http2Stream stream(int streamId)
    {
        return streamId == 0 ? null : connection.http2Streams.get(streamId);
    }

    private void writeHttp2Frame(
        Http2Stream stream,
        Http2FrameType type,
        int sizeofGuess,
        Flyweight.Builder.Visitor visitor)
    {
        if (canStreamWrite(stream, type))
        {
            int sizeof = writer.queueHttp2Frame(sizeofGuess, visitor);
            System.out.printf("<- %d %s sizeof=%d\n", System.currentTimeMillis()%100000, type, sizeof);

            assert sizeof >= 9;
        }
    }

    private void writeHttp2DataFrameWithoutHeader(
        Http2Stream stream,
        Http2FrameType type,
        long address,
        int length,
        long regionStreamId)
    {
        if (canStreamWrite(stream, type))
        {
            System.out.printf("<- %s %s length=%d\n", System.currentTimeMillis()%100000, type, length);
            writer.queueHttp2Data(address, length, regionStreamId);

            stream.http2OutWindow -= length;
            connection.http2OutWindow -= length;
            stream.totalOutData += length;
        }

        // TODO ACK
    }

    private static boolean canStreamWrite(Http2Stream stream, Http2FrameType type)
    {
        // After RST_STREAM is written, don't write any frame in the stream
        return stream == null || type == RST_STREAM || stream.state != Http2Connection.State.CLOSED;
    }

    private void writeHttp2FrameAndFlush(
        Http2Stream stream,
        Http2FrameType type,
        int sizeofGuess,
        Flyweight.Builder.Visitor visitor)
    {
        writer.flushBegin();
        writeHttp2Frame(stream, type, sizeofGuess, visitor);
        writer.flushEnd();
    }

    @Override
    public void close()
    {
        writer.close();
    }

    private class Entry implements WriteScheduler.Entry
    {
        final int streamId;
        final int length;
        final int sizeof;
        final Http2FrameType type;
        final Flyweight.Builder.Visitor visitor;
        final Http2Stream stream;

        Entry(Http2Stream stream, int streamId, int length, Http2FrameType type,
              Flyweight.Builder.Visitor visitor)
        {
            this.stream = stream;
            this.streamId = streamId;
            this.length = length;
            this.sizeof = length + 9;
            this.type = type;
            this.visitor = visitor;

            entryCount++;
        }

        boolean fits()
        {
            return hasSpaceForFrames(length);
        }

        void write()
        {
            writeHttp2Frame(stream, type, sizeof, visitor);
        }

    }

    private class DataEosEntry extends Entry
    {
        DataEosEntry(Http2Stream stream, int streamId, int length, Http2FrameType type,
                     Flyweight.Builder.Visitor visitor)
        {
            super(stream, streamId, length, type, visitor);
        }

        @Override
        void write()
        {
            super.write();
            connection.closeStream(stream);
        }
    }

    private class DataEntry extends Entry
    {
        final long address;
        final long regionStreamId;

        DataEntry(
                Http2Stream stream,
                int streamId,
                Http2FrameType type,
                long address,
                int length,
                long regionStreamId)
        {
            super(stream, streamId, length, type, null);
            this.address = address;
            this.regionStreamId = regionStreamId;

            assert streamId != 0;
        }

        boolean fits()
        {
            // limit by nuklei window, http2 windows, peer's max frame size
            int min = Math.min((int) connection.http2OutWindow, (int) stream.http2OutWindow);
            min = Math.min(min, length);
            min = Math.min(min, connection.remoteSettings.maxFrameSize);
            min = Math.min(min, writer.remaining() - 9);

            if (min > 0)
            {
                int remaining = length - min;
                if (remaining > 0)
                {
                    entryCount--;
                    stream.replyQueue.poll();
                    DataEntry entry1 = new DataEntry(stream, streamId, type, address, min, regionStreamId);
                    DataEntry entry2 = new DataEntry(stream, streamId, type, address, remaining, regionStreamId);

                    stream.replyQueue.addFirst(entry2);
                    stream.replyQueue.addFirst(entry1);
                }
            }

            return min > 0;
        }

        @Override
        void write()
        {
            Flyweight.Builder.Visitor visitor = http2Writer.visitDataHeader(streamId, length);
            writeHttp2Frame(stream, type, length, visitor);
            writeHttp2DataFrameWithoutHeader(stream, type, address, length, regionStreamId);
        }

        public String toString()
        {
            return String.format("length=%d", length);
        }

    }

}
