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
import org.reaktivity.nukleus.http2.internal.types.Flyweight;
import org.reaktivity.nukleus.http2.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType;

import java.util.Deque;
import java.util.LinkedList;
import java.util.function.Consumer;

import static org.reaktivity.nukleus.http2.internal.routable.stream.Slab.NO_SLOT;
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
    private final MutableDirectBuffer reply = new UnsafeBuffer(new byte[0]);
    private final MutableDirectBuffer accumulated = new UnsafeBuffer(new byte[0]);
    private final SourceInputStreamFactory.SourceInputStream connection;
    private final Target target;
    private final long targetId;
    private final long sourceOutputEstId;
    private final Deque<ConnectionEntry> replyQueue;

    private int replySlot = NO_SLOT;
    private CircularEntryBuffer replyBuffer;
    private Slab slab;
    private boolean end;
    private boolean endSent;

    NukleusWriteScheduler(
            SourceInputStreamFactory.SourceInputStream connection,
            long sourceOutputEstId,
            Slab slab,
            Target target,
            long targetId)
    {
        this.connection = connection;
        this.sourceOutputEstId = sourceOutputEstId;
        this.slab = slab;
        this.target = target;
        this.targetId = targetId;
        this.replyQueue = new LinkedList<>();
    }

    public boolean http2(
            int streamId,
            int lengthGuess,
            Http2FrameType type,
            Flyweight.Builder.Visitor visitor,
            Consumer<Integer> progress)
    {
        return http2(streamId, lengthGuess, type, visitor, progress, true);
    }

    public boolean http2(
            int streamId,
            int lengthGuess,
            Http2FrameType type,
            Flyweight.Builder.Visitor visitor,
            Consumer<Integer> progress,
            boolean flush)
    {
        acquireReplyBuffer();    // TODO return value
        CircularEntryBuffer cb = replyBuffer;
        int offset = cb.writeOffset(lengthGuess);
        if (offset != -1)
        {
            int length = visitor.visit(reply, offset, lengthGuess);
            cb.write(offset, length);
            ConnectionEntry entry = new ConnectionEntry(streamId, offset, length, type, progress);
            replyQueue.add(entry);
            if (flush)
            {
                onWindow();                     // check if partial write is possible
            }
        }
        return offset != -1;
    }

    @Override
    public boolean windowUpdate(int streamId, int update)
    {
        Flyweight.Builder.Visitor window = target.visitWindowUpdate(streamId, update);
        int sizeof = 9 + 4;             // +9 for HTTP2 framing, +4 window size increment
        return http2(streamId, sizeof, WINDOW_UPDATE, window, null);
    }

    @Override
    public boolean pingAck(DirectBuffer buffer, int offset, int length)
    {
        int sizeof = 9 + 8;             // +9 for HTTP2 framing, +8 for a ping
        Flyweight.Builder.Visitor ping = target.visitPingAck(buffer, offset, length);
        return http2(0, sizeof, PING, ping, null);
    }

    @Override
    public boolean goaway(int lastStreamId, Http2ErrorCode errorCode)
    {
        int sizeof = 9 + 8;             // +9 for HTTP2 framing, +8 for goaway payload
        Flyweight.Builder.Visitor goaway = target.visitGoaway(lastStreamId, errorCode);
        return http2(0, sizeof, GO_AWAY, goaway, null);
    }

    @Override
    public boolean rst(int streamId, Http2ErrorCode errorCode)
    {
        int sizeof = 9 + 4;             // +9 for HTTP2 framing, +4 for RST_STREAM payload
        Flyweight.Builder.Visitor rst = target.visitRst(streamId, errorCode);
        return http2(streamId, sizeof, RST_STREAM, rst, null);
    }

    @Override
    public boolean settings(int maxConcurrentStreams)
    {
        int sizeof = 9 + 6;             // +9 for HTTP2 framing, +6 for a setting
        Flyweight.Builder.Visitor settings = target.visitSettings(maxConcurrentStreams);
        return http2(0, sizeof, SETTINGS, settings, null);
    }

    @Override
    public boolean settingsAck()
    {
        int sizeof = 9;                 // +9 for HTTP2 framing
        Flyweight.Builder.Visitor settings = target.visitSettingsAck();
        return http2(0, sizeof, SETTINGS, settings, null);
    }

    @Override
    public boolean headers(int streamId, ListFW<HttpHeaderFW> headers)
    {
        int sizeof = 9 + headersLength(headers);    // +9 for HTTP2 framing
        Flyweight.Builder.Visitor data = target.visitHeaders(streamId, headers, connection::mapHeaders);
        return http2(streamId, sizeof, HEADERS, data, null);
    }

    @Override
    public boolean pushPromise(int streamId, int promisedStreamId, ListFW<HttpHeaderFW> headers,
                               Consumer<Integer> progress)
    {
        int sizeof = 9 + 4 + headersLength(headers);    // +9 for HTTP2 framing, +4 for promised stream id
        Flyweight.Builder.Visitor data = target.visitPushPromise(streamId, promisedStreamId, headers, connection::mapPushPromize);
        return http2(streamId, sizeof, PUSH_PROMISE, data, null);
    }

    @Override
    public boolean data(int streamId, DirectBuffer buffer, int offset, int length, Consumer<Integer> progress)
    {
        return data(streamId, buffer, offset, length, progress, true);
    }

    public boolean data(int streamId, DirectBuffer buffer, int offset, int length, Consumer<Integer> progress, boolean flush)
    {
        assert length > 0;
        int sizeof = 9 + length;    // +9 for HTTP2 framing
        Flyweight.Builder.Visitor data = target.visitData(streamId, buffer, offset, length);
        return http2(streamId, sizeof, DATA, data, progress, flush);
    }

    @Override
    public void doEnd()
    {
        end = true;
        if (!buffered() && !endSent)
        {
            endSent = true;
            target.doEnd(targetId);
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
        int sizeof = 9;    // +9 for HTTP2 framing
        Flyweight.Builder.Visitor data = target.visitDataEos(streamId);
        return http2(streamId, sizeof, DATA, data, null);
    }

    @Override
    public void onWindow()
    {
        if (connection.outWindow < connection.outWindowThreshold)
        {
            // Instead of sending small updates, wait until a bigger window accumulates
            return;
        }

        int accumulatedSlot = NO_SLOT;
        try
        {
            accumulatedSlot = slab.acquire(sourceOutputEstId);
            if (accumulatedSlot == NO_SLOT)
            {
                connection.cleanConnection();
                return;
            }
            MutableDirectBuffer accumulatedBuffer = slab.buffer(accumulatedSlot, this::accumulated);

            ConnectionEntry entry;
            int offset = 0;
            while ((entry = pop()) != null)
            {
                acquireReplyBuffer();
                accumulatedBuffer.putBytes(offset, reply, entry.offset, entry.length);
                offset += entry.length;

                if (entry.progress != null)
                {
                    entry.progress.accept(entry.payload);
                }
            }

            if (offset > 0)
            {
                target.doData(targetId, accumulatedBuffer, 0, offset);
            }


            if (!buffered())
            {
                releaseReplyBuffer();

                if (end && !endSent)
                {
                    endSent = true;
                    target.doEnd(targetId);
                }
            }
        }
        finally
        {
            if (accumulatedSlot != NO_SLOT)
            {
                slab.release(accumulatedSlot);
            }
        }
    }

    private boolean buffered()
    {
        return !replyQueue.isEmpty();
    }

    private ConnectionEntry pop()
    {
        if (buffered())
        {
            ConnectionEntry entry = replyQueue.peek();
            if (entry != null && entry.fits())
            {
                entry = replyQueue.poll();
                replyBuffer.read(entry.offset, entry.length);
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

    private MutableDirectBuffer reply(MutableDirectBuffer buffer)
    {
        reply.wrap(buffer.addressOffset(), buffer.capacity());
        return reply;
    }

    private MutableDirectBuffer accumulated(MutableDirectBuffer buffer)
    {
        accumulated.wrap(buffer.addressOffset(), buffer.capacity());
        return accumulated;
    }

    /*
     * @return true if there is a buffer
     *         false if all slots are taken
     */
    private MutableDirectBuffer acquireReplyBuffer()
    {
        if (replySlot == NO_SLOT)
        {
            replySlot = slab.acquire(sourceOutputEstId);
            if (replySlot != NO_SLOT)
            {
                int capacity = slab.buffer(replySlot).capacity();
                replyBuffer = new CircularEntryBuffer(capacity);
            }
        }
        return replySlot != NO_SLOT ? slab.buffer(replySlot, this::reply) : null;
    }

    private void releaseReplyBuffer()
    {
        if (replySlot != NO_SLOT)
        {
            slab.release(replySlot);
            replySlot = NO_SLOT;
            replyBuffer = null;
        }
    }

    private class ConnectionEntry
    {
        final int streamId;
        final int offset;
        final int length;
        final int framing;
        final int payload;
        final Http2FrameType type;
        private final Consumer<Integer> progress;

        ConnectionEntry(
                int streamId,
                int offset,
                int length,
                Http2FrameType type,
                Consumer<Integer> progress)
        {
            this(streamId, offset, length, 9, length - 9, type, progress);
        }

        ConnectionEntry(
                int streamId,
                int offset,
                int length,
                int framing,
                int payload,
                Http2FrameType type,
                Consumer<Integer> progress)
        {
            assert framing >= 0;
            assert payload >= 0;
            assert framing + payload == length;

            this.streamId = streamId;
            this.offset = offset;
            this.length = length;
            this.framing = framing;
            this.payload = payload;
            this.type = type;
            this.progress = progress;
        }

        boolean fits()
        {
            int entry1Length = Math.min(length, connection.outWindow);
            if (entry1Length > 0)
            {
                int entry2Length = length - entry1Length;
                if (entry2Length > 0)
                {
                    // Splits this entry into two entries
                    replyQueue.poll();

                    // Construct such that first entry fits under connection.outWindow
                    int entry1Framing = Math.min(framing, connection.outWindow);
                    int entry1Payload = entry1Length - entry1Framing;
                    assert entry1Framing >= 0;
                    assert entry1Payload >= 0;
                    ConnectionEntry entry1 = new ConnectionEntry(streamId, offset, entry1Length,
                            entry1Framing, entry1Payload, type, progress);

                    // Construct second entry with the remaining
                    int entry2Framing = framing - entry1Framing;
                    int entry2Payload = entry2Length - entry2Framing;
                    assert entry2Framing >= 0;
                    assert entry2Payload >= 0;
                    ConnectionEntry entry2 = new ConnectionEntry(streamId, offset + entry1Length, entry2Length,
                            entry2Framing, entry2Payload, type, progress);

                    assert entry1Length + entry2Length == length;
                    assert entry1Framing + entry2Framing == framing;
                    assert entry1Payload + entry2Payload == payload;

                    replyQueue.addFirst(entry2);
                    replyQueue.addFirst(entry1);
                }
            }

            return entry1Length > 0;
        }

        void adjustWindows()
        {
            connection.outWindow -= length;
        }

        public String toString()
        {
            return String.format("streamId=%d type=%s offset=%d length=%d", streamId, type, offset, length);
        }

    }

}
