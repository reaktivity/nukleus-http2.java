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

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import static org.reaktivity.nukleus.http2.internal.routable.stream.Slab.NO_SLOT;

public class SimpleWriteScheduler implements WriteScheduler
{
    private final Slab slab;

    private final long targetId;
    private final List<WriteEntry> entries;

    private int slot = NO_SLOT;
    private int slotPosition;
    private MutableDirectBuffer buffer;

    private Target target;
    private boolean eos;
    private int window;

    SimpleWriteScheduler(Slab slab, Target target, long targetId)
    {
        this.slab = slab;
        this.target = target;
        this.targetId = targetId;
        this.entries = new ArrayList<>();
    }

    public boolean http2(int length, Flyweight.Builder.Visitor visitor)
    {
        assert !eos;
        boolean error = false;

        if (slot == NO_SLOT && length <= window)
        {
            assert entries.isEmpty();

            target.doHttp2(targetId, visitor);
            window -= length;
        }
        else
        {
            boolean acquired = acquireBuffer(targetId);
            if (acquired)
            {
                length = visitor.visit(buffer, slotPosition, buffer.capacity());
                slotPosition += length;
                entries.add(new WriteEntry(length, false));
                System.out.println("doHttp2: No of entries = " + entries.size());
            }
            else
            {
                error = true;
            }
        }
        return error;
    }

    @Override
    public boolean windowUpdate(int streamId, int update)
    {
        Flyweight.Builder.Visitor window = target.visitWindowUpdate(streamId, update);
        return http2(9+4, window);    // +9 for HTTP2 framing, +4 window size increment
    }

    @Override
    public boolean pingAck(DirectBuffer buffer, int offset, int length)
    {
        Flyweight.Builder.Visitor ping = target.visitPingAck(buffer, offset, length);
        return http2(9+8, ping);    // +9 for HTTP2 framing, +8 for a ping
    }

    @Override
    public boolean goaway(int lastStreamId, Http2ErrorCode errorCode)
    {
        Flyweight.Builder.Visitor goaway = target.visitGoaway(lastStreamId, errorCode);
        return http2(9+8, goaway);    // +9 for HTTP2 framing, +8 for goaway payload
    }

    @Override
    public boolean rst(int streamId, Http2ErrorCode errorCode)
    {
        Flyweight.Builder.Visitor rst = target.visitRst(streamId, errorCode);
        return http2(9+4, rst);    // +9 for HTTP2 framing, +4 for RST_STREAM payload
    }

    @Override
    public boolean settings(int maxConcurrentStreams)
    {
        Flyweight.Builder.Visitor settings = target.visitSettings(maxConcurrentStreams);
        return http2(9+6, settings);    // +9 for HTTP2 framing, +6 for a setting
    }

    @Override
    public boolean settingsAck()
    {
        Flyweight.Builder.Visitor settings = target.visitSettingsAck();
        return http2(9, settings);    // +9 for HTTP2 framing
    }

    @Override
    public boolean headers(int streamId, ListFW<HttpHeaderFW> headers,
                           BiFunction<HttpHeaderFW, HpackHeaderFieldFW.Builder, HpackHeaderFieldFW> mapper)
    {
        int length = headersLength(headers);

        Flyweight.Builder.Visitor data = target.visitHeaders(streamId, headers, mapper);
        return http2(9 + length, data);      // +9 for HTTP2 framing
    }

    @Override
    public boolean pushPromise(int streamId, int promisedStreamId, ListFW<HttpHeaderFW> headers,
                               BiFunction<HttpHeaderFW, HpackHeaderFieldFW.Builder, HpackHeaderFieldFW> mapper)
    {
        int length = headersLength(headers);

        Flyweight.Builder.Visitor data = target.visitPushPromise(streamId, promisedStreamId, headers, mapper);
        return http2(9 + 4 + length, data);      // +9 for HTTP2 framing, +4 for promised stream id
    }

    @Override
    public boolean data(int streamId, DirectBuffer buffer, int offset, int length)
    {
        Flyweight.Builder.Visitor data = target.visitData(streamId, buffer, offset, length);
        return http2(9 + length, data);      // +9 for HTTP2 framing
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
        Flyweight.Builder.Visitor data = target.visitDataEos(streamId);
        return http2(9, data);      // +9 for HTTP2 framing
    }

    public void doEnd()
    {
        eos = true;
        if (entries.isEmpty())
        {
            target.doEnd(targetId);
        }
    }

    public void flush(int windowUpdate)
    {
        int i = 0;
        int offset = 0;

        window += windowUpdate;
        while (i < entries.size())
        {
            WriteEntry entry = entries.get(i);
            if (entry.length <= window)
            {
                MutableDirectBuffer buffer = slab.buffer(slot);
                target.doData(targetId, buffer, offset, entry.length);
                window -= entry.length;
            }
            else
            {
                if (entry.splittable)
                {
                    // Can break big DATA frames and write them out
                    throw new UnsupportedOperationException("TODO break big DATA frames");
                }

                // Also, can make progress on other streams, but this simple scheduler doesn't do it
                break;
            }
            i++;
            offset += entry.length;
        }

        if (i > 0)
        {
            MutableDirectBuffer buffer = slab.buffer(slot);
            slotPosition -= offset;
            buffer.putBytes(0, buffer, offset, slotPosition);       // shift remaining bytes in the buffer
            entries.subList(0, i).clear();
        }

        if (entries.isEmpty() && eos)
        {
            target.doEnd(targetId);
        }

        if (entries.isEmpty())
        {
            releaseBuffer();
        }
        System.out.println("flush: " + i + " No of entries = " + entries.size());
    }

    /*
     * @return true if there is a buffer
     *         false if all slots are taken
     */
    private boolean acquireBuffer(long streamId)
    {
        if (slot == NO_SLOT)
        {
            slot = slab.acquire(streamId);
            if (slot != NO_SLOT)
            {
                buffer = slab.buffer(slot);
                slotPosition = 0;
            }
        }
        return slot != NO_SLOT;
    }

    private void releaseBuffer()
    {
        if (slot != NO_SLOT)
        {
            slab.release(slot);
            slot = NO_SLOT;
            slotPosition = 0;
            buffer = null;
        }
    }

    private static class WriteEntry
    {
        boolean splittable;
        int length;

        WriteEntry(int length, boolean splittable)
        {
            this.length = length;
            this.splittable = splittable;
        }

    }

}
