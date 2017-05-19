package org.reaktivity.nukleus.http2.internal.routable.stream;

import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.http2.internal.routable.Target;
import org.reaktivity.nukleus.http2.internal.types.Flyweight;

import java.util.ArrayList;
import java.util.List;

import static org.reaktivity.nukleus.http2.internal.routable.stream.Slab.NO_SLOT;

public class BufferingWriteScheduler implements WriteScheduler
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

    BufferingWriteScheduler(Slab slab, Target target, long targetId)
    {
        this.slab = slab;
        this.target = target;
        this.targetId = targetId;
        this.entries = new ArrayList<>();
    }

    public void doHttp2(int length, int http2StreamId, Flyweight.Builder.Visitor visitor)
    {
        assert !eos;

        if (slot == NO_SLOT && length <= window)
        {
            assert entries.isEmpty();

            target.doHttp2(targetId, visitor);
            window -= length;
        }
        else
        {
            boolean acquired = acquireBuffer(targetId);
            if (!acquired)
            {
                throw new UnsupportedOperationException("TODO no buffer");
            }
            int sizeOf = visitor.visit(buffer, slotPosition, buffer.capacity());
            slotPosition += sizeOf;
            entries.add(new WriteEntry(http2StreamId, sizeOf, false));
            System.out.println("doHttp2: No of entries = " + entries.size());
        }
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
                if (entry.split)
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
        System.out.println("flush: No of entries = " + entries.size());
    }

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

    static class WriteEntry
    {
        int streamId;
        boolean split;
        int length;

        WriteEntry(int streamId, int length, boolean split)
        {
            this.streamId = streamId;
            this.length = length;
            this.split = split;
        }

    }

}
