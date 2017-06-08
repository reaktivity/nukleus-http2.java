package org.reaktivity.nukleus.http2.internal.routable.stream;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.http2.internal.routable.Target;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;

import java.util.function.UnaryOperator;

import static org.reaktivity.nukleus.http2.internal.routable.stream.Slab.NO_SLOT;

public class HttpWriteScheduler
{
    private final MutableDirectBuffer read = new UnsafeBuffer(new byte[0]);
    private final MutableDirectBuffer write = new UnsafeBuffer(new byte[0]);

    private final Slab slab;
    private final Target target;
    private final long targetId;

    private SourceInputStreamFactory.Http2Stream stream;
    private int targetSlot = NO_SLOT;
    private RingDirectBuffer targetBuffer;
    private boolean endStream;

    HttpWriteScheduler(Slab slab, Target target, long targetId, SourceInputStreamFactory.Http2Stream stream)
    {
        this.slab = slab;
        this.target = target;
        this.targetId = targetId;
        this.stream = stream;
    }

    boolean onData(Http2DataFW http2DataRO)
    {
        endStream = http2DataRO.endStream();

        if (targetBuffer == null)
        {
            int data = http2DataRO.dataLength();
            int toHttp = Math.min(data, stream.targetWindow);
            int toSlab = data - toHttp;

            // Send to http if there is available window
            if (toHttp > 0)
            {
                target.doHttpData(targetId, http2DataRO.buffer(), http2DataRO.dataOffset(), toHttp);
                stream.sendHttp2Window(toHttp);
            }

            // Store the remaining to a buffer
            if (toSlab > 0)
            {
                MutableDirectBuffer dst = acquire(this::write);
                if (dst != null)
                {
                    return targetBuffer.write(dst, http2DataRO.buffer(), http2DataRO.dataOffset() + toHttp, toSlab);
                }
                return false;                           // No slots
            }

            // since there is no data is pending, we can send END frame
            if (endStream)
            {
                target.doHttpEnd(targetId);
            }

            return true;
        }
        else
        {
            // Store the data in the existing buffer
            MutableDirectBuffer dst = acquire(this::write);
            return targetBuffer.write(dst, http2DataRO.buffer(), http2DataRO.dataOffset(), http2DataRO.dataLength());
        }
    }

    void onWindow()
    {
        if (targetBuffer != null)
        {
            int toHttp = Math.min(targetBuffer.size(), stream.targetWindow);

            // Send to http if there is available window
            if (toHttp > 0)
            {
                MutableDirectBuffer dst = acquire(this::read);
                int offset1 = targetBuffer.readOffset();
                int read1 = targetBuffer.read(toHttp);
                target.doHttpData(targetId, dst, offset1, read1);

                // toHttp may span across the boundary, one more read may be required
                if (read1 != toHttp)
                {
                    int offset2 = targetBuffer.readOffset();
                    int read2 = targetBuffer.read(toHttp-read1);
                    assert read1 + read2 == toHttp;

                    target.doHttpData(targetId, dst, offset2, read2);
                }

                stream.sendHttp2Window(toHttp);
            }

            if (targetBuffer.size() == 0)
            {
                // since there is no data is pending in slab, we can send END frame right away
                if (endStream)
                {
                    target.doHttpEnd(targetId);
                }

                release();
            }
        }
    }

    void onReset()
    {
        release();
    }

    /*
     * @return true if there is a buffer
     *         false if all slots are taken
     */
    private MutableDirectBuffer acquire(UnaryOperator<MutableDirectBuffer> change)
    {
        if (targetSlot == NO_SLOT)
        {
            targetSlot = slab.acquire(targetId);
            if (targetSlot != NO_SLOT)
            {
                int capacity = slab.buffer(targetSlot).capacity();
                targetBuffer = new RingDirectBuffer(capacity);
            }
        }
        return targetSlot != NO_SLOT ? slab.buffer(targetSlot, change) : null;
    }

    private void release()
    {
        if (targetSlot != NO_SLOT)
        {
            slab.release(targetSlot);
            targetSlot = NO_SLOT;
            targetBuffer = null;
        }
    }

    private MutableDirectBuffer read(MutableDirectBuffer buffer)
    {
        read.wrap(buffer.addressOffset(), buffer.capacity());
        return read;
    }

    private MutableDirectBuffer write(MutableDirectBuffer buffer)
    {
        write.wrap(buffer.addressOffset(), buffer.capacity());
        return write;
    }

}
