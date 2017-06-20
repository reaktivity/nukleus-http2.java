package org.reaktivity.nukleus.http2.internal;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.http2.internal.routable.Route;
import org.reaktivity.nukleus.http2.internal.routable.stream.CircularDirectBuffer;
import org.reaktivity.nukleus.http2.internal.routable.stream.HttpWriteScheduler;
import org.reaktivity.nukleus.http2.internal.routable.stream.SourceInputStreamFactory;
import org.reaktivity.nukleus.http2.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http2.internal.types.stream.WindowFW;

import java.util.Deque;
import java.util.LinkedList;
import java.util.function.UnaryOperator;

import static org.reaktivity.nukleus.http2.internal.routable.stream.Slab.NO_SLOT;

public class Http2Stream2 {
    private final Http2Connection connection;
    private final HttpWriteScheduler httpWriteScheduler;
    final int http2StreamId;
    final long targetId;
    final Route route;
    Http2Connection.State state;
    long http2OutWindow;
    long http2InWindow;

    long contentLength;
    long totalData;
    int targetWindow;

    private int replySlot = NO_SLOT;
    CircularDirectBuffer replyBuffer;
    Deque replyQueue;
    public boolean endStream;
    long totalOutData;

    Http2Stream(Http2Connection connection, int http2StreamId, Http2Connection.State state, Route route)
    {
        this.connection = connection;
        this.http2StreamId = http2StreamId;
        this.targetId = supplyStreamId.getAsLong();
        this.http2InWindow = connection.localSettings.initialWindowSize;
        this.http2OutWindow = connection.remoteSettings.initialWindowSize;
        this.state = state;
        this.route = route;
        this.httpWriteScheduler = new HttpWriteScheduler(frameSlab, route.target(), targetId, this);
    }

    boolean isClientInitiated()
    {
        return http2StreamId%2 == 1;
    }

    private void onData()
    {
        httpWriteScheduler.onData(http2DataRO);
    }

    private void onThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        switch (msgTypeId)
        {
            case WindowFW.TYPE_ID:
                windowRO.wrap(buffer, index, index + length);
                int update = windowRO.update();
                targetWindow += update;
                httpWriteScheduler.onWindow();
                break;
            case ResetFW.TYPE_ID:
                doReset(buffer, index, length);
                break;
            default:
                // ignore
                break;
        }
    }

    /*
     * @return true if there is a buffer
     *         false if all slots are taken
     */
    MutableDirectBuffer acquireReplyBuffer(UnaryOperator<MutableDirectBuffer> change)
    {
        if (replySlot == NO_SLOT)
        {
            replySlot = frameSlab.acquire(connection.sourceOutputEstId);
            if (replySlot != NO_SLOT)
            {
                int capacity = frameSlab.buffer(replySlot).capacity();
                replyBuffer = new CircularDirectBuffer(capacity);
                replyQueue = new LinkedList();
            }
        }
        return replySlot != NO_SLOT ? frameSlab.buffer(replySlot, change) : null;
    }

    void releaseReplyBuffer()
    {
        if (replySlot != NO_SLOT)
        {
            frameSlab.release(replySlot);
            replySlot = NO_SLOT;
            replyBuffer = null;
            replyQueue = null;
        }
    }

    private void doReset(
            DirectBuffer buffer,
            int index,
            int length)
    {
        resetRO.wrap(buffer, index, index + length);
        httpWriteScheduler.onReset();
        releaseReplyBuffer();
        //source.doReset(sourceId);
    }

    void sendHttp2Window(int update)
    {
        targetWindow -= update;

        http2InWindow += update;
        connection.http2InWindow += update;

        // connection-level flow-control
        connection.writeScheduler.windowUpdate(0, update);

        // stream-level flow-control
        connection.writeScheduler.windowUpdate(http2StreamId, update);
    }

}
