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
package org.reaktivity.nukleus.http2.internal.client;

import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http2.internal.CircularDirectBuffer;
import org.reaktivity.nukleus.http2.internal.Http2ConnectionState;

import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;

import org.agrona.MutableDirectBuffer;

class Http2ClientStream
{
    final int http2StreamId;
    final long acceptCorrelationId;
    final MessageConsumer acceptThrottle;
    final long acceptStreamId;
    long acceptReplyStreamId;
    Http2ConnectionState state;

    int http2Window; // keeps track of window for sending to http2
    int httpWindow; // keeps track of the window for sending to http (and receiving from http2)

    // set to true when the first window is sent to the accept throttle
    boolean initialAcceptWindowSent = false;

    // this will be set when the END_STREAM flag should be sent on this stream, but the data is buffered
    boolean endStreamShouldBeSent = false;

    long contentLength;
    long totalData;

    // buffer to be used for DATA frames for this stream, in case there is no window (nukleus, connection and stream)
    private int bufferSlot = NO_SLOT;
    CircularDirectBuffer circularBuffer;
    private final BufferPool bufferPool;

    boolean endSent = false; // will become true when an END_STREAM will be sent to the server

    Http2ClientStream(int http2StreamId, int localInitialWindowSize, int remoteInitialWindowSize,
            Http2ConnectionState state, long acceptCorrelationId, MessageConsumer acceptThrottle,
            long acceptStreamId, BufferPool bufferPool)
    {
        this.http2StreamId = http2StreamId;
        this.acceptCorrelationId = acceptCorrelationId;
        this.httpWindow = localInitialWindowSize;
        this.http2Window = remoteInitialWindowSize;
        this.state = state;
        this.acceptThrottle = acceptThrottle;
        this.acceptStreamId = acceptStreamId;
        this.bufferPool = bufferPool;
    }

    MutableDirectBuffer acquireBuffer()
    {
        if (bufferSlot == NO_SLOT)
        {
            bufferSlot = bufferPool.acquire(acceptStreamId);
            if (bufferSlot != NO_SLOT)
            {
                circularBuffer = new CircularDirectBuffer(bufferPool.slotCapacity());
            }
        }
        return bufferSlot != NO_SLOT ? bufferPool.buffer(bufferSlot) : null;
    }

    void releaseBuffer()
    {
        if (bufferSlot != NO_SLOT)
        {
            bufferPool.release(bufferSlot);
            bufferSlot = NO_SLOT;
            circularBuffer = null;
        }
    }
}
