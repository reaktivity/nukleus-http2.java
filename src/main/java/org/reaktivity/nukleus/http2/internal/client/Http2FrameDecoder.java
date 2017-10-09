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

import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ContinuationFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2HeadersFW;

class Http2FrameDecoder
{
    private final ClientStreamFactory factory;

    Http2ErrorCode connectionError = null;
    Http2ErrorCode streamError = null;

    boolean expectContinuation;
    int expectContinuationStreamId;
    private int headersSlotPosition = 0;
    private int headersSlotIndex = NO_SLOT;

    Http2FrameDecoder(ClientStreamFactory factory)
    {
        this.factory = factory;
    }

    /**
     * Assembles a complete HTTP2 headers block, including any continuations.
     * This method must be called only with HEADERS or CONTINUATION frame
     *
     * @return true if a complete HTTP2 headers is assembled, false otherwise.
     *          In case of error connectionError and streamError are set accordingly.
     *          Result frame will be found in factory.blockRO
     */
    boolean decodeHttp2Headers()
    {
        if (Http2FrameType.HEADERS == factory.http2RO.type())
        {
                int streamId = factory.http2RO.streamId();
                if (streamId == 0)
                {
                    connectionError = Http2ErrorCode.PROTOCOL_ERROR;
                    return false;
                }

                Http2HeadersFW headersFw = factory.headersRO.wrap(
                        factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());
                int parentStreamId = headersFw.parentStream();
                if (parentStreamId == streamId)
                {
                    // 5.3.1 A stream cannot depend on itself
                    streamError = Http2ErrorCode.PROTOCOL_ERROR;
                    return false;
                }
                if (headersFw.dataLength() < 0)
                {
                    connectionError = Http2ErrorCode.PROTOCOL_ERROR;
                    return false;
                }

                return http2HeadersAvailable(headersFw.buffer(), headersFw.dataOffset(),
                        headersFw.dataLength(), headersFw.endHeaders(), headersFw.streamId());
        }

        // we have Http2FrameType.CONTINUATION
        Http2ContinuationFW continuationFw = factory.continuationRO.wrap(
                factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());
        return http2HeadersAvailable(
                continuationFw.payload(), 0, continuationFw.payload().capacity(),
                continuationFw.endHeaders(), continuationFw.streamId());
    }

    /**
     * Assembles a complete HTTP2 headers block (including any continuations). The
     * flyweight is wrapped with the buffer (it could be given buffer or slab)
     *
     * @return true if a complete HTTP2 headers is assembled in factory.blockRO
     *         false otherwise
     */
    private boolean http2HeadersAvailable(DirectBuffer buffer, int offset, int length, boolean endHeaders, int streamId)
    {
        if (endHeaders)
        { // complete headers received, wrapping them
            DirectBuffer localBuffer = buffer;
            int localOffset = offset;
            int localLength = length;
            if (headersSlotPosition > 0)
            {
                MutableDirectBuffer headersBuffer = factory.bufferPool.buffer(headersSlotIndex);
                headersBuffer.putBytes(headersSlotPosition, buffer, offset, length);
                headersSlotPosition += length;
                localBuffer = headersBuffer;
                localOffset = 0;
                localLength = headersSlotPosition;
            }
            int maxLimit = localOffset + localLength;
            expectContinuation = false;

            factory.blockRO.wrap(localBuffer, localOffset, maxLimit);
            return true;
        }

        // we still need to receive additional data, we will save current payload
        if (headersSlotIndex == NO_SLOT)
        {
            headersSlotIndex = factory.bufferPool.acquire(0);
            if (headersSlotIndex == NO_SLOT)
            {
                // all slots are in use, just reset the connection
                connectionError = Http2ErrorCode.INTERNAL_ERROR;
                return false;
            }
            headersSlotPosition = 0;
        }
        MutableDirectBuffer headersBuffer = factory.bufferPool.buffer(headersSlotIndex);
        headersBuffer.putBytes(headersSlotPosition, buffer, offset, length);
        headersSlotPosition += length;
        expectContinuation = true;
        expectContinuationStreamId = streamId;

        return false;
    }

    /**
     * Resets the headers state
     */
    void headerStateReset()
    {
        if (headersSlotIndex != NO_SLOT)
        {
            factory.bufferPool.release(headersSlotIndex);      // early release, but fine
            headersSlotIndex = NO_SLOT;
            headersSlotPosition = 0;
        }

        expectContinuation = false;
        expectContinuationStreamId = -1;
        connectionError = null;
        streamError = null;
    }

    /**
     * Checks if there are connection or stream errors from last operation
     * @return true if there is a connection or a stream error
     */
    boolean hasErrors()
    {
        return connectionError != null || streamError != null;
    }
}
