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
package org.reaktivity.nukleus.http2.internal.types.stream;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.http2.internal.types.Flyweight;

import java.nio.ByteOrder;
import java.util.function.Consumer;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.reaktivity.nukleus.http2.internal.types.stream.Flags.END_HEADERS;
import static org.reaktivity.nukleus.http2.internal.types.stream.FrameType.CONTINUATION;

/*

    Flyweight for HTTP2 CONTINUATION frame

    +-----------------------------------------------+
    |                 Length (24)                   |
    +---------------+---------------+---------------+
    |   Type (8)    |   Flags (8)   |
    +-+-------------+---------------+-------------------------------+
    |R|                 Stream Identifier (31)                      |
    +=+=============+===============================================+
    |                   Header Block Fragment (*)                 ...
    +---------------------------------------------------------------+

 */
public class ContinuationFW extends Flyweight
{
    private static final int LENGTH_OFFSET = 0;
    private static final int TYPE_OFFSET = 3;
    private static final int FLAGS_OFFSET = 4;
    private static final int STREAM_ID_OFFSET = 5;
    private static final int PAYLOAD_OFFSET = 9;

    private final HpackHeaderBlockFW headerBlockRO = new HpackHeaderBlockFW();

    public int payloadLength()
    {
        return Http2FrameFW.payloadLength(buffer(), offset());
    }

    public FrameType type()
    {
        return CONTINUATION;
    }

    public byte flags()
    {
        return buffer().getByte(offset() + FLAGS_OFFSET);
    }

    public int streamId()
    {
        return buffer().getInt(offset() + STREAM_ID_OFFSET, BIG_ENDIAN) & 0x7F_FF_FF_FF;
    }

    public boolean endHeaders()
    {
        return Flags.endHeaders(flags());
    }

    public void forEach(Consumer<HpackHeaderFieldFW> headerField)
    {
        headerBlockRO.forEach(headerField);
    }

    @Override
    public int limit()
    {
        return offset() + PAYLOAD_OFFSET + payloadLength();
    }

    @Override
    public ContinuationFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);

        int streamId = Http2FrameFW.streamId(buffer, offset);
        if (streamId == 0)
        {
            throw new IllegalArgumentException(
                    String.format("Invalid CONTINUATION frame stream-id=%d (must not be 0)", streamId));
        }

        FrameType type = Http2FrameFW.type(buffer, offset);
        if (type != CONTINUATION)
        {
            throw new IllegalArgumentException(String.format("Invalid CONTINUATION frame type=%s", type));
        }

        headerBlockRO.wrap(buffer(), offset() + PAYLOAD_OFFSET, offset() + PAYLOAD_OFFSET + payloadLength());

        checkLimit(limit(), maxLimit);
        return this;
    }

    @Override
    public String toString()
    {
        return String.format("%s frame <length=%s, type=%s, flags=%s, id=%s>",
                type(), payloadLength(), type(), flags(), streamId());
    }

    public static final class Builder extends Flyweight.Builder<ContinuationFW>
    {
        private final HpackHeaderBlockFW.Builder blockRW = new HpackHeaderBlockFW.Builder();

        public Builder()
        {
            super(new ContinuationFW());
        }

        @Override
        public Builder wrap(MutableDirectBuffer buffer, int offset, int maxLimit)
        {
            super.wrap(buffer, offset, maxLimit);

            Http2FrameFW.putPayloadLength(buffer, offset, 0);

            buffer.putByte(offset + TYPE_OFFSET, CONTINUATION.type());

            buffer.putByte(offset + FLAGS_OFFSET, (byte) 0);

            limit(offset + PAYLOAD_OFFSET);

            blockRW.wrap(buffer, offset + PAYLOAD_OFFSET, maxLimit);

            return this;
        }

        public Builder streamId(int streamId)
        {
            buffer().putInt(offset() + STREAM_ID_OFFSET, streamId, ByteOrder.BIG_ENDIAN);
            return this;
        }

        public Builder endHeaders()
        {
            buffer().putByte(offset() + FLAGS_OFFSET, END_HEADERS);
            return this;
        }

        public Builder header(Consumer<HpackHeaderFieldFW.Builder> mutator)
        {
            blockRW.header(mutator);
            int length = blockRW.limit() - offset() - PAYLOAD_OFFSET;
            Http2FrameFW.putPayloadLength(buffer(), offset(), length);
            limit(blockRW.limit());
            return this;
        }

    }
}

