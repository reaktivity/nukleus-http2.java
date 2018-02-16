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

import static java.nio.ByteOrder.BIG_ENDIAN;

/*
    Flyweight for for all HTTP2 frame headers

    +-----------------------------------------------+
    |                 Length (24)                   |
    +---------------+---------------+---------------+
    |   Type (8)    |   Flags (8)   |
    +-+-------------+---------------+-------------------------------+
    |R|                 Stream Identifier (31)                      |
    +=+=============================================================+

 */
public class Http2FrameHeaderFW extends Flyweight
{
    private static final int LENGTH_OFFSET = 0;
    private static final int TYPE_OFFSET = 3;
    private static final int FLAGS_OFFSET = 4;
    private static final int STREAM_ID_OFFSET = 5;
    private static final int PAYLOAD_OFFSET = 9;

    public int payloadLength()
    {
        int length = (buffer().getByte(offset() + LENGTH_OFFSET) & 0xFF) << 16;
        length += (buffer().getShort(offset() + LENGTH_OFFSET + 1, BIG_ENDIAN) & 0xFF_FF);
        return length;
    }

    public Http2FrameType type()
    {
        return Http2FrameType.get(buffer().getByte(offset() + TYPE_OFFSET));
    }

    public final byte flags()
    {
        return buffer().getByte(offset() + FLAGS_OFFSET);
    }

    public final boolean endStream()
    {
        return Http2Flags.endStream(flags());
    }

    public int streamId()
    {
        return buffer().getInt(offset() + STREAM_ID_OFFSET, BIG_ENDIAN) & 0x7F_FF_FF_FF;
    }

    public final int payloadOffset()
    {
        return offset() + PAYLOAD_OFFSET;
    }

    @Override
    public final int limit()
    {
        return offset() + PAYLOAD_OFFSET;
    }

    @Override
    public Http2FrameHeaderFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        if (maxLimit - offset < 9)
        {
            throw new IllegalArgumentException("Invalid HTTP2 frame header (not enough bytes for 9-octet header)");
        }
        super.wrap(buffer, offset, maxLimit);

        checkLimit(limit(), maxLimit);

        return this;
    }

    public Http2FrameHeaderFW canWrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        if (maxLimit - offset < 9)
        {
            return null;            // Not enough bytes for 9-octet header
        }

        return wrap(buffer, offset, maxLimit);
    }

    @Override
    public String toString()
    {
        return String.format("%s frame <length=%s, flags=%s, id=%s>",
                type(), payloadLength(), flags(), streamId());
    }

    public static class Builder extends Flyweight.Builder<Http2FrameHeaderFW>
    {

        public Builder()
        {
            super(new Http2FrameHeaderFW());
        }

        @Override
        public Http2FrameHeaderFW.Builder wrap(MutableDirectBuffer buffer, int offset, int maxLimit)
        {
            super.wrap(buffer, offset, maxLimit);

            buffer.putByte(offset + TYPE_OFFSET, (byte) 0);
            buffer.putByte(offset + FLAGS_OFFSET, (byte) 0);
            buffer.putInt(offset + STREAM_ID_OFFSET, 0, BIG_ENDIAN);
            payloadLength(0);

            return this;
        }

        public final Http2FrameHeaderFW.Builder type(Http2FrameType type)
        {
            buffer().putByte(offset() + TYPE_OFFSET, type.type());
            return this;
        }

        public final Http2FrameHeaderFW.Builder flags(byte f)
        {
            byte flags = buffer().getByte(offset() + FLAGS_OFFSET);
            flags |= f;
            buffer().putByte(offset() + FLAGS_OFFSET, flags);
            return this;
        }

        public final Http2FrameHeaderFW.Builder streamId(int streamId)
        {
            buffer().putInt(offset() + STREAM_ID_OFFSET, streamId, BIG_ENDIAN);
            return this;
        }

        public final Http2FrameHeaderFW.Builder payloadLength(int length)
        {
            buffer().putShort(offset() + LENGTH_OFFSET, (short) ((length & 0x00_FF_FF_00) >>> 8), BIG_ENDIAN);
            buffer().putByte(offset() + LENGTH_OFFSET + 2, (byte) (length & 0x00_00_00_FF));

            limit(offset() + PAYLOAD_OFFSET);
            return this;
        }

    }

}
