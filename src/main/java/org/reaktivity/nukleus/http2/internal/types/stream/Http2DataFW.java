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
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.http2.internal.types.Flyweight;

import java.nio.ByteOrder;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.reaktivity.nukleus.http2.internal.types.stream.FrameType.DATA;

/*
    Flyweight for HTTP2 DATA frame

    +-----------------------------------------------+
    |                 Length (24)                   |
    +---------------+---------------+---------------+
    |   Type (8)    |   Flags (8)   |
    +-+-------------+---------------+-------------------------------+
    |R|                 Stream Identifier (31)                      |
    +=+=============+===============================================+
    |Pad Length? (8)|
    +---------------+-----------------------------------------------+
    |                            Data (*)                         ...
    +---------------------------------------------------------------+
    |                           Padding (*)                       ...
    +---------------------------------------------------------------+

 */
public class Http2DataFW extends Flyweight
{
    private static final int LENGTH_OFFSET = 0;
    private static final int TYPE_OFFSET = 3;
    private static final int FLAGS_OFFSET = 4;
    private static final int STREAM_ID_OFFSET = 5;
    private static final int PAYLOAD_OFFSET = 9;

    private final AtomicBuffer dataRO = new UnsafeBuffer(new byte[0]);

    public int payloadLength()
    {
        int length = (buffer().getByte(offset() + LENGTH_OFFSET) & 0xFF) << 16;
        length += (buffer().getByte(offset() + LENGTH_OFFSET + 1) & 0xFF) << 8;
        length += buffer().getByte(offset() + LENGTH_OFFSET + 2) & 0xFF;
        return length;
    }

    public FrameType type()
    {
        //assert buffer().getByte(offset() + TYPE_OFFSET) == DATA.getType();
        return DATA;
    }

    public byte flags()
    {
        return buffer().getByte(offset() + FLAGS_OFFSET);
    }

    public int streamId()
    {
        return buffer().getInt(offset() + STREAM_ID_OFFSET, BIG_ENDIAN) & 0x7F_FF_FF_FF;
    }

    private boolean padding()
    {
        return Flags.padded(flags());
    }

    private boolean endStream()
    {
        return Flags.endStream(flags());
    }

    public int dataOffset()
    {
        int payloadOffset = offset() + PAYLOAD_OFFSET;
        return  padding() ? payloadOffset + 1 : payloadOffset;
    }

    public int dataLength()
    {
        if (padding())
        {
            int paddingLength = buffer().getByte(offset() + PAYLOAD_OFFSET);
            return payloadLength() - paddingLength - 1;
        }
        else
        {
            return payloadLength();
        }
    }

    public DirectBuffer data()
    {
        return dataRO;
    }

    @Override
    public int limit()
    {
        return offset() + PAYLOAD_OFFSET + payloadLength();
    }

    @Override
    public Http2DataFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);

        dataRO.wrap(buffer, dataOffset(), dataLength());

        checkLimit(limit(), maxLimit);

        return this;
    }

    @Override
    public String toString()
    {
        return String.format("%s frame <length=%s, type=%s, flags=%s, id=%s>",
                type(), payloadLength(), type(), flags(), streamId());
    }

    public static final class Builder extends Flyweight.Builder<Http2DataFW>
    {
        public Builder()
        {
            super(new Http2DataFW());
        }

        @Override
        public Builder wrap(MutableDirectBuffer buffer, int offset, int maxLimit)
        {
            super.wrap(buffer, offset, maxLimit);

            Http2FrameFW.putPayloadLength(buffer, offset, 0);

            buffer.putByte(offset + TYPE_OFFSET, DATA.type());

            buffer.putByte(offset + FLAGS_OFFSET, (byte) 0);

            return this;
        }

        public Builder streamId(int streamId)
        {
            buffer().putInt(offset() + STREAM_ID_OFFSET, streamId, ByteOrder.BIG_ENDIAN);
            return this;
        }

        public Builder endStream()
        {
            buffer().putByte(offset() + FLAGS_OFFSET, (byte) 1);
            return this;
        }

        public Builder payload(DirectBuffer buffer)
        {
            return payload(buffer, 0, buffer.capacity());
        }

        public Builder payload(DirectBuffer payload, int offset, int length)
        {
            buffer().putBytes(offset() + PAYLOAD_OFFSET, payload, offset, length);

            Http2FrameFW.putPayloadLength(buffer(), offset(), length);

            super.limit(offset() + length + 9);

            return this;
        }
    }
}

