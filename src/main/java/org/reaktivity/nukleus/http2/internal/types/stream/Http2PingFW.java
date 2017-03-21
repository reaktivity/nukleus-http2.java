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

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2Flags.ACK;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.PING;

/*

    Flyweight for HTTP2 PING frame

    +-----------------------------------------------+
    |                 Length (24)                   |
    +---------------+---------------+---------------+
    |   Type (8)    |   Flags (8)   |
    +-+-------------+---------------+-------------------------------+
    |R|                 Stream Identifier (31)                      |
    +=+=============+===============================================+
    |                                                               |
    |                      Opaque Data (64)                         |
    |                                                               |
    +---------------------------------------------------------------+

 */
public class Http2PingFW extends Flyweight
{
    private static final int LENGTH_OFFSET = 0;
    private static final int TYPE_OFFSET = 3;
    private static final int FLAGS_OFFSET = 4;
    private static final int STREAM_ID_OFFSET = 5;
    private static final int PAYLOAD_OFFSET = 9;

    public int payloadOffset()
    {
        return offset() + PAYLOAD_OFFSET;
    }

    public int payloadLength()
    {
        int length = (buffer().getByte(offset() + LENGTH_OFFSET) & 0xFF) << 16;
        length += (buffer().getByte(offset() + LENGTH_OFFSET + 1) & 0xFF) << 8;
        length += buffer().getByte(offset() + LENGTH_OFFSET + 2) & 0xFF;

        // assert length == 8; caller needs to validate
        return length;
    }

    public Http2FrameType type()
    {
        //assert buffer().getByte(offset() + TYPE_OFFSET) == PING.getType();
        return PING;
    }

    public byte flags()
    {
        return buffer().getByte(offset() + FLAGS_OFFSET);
    }

    // streamId == 0, caller to validate
    public int streamId()
    {
        return buffer().getInt(offset() + STREAM_ID_OFFSET, BIG_ENDIAN) & 0x7F_FF_FF_FF;
    }

    public boolean ack()
    {
        return Http2Flags.ack(flags());
    }

    @Override
    public int limit()
    {
        return offset() + PAYLOAD_OFFSET + payloadLength();
    }

    @Override
    public Http2PingFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);
        checkLimit(limit(), maxLimit);
        return this;
    }

    @Override
    public String toString()
    {
        return String.format("%s frame <length=%s, type=%s, flags=%s, id=%s>",
                type(), payloadLength(), type(), flags(), streamId());
    }

    public static final class Builder extends Flyweight.Builder<Http2PingFW>
    {

        public Builder()
        {
            super(new Http2PingFW());
        }

        @Override
        public Builder wrap(MutableDirectBuffer buffer, int offset, int maxLimit)
        {
            super.wrap(buffer, offset, maxLimit);

            buffer.putByte(offset + LENGTH_OFFSET, (byte) 0);
            buffer.putByte(offset + LENGTH_OFFSET + 1, (byte) 0);
            buffer.putByte(offset + LENGTH_OFFSET + 2, (byte) 8);

            buffer.putByte(offset + TYPE_OFFSET, PING.getType());

            buffer.putByte(offset + FLAGS_OFFSET, (byte) 0);

            buffer.putInt(offset + STREAM_ID_OFFSET, 0, ByteOrder.BIG_ENDIAN);

            limit(offset + PAYLOAD_OFFSET + 8);

            return this;
        }

        public Builder ack()
        {
            buffer().putByte(offset() + FLAGS_OFFSET, ACK);

            return this;
        }

        public Http2PingFW.Builder payload(DirectBuffer payload, int offset, int length)
        {
            assert length == 8;

            buffer().putBytes(offset() + PAYLOAD_OFFSET, payload, offset, 8);

            buffer().putByte(offset() + LENGTH_OFFSET, (byte) 0);
            buffer().putByte(offset() + LENGTH_OFFSET + 1, (byte) 0);
            buffer().putByte(offset() + LENGTH_OFFSET + 2, (byte) 8);

            super.limit(offset() + PAYLOAD_OFFSET + 8);

            return this;
        }

    }
}

