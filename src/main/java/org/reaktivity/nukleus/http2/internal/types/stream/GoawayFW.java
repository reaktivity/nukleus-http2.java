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
import static org.reaktivity.nukleus.http2.internal.types.stream.FrameType.GO_AWAY;

/*

    Flyweight for HTTP2 GOAWAY frame

    +-----------------------------------------------+
    |                 Length (24)                   |
    +---------------+---------------+---------------+
    |   Type (8)    |   Flags (8)   |
    +-+-------------+---------------+-------------------------------+
    |R|                 Stream Identifier (31)                      |
    +=+=============+===============================================+
    |R|                  Last-Stream-ID (31)                        |
    +-+-------------------------------------------------------------+
    |                      Error Code (32)                          |
    +---------------------------------------------------------------+
    |                  Additional Debug Data (*)                    |
    +---------------------------------------------------------------+

 */
public class GoawayFW extends Flyweight
{
    private static final int LENGTH_OFFSET = 0;
    private static final int TYPE_OFFSET = 3;
    private static final int FLAGS_OFFSET = 4;
    private static final int STREAM_ID_OFFSET = 5;
    private static final int LAST_STREAM_ID_OFFSET = 9;
    private static final int ERROR_CODE_OFFSET = 13;
    private static final int PAYLOAD_OFFSET = 9;

    public int payloadLength()
    {
        return Http2FrameFW.payloadLength(buffer(), offset());
    }

    public FrameType type()
    {
        return GO_AWAY;
    }

    public byte flags()
    {
        return buffer().getByte(offset() + FLAGS_OFFSET);
    }

    public int streamId()
    {
        return 0;
    }

    public int lastStreamId()
    {
        return buffer().getInt(offset() + LAST_STREAM_ID_OFFSET, BIG_ENDIAN) & 0x7F_FF_FF_FF;
    }

    public int errorCode()
    {
        return buffer().getInt(offset() + ERROR_CODE_OFFSET, BIG_ENDIAN);
    }

    @Override
    public int limit()
    {
        return offset() + PAYLOAD_OFFSET + payloadLength();
    }

    @Override
    public GoawayFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);

        int streamId = Http2FrameFW.streamId(buffer, offset);
        if (streamId != 0)
        {
            throw new Http2Exception(String.format("Invalid stream-id=%d for GOAWAY frame", streamId));
        }

        FrameType type = Http2FrameFW.type(buffer, offset);
        if (type != GO_AWAY)
        {
            throw new Http2Exception(String.format("Invalid type=%s for GOAWAY frame", type));
        }

        checkLimit(limit(), maxLimit);
        return this;
    }

    @Override
    public String toString()
    {
        return String.format("%s frame <length=%s, type=%s, flags=%s, id=%s>",
                type(), payloadLength(), type(), flags(), streamId());
    }

    public static final class Builder extends Flyweight.Builder<GoawayFW>
    {

        public Builder()
        {
            super(new GoawayFW());
        }

        @Override
        public Builder wrap(MutableDirectBuffer buffer, int offset, int maxLimit)
        {
            super.wrap(buffer, offset, maxLimit);

            // not including "Additional Debug Data"
            Http2FrameFW.putPayloadLength(buffer, offset, 8);

            buffer.putByte(offset + TYPE_OFFSET, GO_AWAY.getType());

            buffer.putByte(offset + FLAGS_OFFSET, (byte) 0);

            buffer.putInt(offset + STREAM_ID_OFFSET, 0, BIG_ENDIAN);

            limit(offset + PAYLOAD_OFFSET + 8);   // +4 for last stream id, +4 for error code

            return this;
        }

        public GoawayFW.Builder lastStreamId(int lastStreamId)
        {
            buffer().putInt(offset() + LAST_STREAM_ID_OFFSET, lastStreamId, BIG_ENDIAN);
            return this;
        }

        public GoawayFW.Builder errorCode(ErrorCode error)
        {
            buffer().putInt(offset() + ERROR_CODE_OFFSET, error.errorCode, BIG_ENDIAN);
            return this;
        }

    }
}
