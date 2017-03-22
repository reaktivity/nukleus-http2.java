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

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.reaktivity.nukleus.http2.internal.types.stream.FrameType.RST_STREAM;

/*

    Flyweight for HTTP2 RST_STREAM frame

    +-----------------------------------------------+
    |                 Length (24)                   |
    +---------------+---------------+---------------+
    |   Type (8)    |   Flags (8)   |
    +-+-------------+---------------+-------------------------------+
    |R|                 Stream Identifier (31)                      |
    +=+=============+===============================================+
    |                        Error Code (32)                        |
    +---------------------------------------------------------------+

 */
public class RstStreamFW extends Http2FrameFW
{
    private static final int PAYLOAD_OFFSET = 9;

    @Override
    public int payloadLength()
    {
        return 4;
    }

    @Override
    public FrameType type()
    {
        return RST_STREAM;
    }

    public int errorCode()
    {
        return buffer().getInt(offset() + PAYLOAD_OFFSET, BIG_ENDIAN);
    }

    @Override
    public RstStreamFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);

        int streamId = super.streamId();
        if (streamId == 0)
        {
            throw new IllegalArgumentException(String.format("Invalid RST_STREAM frame stream-id=%d (must not be 0)", streamId));
        }

        FrameType type = super.type();
        if (type != RST_STREAM)
        {
            throw new IllegalArgumentException(String.format("Invalid RST_STREAM frame type=%s", type));
        }

        int payloadLength = super.payloadLength();
        if (payloadLength != 4)
        {
            throw new IllegalArgumentException(String.format("Invalid RST_STREAM frame length=%d (must be 4)", payloadLength));
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

    public static final class Builder extends Http2FrameFW.Builder<Builder, RstStreamFW>
    {

        public Builder()
        {
            super(new RstStreamFW());
        }

        @Override
        public Builder wrap(MutableDirectBuffer buffer, int offset, int maxLimit)
        {
            super.wrap(buffer, offset, maxLimit);
            payloadLength(4);
            return this;
        }

        public Builder errorCode(ErrorCode errorCode)
        {
            buffer().putInt(offset() + PAYLOAD_OFFSET, errorCode.errorCode, BIG_ENDIAN);
            return this;
        }

    }
}

