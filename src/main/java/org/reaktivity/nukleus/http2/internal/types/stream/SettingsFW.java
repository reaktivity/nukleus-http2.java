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
import org.reaktivity.nukleus.http2.internal.types.ListFW;

import java.util.function.Consumer;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.reaktivity.nukleus.http2.internal.types.stream.Flags.ACK;
import static org.reaktivity.nukleus.http2.internal.types.stream.FrameType.SETTINGS;

/*
    Flyweight for HTTP2 SETTINGS frame

    +-----------------------------------------------+
    |                 Length (24)                   |
    +---------------+---------------+---------------+
    |   Type (8)    |   Flags (8)   |
    +-+-------------+---------------+-------------------------------+
    |R|                 Stream Identifier (31)                      |
    +=+=============================+===============================+
    |       Identifier (16)         |
    +-------------------------------+-------------------------------+
    |                        Value (32)                             |
    +---------------------------------------------------------------+
    |       ...                     |
    +-------------------------------+-------------------------------+
    |                        ...                                    |
    +---------------------------------------------------------------+

 */
public class SettingsFW extends Flyweight
{
    private static final int HEADER_TABLE_SIZE = 1;
    private static final int ENABLE_PUSH = 2;
    private static final int MAX_CONCURRENT_STREAMS = 3;
    private static final int INITIAL_WINDOW_SIZE = 4;
    private static final int MAX_FRAME_SIZE = 5;
    private static final int MAX_HEADER_LIST_SIZE = 6;

    private static final int LENGTH_OFFSET = 0;
    private static final int TYPE_OFFSET = 3;
    private static final int FLAGS_OFFSET = 4;
    private static final int STREAM_ID_OFFSET = 5;
    private static final int PAYLOAD_OFFSET = 9;

    private final ListFW<SettingFW> listFW = new ListFW<>(new SettingFW());

    public int payloadLength()
    {
        return Http2FrameFW.payloadLength(buffer(), offset());
    }

    public FrameType type()
    {
        return SETTINGS;
    }

    public byte flags()
    {
        return buffer().getByte(offset() + FLAGS_OFFSET);
    }

    public boolean ack()
    {
        return Flags.ack(flags());
    }

    public int streamId()
    {
        return buffer().getInt(offset() + STREAM_ID_OFFSET, BIG_ENDIAN) & 0x7F_FF_FF_FF;
    }

    public long headerTableSize()
    {
        return settings(HEADER_TABLE_SIZE);
    }

    public long enablePush()
    {
        return settings(ENABLE_PUSH);
    }

    public long maxConcurrentStreams()
    {
        return settings(MAX_CONCURRENT_STREAMS);
    }

    public long initialWindowSize()
    {
        return settings(INITIAL_WINDOW_SIZE);
    }

    public long maxFrameSize()
    {
        return settings(MAX_FRAME_SIZE);
    }

    public long maxHeaderListSize()
    {
        return settings(MAX_HEADER_LIST_SIZE);
    }

    public long settings(int key)
    {
        long[] value = new long[] { -1L };

        listFW.forEach(x ->
        {
            if (x.id() == key)
            {
                value[0] = x.value();
            }
        });
        return value[0];
    }

    @Override
    public int limit()
    {
        return offset() + PAYLOAD_OFFSET + payloadLength();
    }

    @Override
    public SettingsFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);

        int streamId = Http2FrameFW.streamId(buffer, offset);
        if (streamId != 0)
        {
            throw new Http2Exception(String.format("Invalid SETTINGS frame stream-id=%d", streamId));
        }

        FrameType type = Http2FrameFW.type(buffer, offset);
        if (type != SETTINGS)
        {
            throw new Http2Exception(String.format("Invalid SETTINGS frame type=%s", type));
        }

        int payloadLength = Http2FrameFW.payloadLength(buffer, offset);
        if (payloadLength%6 != 0)
        {
            throw new Http2Exception(String.format("Invalid SETTINGS frame length=%d", payloadLength));
        }

        listFW.wrap(buffer, offset + PAYLOAD_OFFSET, limit());
        checkLimit(limit(), maxLimit);
        return this;
    }

    @Override
    public String toString()
    {
        return String.format("%s frame <length=%s, type=%s, flags=%s, id=%s>",
                type(), payloadLength(), type(), flags(), streamId());
    }

    public static final class Builder extends Flyweight.Builder<SettingsFW>
    {
        private final ListFW.Builder<SettingFW.Builder, SettingFW> settingsRW =
                new ListFW.Builder<>(new SettingFW.Builder(), new SettingFW());

        public Builder()
        {
            super(new SettingsFW());
        }

        @Override
        public Builder wrap(MutableDirectBuffer buffer, int offset, int maxLimit)
        {
            super.wrap(buffer, offset, maxLimit);

            Http2FrameFW.putPayloadLength(buffer, offset, 0);

            buffer().putByte(offset() + TYPE_OFFSET, SETTINGS.getType());

            buffer().putByte(offset() + FLAGS_OFFSET, (byte) 0);

            buffer().putInt(offset() + STREAM_ID_OFFSET, 0);

            super.limit(offset() + PAYLOAD_OFFSET);

            settingsRW.wrap(buffer, offset + PAYLOAD_OFFSET, maxLimit);
            return this;
        }

        public Builder ack()
        {
            buffer().putByte(offset() + FLAGS_OFFSET, ACK);
            return this;
        }

        public Builder headerTableSize(long size)
        {
            addSetting(x -> x.setting(HEADER_TABLE_SIZE, size));
            return this;
        }

        public Builder enablePush()
        {
            addSetting(x -> x.setting(ENABLE_PUSH, 1L));
            return this;
        }

        public Builder maxConcurrentStreams(long streams)
        {
            addSetting(x -> x.setting(MAX_CONCURRENT_STREAMS, streams));
            return this;
        }

        public Builder initialWindowSize(long size)
        {
            addSetting(x -> x.setting(INITIAL_WINDOW_SIZE, size));
            return this;
        }

        public Builder maxFrameSize(long size)
        {
            addSetting(x -> x.setting(MAX_FRAME_SIZE, size));
            return this;
        }

        public Builder maxHeaderListSize(long size)
        {
            addSetting(x -> x.setting(MAX_HEADER_LIST_SIZE, size));
            return this;
        }

        private Builder addSetting(Consumer<SettingFW.Builder> mutator)
        {
            settingsRW.item(mutator);
            int length = settingsRW.limit() - offset() - PAYLOAD_OFFSET;
            Http2FrameFW.putPayloadLength(buffer(), offset(), length);
            limit(settingsRW.limit());
            return this;
        }

    }
}

