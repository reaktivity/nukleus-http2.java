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

import static org.reaktivity.nukleus.http2.internal.types.stream.Http2Flags.ACK;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.SETTINGS;

public class Http2SettingsFW extends Flyweight {
    private static final int HEADER_TABLE_SIZE = 1;
    public static final int ENABLE_PUSH = 2;
    public static final int MAX_CONCURRENT_STREAMS = 3;
    public static final int INITIAL_WINDOW_SIZE = 4;
    public static final int MAX_FRAME_SIZE = 5;
    public static final int MAX_HEADER_LIST_SIZE = 6;

    private static final int LENGTH_OFFSET = 0;
    private static final int TYPE_OFFSET = 3;
    private static final int FLAGS_OFFSET = 4;
    private static final int STREAM_ID_OFFSET = 5;
    private static final int PAYLOAD_OFFSET = 9;

    private final AtomicBuffer payloadRO = new UnsafeBuffer(new byte[0]);

    public int payloadLength() {
        int length = (buffer().getByte(offset() + LENGTH_OFFSET) & 0xFF) << 16;
        length += (buffer().getByte(offset() + LENGTH_OFFSET + 1) & 0xFF) << 8;
        length += buffer().getByte(offset() + LENGTH_OFFSET + 2) & 0xFF;

        assert length % 6 == 0;
        return length;
    }

    public Http2FrameType type() {
        assert buffer().getByte(offset() + TYPE_OFFSET) == SETTINGS.getType();
        return SETTINGS;
    }

    public byte flags() {
        return buffer().getByte(offset() + FLAGS_OFFSET);
    }

    public int streamId() {
        // Most significant bit is reserved and is ignored when receiving
        int streamId = buffer().getInt(offset() + STREAM_ID_OFFSET) & 0x7F_FF_FF_FF;
        assert streamId == 0;
        return streamId;
    }

    public long headerTableSize() {
        return settings(HEADER_TABLE_SIZE);
    }

    public long enablePush() {
        return settings(ENABLE_PUSH);
    }

    public long maxConcurrentStreams() {
        return settings(MAX_CONCURRENT_STREAMS);
    }

    public long initialWindowSize() {
        return settings(INITIAL_WINDOW_SIZE);
    }

    public long maxFrameSize() {
        return settings(MAX_FRAME_SIZE);
    }

    public long maxHeaderListSize() {
        return settings(MAX_HEADER_LIST_SIZE);
    }

    public long settings(int key) {
        int payloadLength = payloadLength();
        int noSettings = payloadLength/6;
        if (noSettings > 0) {
            for (int i = 0; i < noSettings; i++) {
                int got = buffer().getShort(offset() + PAYLOAD_OFFSET + 6 * i);
                if (key == got) {
                    return buffer().getInt(offset() + PAYLOAD_OFFSET + 6 * i + 2);
                }
            }
        }

        return -1L;
    }

    public DirectBuffer payload()
    {
        return payloadRO;
    }

    @Override
    public int limit()
    {
        return offset() + PAYLOAD_OFFSET + payloadLength();
    }

    @Override
    public Http2SettingsFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);

        payloadRO.wrap(buffer, offset() + PAYLOAD_OFFSET, payloadLength());

        checkLimit(limit(), maxLimit);

        return this;
    }

    @Override
    public String toString()
    {
        return String.format("%s frame <length=%s, type=%s, flags=%s, id=%s>",
                type(), length(), type(), flags(), streamId());
    }

    // TODO use ListFW
    public static final class Builder extends Flyweight.Builder<Http2SettingsFW>
    {
        public Builder()
        {
            super(new Http2SettingsFW());
        }

        @Override
        public Builder wrap(MutableDirectBuffer buffer, int offset, int maxLimit)
        {
            super.wrap(buffer, offset, maxLimit);

            buffer().putByte(offset() + LENGTH_OFFSET, (byte) 0);
            buffer().putByte(offset() + LENGTH_OFFSET+1, (byte) 0);
            buffer().putByte(offset() + LENGTH_OFFSET+2, (byte) 0);

            buffer().putByte(offset() + TYPE_OFFSET, SETTINGS.getType());

            buffer().putByte(offset() + FLAGS_OFFSET, (byte) 0);

            buffer().putInt(offset() + STREAM_ID_OFFSET, 0);

            super.limit(offset() + 9);

            return this;
        }

        public Builder ack() {
            buffer().putByte(offset() + FLAGS_OFFSET, ACK);
            return this;
        }

        public Builder headerTableSize(long size)
        {
            addSetting(HEADER_TABLE_SIZE, size);
            return this;
        }

        public Builder enablePush()
        {
            addSetting(ENABLE_PUSH, 1L);
            return this;
        }

        public Builder maxConcurrentStreams(long streams)
        {
            addSetting(MAX_CONCURRENT_STREAMS, streams);
            return this;
        }

        public Builder initialWindowSize(long size)
        {
            addSetting(INITIAL_WINDOW_SIZE, size);
            return this;
        }

        public Builder maxFrameSize(long size)
        {
            addSetting(MAX_FRAME_SIZE, size);
            return this;
        }

        public Builder maxHeaderListSize(long size)
        {
            addSetting(MAX_HEADER_LIST_SIZE, size);
            return this;
        }

        private void addSetting(int key, long value) {
            int curLimit = limit();
            limit(curLimit + 6);

            int length = limit() - 9;
            buffer().putByte(offset() + LENGTH_OFFSET, (byte) ((length & 0x00_FF_00_00) >>> 16));
            buffer().putByte(offset() + LENGTH_OFFSET +1, (byte) ((length & 0x00_00_FF_00) >>> 8));
            buffer().putByte(offset() + LENGTH_OFFSET + 2, (byte) ((length & 0x00_00_00_FF)));

            buffer().putShort(curLimit, (short) key, ByteOrder.BIG_ENDIAN);
            buffer().putInt(curLimit + 2, (int) value, ByteOrder.BIG_ENDIAN);
        }

    }
}

