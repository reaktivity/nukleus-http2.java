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
import java.util.Map;
import java.util.function.Consumer;

import static org.reaktivity.nukleus.http2.internal.types.stream.Http2Flags.END_HEADERS;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2Flags.END_STREAM;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2Flags.PADDED;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2Flags.PRIORITY;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.DATA;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.HEADERS;

/*

    Flyweight for HTTP2 HEADERS frame

    +-----------------------------------------------+
    |                 Length (24)                   |
    +---------------+---------------+---------------+
    |   Type (8)    |   Flags (8)   |
    +-+-------------+---------------+-------------------------------+
    |R|                 Stream Identifier (31)                      |
    +=+=============+===============================================+
    |Pad Length? (8)|
    +-+-------------+-----------------------------------------------+
    |E|                 Stream Dependency? (31)                     |
    +-+-------------+-----------------------------------------------+
    |  Weight? (8)  |
    +-+-------------+-----------------------------------------------+
    |                   Header Block Fragment (*)                 ...
    +---------------------------------------------------------------+
    |                           Padding (*)                       ...
    +---------------------------------------------------------------+

 */
public class Http2HeadersFW extends Flyweight {
    private static final int LENGTH_OFFSET = 0;
    private static final int TYPE_OFFSET = 3;
    private static final int FLAGS_OFFSET = 4;
    private static final int STREAM_ID_OFFSET = 5;
    private static final int PAYLOAD_OFFSET = 9;

    private final HpackHeaderBlockFW headerBlockRO = new HpackHeaderBlockFW();

    public int payloadLength() {
        int length = (buffer().getByte(offset() + LENGTH_OFFSET) & 0xFF) << 16;
        length += (buffer().getByte(offset() + LENGTH_OFFSET + 1) & 0xFF) << 8;
        length += buffer().getByte(offset() + LENGTH_OFFSET + 2) & 0xFF;
        return length;
    }

    public Http2FrameType type() {
        assert buffer().getByte(offset() + TYPE_OFFSET) == HEADERS.getType();
        return HEADERS;
    }

    public byte flags() {
        return buffer().getByte(offset() + FLAGS_OFFSET);
    }

    public int streamId() {
        // Most significant bit is reserved and is ignored when receiving
        int streamId = buffer().getInt(offset() + STREAM_ID_OFFSET) & 0x7F_FF_FF_FF;
        assert streamId != 0;
        return streamId;
    }

    public boolean padded() {
        return Http2Flags.padded(flags());
    }

    public boolean endStream() {
        return Http2Flags.endStream(flags());
    }

    public boolean endHeaders() {
        return Http2Flags.endHeaders(flags());
    }

    public boolean priority() {
        return Http2Flags.priority(flags());
    }

    private int dataOffset() {
        int dataOffset = offset() + PAYLOAD_OFFSET;
        if (padded()) {
            dataOffset++;        // +1 for Pad Length
        }
        if (priority()) {
            dataOffset += 5;      // +4 for Stream Dependency, +1 for Weight
        }
        return dataOffset;
    }

    public int dataLength() {
        int dataLength = payloadLength();
        if (padded()) {
            int paddingLength = buffer().getByte(offset() + PAYLOAD_OFFSET);
            dataLength -= (paddingLength + 1);    // -1 for Pad Length, -Padding
        }

        if (priority()) {
            dataLength -= 5;    // -4 for Stream Dependency, -1 for Weight
        }

        return dataLength;
    }

    public void headers(Consumer<HpackHeaderFieldFW> headerField) {
        headerBlockRO.forEach(headerField);
    }

    @Override
    public int limit()
    {
        return offset() + PAYLOAD_OFFSET + payloadLength();
    }

    @Override
    public Http2HeadersFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);
        headerBlockRO.wrap(buffer(), dataOffset(), dataOffset() + dataLength());

        checkLimit(limit(), maxLimit);
        return this;
    }

    @Override
    public String toString()
    {
        return String.format("%s frame <length=%s, type=%s, flags=%s, id=%s>",
                type(), length(), type(), flags(), streamId());
    }

    public static final class Builder extends Flyweight.Builder<Http2HeadersFW>
    {
        public Builder()
        {
            super(new Http2HeadersFW());
        }

        @Override
        public Builder wrap(MutableDirectBuffer buffer, int offset, int maxLimit)
        {
            super.wrap(buffer, offset, maxLimit);

            int length = 0;
            buffer().putByte(offset() + LENGTH_OFFSET, (byte) ((length & 0x00_FF_00_00) >>> 16));
            buffer().putByte(offset() + LENGTH_OFFSET +1, (byte) ((length & 0x00_00_FF_00) >>> 8));
            buffer().putByte(offset() + LENGTH_OFFSET + 2, (byte) ((length & 0x00_00_00_FF)));

            buffer().putByte(offset() + TYPE_OFFSET, DATA.getType());

            buffer().putByte(offset() + FLAGS_OFFSET, (byte) 0);

            return this;
        }

        public Builder streamId(int streamId) {
            buffer().putInt(offset() + STREAM_ID_OFFSET, streamId, ByteOrder.BIG_ENDIAN);
            return this;
        }

        public Builder padded(boolean padded) {
            buffer().putByte(offset() + FLAGS_OFFSET, PADDED);
            return this;
        }

        public Builder endStream() {
            buffer().putByte(offset() + FLAGS_OFFSET, END_STREAM);
            return this;
        }

        public Builder endHeaders() {
            buffer().putByte(offset() + FLAGS_OFFSET, END_HEADERS);
            return this;
        }

        public Builder priority() {
            buffer().putByte(offset() + FLAGS_OFFSET, PRIORITY);
            return this;
        }

        public Builder headers(Map<String, String> headers) {
//            HpackEncoder encoder = new HpackEncoder(4096);
//            HeaderMap map = new HeaderMap();
//            headers.forEach((k,v) -> {
//                k = k.substring(1);
//                HttpString hs = new HttpString(k);
//                map.add(hs, v);
//            });
//
//            ByteBuffer bb = ByteBuffer.wrap(new byte[4096]);
//            //bb.flip();
//            encoder.encode(map, bb);
//            bb.flip();
//
//            // TODO padded and priority flag
//            buffer().putBytes(offset() + LENGTH_OFFSET, bb, bb.position(), bb.limit());
//
//            limit(offset() + 9 + bb.limit());
//
//            int length = bb.limit()-bb.position();
//            buffer().putByte(offset() + LENGTH_OFFSET, (byte) ((length & 0x00_FF_00_00) >>> 16));
//            buffer().putByte(offset() + LENGTH_OFFSET +1, (byte) ((length & 0x00_00_FF_00) >>> 8));
//            buffer().putByte(offset() + LENGTH_OFFSET + 2, (byte) ((length & 0x00_00_00_FF)));
            return this;
        }

    }
}

