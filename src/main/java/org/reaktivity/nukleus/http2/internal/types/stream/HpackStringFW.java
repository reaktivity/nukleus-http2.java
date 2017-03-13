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

import static java.nio.charset.StandardCharsets.UTF_8;

/*
 * Flyweight for HPACK String Literal Representation
 *
 *   0   1   2   3   4   5   6   7
 * +---+---+---+---+---+---+---+---+
 * | H |    String Length (7+)     |
 * +---+---------------------------+
 * |  String Data (Length octets)  |
 * +-------------------------------+
 *
 */
public class HpackStringFW extends Flyweight
{

    private final HpackIntegerFW integerRO = new HpackIntegerFW(7);
    private final AtomicBuffer payloadRO = new UnsafeBuffer(new byte[0]);

    public boolean huffman()
    {
        return (buffer().getByte(offset()) & 0x80) != 0;
    }

    public DirectBuffer payload()
    {
        return payloadRO;
    }

    @Override
    public int limit()
    {
        return integerRO.limit() + integerRO.integer();
    }

    @Override
    public HpackStringFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);

        integerRO.wrap(buffer, offset, maxLimit);
        payloadRO.wrap(buffer, integerRO.limit(), integerRO.integer());

        checkLimit(limit(), maxLimit);
        return this;
    }

    public static final class Builder extends Flyweight.Builder<HpackStringFW>
    {
        private final HpackIntegerFW.Builder integerRW = new HpackIntegerFW.Builder(7);

        public Builder()
        {
            super(new HpackStringFW());
        }

        @Override
        public HpackStringFW.Builder wrap(MutableDirectBuffer buffer, int offset, int maxLimit)
        {
            super.wrap(buffer, offset, maxLimit);
            buffer().putByte(offset(), (byte) 0x00);
            integerRW.wrap(buffer(), offset(), maxLimit());
            return this;
        }

        public HpackStringFW.Builder huffman()
        {
            throw new UnsupportedOperationException("TODO");
//            buffer().putByte(offset(), (byte) 0x80);
//            return this;
        }

        public HpackStringFW.Builder string(DirectBuffer value, int offset, int length)
        {
            integerRW.integer(length);
            buffer().putBytes(integerRW.limit(), value, offset, length);
            limit(integerRW.limit() + length);

            return this;
        }

        public HpackStringFW.Builder string(String str)
        {
            byte[] bytes = str.getBytes(UTF_8);
            integerRW.integer(bytes.length);
            buffer().putBytes(integerRW.limit(), bytes);
            limit(integerRW.limit() + bytes.length);

            return this;
        }

    }

}
