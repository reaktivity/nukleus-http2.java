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

import java.util.function.Consumer;

import static org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW.LiteralType.WITHOUT_INDEXING;

/*
 * Flyweight for HPACK Header Field
 */
public class HpackHeaderFieldFW extends Flyweight
{

    private final HpackIntegerFW indexedRO = new HpackIntegerFW(7);
    private final HpackLiteralHeaderFieldFW literalRO = new HpackLiteralHeaderFieldFW();
    private final HpackIntegerFW updateRO = new HpackIntegerFW(5);

    @Override
    public int limit()
    {
        switch (type())
        {
            case INDEXED :
                return indexedRO.limit();
            case LITERAL :
                return literalRO.limit();
            case UPDATE:
                return updateRO.limit();
        }
        return 0;
    }

    public enum HeaderFieldType
    {
        INDEXED,        // Indexed Header Field Representation
        LITERAL,        // Literal Header Field Representation
        UPDATE          // Dynamic Table Size Update
    }

    public HeaderFieldType type()
    {
        byte b = buffer().getByte(offset());

        if ((b & 0b1000_0000) == 0b1000_0000)
        {
            return HeaderFieldType.INDEXED;
        }
        else if ((b & 0b1100_0000) == 0b0100_0000 || (b & 0b1111_0000) == 0 || (b & 0b1111_0000) == 0b0001_0000)
        {
            return HeaderFieldType.LITERAL;
        }
        else if ((b & 0b1110_0000) == 0b0010_0000)
        {
            return HeaderFieldType.UPDATE;
        }

        return null;
    }

    public int index()
    {
        assert type() == HeaderFieldType.INDEXED;

        return indexedRO.integer();
    }

    public HpackLiteralHeaderFieldFW literal()
    {
        assert type() == HeaderFieldType.LITERAL;

        return literalRO;
    }

    @Override
    public HpackHeaderFieldFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);

        switch (type())
        {
            case INDEXED :
                indexedRO.wrap(buffer(), offset, maxLimit());
                break;
            case LITERAL :
                literalRO.wrap(buffer(), offset, maxLimit());
                break;
            case UPDATE:
                updateRO.wrap(buffer(), offset, maxLimit());
                break;
        }

        checkLimit(limit(), maxLimit);
        return this;
    }

    public static final class Builder extends Flyweight.Builder<HpackHeaderFieldFW>
    {
        private final HpackIntegerFW.Builder indexedRW = new HpackIntegerFW.Builder(7);
        private final HpackLiteralHeaderFieldFW.Builder literalRW = new HpackLiteralHeaderFieldFW.Builder();

        public Builder()
        {
            super(new HpackHeaderFieldFW());
        }

        @Override
        public HpackHeaderFieldFW.Builder wrap(MutableDirectBuffer buffer, int offset, int maxLimit)
        {
            super.wrap(buffer, offset, maxLimit);
            return this;
        }

        public HpackHeaderFieldFW.Builder indexed(int index)
        {
            buffer().putByte(offset(), (byte) 0x80);
            indexedRW.wrap(buffer(), offset(), maxLimit());
            indexedRW.integer(index).build();
            limit(indexedRW.limit());
            return this;
        }

        public HpackHeaderFieldFW.Builder literal(Consumer<HpackLiteralHeaderFieldFW.Builder> mutator)
        {
            literalRW.wrap(buffer(), offset(), maxLimit());
            mutator.accept(literalRW);
            limit(literalRW.build().limit());
            return this;
        }

        public HpackHeaderFieldFW.Builder literal(String name, String value)
        {
            return literal(x -> x.type(WITHOUT_INDEXING).name(name).value(value));
        }

    }

}
