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
import org.reaktivity.nukleus.http2.internal.types.Flyweight;

/*
 * Flyweight for HPACK Literal Header Field
 */
public class HpackLiteralHeaderFieldFW extends Flyweight {

    private final HpackIntegerFW integer6RO = new HpackIntegerFW(6);
    private final HpackIntegerFW integer4RO = new HpackIntegerFW(4);

    private final HpackStringFW nameRO = new HpackStringFW();
    private final HpackStringFW valueRO = new HpackStringFW();

    @Override
    public int limit()
    {
        return valueRO.limit();
    }

    public enum LiteralType
    {
        INCREMENTAL_INDEXING,   // Literal Header Field with Incremental Indexing
        WITHOUT_INDEXING,       // Literal Header Field without Indexing
        NEVER_INDEXED,          // Literal Header Field Never Indexed
    }

    public enum NameType {
        INDEXED,                // Literal Header Field -- Indexed Name
        NEW                     // Literal Header Field -- New Name
    }

    public LiteralType literalType() {
        byte b = buffer().getByte(offset());

        if ((b & 0b1100_0000) == 0b0100_0000) {
            return LiteralType.INCREMENTAL_INDEXING;
        } else if ((b & 0b1111_0000) == 0) {
            return LiteralType.WITHOUT_INDEXING;
        } else if ((b & 0b1111_0000) == 0b0001_0000) {
            return LiteralType.NEVER_INDEXED;
        }

        return null;
    }


    public int nameIndex() {
        assert nameType() == NameType.INDEXED;

        switch (literalType()) {
            case INCREMENTAL_INDEXING:
                return integer6RO.integer();
            case WITHOUT_INDEXING:
            case NEVER_INDEXED:
                return integer4RO.integer();
        }

        return 0;
    }

    public NameType nameType() {
        switch (literalType()) {
            case INCREMENTAL_INDEXING:
                return integer6RO.integer() == 0 ? NameType.NEW : NameType.INDEXED;
            case WITHOUT_INDEXING:
            case NEVER_INDEXED:
                return integer4RO.integer() == 0 ? NameType.NEW : NameType.INDEXED;
        }

        return null;
    }

    public HpackStringFW nameLiteral() {
        assert nameType() == NameType.NEW;

        return valueRO;
    }

    public HpackStringFW valueLiteral() {
        return valueRO;
    }

    @Override
    public HpackLiteralHeaderFieldFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);

        switch (literalType()) {
            case INCREMENTAL_INDEXING:
                integer6RO.wrap(buffer(), offset, maxLimit());
                literalHeader(integer6RO);
                break;
            case WITHOUT_INDEXING:
            case NEVER_INDEXED:
                integer4RO.wrap(buffer(), offset, maxLimit());
                literalHeader(integer4RO);
                break;
        }

        checkLimit(limit(), maxLimit);
        return this;
    }

    private void literalHeader(HpackIntegerFW integerRO) {
        int index = integerRO.integer();
        int offset = integerRO.limit();
        if (index == 0) {
            nameRO.wrap(buffer(), offset, maxLimit());
            offset = nameRO.limit();
        }
        valueRO.wrap(buffer(), offset, maxLimit());
    }
/*
    public static final class Builder extends Flyweight.Builder<HpackHeaderFieldFW>
    {
        private final HpackIntegerFW.Builder integerRW = new HpackIntegerFW.Builder(7);

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

        public HpackHeaderFieldFW.Builder string(String str, boolean huffman) {
            if (huffman) {
                throw new UnsupportedOperationException("TODO Not yet implemented");
            }
            if (huffman) {
                buffer().putByte(offset(), (byte) 0x80);
            }
            integerRW.wrap(buffer(), offset(), maxLimit()).integer(str.length(), 7).build();
            int offset = integerRW.limit();
            for(int i=0; i < str.length(); i++) {
                buffer().putByte(offset + i, (byte) str.charAt(i));
            }
            limit(offset + str.length());

            return this;
        }

    } */

}
