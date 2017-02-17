package org.reaktivity.nukleus.http2.internal.types.stream;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.http2.internal.types.Flyweight;

// TODO should we restrict the no of octets ?
// TODO enough data in buf ?

public class HpackIntegerFW extends Flyweight {

    private static final int[] TWON_TABLE;

    private int decodedOctets;

    static {
        TWON_TABLE = new int[32];
        for (int i = 0; i < 32; ++i) {
            TWON_TABLE[i] = 1 << i;
        }
    }

    /*
     * decode I from the next N bits
     * if I < 2^N - 1, return I
     * else
     *     M = 0
     *     repeat
     *         B = next octet
     *         I = I + (B & 127) * 2^M
     *         M = M + 7
     *     while B & 128 == 128
     * return I
     */
    public int integer(int n) {
        assert n >= 1;
        assert n <= 8;

        int i = (TWON_TABLE[n] - 1) & buffer().getByte(offset());
        decodedOctets++;
        if (i < TWON_TABLE[n] - 1) {
            return i;
        } else {
            int m = 0;
            int b;
            do {
                b = buffer().getByte(offset()+decodedOctets);
                decodedOctets++;
                i = i + (b & 127) * TWON_TABLE[m];
                m += 7;
            } while ((b & 128) == 128);
        }
        return i;
    }

    @Override
    public int limit()
    {
        return offset() + decodedOctets;
    }

    @Override
    public HpackIntegerFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        decodedOctets = 0;
        super.wrap(buffer, offset, maxLimit);
        return this;
    }

    public static final class Builder extends Flyweight.Builder<HpackIntegerFW>
    {
        public Builder()
        {
            super(new HpackIntegerFW());
        }

        @Override
        public HpackIntegerFW.Builder wrap(MutableDirectBuffer buffer, int offset, int maxLimit)
        {
            super.wrap(buffer, offset, maxLimit);
            return this;
        }

        /*
         * Encodes integer in HPACK representation
         *
         *  if I < 2^N - 1, encode I on N bits
         *  else
         *      encode (2^N - 1) on N bits
         *      I = I - (2^N - 1)
         *      while I >= 128
         *          encode (I % 128 + 128) on 8 bits
         *          I = I / 128
         *      encode I on 8 bits
         *
         * @param offset offset for current octet
         * @param n number of bits of the prefix
         */
        public HpackIntegerFW.Builder integer(int value, int n) {
            assert n >= 1;
            assert n <= 8;
            int twoNminus1 = TWON_TABLE[n]-1;
            int i = offset();

            byte cur = buffer().getByte(i);
            if (value < twoNminus1) {
                buffer().putByte(i++, (byte) (cur | value));
            } else {
                buffer().putByte(i++, (byte) (cur | twoNminus1));
                value = value - twoNminus1;
                while (value >= 128) {
                    buffer().putByte(i++, (byte) (value % 128 + 128));
                    value = value / 128;
                }
                buffer().putByte(i++, (byte) value);
            }

            limit(i);

            return this;
        }

    }

}

