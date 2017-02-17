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
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HpackIntegerFWTest {

    @Test
    public void encode() {
        // Encoding 10 Using a 5-Bit Prefix
        int value = 10;
        int n = 5;
        byte[] bytes = new byte[100];
        bytes[1] = (byte) 0xe0;
        MutableDirectBuffer buffer = new UnsafeBuffer(bytes);
        HpackIntegerFW.Builder builder = new HpackIntegerFW.Builder();
        HpackIntegerFW fw = builder
                .wrap(buffer, 1, buffer.capacity())
                .integer(value, n)
                .build();
        assertEquals((byte) 0xea, bytes[1]);

        assertEquals(value, fw.integer(n));
        assertEquals(2, fw.limit());
        assertEquals(1, fw.length());
    }

    @Test
    public void decode() {
        // Decoding 10 Using a 5-Bit Prefix
        int value = 10;
        int n = 5;
        byte[] bytes = new byte[100];
        bytes[1] = (byte) 0xea;
        DirectBuffer buffer = new UnsafeBuffer(bytes);
        HpackIntegerFW fw = new HpackIntegerFW();
        int got = fw
                .wrap(buffer, 1, buffer.capacity())
                .integer(n);

        assertEquals(value, got);
        assertEquals(2, fw.limit());
        assertEquals(1, fw.length());
    }

    @Test
    public void encode1() {
        // Encoding 1337 Using a 5-Bit Prefix
        int value = 1337;
        int n = 5;
        byte[]bytes = new byte[100];
        bytes[1] = (byte) 0x00;
        MutableDirectBuffer buffer = new UnsafeBuffer(bytes);
        HpackIntegerFW.Builder builder = new HpackIntegerFW.Builder();
        HpackIntegerFW fw = builder
                .wrap(buffer, 1, buffer.capacity())
                .integer(value, n)
                .build();
        assertEquals((byte) 0x1f, bytes[1]);
        assertEquals((byte) 0x9a, bytes[2]);
        assertEquals((byte) 0x0a, bytes[3]);

        assertEquals(value, fw.integer(n));
        assertEquals(4, fw.limit());
        assertEquals(3, fw.length());
    }

    @Test
    public void decode1() {
        // Decoding 1337 Using a 5-Bit Prefix
        int value = 1337;
        int n = 5;
        byte[]bytes = new byte[100];
        bytes[1] = (byte) 0x1f;
        bytes[2] = (byte) 0x9a;
        bytes[3] = (byte) 0x0a;
        DirectBuffer buffer = new UnsafeBuffer(bytes);
        HpackIntegerFW fw = new HpackIntegerFW();
        int got = fw
                .wrap(buffer, 1, buffer.capacity())
                .integer(n);

        assertEquals(value, got);
        assertEquals(4, fw.limit());
        assertEquals(3, fw.length());
    }

    @Test
    public void encode2() {
        // Encoding 1337 Using a 5-Bit Prefix
        int value = 1337;
        int n = 5;
        byte[] bytes = new byte[100];
        bytes[1] = (byte) 0xe0;
        MutableDirectBuffer buffer = new UnsafeBuffer(bytes);
        HpackIntegerFW.Builder builder = new HpackIntegerFW.Builder();
        HpackIntegerFW fw = builder
                .wrap(buffer, 1, buffer.capacity())
                .integer(value, n)
                .build();
        assertEquals((byte) 0xff, bytes[1]);
        assertEquals((byte) 0x9a, bytes[2]);
        assertEquals((byte) 0x0a, bytes[3]);

        assertEquals(value, fw.integer(n));
        assertEquals(4, fw.limit());
        assertEquals(3, fw.length());
    }

    @Test
    public void decode2() {
        // Decoding 1337 Using a 5-Bit Prefix
        int value = 1337;
        int n = 5;
        byte[] bytes = new byte[100];
        bytes[1] = (byte) 0xff;
        bytes[2] = (byte) 0x9a;
        bytes[3] = (byte) 0x0a;
        DirectBuffer buffer = new UnsafeBuffer(bytes);
        HpackIntegerFW fw = new HpackIntegerFW();
        int got = fw
                .wrap(buffer, 1, buffer.capacity())
                .integer(n);

        assertEquals(value, got);
        assertEquals(4, fw.limit());
        assertEquals(3, fw.length());
    }

    @Test
    public void encode3() {
        // Encoding 42 Starting at an Octet Boundary (n = 8)
        int value = 42;
        int n = 8;
        byte[] bytes = new byte[100];
        MutableDirectBuffer buffer = new UnsafeBuffer(bytes);
        HpackIntegerFW.Builder builder = new HpackIntegerFW.Builder();
        HpackIntegerFW fw = builder
                .wrap(buffer, 2, buffer.capacity())
                .integer(value, n)
                .build();
        assertEquals((byte) 0x2a, bytes[2]);

        assertEquals(value, fw.integer(n));
        assertEquals(3, fw.limit());
        assertEquals(1, fw.length());
    }

    @Test
    public void decode3() {
        // Decoding 42 Starting at an Octet Boundary (n = 8)
        int value = 42;
        int n = 8;
        byte[] bytes = new byte[100];
        bytes[2] = 0x2a;
        DirectBuffer buffer = new UnsafeBuffer(bytes);
        HpackIntegerFW fw = new HpackIntegerFW();
        int got = fw
                .wrap(buffer, 2, buffer.capacity())
                .integer(n);

        assertEquals(value, got);
        assertEquals(3, fw.limit());
        assertEquals(1, fw.length());
    }

}