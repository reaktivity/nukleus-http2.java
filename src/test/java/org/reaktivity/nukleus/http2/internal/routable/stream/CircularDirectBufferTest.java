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
package org.reaktivity.nukleus.http2.internal.routable.stream;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CircularDirectBufferTest {

    @Test
    public void add()
    {
        MutableDirectBuffer buffer = new UnsafeBuffer(new byte[100]);
        CircularDirectBuffer cb = new CircularDirectBuffer(buffer);

        for(int i=0; i < 100; i++) {
            assertTrue(cb.add(new UnsafeBuffer(new byte[20])));
            assertTrue(cb.add(new UnsafeBuffer(new byte[20])));
            assertTrue(cb.add(new UnsafeBuffer(new byte[20])));
            assertTrue(cb.add(new UnsafeBuffer(new byte[20])));
            assertFalse(cb.add(new UnsafeBuffer(new byte[20])));

            cb.remove(20);
            cb.remove(20);
            cb.remove(20);
            cb.remove(20);
        }
    }

    @Test
    public void add2()
    {
        MutableDirectBuffer buffer = new UnsafeBuffer(new byte[100]);
        CircularDirectBuffer cb = new CircularDirectBuffer(buffer);

        assertTrue(cb.add(new UnsafeBuffer(new byte[20])));
        assertTrue(cb.add(new UnsafeBuffer(new byte[20])));
        assertTrue(cb.add(new UnsafeBuffer(new byte[20])));
        assertTrue(cb.add(new UnsafeBuffer(new byte[20])));
        assertFalse(cb.add(new UnsafeBuffer(new byte[20])));

        cb.remove(20);
        assertFalse(cb.add(new UnsafeBuffer(new byte[20])));
        assertTrue(cb.add(new UnsafeBuffer(new byte[19])));
    }

    @Test
    public void remove()
    {
        MutableDirectBuffer buffer = new UnsafeBuffer(new byte[100]);
        CircularDirectBuffer cb = new CircularDirectBuffer(buffer);

        for(int i=0; i < 100; i++)
        {
            assertTrue(cb.add(new UnsafeBuffer(new byte[20])));
            cb.remove(20);
        }
    }

    @Test
    public void testRandom()
    {
        MutableDirectBuffer buffer = new UnsafeBuffer(new byte[100]);
        CircularDirectBuffer cb = new CircularDirectBuffer(buffer);
        Random random = new Random(System.currentTimeMillis());
        List<Integer> list = new LinkedList<>();
        IntStream.range(1, 1000).forEach(x ->
        {
            int adds = random.nextInt(10) + 1;
            for(int i=0; i < adds; i++)
            {
                int no = random.nextInt(30) + 1;
                DirectBuffer buf = buf(no);
                boolean added = cb.add(buf);
                if (added)
                {
                    list.add(no);
                }
            }

            int removes = random.nextInt(10) + 1;
            for(int i=0; i < removes && !list.isEmpty(); i++)
            {
                cb.remove(list.remove(0));
            }
        });
    }

    private DirectBuffer buf(int no)
    {
        byte[] bytes = new byte[no];
        for(int i=0; i < bytes.length; i++)
        {
            bytes[i] = (byte) no;
        }
        return new UnsafeBuffer(bytes);
    }

}
