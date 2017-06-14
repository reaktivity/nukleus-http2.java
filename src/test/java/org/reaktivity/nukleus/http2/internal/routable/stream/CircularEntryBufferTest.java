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

import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class CircularEntryBufferTest
{

    @Test
    public void add()
    {
        CircularEntryBuffer cb = new CircularEntryBuffer(100);

        for(int i=0; i < 100; i++)
        {
            int offset = cb.writeOffset(20);
            assertNotEquals(-1, offset);
            cb.write(offset, 20);

            offset = cb.writeOffset(20);
            assertNotEquals(-1, offset);
            cb.write(offset, 20);

            offset = cb.writeOffset(20);
            assertNotEquals(-1, offset);
            cb.write(offset, 20);

            offset = cb.writeOffset(20);
            assertNotEquals(-1, offset);
            cb.write(offset, 20);

            offset = cb.writeOffset(20);
            assertEquals(-1, offset);

            cb.read(20);
            cb.read(20);
            cb.read(20);
            cb.read(20);
        }
    }

    @Test
    public void add2()
    {
        CircularEntryBuffer cb = new CircularEntryBuffer(100);

        int offset = cb.writeOffset(20);
        assertNotEquals(-1, offset);
        cb.write(offset, 20);

        offset = cb.writeOffset(20);
        assertNotEquals(-1, offset);
        cb.write(offset, 20);

        offset = cb.writeOffset(20);
        assertNotEquals(-1, offset);
        cb.write(offset, 20);

        offset = cb.writeOffset(20);
        assertNotEquals(-1, offset);
        cb.write(offset, 20);

        offset = cb.writeOffset(20);
        assertEquals(-1, offset);

        cb.read(20);

        offset = cb.writeOffset(20);
        assertEquals(-1, offset);

        offset = cb.writeOffset(19);
        assertNotEquals(-1, offset);
        cb.write(offset, 19);
    }

    @Test
    public void remove()
    {
        CircularEntryBuffer cb = new CircularEntryBuffer(100);

        for(int i=0; i < 100; i++)
        {
            int offset = cb.writeOffset(20);
            assertNotEquals(-1, offset);
            cb.write(offset, 20);

            cb.read(20);
        }
    }

    @Test
    public void testRandom()
    {
        CircularEntryBuffer cb = new CircularEntryBuffer(100);
        Random random = new Random(System.currentTimeMillis());
        List<Integer> list = new LinkedList<>();
        IntStream.range(1, 1000).forEach(x ->
        {
            int adds = random.nextInt(10) + 1;
            for(int i=0; i < adds; i++)
            {
                int no = random.nextInt(30) + 1;
                int offset = cb.writeOffset(no);
                if (offset != -1)
                {
                    cb.write(offset, no);
                    list.add(no);
                }
            }

            int removes = random.nextInt(10) + 1;
            for(int i=0; i < removes && !list.isEmpty(); i++)
            {
                cb.read(list.remove(0));
            }
        });
    }

}
