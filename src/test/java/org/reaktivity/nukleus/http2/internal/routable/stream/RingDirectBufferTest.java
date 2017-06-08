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

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RingDirectBufferTest
{

    @Test
    public void add()
    {
        int capacity = 100;

        MutableDirectBuffer src = new UnsafeBuffer(new byte[capacity]);
        MutableDirectBuffer dst = new UnsafeBuffer(new byte[capacity]);
        RingDirectBuffer cb = new RingDirectBuffer(capacity);

        for(int i=0; i < capacity; i++)
        {
            boolean written = cb.write(dst, src, 0, 20);
            assertTrue(written);

            written = cb.write(dst, src, 0, 20);
            assertTrue(written);

            written = cb.write(dst, src, 0, 20);
            assertTrue(written);

            written = cb.write(dst, src, 0, 20);
            assertTrue(written);

            written = cb.write(dst, src, 0, 20);
            assertTrue(written);

            written = cb.write(dst, src, 0, 20);
            assertFalse(written);

            cb.read(20);
            cb.read(20);
            cb.read(20);
            cb.read(20);
            cb.read(20);
        }
    }

}
