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
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.http2.internal.types.Flyweight;

/*
 *  Flyweight for HTTP2 client preface
 */
public class Http2PrefaceFW extends Flyweight
{

    private static final byte[] PRI_REQUEST =
            { 'P', 'R', 'I', ' ', '*', ' ', 'H', 'T', 'T', 'P', '/', '2', '.', '0', '\r', '\n', '\r', '\n',
                    'S', 'M', '\r', '\n', '\r', '\n' };

    private final AtomicBuffer payloadRO = new UnsafeBuffer(new byte[0]);

    @Override
    public int limit()
    {
        return offset() + PRI_REQUEST.length;
    }

    @Override
    public Http2PrefaceFW wrap(DirectBuffer buffer, int offset, int maxLimit)
    {
        super.wrap(buffer, offset, maxLimit);

        payloadRO.wrap(buffer, offset(), PRI_REQUEST.length);

        checkLimit(limit(), maxLimit);

        return this;
    }

}

