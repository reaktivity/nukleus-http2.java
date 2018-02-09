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
package org.reaktivity.nukleus.http2.internal;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http2.internal.types.Flyweight;
import org.reaktivity.nukleus.http2.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http2.internal.types.stream.TransferFW;

import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

class HttpWriter
{
    private static final DirectBuffer SOURCE_NAME_BUFFER = new UnsafeBuffer("http2".getBytes(UTF_8));
    private static final int FIN = 0x01;
    private static final int RST = 0x02;

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final TransferFW.Builder writeRW = new TransferFW.Builder();
    private final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();

    private final MutableDirectBuffer writeBuffer;

    HttpWriter(MutableDirectBuffer writeBuffer)
    {
        this.writeBuffer = writeBuffer;
    }

    // HTTP begin frame's extension data is written using the given buffer
    void doHttpBegin(
            MessageConsumer target,
            long targetId,
            long targetRef,
            long correlationId,
            DirectBuffer extBuffer,
            int extOffset,
            int extLength)
    {
        BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                               .streamId(targetId)
                               .source(SOURCE_NAME_BUFFER, 0, SOURCE_NAME_BUFFER.capacity())
                               .sourceRef(targetRef)
                               .correlationId(correlationId)
                               .extension(extBuffer, extOffset, extLength)
                               .build();

        target.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    void doHttpBegin(
            MessageConsumer target,
            long targetId,
            long targetRef,
            long correlationId,
            Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
    {
        BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                               .streamId(targetId)
                               .source(SOURCE_NAME_BUFFER, 0, SOURCE_NAME_BUFFER.capacity())
                               .sourceRef(targetRef)
                               .correlationId(correlationId)
                               .extension(e -> e.set(visitHttpBeginEx(mutator)))
                               .build();

        target.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private Flyweight.Builder.Visitor visitHttpBeginEx(
            Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers)
    {
        return (buffer, offset, limit) ->
                httpBeginExRW.wrap(buffer, offset, limit)
                             .headers(headers)
                             .build()
                             .sizeof();
    }
}
