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
import org.reaktivity.nukleus.http2.internal.types.OctetsFW;
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http2.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderBlockFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2GoawayFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2HeadersFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PingFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PushPromiseFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2RstStreamFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2WindowUpdateFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http2.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http2.internal.types.stream.WindowFW;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Target
{
    private static final DirectBuffer SOURCE_NAME_BUFFER = new UnsafeBuffer("http2".getBytes(UTF_8));

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();

    final WindowFW.Builder windowRW = new WindowFW.Builder();
    final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();
    private final Http2SettingsFW.Builder settingsRW = new Http2SettingsFW.Builder();
    private final Http2RstStreamFW.Builder http2ResetRW = new Http2RstStreamFW.Builder();
    private final Http2GoawayFW.Builder goawayRW = new Http2GoawayFW.Builder();
    private final Http2PingFW.Builder pingRW = new Http2PingFW.Builder();
    private final Http2WindowUpdateFW.Builder http2WindowRW = new Http2WindowUpdateFW.Builder();
    private final Http2DataFW.Builder http2DataRW = new Http2DataFW.Builder();
    private final Http2HeadersFW.Builder http2HeadersRW = new Http2HeadersFW.Builder();
    private final Http2PushPromiseFW.Builder pushPromiseRW = new Http2PushPromiseFW.Builder();

    final MessageConsumer target;
    private final MutableDirectBuffer writeBuffer;


    public Target(MessageConsumer target, MutableDirectBuffer writeBuffer)
    {
        this.target = target;
        this.writeBuffer = writeBuffer;
    }

    public void doBegin(
            long targetId,
            long targetRef,
            long correlationId)
    {
        BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                               .streamId(targetId)
                               .source(SOURCE_NAME_BUFFER, 0, SOURCE_NAME_BUFFER.capacity())
                               .sourceRef(targetRef)
                               .correlationId(correlationId)
                               .build();

        target.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    public void doData(
            long targetId,
            DirectBuffer payload,
            int offset,
            int length)
    {
        DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                            .streamId(targetId)
                            .payload(p -> p.set(payload, offset, length))
                            .build();

        target.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    public void doEnd(
            long targetId)
    {
        EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                         .streamId(targetId)
                         .build();

        target.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    public void doHttpBegin(
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

    // HTTP begin frame's extension data is written using the given buffer
    public void doHttpBegin(
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
                               .extension(e -> e.set(extBuffer, extOffset, extLength))
                               .build();

        target.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    public void doHttpData(
            long targetId,
            DirectBuffer payload,
            int offset,
            int length)
    {
        DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                            .streamId(targetId)
                            .payload(p -> p.set(payload, offset, length))
                            .build();

        target.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    public void doHttpEnd(
            long targetId)
    {
        EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                         .streamId(targetId)
                         .build();

        target.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    public int doHttp2(
            long targetId,
            Flyweight.Builder.Visitor visitor)
    {
        DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                            .streamId(targetId)
                            .payload(p -> p.set(visitor))
                            .build();

        target.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
        return data.length();
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

    public Flyweight.Builder.Visitor visitSettings(
            int maxConcurrentStreams)
    {
        return (buffer, offset, limit) ->
                settingsRW.wrap(buffer, offset, limit)
                          .maxConcurrentStreams(maxConcurrentStreams)
                          .build()
                          .sizeof();
    }

    public Flyweight.Builder.Visitor visitSettingsAck()
    {
        return (buffer, offset, limit) ->
                settingsRW.wrap(buffer, offset, limit)
                          .ack()
                          .build()
                          .sizeof();
    }

    public Flyweight.Builder.Visitor visitRst(
            int streamId,
            Http2ErrorCode errorCode)
    {
        return (buffer, offset, limit) ->
                http2ResetRW.wrap(buffer, offset, limit)
                       .streamId(streamId)
                       .errorCode(errorCode)
                       .build()
                       .sizeof();
    }

    public Flyweight.Builder.Visitor visitGoaway(
            int lastStreamId,
            Http2ErrorCode errorCode)
    {
        return (buffer, offset, limit) ->
                goawayRW.wrap(buffer, offset, limit)
                        .lastStreamId(lastStreamId)
                        .errorCode(errorCode)
                        .build()
                        .sizeof();
    }

    public Flyweight.Builder.Visitor visitPingAck(
            DirectBuffer payloadBuffer, int payloadOffset, int payloadLength)
    {
        return (buffer, offset, limit) ->
                pingRW.wrap(buffer, offset, limit)
                      .ack()
                      .payload(payloadBuffer, payloadOffset, payloadLength)
                      .build()
                      .sizeof();
    }

    public Flyweight.Builder.Visitor visitWindowUpdate(
            int streamId,
            int update)
    {
        return (buffer, offset, limit) ->
                http2WindowRW.wrap(buffer, offset, limit)
                        .streamId(streamId)
                        .size(update)
                        .build()
                        .sizeof();
    }

    public Flyweight.Builder.Visitor visitData(
            int streamId,
            DirectBuffer payloadBuffer,
            int payloadOffset,
            int payloadLength)
    {
        return (buffer, offset, limit) ->
                http2DataRW.wrap(buffer, offset, limit)
                           .streamId(streamId)
                           .payload(payloadBuffer, payloadOffset, payloadLength)
                           .build()
                           .sizeof();
    }

    public Flyweight.Builder.Visitor visitDataEos(int streamId)
    {
        return (buffer, offset, limit) ->
                http2DataRW.wrap(buffer, offset, limit)
                           .streamId(streamId)
                           .endStream()
                           .build()
                           .sizeof();
    }

    public Flyweight.Builder.Visitor visitHeaders(
            int streamId,
            ListFW<HttpHeaderFW> headers,
            BiConsumer<ListFW<HttpHeaderFW>, HpackHeaderBlockFW.Builder> builder)
    {
        return (buffer, offset, limit) ->
                http2HeadersRW.wrap(buffer, offset, limit)
                              .streamId(streamId)
                              .endHeaders()
                              .headers(b -> builder.accept(headers, b))
                              .build()
                              .sizeof();
    }

    public Flyweight.Builder.Visitor visitPushPromise(
            int streamId,
            int promisedStreamId,
            ListFW<HttpHeaderFW> headers,
            BiConsumer<ListFW<HttpHeaderFW>, HpackHeaderBlockFW.Builder> builder)
    {
        return (buffer, offset, limit) ->
                pushPromiseRW.wrap(buffer, offset, limit)
                             .streamId(streamId)
                             .promisedStreamId(promisedStreamId)
                             .endHeaders()
                             .headers(b -> builder.accept(headers, b))
                             .build()
                             .sizeof();
    }

    void doBegin(
            final MessageConsumer target,
            final long targetId,
            final long targetRef,
            final long correlationId)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .streamId(targetId)
                                     .source("http2")
                                     .sourceRef(targetRef)
                                     .correlationId(correlationId)
                                     .extension(e -> e.reset())
                                     .build();

        target.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    void doData(
            final MessageConsumer target,
            final long targetId,
            final OctetsFW payload)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                  .streamId(targetId)
                                  .payload(p -> p.set(payload.buffer(), payload.offset(), payload.sizeof()))
                                  .extension(e -> e.reset())
                                  .build();

        target.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    void doEnd(
            final MessageConsumer target,
            final long targetId)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                               .streamId(targetId)
                               .extension(e -> e.reset())
                               .build();

        target.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    void doWindow(
            final MessageConsumer throttle,
            final long throttleId,
            final int writableBytes,
            final int writableFrames)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                        .streamId(throttleId)
                                        .update(writableBytes)
                                        .frames(writableFrames)
                                        .build();

        throttle.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    void doReset(
            final MessageConsumer throttle,
            final long throttleId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .streamId(throttleId)
                                     .build();

        throttle.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }
}
