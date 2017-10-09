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
package org.reaktivity.nukleus.http2.internal.client;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http2.internal.types.Flyweight;
import org.reaktivity.nukleus.http2.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderBlockFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2Flags;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2GoawayFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2HeadersFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PingFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PrefaceFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PushPromiseFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2RstStreamFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2WindowUpdateFW;

import java.util.function.BiConsumer;

class Http2Writer
{
    private final DataFW.Builder dataRW = new DataFW.Builder();

    private final Http2SettingsFW.Builder settingsRW = new Http2SettingsFW.Builder();
    private final Http2RstStreamFW.Builder http2ResetRW = new Http2RstStreamFW.Builder();
    private final Http2GoawayFW.Builder goawayRW = new Http2GoawayFW.Builder();
    private final Http2PingFW.Builder pingRW = new Http2PingFW.Builder();
    private final Http2WindowUpdateFW.Builder http2WindowRW = new Http2WindowUpdateFW.Builder();
    private final Http2DataFW.Builder http2DataRW = new Http2DataFW.Builder();
    private final Http2HeadersFW.Builder http2HeadersRW = new Http2HeadersFW.Builder();
    private final Http2PushPromiseFW.Builder pushPromiseRW = new Http2PushPromiseFW.Builder();
    private final Http2PrefaceFW.Builder prefaceRW = new Http2PrefaceFW.Builder();

    private final MutableDirectBuffer writeBuffer;
    private int accumulatedLength;
    private final MessageConsumer target;
    private final long targetId;

    Http2Writer(MutableDirectBuffer writeBuffer, MessageConsumer target, long targetId)
    {
        this.writeBuffer = writeBuffer;
        this.target = target;
        this.targetId = targetId;
    }

    int http2Frame(Flyweight.Builder.Visitor visitor)
    {
        int length = visitor.visit(writeBuffer, DataFW.FIELD_OFFSET_PAYLOAD + accumulatedLength, writeBuffer.capacity());
        accumulatedLength += length;

        return length;
    }

    void flush()
    {
        int offset = DataFW.FIELD_OFFSET_PAYLOAD;
        while (accumulatedLength > 0)
        {
            int chunk = Math.min(accumulatedLength, 65535);     // limit by nukleus DATA frame length (2 bytes)

            DataFW data = dataRW.wrap(writeBuffer, offset - DataFW.FIELD_OFFSET_PAYLOAD, offset + chunk)
                                .streamId(targetId)
                                .payload(p -> p.set((b, o, l) -> chunk))
                                .build();

            target.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
            offset += chunk;
            accumulatedLength -= chunk;
        }

        assert accumulatedLength == 0;
    }


    // visitors
    Flyweight.Builder.Visitor visitSettings(
            int maxConcurrentStreams,
            int initialWindowSize)
    {
        return (buffer, offset, limit) ->
                settingsRW.wrap(buffer, offset, limit)
                          .maxConcurrentStreams(maxConcurrentStreams)
                          .initialWindowSize(initialWindowSize)
                          .build()
                          .sizeof();
    }

    Flyweight.Builder.Visitor visitSettingsAck()
    {
        return (buffer, offset, limit) ->
                settingsRW.wrap(buffer, offset, limit)
                          .ack()
                          .build()
                          .sizeof();
    }

    Flyweight.Builder.Visitor visitRst(
            int http2StreamId,
            Http2ErrorCode errorCode)
    {
        return (buffer, offset, limit) ->
                http2ResetRW.wrap(buffer, offset, limit)
                       .streamId(http2StreamId)
                       .errorCode(errorCode)
                       .build()
                       .sizeof();
    }

    Flyweight.Builder.Visitor visitGoaway(
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

    Flyweight.Builder.Visitor visitPingAck(
            DirectBuffer payloadBuffer, int payloadOffset, int payloadLength)
    {
        return (buffer, offset, limit) ->
                pingRW.wrap(buffer, offset, limit)
                      .ack()
                      .payload(payloadBuffer, payloadOffset, payloadLength)
                      .build()
                      .sizeof();
    }

    Flyweight.Builder.Visitor visitWindowUpdate(
            int http2StreamId,
            int update)
    {
        return (buffer, offset, limit) ->
                http2WindowRW.wrap(buffer, offset, limit)
                        .streamId(http2StreamId)
                        .size(update)
                        .build()
                        .sizeof();
    }

    Flyweight.Builder.Visitor visitData(
            int http2StreamId,
            DirectBuffer payloadBuffer,
            int payloadOffset,
            int payloadLength)
    {
        return (buffer, offset, limit) ->
                http2DataRW.wrap(buffer, offset, limit)
                           .streamId(http2StreamId)
                           .payload(payloadBuffer, payloadOffset, payloadLength)
                           .build()
                           .sizeof();
    }

    Flyweight.Builder.Visitor visitDataEos(int http2StreamId)
    {
        assert http2StreamId != 0;

        return (buffer, offset, limit) ->
                http2DataRW.wrap(buffer, offset, limit)
                           .streamId(http2StreamId)
                           .endStream()
                           .build()
                           .sizeof();
    }

    Flyweight.Builder.Visitor visitHeaders(
            int http2StreamId,
            byte flags,
            ListFW<HttpHeaderFW> headers,
            BiConsumer<ListFW<HttpHeaderFW>, HpackHeaderBlockFW.Builder> builder)
    {
        byte headersFlags = (byte) (flags | Http2Flags.END_HEADERS);

        return (buffer, offset, limit) ->
                http2HeadersRW.wrap(buffer, offset, limit)
                              .streamId(http2StreamId)
                              .flags(headersFlags)
                              .headers(b -> builder.accept(headers, b))
                              .build()
                              .sizeof();
    }

    Flyweight.Builder.Visitor visitHeaders(
            int http2StreamId,
            byte flags,
            DirectBuffer srcBuffer,
            int srcOffset,
            int srcLength)
    {
        assert http2StreamId != 0;

        byte headersFlags = (byte) (flags | Http2Flags.END_HEADERS);
        return (buffer, offset, limit) ->
                http2HeadersRW.wrap(buffer, offset, limit)
                              .streamId(http2StreamId)
                              .flags(headersFlags)
                              .payload(srcBuffer, srcOffset, srcLength)
                              .build()
                              .sizeof();
    }

    Flyweight.Builder.Visitor visitPushPromise(
            int http2StreamId,
            int promisedStreamId,
            ListFW<HttpHeaderFW> headers,
            BiConsumer<ListFW<HttpHeaderFW>, HpackHeaderBlockFW.Builder> builder)
    {
        assert http2StreamId != 0;

        return (buffer, offset, limit) ->
                pushPromiseRW.wrap(buffer, offset, limit)
                             .streamId(http2StreamId)
                             .promisedStreamId(promisedStreamId)
                             .endHeaders()
                             .headers(b -> builder.accept(headers, b))
                             .build()
                             .sizeof();
    }

    Flyweight.Builder.Visitor visitPushPromise(
            int http2StreamId,
            int promisedStreamId,
            DirectBuffer headersBuffer,
            int headersOffset,
            int headersLength)
    {
        assert http2StreamId != 0;

        return (buffer, offset, limit) ->
                pushPromiseRW.wrap(buffer, offset, limit)
                             .streamId(http2StreamId)
                             .promisedStreamId(promisedStreamId)
                             .endHeaders()
                             .headers(headersBuffer, headersOffset, headersLength)
                             .build()
                             .sizeof();
    }

    Flyweight.Builder.Visitor visitPreface()
    {
        return (buffer, offset, limit) ->
                prefaceRW.wrap(buffer, offset, limit)
                          .build()
                          .sizeof();
    }
}
