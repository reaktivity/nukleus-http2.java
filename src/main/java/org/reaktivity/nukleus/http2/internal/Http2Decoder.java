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
import org.reaktivity.nukleus.buffer.DirectBufferBuilder;
import org.reaktivity.nukleus.buffer.MemoryManager;
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.stream.AckFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PrefaceFW;
import org.reaktivity.nukleus.http2.internal.types.stream.RegionFW;
import org.reaktivity.nukleus.http2.internal.types.stream.TransferFW;

import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.reaktivity.nukleus.http2.internal.Http2Decoder.State.HEADER;
import static org.reaktivity.nukleus.http2.internal.Http2Decoder.State.PAYLOAD;
import static org.reaktivity.nukleus.http2.internal.Http2Decoder.State.PREFACE;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.DATA;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2PrefaceFW.PRI_REQUEST;

public class Http2Decoder
{
    private final MemoryManager memoryManager;
    private final Consumer<Http2PrefaceFW> prefaceConsumer;
    private final Consumer<Http2FrameFW> frameConsumer;
    private final Supplier<DirectBufferBuilder> supplyBufferBuilder;
    private final int maxFrameSize;

    private final Http2PrefaceFW prefaceRO;
    private final Http2FrameHeaderFW frameHeaderRO;
    private final Http2FrameFW frameRO;
    private final ListFW.Builder<RegionFW.Builder, RegionFW> regionsRW;

    // TODO use regionsRW to build composite buffer on demand
    private DirectBufferBuilder compositeBufferBuilder;
    private int compositeBufferLength;
    private int frameSize;
    private State state;

    enum State
    {
        PREFACE,
        HEADER,
        PAYLOAD
    }

    Http2Decoder(
        MemoryManager memoryManager,
        Supplier<DirectBufferBuilder> supplyBufferBuilder,
        int maxFrameSize,
        ListFW.Builder<RegionFW.Builder, RegionFW> regionsRW,
        Http2PrefaceFW prefaceRO,
        Http2FrameHeaderFW frameHeaderRO,
        Http2FrameFW frameRO,
        Consumer<Http2PrefaceFW> prefaceConsumer,
        Consumer<Http2FrameFW> frameConsumer)
    {
        this.memoryManager = memoryManager;
        this.supplyBufferBuilder = supplyBufferBuilder;
        this.maxFrameSize = maxFrameSize;
        this.regionsRW = regionsRW;
        this.prefaceConsumer = prefaceConsumer;
        this.frameConsumer = frameConsumer;

        compositeBufferBuilder = supplyBufferBuilder.get();
        this.prefaceRO = prefaceRO;
        this.frameHeaderRO = frameHeaderRO;
        this.frameRO = frameRO;
        state = PREFACE;
    }

    void decode(ListFW<RegionFW> regionsRO)
    {
        regionsRO.forEach(r -> processRegion(r.address(), r.length(), r.streamId()));
    }

    private void processRegion(
        final long address,
        final int length,
        final long streamId)
    {
        long offset = address;
        int remaining = length;

        int consumed;
        while(remaining > 0)
        {
            switch (state)
            {
                case PREFACE:
                    consumed = processPreface(offset, remaining, streamId);
                    break;
                case HEADER:
                    consumed = processFrameHeader(offset, remaining, streamId);
                    break;
                case PAYLOAD:
                    consumed = processFrame(offset, remaining, streamId);
                    break;
                default:
                    throw new IllegalStateException();
            }
            offset += consumed;
            remaining -= consumed;
        }
    }

    // @return no of bytes consumed
    private int processFrame(
        long address,
        int length,
        long streamId)
    {
        int remaining = Math.min(frameSize - compositeBufferLength, length);
        regionsRW.item(r -> r.address(address).length(remaining).streamId(streamId));
        compositeBufferBuilder.wrap(memoryManager.resolve(address), remaining);
        compositeBufferLength += remaining;
        if (compositeBufferLength == frameSize)
        {
            DirectBuffer compositeBuffer = compositeBufferBuilder.build();
            processFrame(compositeBuffer);
        }

        return remaining;
    }

    private void processFrame(
        DirectBuffer compositeBuffer)
    {
        assert compositeBuffer.capacity() == compositeBufferLength;
        Http2FrameFW frame = frameRO.wrap(compositeBuffer, 0, compositeBufferLength);
        frameConsumer.accept(frame);
        compositeBufferLength = 0;
        frameSize = 0;
        compositeBufferBuilder = supplyBufferBuilder.get();
        state = HEADER;
    }

    // @return no of bytes consumed
    private int processFrameHeader(
        long address,
        int length,
        long streamId)
    {
        int remaining = Math.min(9 - compositeBufferLength, length);
        regionsRW.item(r -> r.address(address).length(remaining).streamId(streamId));
        compositeBufferBuilder.wrap(memoryManager.resolve(address), remaining);
        compositeBufferLength += remaining;
        if (compositeBufferLength == 9)
        {
            // complete frame header is in compositeBuffer
            DirectBuffer compositeBuffer = compositeBufferBuilder.build();
            assert compositeBuffer.capacity() == compositeBufferLength;
            Http2FrameHeaderFW frameHeader = frameHeaderRO.wrap(compositeBuffer, 0, compositeBufferLength);
            if (frameHeader.payloadLength() == 0)
            {
                // complete frame is in compositeBuffer
                processFrame(compositeBuffer);
            }
            else
            {
                frameSize = frameHeader.payloadLength() + 9;
                compositeBufferBuilder = supplyBufferBuilder.get();
                compositeBufferBuilder.wrap(compositeBuffer, 0, compositeBufferLength);
                state = PAYLOAD;
            }
        }

        return remaining;
    }

    // @return no of bytes consumed
    private int processPreface(
        long address,
        int length,
        long streamId)
    {
        int remaining = Math.min(PRI_REQUEST.length - compositeBufferLength, length);
        regionsRW.item(r -> r.address(address).length(remaining).streamId(streamId));
        compositeBufferBuilder.wrap(memoryManager.resolve(address), remaining);
        compositeBufferLength += remaining;
        if (compositeBufferLength == PRI_REQUEST.length)
        {
            DirectBuffer compositeBuffer = compositeBufferBuilder.build();
            assert compositeBuffer.capacity() == compositeBufferLength;
            prefaceRO.wrap(compositeBuffer, 0, compositeBufferLength);
            prefaceConsumer.accept(prefaceRO);
            // TODO prefaceRO.error()
            compositeBufferLength = 0;
            compositeBufferBuilder = supplyBufferBuilder.get();
            state = HEADER;
        }

        return remaining;
    }

    static void ackAll(
        ListFW<RegionFW> regions,
        AckFW.Builder ackRW)
    {
        regions.forEach(r ->
            ackRW.regionsItem(ar -> ar.address(r.address()).length(r.length()).streamId(r.streamId()))
        );
    }

    static void ackForData(
        ListFW<RegionFW> regions,
        Http2FrameFW http2RO,
        Http2DataFW http2DataRO,
        AckFW.Builder ackRW)
    {
        assert http2RO.type() == DATA;
        Http2DataFW dataRO = http2DataRO.wrap(http2RO.buffer(), http2RO.offset(), http2RO.limit());

        int dataStart = 9 + (dataRO.padding() ? 1 : 0);
        int dataEnd = dataStart + dataRO.dataLength();

        int[] total = new int[1];
        regions.forEach(r ->
        {
            // add intersection of this region and  (0 .. dataStart)
            if (dataStart - total[0] > 0)
            {
                int length = Math.min(dataStart - total[0], r.length());
                ackRW.regionsItem(ar -> ar.address(r.address()).length(length).streamId(r.streamId()));
            }

            // add intersection of this region and (dataEnd .. frame end)
            if (total[0] + r.length() > dataEnd)
            {
                int offset = total[0] < dataEnd ? dataEnd - total[0] : 0;
                int length = r.length() - offset;
                ackRW.regionsItem(ar -> ar.address(r.address() + offset).length(length).streamId(r.streamId()));
            }
            total[0] += r.length();
        });
    }

    static void transferForData(
        ListFW<RegionFW> regions,
        Http2FrameFW http2RO,
        Http2DataFW http2DataRO,
        TransferFW.Builder transferRW)
    {
        assert http2RO.type() == DATA;
        Http2DataFW dataRO = http2DataRO.wrap(http2RO.buffer(), http2RO.offset(), http2RO.limit());
        if (dataRO.padding())
        {
            throw new UnsupportedOperationException("TODO padding is not yet implemented");
        }

        int dataStart = 9 + (dataRO.padding() ? 1 : 0);
        int dataEnd = dataStart + dataRO.dataLength();

        // Transfer for application payload
        int[] total = new int[1];
        regions.forEach(r ->
        {
            // TODO for padding need to break up regions
            if (total[0] >= dataStart)
            {
                transferRW.regionsItem(ar -> ar.address(r.address()).length(r.length()).streamId(r.streamId()));
            }
            total[0] += r.length();
        });
    }

}
