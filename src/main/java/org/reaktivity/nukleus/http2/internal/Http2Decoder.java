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
import org.reaktivity.nukleus.http2.internal.types.stream.Http2Flags;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PrefaceFW;
import org.reaktivity.nukleus.http2.internal.types.stream.RegionFW;

import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.reaktivity.nukleus.http2.internal.Http2Decoder.State.HEADER;
import static org.reaktivity.nukleus.http2.internal.Http2Decoder.State.PAYLOAD;
import static org.reaktivity.nukleus.http2.internal.Http2Decoder.State.PREFACE;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2PrefaceFW.PRI_REQUEST;

public class Http2Decoder
{
    private final MemoryManager memoryManager;
    private final Consumer<Http2FrameFW> frameConsumer;
    private final Supplier<DirectBufferBuilder> supplyBufferBuilder;
    private final int maxFrameSize;

    private final Http2PrefaceFW prefaceRO;
    private final Http2FrameHeaderFW frameHeaderRO;
    private final Http2FrameFW frameRO;
    private final RegionConsumer prefaceRegionConsumer;
    private final RegionConsumer framingRegionConsumer;
    private final RegionConsumer payloadRegionConsumer;

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
        Consumer<Http2FrameFW> frameConsumer,
        RegionConsumer prefaceRegionConsumer,
        RegionConsumer framingRegionConsumer,
        RegionConsumer payloadRegionConsumer)
    {
        this.memoryManager = memoryManager;
        this.supplyBufferBuilder = supplyBufferBuilder;
        this.maxFrameSize = maxFrameSize;
        this.frameConsumer = frameConsumer;
        this.prefaceRegionConsumer = prefaceRegionConsumer;
        this.framingRegionConsumer = framingRegionConsumer;
        this.payloadRegionConsumer = payloadRegionConsumer;
        compositeBufferBuilder = supplyBufferBuilder.get();
        prefaceRO = new Http2PrefaceFW();
        frameHeaderRO = new Http2FrameHeaderFW();
        frameRO = new Http2FrameFW();
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
        payloadRegionConsumer.accept(address, remaining, streamId);

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
        framingRegionConsumer.accept(address, remaining, streamId);
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
        prefaceRegionConsumer.accept(address, remaining, streamId);
        compositeBufferBuilder.wrap(memoryManager.resolve(address), remaining);
        compositeBufferLength += remaining;
        if (compositeBufferLength == PRI_REQUEST.length)
        {
            DirectBuffer compositeBuffer = compositeBufferBuilder.build();
            assert compositeBuffer.capacity() == compositeBufferLength;
            prefaceRO.wrap(compositeBuffer, 0, compositeBufferLength);
            // TODO prefaceRO.error()
            compositeBufferLength = 0;
            compositeBufferBuilder = supplyBufferBuilder.get();
            state = HEADER;
        }

        return remaining;
    }

}
