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
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.DirectBufferBuilder;
import org.reaktivity.nukleus.buffer.MemoryManager;
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.stream.AckFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2PrefaceFW;
import org.reaktivity.nukleus.http2.internal.types.stream.RegionFW;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.reaktivity.nukleus.http2.internal.Http2Decoder.State.HEADER;
import static org.reaktivity.nukleus.http2.internal.Http2Decoder.State.PAYLOAD;
import static org.reaktivity.nukleus.http2.internal.Http2Decoder.State.PREFACE;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2PrefaceFW.PRI_REQUEST;

public class Http2Decoder {
    private final MemoryManager memoryManager;
    private final Consumer<Http2FrameFW> frameConsumer;
    private final Supplier<DirectBufferBuilder> supplyBufferBuilder;
    private final int maxFrameSize;

    ListFW.Builder<RegionFW.Builder, RegionFW> list;
    AckFW.Builder ackRW;

    Http2PrefaceFW prefaceRO = new Http2PrefaceFW();
    Http2FrameHeaderFW frameHeaderRO = new Http2FrameHeaderFW();
    Http2FrameFW frameRO = new Http2FrameFW();

    DirectBufferBuilder compositeBufferBuilder;
    private int compositeBufferLength;
    private int frameSize;
    private State state;

    enum State {
        PREFACE,
        HEADER,
        PAYLOAD
    }

    Http2Decoder(
            MemoryManager memoryManager,
            Supplier<DirectBufferBuilder> supplyBufferBuilder,
            int maxFrameSize,
            Consumer<Http2FrameFW> frameConsumer,
            BiConsumer<Long, Integer> framingRegionConsumer,
            BiConsumer<Long, Integer> payloadRegionConsumer) {
        this.memoryManager = memoryManager;
        this.supplyBufferBuilder = supplyBufferBuilder;
        this.maxFrameSize = maxFrameSize;
        this.frameConsumer = frameConsumer;
        compositeBufferBuilder = supplyBufferBuilder.get();
        state = PREFACE;
    }

    void decode(ListFW<RegionFW> regionsRO) {
        regionsRO.forEach(r -> processRegion(r.address(), r.length()));


//        lastAddress = address1;
//        lastLength = length1;
//
//        regionsRO.forEach(r -> {
//            if (coalesced(r, lastAddress, lastLength)) {
//                lastLength += r.length();
//            } else {
//                processRegion(r, lastAddress, lastLength);
//                lastAddress = r.address();
//                lastLength = r.length();
//            }
//        });
//        processRegion(lastAddress, lastLength);
//
//        newAddress1 = 0;
//        newLength1 = 0;
//        newTotalSize = length1 + length2;
//        regionsRO.forEach(this::processWrapped);
//        address1 = newAddress1;
//        length1 = newLength1;

//
//        while (!connectionRegions.isEmpty())
//        {
//            int totalSize = connectionRegionsSize();
//            if (totalSize < 3)
//            {
//                break;
//            }
//            int frameSize = frameSize();
//            if (totalSize < frameSize)
//            {
//                break;
//            }
//            if (frameSize <= region.length) {
//                // Whole frame is available
//                stagingBuf.wrap(region.address, region.length);
//                processFrame(frameRegions, stagingBuf, 0, frameSize, frameConsumer);
//                int remaining = region.length - frameSize;
//                if (remaining > 0) {
//                    connectionRegions.updateFirst(remaining);
//                }
//                else
//                {
//                    connectionRegions.remove(0);
//                }
//            } else if ( >= frameSize) {
//                // partial frame in the first region
//                fillBuf(frameSize);
//                processFrame(frameRegions, stagingBuf, 0, frameSize, frameConsumer);
//            }
//        }
    }

//    void fillBuf(int frameSize)
//    {
//        int offset = 0;
//        int remaining = 0;
//
//        long address = memoryManager.acquire(frameSize);
//        stagingBuf.wrap(address, frameSize);
//        for(Region region : connectionRegions) {
//            regionBuf.wrap(region.address, region.length);
//            int copy = Math.min(region.length, remaining);
//            stagingBuf.putBytes(offset, regionBuf, 0, copy);
//            offset += copy;
//            remaining -= copy;
//            if (remaining == 0)
//            {
//                break;
//            }
//        }
//        //memoryManager.release(address, frameSize);      // TODO do not release
//    }
//
//    // copies size bytes from regions to dst buffer
//    private void fillBuf(List<Region> regions, MutableDirectBuffer dst, int size)
//    {
//        int offset = 0;
//        int remaining = size;
//
//        Iterator<Region> it = regions.iterator();
//        while(remaining > 0 && it.hasNext()) {
//            Region region = it.next();
//            regionBuf.wrap(region.address, region.length);
//            int copyBytes = Math.min(region.length, remaining);
//            dst.putBytes(offset, regionBuf, 0, copyBytes);
//            offset += copyBytes;
//            remaining -= copyBytes;
//        }
//    }

//    List<Region> frameRegions(final int length)
//    {
//        int remaining = length;
//        int i = 0;
//        int part1 = 0, part2 = 0;
//        for (; i < connectionRegions.size(); i++) {
//            Region region = connectionRegions.get(i);
//            part1 = Math.min(remaining, region.length);
//            remaining -= part1;
//            if (remaining == 0)
//            {
//                part2 = region.length - part1;
//                break;
//            }
//        }
//        // split i th region into two
//        Region newRegion = new Region(connectionRegions.get(i).address + part1, part2);
//        connectionRegions.get(i).remaining(part1);
//
//        // 0..i current frame regions, i+1..end remaining unparsed connection regions
//        connectionRegions.add(i + 1, newRegion);
//        List<Region> frameRegions = new ArrayList<>(connectionRegions.subList(0, i + 1));
//        connectionRegions.subList(0, i + 1).clear();
//        return frameRegions;
//    }

//    private int connectionRegionsSize() {
//        int size = 0;
//        for (Region region : connectionRegions) {
//            size += region.length;
//        }
//        return size;
//    }
//
//    private void processFrame(DirectBuffer buf, int offset, int length, Consumer<Http2FrameFW> frameConsumer) {
//        factory.http2RO.wrap(buf, offset, length);
//        frameConsumer.accept(factory.http2RO);
//    }
//
//    private int frameSize()
//    {
//        Region region = connectionRegions.get(0);
//        DirectBuffer buf;
//        if (region.length >= 3)
//        {
//            regionBuf.wrap(region.address, region.length);
//            buf = regionBuf;
//        }
//        else
//        {
//            fillBuf(connectionRegions, lengthBuf, 3);
//            buf = lengthBuf;
//        }
//        return frameSize(buf);
//    }

//    private static int frameSize(RegionFW region, final DirectBuffer buffer)
//    {
//        return frameSize(region.address(), region.length(), buffer);
//    }
//
//    private static int frameSize(Region region, final DirectBuffer buffer)
//    {
//        return frameSize(region.address, region.length, buffer);
//    }
//
//    private static int frameSize(final long address, final int length, final DirectBuffer buffer)
//    {
//        assert length >= 3;
//
//        buffer.wrap(address, length);
//        int frameLength = (buffer.getByte(0) & 0xFF) << 16;
//        frameLength += (buffer.getShort(1, BIG_ENDIAN) & 0xFF_FF);
//        return frameLength + 9;                      // +3 for length, +1 type, +1 flags, +4 stream-id
//    }

//    private static int frameSize(final DirectBuffer buffer) {
//        int frameLength = (buffer.getByte(0) & 0xFF) << 16;
//        frameLength += (buffer.getShort(1, BIG_ENDIAN) & 0xFF_FF);
//        return frameLength + 9;                      // +3 for length, +1 type, +1 flags, +4 stream-id
//    }

//    private void processWrapped(RegionFW region) {
//        if (newTotalSize + region.length() > totalSize) {
//            if (length2 != 0) {
//                if (address2 + region.length() == region.address()) {
//                    length2 += region.length();
//                } else {
//                    // TODO error - not contiguous
//                }
//            } else if (length1 != 0) {
//                if (address1 + region.length() == region.address()) {
//                    length1 += region.length();
//                } else {
//                    address2 = region.address();
//                    length2 = region.length();
//                }
//            } else {
//                int offset = totalSize - newTotalSize;
//                address1 = region.address() + offset;
//                length1 = region.length() - offset;
//            }
//        } else {
//            newTotalSize += region.length();
//        }
//    }

    private void processRegion(
        final long address,
        final int length)
    {
        long offset = address;
        int remaining = length;

        int consumed;
        while(remaining > 0) {
            switch (state)
            {
                case PREFACE:
                    consumed = processPreface(offset, remaining);
                    break;
                case HEADER:
                    consumed = processFrameHeader(offset, remaining);
                    break;
                case PAYLOAD:
                    consumed = processFrame(offset, remaining);
                    break;
                default:
                    throw new IllegalStateException();
            }
            offset += consumed;
            remaining -= consumed;
        }
    }

    // @return no of bytes consumed
    private int processFrame(long address, int length) {
        int remaining = Math.min(frameSize - compositeBufferLength, length);

        compositeBufferBuilder.wrap(memoryManager.resolve(address), remaining);
        compositeBufferLength += remaining;
        if (compositeBufferLength == frameSize) {
            DirectBuffer compositeBuffer = compositeBufferBuilder.build();
            assert compositeBuffer.capacity() == compositeBufferLength;
            Http2FrameFW frame = frameRO.wrap(compositeBuffer, 0, compositeBufferLength);
            frameConsumer.accept(frame);
            compositeBufferLength = 0;
            compositeBufferBuilder = supplyBufferBuilder.get();
            state = HEADER;
        }

        return remaining;
    }

    // @return no of bytes consumed
    private int processFrameHeader(long address, int length)
    {
        int remaining = Math.min(9 - compositeBufferLength, length);
        compositeBufferBuilder.wrap(memoryManager.resolve(address), remaining);
        compositeBufferLength += remaining;
        if (compositeBufferLength == 9)
        {
            // Have complete frame header in compositeBuffer
            DirectBuffer compositeBuffer = compositeBufferBuilder.build();
            assert compositeBuffer.capacity() == compositeBufferLength;
            Http2FrameHeaderFW frameHeader = frameHeaderRO.wrap(compositeBuffer, 0, compositeBufferLength);

            if (frameHeader.payloadLength() == 0)
            {
                Http2FrameFW frame = frameRO.wrap(compositeBuffer, 0, compositeBufferLength);
                frameConsumer.accept(frame);
                compositeBufferLength = 0;
                compositeBufferBuilder = supplyBufferBuilder.get();
                state = HEADER;
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
    private int processPreface(long address, int length)
    {
        int remaining = Math.min(PRI_REQUEST.length - compositeBufferLength, length);
        compositeBufferBuilder.wrap(memoryManager.resolve(address), remaining);
        compositeBufferLength += remaining;
        if (compositeBufferLength == PRI_REQUEST.length)
        {
            DirectBuffer compositeBuffer = compositeBufferBuilder.build();
            assert compositeBuffer.capacity() == compositeBufferLength;
            prefaceRO.wrap(compositeBuffer, 0, compositeBufferLength);
            if (prefaceRO.error()) {
                throw new IllegalStateException();
            }
            compositeBufferLength = 0;

            compositeBufferBuilder = supplyBufferBuilder.get();
            state = HEADER;
        }

        return remaining;
    }

//    private void processCompositeBuffer(DirectBuffer compositeBuffer)
//    {
//        int offset = 0;
//        while (true)
//        {
//            Http2FrameFW frame = http2FrameRO.canWrap(compositeBuffer, offset, compositeBufferLength);
//            if (frame != null)
//            {
//                frameConsumer.accept(frame);
//                offset += frame.sizeof();
//                compositeBufferLength -= frame.sizeof();
//            }
//            else
//            {
//                compositeBufferBuilder = supplyBufferBuilder.get();
//                compositeBufferBuilder.wrap(compositeBuffer, offset, compositeBufferLength);
//                break;
//            }
//        }
//    }

//    private void processRegion(RegionFW region, ListFW<RegionFW> regionsList) {
//        regionBuf.wrap(region.address(), region.length());
//        int regionOffset = 0;
//        int regionRemaining = region.length();
//        int copyBytes = -1;
//
//        while(regionRemaining > 0) {
//            copyBytes = Math.min(stagingBuf.capacity() - stagingOffset, regionRemaining);
//            if (copyBytes > 0)
//            {
//                stagingBuf.putBytes(stagingOffset, regionBuf, regionOffset, copyBytes);
//                stagingOffset += copyBytes;
//                regionOffset += copyBytes;
//                regionRemaining -= copyBytes;
//
//                Http2FrameFW frame = http2RO.canWrap(stagingBuf, 0, stagingOffset);
//                if (frame != null)
//                {
//                    totalSize += frame.sizeof();
//
//                    frameConsumer.accept(frame);
//                    int remaining = stagingOffset - frame.sizeof();
//                    // Move remaining data to the front of staging buffer
//                    stagingBuf.putBytes(0, stagingBuf, frame.sizeof(), remaining);
//                    stagingOffset = remaining;
//                }
//            }
//            else
//            {
//                break;
//            }
//        }
//        if (copyBytes == 0)
//        {
//
//        }
//    }

//    private void transfer(ListFW<RegionFW> regionsList, int listOffset, Http2FrameFW frame)
//    {
//        int remaining = frame.sizeof();
//        if (frame.type() == DATA)
//        {
//            if (length1 > 9)
//            {
//                // address1 + 9, l1 -9
//                // address2, length2
//                // r1, r2, ... rn
//            }
//            else if (length1 + length2 > 9)
//            {
//                // offset = 9 - length1
//                // address2 + offset, length2 - offset
//                // r1, r2, ... rn
//            }
//            else
//            {
//
//            }
//        }
//    }

//    private void ack(ListFW<RegionFW> regionsList, Http2FrameFW frame)
//    {
//        if (frame.type() == DATA)
//        {
//
//        }
//    }
//
//    private boolean coalesced(RegionFW region, long lastAddress, int lastLength)
//    {
//        return lastAddress + lastLength == region.address();
//    }

//    static final class Region
//    {
//        long address;
//        int length;
//
//        Region(long address, int length)
//        {
//            this.address = address;
//            this.length = length;
//        }
//
//        void update(long address, int length)
//        {
//            this.address = address;
//            this.length = length;
//        }
//    }

}
