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
import org.junit.Test;
import org.reaktivity.nukleus.buffer.MemoryManager;
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2HeadersFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsFW;
import org.reaktivity.nukleus.http2.internal.types.stream.RegionFW;
import org.reaktivity.reaktor.internal.buffer.DefaultDirectBufferBuilder;
import org.reaktivity.reaktor.internal.layouts.MemoryLayout;
import org.reaktivity.reaktor.internal.memory.DefaultMemoryManager;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW.LiteralType.INCREMENTAL_INDEXING;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.DATA;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.HEADERS;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType.SETTINGS;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2PrefaceFW.PRI_REQUEST;

public class Http2DecoderTest {

    private int frameCount = 0;

    @Test
    public void decode()
    {
        MemoryManager memoryManager = memoryManager();
        MutableDirectBuffer buf = new UnsafeBuffer(new byte[0]);

        long addressOffset = memoryManager.acquire(32768);
        long resolvedOffset = memoryManager.resolve(addressOffset);
        buf.wrap(resolvedOffset, 32768);
        int offset = 0;
        buf.putBytes(offset, PRI_REQUEST);
        offset += PRI_REQUEST.length;

        Http2HeadersFW headers = new Http2HeadersFW.Builder()
                .wrap(buf, offset, buf.capacity())   // non-zero offset
                .header(h -> h.indexed(2))      // :method: GET
                .header(h -> h.indexed(6))      // :scheme: http
                .header(h -> h.indexed(4))      // :path: /
                .header(h -> h.literal(l -> l.type(INCREMENTAL_INDEXING).name(1).value("www.example.com")))
                .endHeaders()
                .streamId(3)
                .build();
        offset = headers.limit();

        byte[] bytes = "123456789012345678901234567890".getBytes();
        DirectBuffer payload = new UnsafeBuffer(bytes);
        Http2DataFW data = new Http2DataFW.Builder()
                .wrap(buf, offset, buf.capacity())
                .streamId(5)
                .payload(payload)
                .build();
        offset = data.limit();

        Http2SettingsFW settings = new Http2SettingsFW.Builder()
                .wrap(buf, offset, buf.capacity())
                .ack()
                .build();
        offset = settings.limit();

        int offset1 = offset;
        int p = 2;
        int fullChunks = (offset - 1) / p;
        List<Region> regions = IntStream.range(0, fullChunks + 1)
                                        .mapToObj(n -> new Region(n, n == fullChunks ? offset1%p : p, 3))
                                        .collect(Collectors.toList());

        MutableDirectBuffer regionBuf = new UnsafeBuffer(new byte[4096]);

        ListFW.Builder<RegionFW.Builder, RegionFW> list = new ListFW.Builder<>(new RegionFW.Builder(), new RegionFW())
                .wrap(regionBuf, 0, regionBuf.capacity());
        regions.forEach(r -> list.item(b -> b.address(addressOffset + 2 * r.address).length(r.length).streamId(r.streamId)));
        ListFW<RegionFW> region = list.build();

        Http2Decoder decoder = new Http2Decoder(memoryManager, DefaultDirectBufferBuilder::new,
                Settings.DEFAULT_MAX_FRAME_SIZE, this::frame, null, null);
        decoder.decode(region);

        assertEquals(3, frameCount);
    }

    void frame(Http2FrameFW frame) {
        switch (frameCount)
        {
            case 0:
                assertEquals(HEADERS, frame.type());
                break;
            case 1:
                assertEquals(DATA, frame.type());
                break;
            case 2:
                assertEquals(SETTINGS, frame.type());
                break;
            default:
                throw new IllegalStateException();
        }
        frameCount++;
    }

    private MemoryManager memoryManager()
    {
        Path outputFile = new File("target/nukleus-itests/memory0").toPath();
        MemoryLayout.Builder mlb = new MemoryLayout.Builder()
                .path(outputFile);

        MemoryLayout layout = mlb.minimumBlockSize(1024)
                                 .maximumBlockSize(65536)
                                 .create(true)
                                 .build();
        return new DefaultMemoryManager(layout);
    }

    private static final class Region
    {
        final long address;
        final int length;
        final long streamId;

        Region(long address, int length, long streamId)
        {
            this.address = address;
            this.length = length;
            this.streamId = streamId;
        }

        public String toString()
        {
            return String.format("[%d - %d]", address, length);
        }
    }
}
