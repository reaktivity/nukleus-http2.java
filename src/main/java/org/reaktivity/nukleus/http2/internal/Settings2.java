package org.reaktivity.nukleus.http2.internal;

public class Settings2 {
    static final int DEFAULT_HEADER_TABLE_SIZE = 4096;
    static final boolean DEFAULT_ENABLE_PUSH = true;
    static final int DEFAULT_MAX_CONCURRENT_STREAMS = 100;
    static final int DEFAULT_INITIAL_WINDOW_SIZE = 65_535;
    static final int DEFAULT_MAX_FRAME_SIZE = 16_384;

    int headerTableSize = DEFAULT_HEADER_TABLE_SIZE;
    boolean enablePush = DEFAULT_ENABLE_PUSH;
    int maxConcurrentStreams = DEFAULT_MAX_CONCURRENT_STREAMS;
    int initialWindowSize = DEFAULT_INITIAL_WINDOW_SIZE;
    int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;
    long maxHeaderListSize;
}
