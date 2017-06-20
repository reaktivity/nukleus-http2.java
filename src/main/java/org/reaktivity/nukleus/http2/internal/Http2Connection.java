package org.reaktivity.nukleus.http2.internal;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.http2.internal.routable.Route;
import org.reaktivity.nukleus.http2.internal.routable.Target;
import org.reaktivity.nukleus.http2.internal.routable.stream.Http2WriteScheduler;
import org.reaktivity.nukleus.http2.internal.routable.stream.WriteScheduler;
import org.reaktivity.nukleus.http2.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.OctetsFW;
import org.reaktivity.nukleus.http2.internal.types.StringFW;
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http2.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackContext;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderBlockFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHuffman;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackStringFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameType;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsId;
import org.reaktivity.nukleus.http2.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http2.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http2.internal.types.stream.WindowFW;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static org.reaktivity.nukleus.http2.internal.Http2Connection.State.HALF_CLOSED_REMOTE;
import static org.reaktivity.nukleus.http2.internal.Http2Connection.State.OPEN;
import static org.reaktivity.nukleus.http2.internal.routable.Route.headersMatch;
import static org.reaktivity.nukleus.http2.internal.routable.stream.Slab.NO_SLOT;
import static org.reaktivity.nukleus.http2.internal.router.RouteKind.OUTPUT_ESTABLISHED;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackContext.TE;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackContext.TRAILERS;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderFieldFW.HeaderFieldType.UNKNOWN;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW.LiteralType.INCREMENTAL_INDEXING;
import static org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW.LiteralType.WITHOUT_INDEXING;
import static org.reaktivity.nukleus.http2.internal.types.stream.Http2PrefaceFW.PRI_REQUEST;

final class Http2Connection
{
    private static final double OUTWINDOW_LOW_THRESHOLD = 0.5;      // TODO configuration

    ServerStreamFactory factory;

    private MessageHandler streamState;
    private MessageHandler throttleState;
    private DecoderState decoderState;

    // slab to assemble a complete HTTP2 frame
    // no need for separate slab per HTTP2 stream as the frames are not fragmented
    private int frameSlotIndex = NO_SLOT;
    private int frameSlotPosition;

    // slab to assemble a complete HTTP2 headers frame(including its continuation frames)
    // no need for separate slab per HTTP2 stream as no interleaved frames of any other type
    // or from any other stream
    private int headersSlotIndex = NO_SLOT;
    private int headersSlotPosition;

    long sourceId;
    int lastStreamId;
    long sourceRef;
    private long correlationId;
    private final int initialWindow = 8192; // TODO config
    int window = initialWindow;
    int outWindow;
    int outWindowThreshold = -1;

    private final WriteScheduler writeScheduler;

    final long sourceOutputEstId;
    private final HpackContext decodeContext;
    private final HpackContext encodeContext;

    final Int2ObjectHashMap<Http2Stream2> http2Streams;      // HTTP2 stream-id --> Http2Stream

    private int lastPromisedStreamId;

    private int noClientStreams;
    private int noPromisedStreams;
    private int maxClientStreamId;
    private int maxPushPromiseStreamId;

    private boolean goaway;
    private Settings2 localSettings;
    private Settings2 remoteSettings;
    private boolean expectContinuation;
    private int expectContinuationStreamId;
    private boolean expectDynamicTableSizeUpdate = true;
    long http2OutWindow;
    private long http2InWindow;

    private boolean prefaceAvailable;
    private boolean http2FrameAvailable;
    private final Consumer<HpackHeaderFieldFW> headerFieldConsumer;
    private final HeadersContext headersContext = new HeadersContext();
    private final EncodeHeadersContext encodeHeadersContext = new EncodeHeadersContext();

    @Override
    public String toString()
    {
        return String.format("%s[source=%s, sourceId=%016x, window=%d]",
                getClass().getSimpleName(), source.routableName(), sourceId, window);
    }

    private Http2Connection()
    {
        this.streamState = this::streamBeforeBegin;
        this.throttleState = this::throttleNextWindow;
        sourceOutputEstId = supplyStreamId.getAsLong();
        http2Streams = new Int2ObjectHashMap<>();
        localSettings = new Settings2();
        remoteSettings = new Settings2();
        decodeContext = new HpackContext(localSettings.headerTableSize, false);
        encodeContext = new HpackContext(remoteSettings.headerTableSize, true);
        writeScheduler = new Http2WriteScheduler(this, sourceOutputEstId, factory.frameSlab, replyTarget, sourceOutputEstId);
        http2InWindow = localSettings.initialWindowSize;
        http2OutWindow = remoteSettings.initialWindowSize;

        BiConsumer<DirectBuffer, DirectBuffer> nameValue =
                ((BiConsumer<DirectBuffer, DirectBuffer>)this::collectHeaders)
                        .andThen(this::mapToHttp)
                        .andThen(this::validatePseudoHeaders)
                        .andThen(this::uppercaseHeaders)
                        .andThen(this::connectionHeaders)
                        .andThen(this::contentLengthHeader)
                        .andThen(this::teHeader);

        Consumer<HpackHeaderFieldFW> consumer = this::validateHeaderFieldType;
        consumer = consumer.andThen(this::dynamicTableSizeUpdate);
        this.headerFieldConsumer = consumer.andThen(h -> decodeHeaderField(h, nameValue));
    }

    private void handleStream(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
    {
        streamState.onMessage(msgTypeId, buffer, index, length);
    }

    private void streamBeforeBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        if (msgTypeId == BeginFW.TYPE_ID)
        {
            processBegin(buffer, index, length);
        }
        else
        {
            processUnexpected(buffer, index, length);
        }
    }

    private void streamAfterBeginOrData(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        switch (msgTypeId)
        {
            case DataFW.TYPE_ID:
                processData(buffer, index, length);
                break;
            case EndFW.TYPE_ID:
                processEnd(buffer, index, length);
                break;
            default:
                processUnexpected(buffer, index, length);
                break;
        }
    }

    private void streamAfterEnd(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        processUnexpected(buffer, index, length);
    }

    private void streamAfterReset(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
    {
        // already sent reset, just ignore the data
    }

    private void streamAfterReplyOrReset(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
    {
        if (msgTypeId == DataFW.TYPE_ID)
        {
            factory.dataRO.wrap(buffer, index, index + length);
            final long streamId = factory.dataRO.streamId();
            factory.doWindow(streamId, length);
        }
        else if (msgTypeId == EndFW.TYPE_ID)
        {
            factory.endRO.wrap(buffer, index, index + length);
            final long streamId = factory.endRO.streamId();

            //source.removeStream(streamId);

            this.streamState = this::streamAfterEnd;
        }
    }

    private void processUnexpected(
            DirectBuffer buffer,
            int index,
            int length)
    {
        factory.frameRO.wrap(buffer, index, index + length);
        long streamId = factory.frameRO.streamId();

        processUnexpected(streamId);
    }

    void processUnexpected(
            long streamId)
    {
        factory.doReset(streamId);
        cleanConnection();
    }

    void cleanConnection()
    {
        replyTarget.removeThrottle(sourceOutputEstId);
        source.removeStream(sourceId);
        for(Http2Stream2 http2Stream : http2Streams.values())
        {
            closeStream(http2Stream);
        }
        http2Streams.clear();
        streamState = this::streamAfterReset;
    }

    private void processBegin(
            DirectBuffer buffer,
            int index,
            int length)
    {
        factory.beginRO.wrap(buffer, index, index + length);

        this.sourceId = factory.beginRO.streamId();
        this.sourceRef = factory.beginRO.sourceRef();
        this.correlationId = factory.beginRO.correlationId();
        this.streamState = this::streamAfterBeginOrData;
        this.decoderState = this::decodePreface;
        factory.doWindow(sourceId, window);

        replyTarget.addThrottle(sourceOutputEstId, this::handleThrottle);
        factory.doBegin(sourceOutputEstId, 0L, correlationId);
        writeScheduler.settings(localSettings.maxConcurrentStreams);
    }

    private void processData(
            DirectBuffer buffer,
            int index,
            int length)
    {
        factory.dataRO.wrap(buffer, index, index + length);

        window -= factory.dataRO.length();

        if (window < 0)
        {
            processUnexpected(buffer, index, length);
        }
        else
        {
            this.window += factory.dataRO.length();
            assert window <= initialWindow;
            factory.doWindow(sourceId, factory.dataRO.length());
            final OctetsFW payload = factory.dataRO.payload();
            final int limit = payload.limit();

            int offset = payload.offset();
            while (offset < limit)
            {
                offset += decoderState.decode(buffer, offset, limit);
            }
        }
    }

    private void processEnd(
            DirectBuffer buffer,
            int index,
            int length)
    {
        factory.endRO.wrap(buffer, index, index + length);
        final long streamId = factory.endRO.streamId();

        decoderState = (b, o, l) -> o;

        source.removeStream(streamId);
        writeScheduler.doEnd();
        for(Http2Stream2 http2Stream : http2Streams.values())
        {
            closeStream(http2Stream);
        }
        http2Streams.clear();

        if (frameSlotIndex != NO_SLOT)
        {
            factory.frameSlab.release(frameSlotIndex);
            frameSlotIndex = NO_SLOT;
            frameSlotPosition = 0;
        }
        if (headersSlotIndex != NO_SLOT)
        {
            factory.headersSlab.release(headersSlotIndex);
            headersSlotIndex = NO_SLOT;
            headersSlotPosition = 0;
        }
    }

    // Decodes client preface
    private int decodePreface(final DirectBuffer buffer, final int offset, final int limit)
    {
        int length = prefaceAvailable(buffer, offset, limit);
        if (!prefaceAvailable)
        {
            return length;
        }
        if (factory.prefaceRO.error())
        {
            processUnexpected(sourceId);
            return limit-offset;
        }
        this.decoderState = this::decodeHttp2Frame;
        return length;
    }

    private int http2FrameLength(DirectBuffer buffer, final int offset, int limit)
    {
        assert limit - offset >= 3;

        int length = (buffer.getByte(offset) & 0xFF) << 16;
        length += (buffer.getShort(offset + 1, BIG_ENDIAN) & 0xFF_FF);
        return length + 9;      // +3 for length, +1 type, +1 flags, +4 stream-id
    }

    /*
     * Assembles a complete HTTP2 client preface and the flyweight is wrapped with the
     * buffer (it could be given buffer or slab)
     *
     * @return no of bytes consumed
     */
    private int prefaceAvailable(DirectBuffer buffer, int offset, int limit)
    {
        int available = limit - offset;

        if (frameSlotPosition > 0 && frameSlotPosition + available >= PRI_REQUEST.length)
        {
            MutableDirectBuffer prefaceBuffer = factory.frameSlab.buffer(frameSlotIndex);
            int remainingLength = PRI_REQUEST.length - frameSlotPosition;
            prefaceBuffer.putBytes(frameSlotPosition, buffer, offset, remainingLength);
            factory.prefaceRO.wrap(prefaceBuffer, 0, PRI_REQUEST.length);
            if (frameSlotIndex != NO_SLOT)
            {
                factory.frameSlab.release(frameSlotIndex);
                frameSlotIndex = NO_SLOT;
                frameSlotPosition = 0;
            }
            prefaceAvailable = true;
            return remainingLength;
        }
        else if (available >= PRI_REQUEST.length)
        {
            factory.prefaceRO.wrap(buffer, offset, offset + PRI_REQUEST.length);
            prefaceAvailable = true;
            return PRI_REQUEST.length;
        }

        assert frameSlotIndex == NO_SLOT;
        frameSlotIndex = factory.frameSlab.acquire(sourceId);
        if (frameSlotIndex == NO_SLOT)
        {
            // all slots are in use, just reset the connection
            factory.doReset(sourceId);
            prefaceAvailable = false;
            return available;               // assume everything is consumed
        }
        frameSlotPosition = 0;
        MutableDirectBuffer prefaceBuffer = factory.frameSlab.buffer(frameSlotIndex);
        prefaceBuffer.putBytes(frameSlotPosition, buffer, offset, available);
        frameSlotPosition += available;
        prefaceAvailable = false;
        return available;
    }

    /*
     * Assembles a complete HTTP2 frame and the flyweight is wrapped with the
     * buffer (it could be given buffer or slab)
     *
     * @return consumed octets
     *         -1 if the frame size is more than the max frame size
     */
    // TODO check slab capacity
    private int http2FrameAvailable(DirectBuffer buffer, int offset, int limit)
    {
        int available = limit - offset;

        if (frameSlotPosition > 0 && frameSlotPosition + available >= 3)
        {
            MutableDirectBuffer frameBuffer = factory.frameSlab.buffer(frameSlotIndex, this::write);
            if (frameSlotPosition < 3)
            {
                frameBuffer.putBytes(frameSlotPosition, buffer, offset, 3 - frameSlotPosition);
            }
            int frameLength = http2FrameLength(frameBuffer, 0, 3);
            if (frameLength > localSettings.maxFrameSize + 9)
            {
                return -1;
            }
            if (frameSlotPosition + available >= frameLength)
            {
                int remainingFrameLength = frameLength - frameSlotPosition;
                frameBuffer.putBytes(frameSlotPosition, buffer, offset, remainingFrameLength);
                factory.http2RO.wrap(frameBuffer, 0, frameLength);
                releaseSlot();
                http2FrameAvailable = true;
                return remainingFrameLength;
            }
        }
        else if (available >= 3)
        {
            int frameLength = http2FrameLength(buffer, offset, limit);
            if (frameLength > localSettings.maxFrameSize + 9)
            {
                return -1;
            }
            if (available >= frameLength)
            {
                factory.http2RO.wrap(buffer, offset, offset + frameLength);
                http2FrameAvailable = true;
                return frameLength;
            }
        }

        if (!acquireSlot())
        {
            http2FrameAvailable = false;
            return available;
        }

        MutableDirectBuffer frameBuffer = factory.frameSlab.buffer(frameSlotIndex, this::write);
        frameBuffer.putBytes(frameSlotPosition, buffer, offset, available);
        frameSlotPosition += available;
        http2FrameAvailable = false;
        return available;
    }

    private boolean acquireSlot()
    {
        if (frameSlotPosition == 0)
        {
            assert frameSlotIndex == NO_SLOT;
            frameSlotIndex = factory.frameSlab.acquire(sourceId);
            if (frameSlotIndex == NO_SLOT)
            {
                // all slots are in use, just reset the connection
                factory.doReset(sourceId);
                http2FrameAvailable = false;
                return false;
            }
        }
        return true;
    }

    private void releaseSlot()
    {
        if (frameSlotIndex != NO_SLOT)
        {
            factory.frameSlab.release(frameSlotIndex);
            frameSlotIndex = NO_SLOT;
            frameSlotPosition = 0;
        }
    }



    /*
     * Assembles a complete HTTP2 headers (including any continuations) if any.
     *
     * @return true if a complete HTTP2 headers is assembled or any other frame
     *         false otherwise
     */
    private boolean http2HeadersAvailable()
    {
        if (expectContinuation)
        {
            if (factory.http2RO.type() != Http2FrameType.CONTINUATION || factory.http2RO.streamId() != expectContinuationStreamId)
            {
                error(Http2ErrorCode.PROTOCOL_ERROR);
                return false;
            }
        }
        else if (factory.http2RO.type() == Http2FrameType.CONTINUATION)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return false;
        }
        switch (factory.http2RO.type())
        {
            case HEADERS:
                int streamId = factory.http2RO.streamId();
                if (streamId == 0 || streamId % 2 != 1 || streamId <= maxClientStreamId)
                {
                    error(Http2ErrorCode.PROTOCOL_ERROR);
                    return false;
                }

                factory.headersRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());
                int parentStreamId = factory.headersRO.parentStream();
                if (parentStreamId == streamId)
                {
                    // 5.3.1 A stream cannot depend on itself
                    streamError(streamId, Http2ErrorCode.PROTOCOL_ERROR);
                    return false;
                }
                if (factory.headersRO.dataLength() < 0)
                {
                    error(Http2ErrorCode.PROTOCOL_ERROR);
                    return false;
                }

                return http2HeadersAvailable(factory.headersRO.buffer(), factory.headersRO.dataOffset(),
                        factory.headersRO.dataLength(), factory.headersRO.endHeaders());

            case CONTINUATION:
                factory.continationRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());
                DirectBuffer payload = factory.continationRO.payload();
                boolean endHeaders = factory.continationRO.endHeaders();

                return http2HeadersAvailable(payload, 0, payload.capacity(), endHeaders);
        }

        return true;
    }

    private MutableDirectBuffer read(MutableDirectBuffer buffer)
    {
        factory.read.wrap(buffer.addressOffset(), buffer.capacity());
        return factory.read;
    }

    private MutableDirectBuffer write(MutableDirectBuffer buffer)
    {
        factory.write.wrap(buffer.addressOffset(), buffer.capacity());
        return factory.write;
    }

    /*
     * Assembles a complete HTTP2 headers (including any continuations) and the
     * flyweight is wrapped with the buffer (it could be given buffer or slab)
     *
     * @return true if a complete HTTP2 headers is assembled
     *         false otherwise
     */
    private boolean http2HeadersAvailable(DirectBuffer buffer, int offset, int length, boolean endHeaders)
    {
        if (endHeaders)
        {
            if (headersSlotPosition > 0)
            {
                MutableDirectBuffer headersBuffer = factory.headersSlab.buffer(headersSlotIndex, this::write);
                headersBuffer.putBytes(headersSlotPosition, buffer, offset, length);
                headersSlotPosition += length;
                buffer = headersBuffer;
                offset = 0;
                length = headersSlotPosition;
            }
            int maxLimit = offset + length;
            expectContinuation = false;
            if (headersSlotIndex != NO_SLOT)
            {
                factory.headersSlab.release(headersSlotIndex);      // early release, but fine
                headersSlotIndex = NO_SLOT;
                headersSlotPosition = 0;
            }

            factory.blockRO.wrap(buffer, offset, maxLimit);
            return true;
        }
        else
        {
            if (headersSlotIndex == NO_SLOT)
            {
                headersSlotIndex = factory.headersSlab.acquire(sourceId);
                if (headersSlotIndex == NO_SLOT)
                {
                    // all slots are in use, just reset the connection
                    factory.doReset(sourceId);
                    return false;
                }
                headersSlotPosition = 0;
            }
            MutableDirectBuffer headersBuffer = factory.headersSlab.buffer(headersSlotIndex, this::write);
            headersBuffer.putBytes(headersSlotPosition, buffer, offset, length);
            headersSlotPosition += length;
            expectContinuation = true;
            expectContinuationStreamId = factory.headersRO.streamId();
        }

        return false;
    }

    private int decodeHttp2Frame(final DirectBuffer buffer, final int offset, final int limit)
    {
        int length = http2FrameAvailable(buffer, offset, limit);
        if (length == -1)
        {
            error(Http2ErrorCode.FRAME_SIZE_ERROR);
            return limit - offset;
        }
        if (!http2FrameAvailable)
        {
            return length;
        }
        Http2FrameType http2FrameType = factory.http2RO.type();
        // Assembles HTTP2 HEADERS and its CONTINUATIONS frames, if any
        if (!http2HeadersAvailable())
        {
            return length;
        }
        switch (http2FrameType)
        {
            case DATA:
                doData();
                break;
            case HEADERS:   // fall-through
            case CONTINUATION:
                doHeaders();
                break;
            case PRIORITY:
                doPriority();
                break;
            case RST_STREAM:
                doRst();
                break;
            case SETTINGS:
                doSettings();
                break;
            case PUSH_PROMISE:
                doPushPromise();
                break;
            case PING:
                doPing();
                break;
            case GO_AWAY:
                doGoAway();
                break;
            case WINDOW_UPDATE:
                doWindow();
                break;
            default:
                // Ignore and discard unknown frame
        }

        return length;
    }

    private void doGoAway()
    {
        int streamId = factory.http2RO.streamId();
        if (goaway)
        {
            if (streamId != 0)
            {
                processUnexpected(sourceId);
            }
        }
        else
        {
            goaway = true;
            Http2ErrorCode errorCode = (streamId != 0) ? Http2ErrorCode.PROTOCOL_ERROR : Http2ErrorCode.NO_ERROR;
            remoteSettings.enablePush = false;      // no new streams
            error(errorCode);
        }
    }

    private void doPushPromise()
    {
        error(Http2ErrorCode.PROTOCOL_ERROR);
    }

    private void doPriority()
    {
        int streamId = factory.http2RO.streamId();
        if (streamId == 0)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        int payloadLength = factory.http2RO.payloadLength();
        if (payloadLength != 5)
        {
            streamError(streamId, Http2ErrorCode.FRAME_SIZE_ERROR);
            return;
        }
        factory.priorityRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());
        int parentStreamId = factory.priorityRO.parentStream();
        if (parentStreamId == streamId)
        {
            // 5.3.1 A stream cannot depend on itself
            streamError(streamId, Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
    }

    private void doHeaders()
    {
        int streamId = factory.http2RO.streamId();

        Http2Stream2 stream = http2Streams.get(streamId);
        if (stream != null)
        {
            // TODO trailers
        }
        if (streamId <= maxClientStreamId)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        maxClientStreamId = streamId;

        if (noClientStreams + 1 > localSettings.maxConcurrentStreams)
        {
            streamError(streamId, Http2ErrorCode.REFUSED_STREAM);
            return;
        }

        State state = factory.http2RO.endStream() ? HALF_CLOSED_REMOTE : OPEN;

        headersContext.reset();

        factory.httpBeginExRW.wrap(scratch, 0, scratch.capacity());

        factory.blockRO.forEach(headerFieldConsumer);
        // All HTTP/2 requests MUST include exactly one valid value for the
        // ":method", ":scheme", and ":path" pseudo-header fields, unless it is
        // a CONNECT request (Section 8.3).  An HTTP request that omits
        // mandatory pseudo-header fields is malformed
        if (!headersContext.error() && (headersContext.method != 1 || headersContext.scheme != 1 || headersContext.path != 1))
        {
            headersContext.streamError = Http2ErrorCode.PROTOCOL_ERROR;
        }
        if (headersContext.error())
        {
            if (headersContext.streamError != null)
            {
                streamError(streamId, headersContext.streamError);
                return;
            }
            if (headersContext.connectionError != null)
            {
                error(headersContext.connectionError);
                return;
            }
        }

        final Optional<Route> optional = resolveTarget(sourceRef, headersContext.headers);
        Route route = optional.get();   // TODO
        stream = newStream(streamId, state, route);
        Target newTarget = route.target();
        final long targetRef = route.targetRef();

        stream.contentLength = headersContext.contentLength;

        HttpBeginExFW beginEx = factory.httpBeginExRW.build();
        newTarget.doHttpBegin(stream.targetId, targetRef, stream.targetId, beginEx.buffer(), beginEx.offset(),
                beginEx.sizeof());
        newTarget.addThrottle(stream.targetId, stream::onThrottle);

        if (factory.headersRO.endStream())
        {
            newTarget.doHttpEnd(stream.targetId);
        }
    }

    private void doRst()
    {
        int streamId = factory.http2RO.streamId();
        if (streamId == 0)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        int payloadLength = factory.http2RO.payloadLength();
        if (payloadLength != 4)
        {
            error(Http2ErrorCode.FRAME_SIZE_ERROR);
            return;
        }
        Http2Stream2 stream = http2Streams.get(streamId);
        if (stream == null || stream.state == State.IDLE)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
        }
        else
        {
            closeStream(stream);
        }
    }

    private void closeStream(Http2Stream2 stream)
    {
        if (stream.isClientInitiated())
        {
            noClientStreams--;
        }
        else
        {
            noPromisedStreams--;
        }
        factory.correlations.remove(stream.targetId);    // remove from Correlations map
        http2Streams.remove(stream.http2StreamId);
        stream.route.target().removeThrottle(stream.targetId);

        stream.route.target().doHttpEnd(stream.targetId);
    }

    private void doWindow()
    {
        int streamId = factory.http2RO.streamId();
        if (factory.http2RO.payloadLength() != 4)
        {
            if (streamId == 0)
            {
                error(Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
            else
            {
                streamError(streamId, Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
        }
        if (streamId != 0)
        {
            State state = state(streamId);
            if (state == State.IDLE)
            {
                error(Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
            Http2Stream2 stream = http2Streams.get(streamId);
            if (stream == null)
            {
                // A receiver could receive a WINDOW_UPDATE frame on a "half-closed (remote)" or "closed" stream.
                // A receiver MUST NOT treat this as an error
                return;
            }
        }
        factory.http2WindowRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());

        // 6.9 WINDOW_UPDATE - legal range for flow-control window increment is 1 to 2^31-1 octets.
        if (factory.http2WindowRO.size() < 1)
        {
            if (streamId == 0)
            {
                error(Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
            else
            {
                streamError(streamId, Http2ErrorCode.PROTOCOL_ERROR);
                return;
            }
        }

        // 6.9.1 A sender MUST NOT allow a flow-control window to exceed 2^31-1 octets.
        if (streamId == 0)
        {
            http2OutWindow += factory.http2WindowRO.size();
            if (http2OutWindow > Integer.MAX_VALUE)
            {
                error(Http2ErrorCode.FLOW_CONTROL_ERROR);
                return;
            }
            writeScheduler.onHttp2Window();
        }
        else
        {
            Http2Stream2 stream = http2Streams.get(streamId);
            stream.http2OutWindow += factory.http2WindowRO.size();
            if (stream.http2OutWindow > Integer.MAX_VALUE)
            {
                streamError(streamId, Http2ErrorCode.FLOW_CONTROL_ERROR);
                return;
            }
            writeScheduler.onHttp2Window(streamId);
        }

    }

    private void doData()
    {
        int streamId = factory.http2RO.streamId();
        Http2Stream2 stream = http2Streams.get(streamId);

        if (streamId == 0 || stream == null || stream.state == State.IDLE)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        if (stream.state == HALF_CLOSED_REMOTE)
        {
            error(Http2ErrorCode.STREAM_CLOSED);
            closeStream(stream);
            return;
        }
        Http2DataFW dataRO = factory.http2DataRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(),
                factory.http2RO.limit());
        if (dataRO.dataLength() < 0)        // because of invalid padding length
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            closeStream(stream);
            return;
        }

        //
        if (stream.http2InWindow < factory.http2RO.payloadLength() || http2InWindow < factory.http2RO.payloadLength())
        {
            streamError(streamId, Http2ErrorCode.FLOW_CONTROL_ERROR);
            return;
        }
        http2InWindow -= factory.http2RO.payloadLength();
        stream.http2InWindow -= factory.http2RO.payloadLength();

        Target newTarget = stream.route.target();
        stream.totalData += factory.http2RO.payloadLength();

        if (dataRO.endStream())
        {
            // 8.1.2.6 A request is malformed if the value of a content-length header field does
            // not equal the sum of the DATA frame payload lengths
            if (stream.contentLength != -1 && stream.totalData != stream.contentLength)
            {
                streamError(streamId, Http2ErrorCode.PROTOCOL_ERROR);
                newTarget.doEnd(stream.targetId);
                return;
            }
        }

        stream.onData();
    }

    private void doSettings()
    {
        if (factory.http2RO.streamId() != 0)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        if (factory.http2RO.payloadLength()%6 != 0)
        {
            error(Http2ErrorCode.FRAME_SIZE_ERROR);
            return;
        }

        factory.settingsRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());

        if (factory.settingsRO.ack() && factory.http2RO.payloadLength() != 0)
        {
            error(Http2ErrorCode.FRAME_SIZE_ERROR);
            return;
        }
        if (!factory.settingsRO.ack())
        {
            factory.settingsRO.accept(this::doSetting);
            writeScheduler.settingsAck();
        }
    }

    private void doSetting(Http2SettingsId id, Long value)
    {
        switch (id)
        {
            case HEADER_TABLE_SIZE:
                remoteSettings.headerTableSize = value.intValue();
                break;
            case ENABLE_PUSH:
                if (!(value == 0L || value == 1L))
                {
                    error(Http2ErrorCode.PROTOCOL_ERROR);
                    return;
                }
                remoteSettings.enablePush = (value == 1L);
                break;
            case MAX_CONCURRENT_STREAMS:
                remoteSettings.maxConcurrentStreams = value.intValue();
                break;
            case INITIAL_WINDOW_SIZE:
                if (value > Integer.MAX_VALUE)
                {
                    error(Http2ErrorCode.FLOW_CONTROL_ERROR);
                    return;
                }
                int old = remoteSettings.initialWindowSize;
                remoteSettings.initialWindowSize = value.intValue();
                int update = value.intValue() - old;

                // 6.9.2. Initial Flow-Control Window Size
                // SETTINGS frame can alter the initial flow-control
                // window size for streams with active flow-control windows
                for(Http2Stream2 http2Stream: http2Streams.values())
                {
                    http2Stream.http2OutWindow += update;           // http2OutWindow can become negative
                    if (http2Stream.http2OutWindow > Integer.MAX_VALUE)
                    {
                        // 6.9.2. Initial Flow-Control Window Size
                        // An endpoint MUST treat a change to SETTINGS_INITIAL_WINDOW_SIZE that
                        // causes any flow-control window to exceed the maximum size as a
                        // connection error of type FLOW_CONTROL_ERROR.
                        error(Http2ErrorCode.FLOW_CONTROL_ERROR);
                        return;
                    }
                }
                break;
            case MAX_FRAME_SIZE:
                if (value < Math.pow(2, 14) || value > Math.pow(2, 24) -1)
                {
                    error(Http2ErrorCode.PROTOCOL_ERROR);
                    return;
                }
                remoteSettings.maxFrameSize = value.intValue();
                break;
            case MAX_HEADER_LIST_SIZE:
                remoteSettings.maxHeaderListSize = value.intValue();
                break;
            default:
                // Ignore the unkonwn setting
                break;
        }
    }

    private void doPing()
    {
        if (factory.http2RO.streamId() != 0)
        {
            error(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        if (factory.http2RO.payloadLength() != 8)
        {
            error(Http2ErrorCode.FRAME_SIZE_ERROR);
            return;
        }
        factory.pingRO.wrap(factory.http2RO.buffer(), factory.http2RO.offset(), factory.http2RO.limit());

        if (!factory.pingRO.ack())
        {
            writeScheduler.pingAck(factory.pingRO.payload(), 0, factory.pingRO.payload().capacity());
        }
    }

    private State state(int streamId)
    {
        Http2Stream2 stream = http2Streams.get(streamId);
        if (stream != null)
        {
            return stream.state;
        }
        if (streamId%2 == 1)
        {
            if (streamId <= maxClientStreamId)
            {
                return State.CLOSED;
            }
        }
        else
        {
            if (streamId <= maxPushPromiseStreamId)
            {
                return State.CLOSED;
            }
        }
        return State.IDLE;
    }

    Optional<Route> resolveTarget(
            long sourceRef,
            Map<String, String> headers)
    {
        final List<Route> routes = supplyRoutes.apply(sourceRef);
        final Predicate<Route> predicate = headersMatch(headers);

        return routes.stream().filter(predicate).findFirst();
    }

    void handleThrottle(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
    {
        throttleState.onMessage(msgTypeId, buffer, index, length);
    }

    private void throttleNextWindow(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        switch (msgTypeId)
        {
            case WindowFW.TYPE_ID:
                processNextWindow(buffer, index, length);
                break;
            case ResetFW.TYPE_ID:
                processReset(buffer, index, length);
                break;
            default:
                // ignore
                break;
        }
    }

    private void processNextWindow(
            DirectBuffer buffer,
            int index,
            int length)
    {
        factory.windowRO.wrap(buffer, index, index + length);
        int update = factory.windowRO.update();
        if (outWindowThreshold == -1)
        {
            outWindowThreshold = (int) (OUTWINDOW_LOW_THRESHOLD * update);
        }
        outWindow += update;
        writeScheduler.onWindow();
    }


    private void processReset(
            DirectBuffer buffer,
            int index,
            int length)
    {
        factory.resetRO.wrap(buffer, index, index + length);
        releaseSlot();
        if (headersSlotIndex != NO_SLOT)
        {
            factory.headersSlab.release(headersSlotIndex);
            headersSlotIndex = NO_SLOT;
            headersSlotPosition = 0;
        }

        cleanConnection();
    }

    void error(Http2ErrorCode errorCode)
    {
        writeScheduler.goaway(lastStreamId, errorCode);
        writeScheduler.doEnd();
    }

    void streamError(int streamId, Http2ErrorCode errorCode)
    {
        writeScheduler.rst(streamId, errorCode);
    }

    private int nextPromisedId()
    {
        lastPromisedStreamId += 2;
        return lastPromisedStreamId;
    }

    /*
     * @param streamId corresponding http2 stream-id on which service response
     *                 will be sent
     * @return a stream id on which PUSH_PROMISE can be sent
     *         -1 otherwise
     */
    private int findPushId(int streamId)
    {
        if (remoteSettings.enablePush && noPromisedStreams+1 < remoteSettings.maxConcurrentStreams)
        {
            // PUSH_PROMISE frames MUST only be sent on a peer-initiated stream
            if (streamId%2 == 0)
            {
                // Find a stream on which PUSH_PROMISE can be sent
                return http2Streams.entrySet()
                                   .stream()
                                   .map(Map.Entry::getValue)
                                   .filter(s -> (s.http2StreamId & 0x01) == 1)     // client-initiated stream
                                   .filter(s -> s.state == OPEN || s.state == HALF_CLOSED_REMOTE)
                                   .mapToInt(s -> s.http2StreamId)
                                   .findAny()
                                   .orElse(-1);
            }
            else
            {
                return streamId;        // client-initiated stream
            }
        }
        return -1;
    }

    private void doPromisedRequest(int http2StreamId, ListFW<HttpHeaderFW> headers)
    {
        Map<String, String> headersMap = new HashMap<>();
        headers.forEach(
                httpHeader -> headersMap.put(httpHeader.name().asString(), httpHeader.value().asString()));
        Optional<Route> optional = resolveTarget(sourceRef, headersMap);
        Route route = optional.get();
        Http2Stream2 http2Stream = newStream(http2StreamId, HALF_CLOSED_REMOTE, route);
        long targetId = http2Stream.targetId;

        Target newTarget = route.target();
        long targetRef = route.targetRef();

        newTarget.doHttpBegin(targetId, targetRef, targetId,
                hs -> headers.forEach(h -> hs.item(b -> b.representation((byte) 0)
                                                         .name(h.name())
                                                         .value(h.value()))));
        newTarget.doHttpEnd(targetId);
        Correlation2 correlation = new Correlation2(correlationId, sourceOutputEstId, writeScheduler,
                this::doPromisedRequest, http2StreamId, encodeContext, this::nextPromisedId, this::findPushId,
                OUTPUT_ESTABLISHED);

        factory.correlations.put(targetId, correlation);
        newTarget.addThrottle(targetId, this::handleThrottle);
    }

    private Http2Stream2 newStream(int http2StreamId, State state, Route route)
    {
        assert http2StreamId != 0;

        Http2Stream2 http2Stream = new Http2Stream2(this, http2StreamId, state, route);
        http2Streams.put(http2StreamId, http2Stream);

        Correlation2 correlation = new Correlation2(correlationId, sourceOutputEstId, writeScheduler,
                this::doPromisedRequest, factory.http2RO.streamId(), encodeContext, this::nextPromisedId, this::findPushId,
                OUTPUT_ESTABLISHED);

        factory.correlations.put(http2Stream.targetId, correlation);
        if (http2Stream.isClientInitiated())
        {
            noClientStreams++;
        }
        else
        {
            noPromisedStreams++;
        }
        return http2Stream;
    }

    private void validateHeaderFieldType(HpackHeaderFieldFW hf)
    {
        if (!headersContext.error() && hf.type() == UNKNOWN)
        {
            headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
        }
    }

    private void dynamicTableSizeUpdate(HpackHeaderFieldFW hf)
    {
        if (!headersContext.error())
        {
            switch (hf.type())
            {
                case INDEXED:
                case LITERAL:
                    expectDynamicTableSizeUpdate = false;
                    break;
                case UPDATE:
                    if (!expectDynamicTableSizeUpdate)
                    {
                        // dynamic table size update MUST occur at the beginning of the first header block
                        headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                        return;
                    }
                    int maxTableSize = hf.tableSize();
                    if (maxTableSize > localSettings.headerTableSize)
                    {
                        headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                        return;
                    }
                    decodeContext.updateSize(hf.tableSize());
                    break;
            }
        }
    }

    private void validatePseudoHeaders(DirectBuffer name, DirectBuffer value)
    {
        if (!headersContext.error())
        {
            if (name.capacity() > 0 && name.getByte(0) == ':')
            {
                // All pseudo-header fields MUST appear in the header block before regular header fields
                if (headersContext.regularHeader)
                {
                    headersContext.streamError = Http2ErrorCode.PROTOCOL_ERROR;
                    return;
                }
                // request pseudo-header fields MUST be one of :authority, :method, :path, :scheme,
                int index = decodeContext.index(name);
                switch (index)
                {
                    case 1:             // :authority
                        break;
                    case 2:             // :method
                        headersContext.method++;
                        break;
                    case 4:             // :path
                        if (value.capacity() > 0)       // :path MUST not be empty
                        {
                            headersContext.path++;
                        }
                        break;
                    case 6:             // :scheme
                        headersContext.scheme++;
                        break;
                    default:
                        headersContext.streamError = Http2ErrorCode.PROTOCOL_ERROR;
                        return;
                }
            }
            else
            {
                headersContext.regularHeader = true;
            }
        }
    }

    private void connectionHeaders(DirectBuffer name, DirectBuffer value)
    {

        if (!headersContext.error() && name.equals(HpackContext.CONNECTION))
        {
            headersContext.streamError = Http2ErrorCode.PROTOCOL_ERROR;
        }
    }

    private void contentLengthHeader(DirectBuffer name, DirectBuffer value)
    {
        if (!headersContext.error() && name.equals(decodeContext.nameBuffer(28)))
        {
            String contentLength = value.getStringWithoutLengthUtf8(0, value.capacity());
            headersContext.contentLength = Long.parseLong(contentLength);
        }
    }

    // 8.1.2.2 TE header MUST NOT contain any value other than "trailers".
    private void teHeader(DirectBuffer name, DirectBuffer value)
    {
        if (!headersContext.error() && name.equals(TE) && !value.equals(TRAILERS))
        {
            headersContext.streamError = Http2ErrorCode.PROTOCOL_ERROR;
        }
    }

    private void uppercaseHeaders(DirectBuffer name, DirectBuffer value)
    {
        if (!headersContext.error())
        {
            for(int i=0; i < name.capacity(); i++)
            {
                if (name.getByte(i) >= 'A' && name.getByte(i) <= 'Z')
                {
                    headersContext.streamError = Http2ErrorCode.PROTOCOL_ERROR;
                }
            }
        }
    }

    // Collect headers into map to resolve target
    // TODO avoid this
    private void collectHeaders(DirectBuffer name, DirectBuffer value)
    {
        if (!headersContext.error())
        {
            String nameStr = name.getStringWithoutLengthUtf8(0, name.capacity());
            String valueStr = value.getStringWithoutLengthUtf8(0, value.capacity());
            headersContext.headers.put(nameStr, valueStr);
        }
    }

    // Writes HPACK header field to http representation in a buffer
    private void mapToHttp(DirectBuffer name, DirectBuffer value)
    {
        if (!headersContext.error())
        {
            factory.httpBeginExRW.headers(b -> b.item(item -> item.representation((byte) 0)
                                                          .name(name, 0, name.capacity())
                                                          .value(value, 0, value.capacity())));
        }
    }

    private void decodeHeaderField(HpackHeaderFieldFW hf,
                                   BiConsumer<DirectBuffer, DirectBuffer> nameValue)
    {
        int index;
        DirectBuffer name = null;
        DirectBuffer value = null;

        switch (hf.type())
        {
            case INDEXED :
                index = hf.index();
                if (!decodeContext.valid(index))
                {
                    headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                    return;
                }
                name = decodeContext.nameBuffer(index);
                value = decodeContext.valueBuffer(index);
                nameValue.accept(name, value);
                break;

            case LITERAL :
                HpackLiteralHeaderFieldFW literalRO = hf.literal();
                if (literalRO.error())
                {
                    headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                    return;
                }
                switch (literalRO.nameType())
                {
                    case INDEXED:
                    {
                        index = literalRO.nameIndex();
                        name = decodeContext.nameBuffer(index);

                        HpackStringFW valueRO = literalRO.valueLiteral();
                        value = valueRO.payload();
                        if (valueRO.huffman())
                        {
                            MutableDirectBuffer dst = new UnsafeBuffer(new byte[4096]); // TODO
                            int length = HpackHuffman.decode(value, dst);
                            if (length == -1)
                            {
                                headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                                return;
                            }
                            value = new UnsafeBuffer(dst, 0, length);
                        }
                        nameValue.accept(name, value);
                    }
                    break;
                    case NEW:
                    {
                        HpackStringFW nameRO = literalRO.nameLiteral();
                        name = nameRO.payload();
                        if (nameRO.huffman())
                        {
                            MutableDirectBuffer dst = new UnsafeBuffer(new byte[4096]); // TODO
                            int length = HpackHuffman.decode(name, dst);
                            if (length == -1)
                            {
                                headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                                return;
                            }
                            name = new UnsafeBuffer(dst, 0, length);
                        }

                        HpackStringFW valueRO = literalRO.valueLiteral();
                        value = valueRO.payload();
                        if (valueRO.huffman())
                        {
                            MutableDirectBuffer dst = new UnsafeBuffer(new byte[4096]); // TODO
                            int length = HpackHuffman.decode(value, dst);
                            if (length == -1)
                            {
                                headersContext.connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                                return;
                            }
                            value = new UnsafeBuffer(dst, 0, length);
                        }
                        nameValue.accept(name, value);
                    }
                    break;
                }
                if (literalRO.literalType() == INCREMENTAL_INDEXING)
                {
                    // make a copy for name and value as they go into dynamic table (outlives current frame)
                    MutableDirectBuffer nameCopy = new UnsafeBuffer(new byte[name.capacity()]);
                    nameCopy.putBytes(0, name, 0, name.capacity());
                    MutableDirectBuffer valueCopy = new UnsafeBuffer(new byte[value.capacity()]);
                    valueCopy.putBytes(0, value, 0, value.capacity());
                    decodeContext.add(nameCopy, valueCopy);
                }
                break;
        }
    }


    void mapPushPromize(ListFW<HttpHeaderFW> httpHeaders, HpackHeaderBlockFW.Builder builder)
    {
        httpHeaders.forEach(h -> builder.header(b -> mapHeader(h, b)));
    }

    void mapHeaders(ListFW<HttpHeaderFW> httpHeaders, HpackHeaderBlockFW.Builder builder)
    {
        encodeHeadersContext.reset();

        httpHeaders.forEach(this::status);
        if (!encodeHeadersContext.status)
        {
            builder.header(b -> b.indexed(8));          // no mandatory :status header, add :status: 200
        }

        httpHeaders.forEach(h ->
        {
            if (validHeader(h))
            {
                builder.header(b -> mapHeader(h, b));
            }
        });
    }

    void status(HttpHeaderFW httpHeader)
    {
        if (!encodeHeadersContext.status)
        {
            StringFW name = httpHeader.name();
            StringFW value = httpHeader.value();
            factory.nameRO.wrap(name.buffer(), name.offset() + 1, name.sizeof() - 1); // +1, -1 for length-prefixed buffer
            factory.valueRO.wrap(value.buffer(), value.offset() + 1, value.sizeof() - 1);

            if (factory.nameRO.equals(encodeContext.nameBuffer(8)))
            {
                encodeHeadersContext.status = true;
            }
        }
    }

    boolean validHeader(HttpHeaderFW httpHeader)
    {
        StringFW name = httpHeader.name();
        StringFW value = httpHeader.value();
        factory.nameRO.wrap(name.buffer(), name.offset() + 1, name.sizeof() - 1); // +1, -1 for length-prefixed buffer
        factory.valueRO.wrap(value.buffer(), value.offset() + 1, value.sizeof() - 1);

        if (factory.nameRO.equals(encodeContext.nameBuffer(1)) ||
                factory.nameRO.equals(encodeContext.nameBuffer(2)) ||
                factory.nameRO.equals(encodeContext.nameBuffer(4)) ||
                factory.nameRO.equals(encodeContext.nameBuffer(6)))
        {
            return false;                             // ignore :authority, :method, :path, :scheme
        }

        return true;
    }

    // Map http1.1 header to http2 header field in HEADERS, PUSH_PROMISE request
    private void mapHeader(HttpHeaderFW httpHeader, HpackHeaderFieldFW.Builder builder)
    {
        StringFW name = httpHeader.name();
        StringFW value = httpHeader.value();
        factory.nameRO.wrap(name.buffer(), name.offset() + 1, name.sizeof() - 1); // +1, -1 for length-prefixed buffer
        factory.valueRO.wrap(value.buffer(), value.offset() + 1, value.sizeof() - 1);

        int index = encodeContext.index(factory.nameRO, factory.valueRO);
        if (index != -1)
        {
            // Indexed
            builder.indexed(index);
        }
        else
        {
            // Literal
            builder.literal(literalBuilder -> buildLiteral(literalBuilder, encodeContext));
        }
    }

    // Building Literal representation of header field
    // TODO dynamic table, huffman, never indexed
    private void buildLiteral(
            HpackLiteralHeaderFieldFW.Builder builder,
            HpackContext hpackContext)
    {
        int nameIndex = hpackContext.index(factory.nameRO);
        builder.type(WITHOUT_INDEXING);
        if (nameIndex != -1)
        {
            builder.name(nameIndex);
        }
        else
        {
            builder.name(factory.nameRO, 0, factory.nameRO.capacity());
        }
        builder.value(factory.valueRO, 0, factory.valueRO.capacity());
    }

    enum State
    {
        IDLE,
        RESERVED_LOCAL,
        RESERVED_REMOTE,
        OPEN,
        HALF_CLOSED_LOCAL,
        HALF_CLOSED_REMOTE,
        CLOSED
    }

    private static final class HeadersContext
    {
        Http2ErrorCode connectionError;
        Map<String, String> headers = new HashMap<>();
        int method;
        int scheme;
        int path;
        boolean regularHeader;
        Http2ErrorCode streamError;
        long contentLength = -1;

        void reset()
        {
            connectionError = null;
            headers.clear();
            method = 0;
            scheme = 0;
            path = 0;
            regularHeader = false;
            streamError = null;
            contentLength = -1;
        }

        boolean error()
        {
            return streamError != null || connectionError != null;
        }
    }

    private static final class EncodeHeadersContext
    {
        boolean status;

        void reset()
        {
            status = false;
        }

    }

    @FunctionalInterface
    private interface DecoderState
    {
        int decode(DirectBuffer buffer, int offset, int length);
    }

}
