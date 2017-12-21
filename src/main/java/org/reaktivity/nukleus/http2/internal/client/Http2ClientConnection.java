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

import static org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW.LiteralType.WITHOUT_INDEXING;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http2.internal.Http2ConnectionState;
import org.reaktivity.nukleus.http2.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http2.internal.types.ListFW;
import org.reaktivity.nukleus.http2.internal.types.OctetsFW;
import org.reaktivity.nukleus.http2.internal.types.String16FW;
import org.reaktivity.nukleus.http2.internal.types.StringFW;
import org.reaktivity.nukleus.http2.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackContext;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderBlockFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.types.stream.HpackLiteralHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2Flags;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2FrameFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2SettingsId;
import org.reaktivity.nukleus.http2.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http2.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http2.internal.types.stream.WindowFW;

class Http2ClientConnection
{
    long nukleusStreamId;

    Http2Settings localSettings = new Http2Settings(100, 0);
    Http2Settings remoteSettings = new Http2Settings(100, Http2Settings.DEFAULT_INITIAL_WINDOW_SIZE);

    String acceptReplyName;
    MessageConsumer acceptReply;


    int maxClientStreamId = -1;
    int maxServerStreamId = -1;
    int noClientStreams;
    int noServerStreams;

    int inHttp2ConnectionWindow; // connection window for reading from http2
                                 // (connection window for writer is kept in Http2Writer)

    private ClientStreamFactory factory;
    private Http2Writer http2Writer;
    boolean prefaceSent = false;
    boolean initConnectionFinished = false; // true when settings ack is sent
    boolean connectionError = false; // will become true when the connection will be closed by an error

    // headers stuff
    private static final Map<String, String> EMPTY_HEADERS = Collections.emptyMap();
    private final HpackContext encodeHpackContext;
    private final DirectBuffer nameRO = new UnsafeBuffer(new byte[0]);
    private final DirectBuffer valueRO = new UnsafeBuffer(new byte[0]);
    private final Http2HeadersDecoder http2HeaderDecoder;
    private final List<String> connectionHeaders = new ArrayList<>();


    final Int2ObjectHashMap<Http2ClientStream> http2Streams = new Int2ObjectHashMap<>();      // HTTP2 stream-id --> Http2Stream

    // accept reply stream-id --> Http2Stream
    final Long2ObjectHashMap<Http2ClientStream> http2StreamsByAcceptReplyIds = new Long2ObjectHashMap<>();

    Http2ClientConnection(ClientStreamFactory factory)
    {
        this.factory = factory;

        encodeHpackContext = new HpackContext(remoteSettings.headerTableSize, true);
        http2HeaderDecoder = new Http2HeadersDecoder(factory, localSettings.headerTableSize);
    }

    // returns a new stream id, if max number of streams is not reached
    int newStreamId()
    {
        if (connectionError || (noClientStreams + 1 > remoteSettings.maxConcurrentStreams))
        {
            return -1;
        }
        else
        {
            maxClientStreamId += 2;
            return maxClientStreamId;
        }
    }

    // will return false if the connection could not be initialized
    public boolean initConnection(BeginFW httpBegin)
    {
        if (prefaceSent)
        {
            return true;
        }

        Map<String, String> headers = extractHttpHeaders(httpBegin);
        RouteFW route = factory.resolveTarget(httpBegin.authorization(), httpBegin.sourceRef(), headers);
        if (route == null)
        {
            return false;
        }

        String connectName = route.target().asString();
        long connectRef = route.targetRef();
        MessageConsumer target = factory.router.supplyTarget(connectName);

        long correlationId = factory.supplyCorrelationId.getAsLong();
        nukleusStreamId = factory.supplyStreamId.getAsLong();

        // TODO not sure if using factory buffer for all connections is safe
        http2Writer = new Http2Writer(factory.writeBuffer, target, this, factory.bufferPool);

        acceptReplyName = httpBegin.source().asString();

        factory.correlations.put(correlationId, this);

        //initiate connect stream
        factory.doBegin(target, nukleusStreamId, connectRef, correlationId);
        factory.router.setThrottle(connectName, nukleusStreamId, this::handleConnectThrottle);

        // send settings
        http2Writer.doPreface();
        http2Writer.doSettings(localSettings.maxConcurrentStreams, localSettings.initialWindowSize);
        inHttp2ConnectionWindow = Http2Settings.DEFAULT_INITIAL_WINDOW_SIZE;
        http2Writer.flush();
        prefaceSent = true;
        return true;
    }

    Map<String, String> extractHttpHeaders(BeginFW begin)
    {
        final OctetsFW extension = begin.extension();

        Map<String, String> headers = EMPTY_HEADERS;
        if (extension.sizeof() > 0)
        {
            final HttpBeginExFW beginEx = extension.get(factory.httpBeginExRO::wrap);
            Map<String, String> headers0 = new LinkedHashMap<>();
            beginEx.headers().forEach(h -> headers0.put(h.name().asString(), h.value().asString()));
            headers = headers0;
        }
        return headers;
    }

    public void doHttp2StreamError(int http2StreamId, Http2ErrorCode errorCode)
    {
        http2Writer.doReset(http2StreamId, errorCode);
        http2Writer.flush();

        closeStream(http2StreamId, true);
    }

    public void closeStream(int http2StreamId, boolean withError)
    {
        Http2ClientStream stream = http2Streams.remove(http2StreamId);
        if (stream == null)
        {
            return;
        }

        if (acceptReply == null)
        { // probably we did not open the accept reply stream, do a reset on the accept
            factory.doReset(stream.acceptThrottle, stream.acceptStreamId);
        }
        else
        {
            if (withError)
            {
                // close accept reply stream
                factory.doAbort(acceptReply, stream.acceptReplyStreamId);
            }
            else
            {
                factory.doEnd(acceptReply, stream.acceptReplyStreamId);
            }
            http2StreamsByAcceptReplyIds.remove(stream.acceptReplyStreamId);
        }

        noClientStreams--;
    }

    public void doHttp2ConnectionError(Http2ErrorCode errorCode)
    {
        http2Writer.doGoaway(maxClientStreamId, errorCode);
        http2Writer.flush();
        connectionError = true;

        cleanupRequests();
        http2Writer.doCleanup();
    }

    // in case the http2 connection is closed, all accept streams must be closed
    //      and the connection removed from the connection manager
    public void cleanupRequests()
    {
        // close all requests
        for (Http2ClientStream stream : http2Streams.values())
        {
            closeStream(stream.http2StreamId, true);
        }

        factory.http2ConnectionManager.removeConnection(nukleusStreamId);
    }

    public void doHttp2Data(int http2StreamId, OctetsFW payload, boolean endStream)
    {
        Http2ClientStream stream = http2Streams.get(http2StreamId);
        if (stream == null)
        {
            return;
        }

        http2Writer.doData(http2StreamId, payload.buffer(), payload.offset(), payload.sizeof(), endStream);
        http2Writer.flush();
    }

    public void doPingAck(DirectBuffer payloadBuffer, int payloadOffset, int payloadLength)
    {
        http2Writer.doPingAck(payloadBuffer, payloadOffset, payloadLength);
    }

    void handleSettings(Http2FrameFW http2Frame)
    {
        if (http2Frame.streamId() != 0)
        {
            doHttp2ConnectionError(Http2ErrorCode.PROTOCOL_ERROR);
            return;
        }
        if (http2Frame.payloadLength()%6 != 0)
        {
            doHttp2ConnectionError(Http2ErrorCode.FRAME_SIZE_ERROR);
            return;
        }

        factory.settingsRO.wrap(http2Frame.buffer(), http2Frame.offset(), http2Frame.limit());

        if (factory.settingsRO.ack() && http2Frame.payloadLength() != 0)
        {
            doHttp2ConnectionError(Http2ErrorCode.FRAME_SIZE_ERROR);
            return;
        }
        if (!factory.settingsRO.ack())
        {
            factory.settingsRO.accept(this::decodeRemoteSetting);
            if (!connectionError)
            {
                http2Writer.doSettingsAck();
                http2Writer.flush();
            }
        }
    }

    private void decodeRemoteSetting(Http2SettingsId id, Long value)
    {
        switch (id)
        {
            case HEADER_TABLE_SIZE:
                remoteSettings.headerTableSize = value.intValue();
                break;
            case ENABLE_PUSH:
                if (!(value == 0L || value == 1L))
                {
                    doHttp2ConnectionError(Http2ErrorCode.PROTOCOL_ERROR);
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
                    doHttp2ConnectionError(Http2ErrorCode.FLOW_CONTROL_ERROR);
                    return;
                }
                int old = remoteSettings.initialWindowSize;
                remoteSettings.initialWindowSize = value.intValue();
                int update = remoteSettings.initialWindowSize - old;
                if (update == 0)
                {
                    return;
                }

                // 6.9.2. Initial Flow-Control Window Size
                // SETTINGS frame can alter the initial flow-control
                // window size for streams with active flow-control windows
                for(Http2ClientStream http2Stream: http2Streams.values())
                {
                    if (http2Stream.http2Window + update > Integer.MAX_VALUE)
                    {
                        // 6.9.2. Initial Flow-Control Window Size
                        // An endpoint MUST treat a change to SETTINGS_INITIAL_WINDOW_SIZE that
                        // causes any flow-control window to exceed the maximum size as a
                        // connection error of type FLOW_CONTROL_ERROR.
                        doHttp2ConnectionError(Http2ErrorCode.FLOW_CONTROL_ERROR);
                        return;
                    }
                    http2Stream.http2Window += update;
                    if (!http2Stream.initialAcceptWindowSent)
                    {
                        updateAcceptStreamWindow(http2Stream.http2StreamId, http2Stream.http2Window);
                        http2Stream.initialAcceptWindowSent = true;
                    }
                }
                break;
            case MAX_FRAME_SIZE:
                if (value < Math.pow(2, 14) || value > Math.pow(2, 24) -1)
                {
                    doHttp2ConnectionError(Http2ErrorCode.PROTOCOL_ERROR);
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

    // opens a new stream by sending headers
    public void sendHttp2Headers(int http2StreamId, BeginFW httpBegin, MessageConsumer acceptThrottle)
    {
        HttpBeginExFW beginEx = httpBegin.extension().get(factory.httpBeginExRO::wrap);
        byte[] flags = new byte[1];
        flags[0] = Http2Flags.NONE;
        beginEx.headers().forEach(h ->
        {
         // +1, -1 for length-prefixed buffer
            factory.nameRO.wrap(h.name().buffer(), h.name().offset() + 1, h.name().sizeof() - 1);
            factory.valueRO.wrap(h.value().buffer(), h.value().offset() + 2, h.value().sizeof() - 2);

            if (factory.nameRO.equals(encodeHpackContext.nameBuffer(2)) &&
                    factory.valueRO.equals(encodeHpackContext.valueBuffer(2)))
            { // in case of a GET, we send also END_STREAM
                flags[0] = Http2Flags.END_STREAM;
            }
            if (factory.nameRO.equals(encodeHpackContext.nameBuffer(28)) &&
                    "0".equals(h.value().asString()))
            { // in case of content length zero, we can also add END_STREAM
                flags[0] = Http2Flags.END_STREAM;
            }
        });

        http2Writer.doHeaders(http2StreamId, flags[0], beginEx.headers(), this::mapHttpHeadersToHttp2);
        http2Writer.flush();

        // also create http2 stream to keep stream specific data
        Http2ClientStream stream = new Http2ClientStream(http2StreamId,
                                localSettings.initialWindowSize,
                                remoteSettings.initialWindowSize,
                                Http2ConnectionState.OPEN,
                                httpBegin.correlationId(),
                                acceptThrottle,
                                httpBegin.streamId(),
                                factory.bufferPool);
        if (flags[0] > 0)
        { // in case the headers have the END_STREAM flag
            stream.endSent = true;
        }
        http2Streams.put(http2StreamId, stream);

        noClientStreams++;
    }

    void mapHttpHeadersToHttp2(ListFW<HttpHeaderFW> httpHeaders, HpackHeaderBlockFW.Builder builder)
    {
        connectionHeaders.clear();
        httpHeaders.forEach(this::checkConnectionHeader);
        httpHeaders.forEach(h ->
        {
            if (validHeader(h))
            {
                builder.header(b -> mapOneHttpHeaderToHttp2(h, b));
            }
        });
    }

    private void checkConnectionHeader(HttpHeaderFW httpHeader)
    {
        StringFW name = httpHeader.name();
        String16FW value = httpHeader.value();
        factory.nameRO.wrap(name.buffer(), name.offset() + 1, name.sizeof() - 1); // +1, -1 for length-prefixed buffer

        if (factory.nameRO.equals(HpackContext.CONNECTION))
        {
            String[] headers = value.asString().split(",");
            for (String header : headers)
            {
                connectionHeaders.add(header.trim());
            }
        }
    }

    private boolean validHeader(HttpHeaderFW httpHeader)
    {
        StringFW name = httpHeader.name();
        String16FW value = httpHeader.value();
        factory.nameRO.wrap(name.buffer(), name.offset() + 1, name.sizeof() - 1); // +1, -1 for length-prefixed buffer
        factory.valueRO.wrap(value.buffer(), value.offset() + 2, value.sizeof() - 2);

        // Removing 8.1.2.2 connection-specific header fields from response
        if (factory.nameRO.equals(encodeHpackContext.nameBuffer(57)) || // transfer-encoding
                factory.nameRO.equals(HpackContext.CONNECTION) ||
                factory.nameRO.equals(HpackContext.KEEP_ALIVE) ||
                factory.nameRO.equals(HpackContext.PROXY_CONNECTION) ||
                factory.nameRO.equals(HpackContext.UPGRADE))
        {
            return false;
        }

        // Removing any header that is nominated by Connection header field
        String nameStr = name.asString();
        for(String connectionHeader: connectionHeaders)
        {
            if (connectionHeader.equals(nameStr))
            {
                return false;
            }
        }

        return true;
    }

    private void mapOneHttpHeaderToHttp2(HttpHeaderFW httpHeader, HpackHeaderFieldFW.Builder builder)
    {
        StringFW name = httpHeader.name();
        String16FW value = httpHeader.value();
        nameRO.wrap(name.buffer(), name.offset() + 1, name.sizeof() - 1); // +1, -1 for length-prefixed buffer
        valueRO.wrap(value.buffer(), value.offset() + 2, value.sizeof() - 2);

        int index = encodeHpackContext.index(nameRO, valueRO);
        if (index != -1)
        {
            // Indexed
            builder.indexed(index);
        }
        else
        {
            // Literal
            builder.literal(literalBuilder -> buildLiteral(literalBuilder, encodeHpackContext));
        }
    }

    // Building Literal representation of header field
    // TODO dynamic table, huffman, never indexed
    private void buildLiteral(
            HpackLiteralHeaderFieldFW.Builder builder,
            HpackContext hpackContext)
    {
        int nameIndex = hpackContext.index(nameRO);
        builder.type(WITHOUT_INDEXING);
        if (nameIndex != -1)
        {
            builder.name(nameIndex);
        }
        else
        {
            builder.name(nameRO, 0, nameRO.capacity());
        }
        builder.value(valueRO, 0, valueRO.capacity());
    }

    // resets a stream (usually because the client aborted its connection)
    public void resetStream(int http2StreamId)
    {
        http2Writer.doReset(http2StreamId, Http2ErrorCode.PROTOCOL_ERROR);
        http2Writer.flush();

        closeStream(http2StreamId, true);
    }

    // closes a stream (usually because the client ended its connection)
    public void endHttp2ConnectStream(int http2StreamId)
    {
        Http2ClientStream clientStream = http2Streams.get(http2StreamId);
        if (clientStream.endSent)
        {
            return;
        }

        http2Writer.doDataEos(http2StreamId);
        http2Writer.flush();
        clientStream.endSent = true;
}

    /**
     * Decodes a full http2 header and transforms it in a list of http headers.
     * In case of error, null is returned and a connection or stream error is sent to the server
     * @param blockRO - the block of http2 headers
     * @param streamId - id of the http2 stream
     * @return the list of http headers in a HttpBeginExFW.
     */
    public HttpBeginExFW decodeHttp2Headers(HpackHeaderBlockFW blockRO, int streamId)
    {
        HttpBeginExFW httpBeginEx = http2HeaderDecoder.decodeHeaders(blockRO);
        if (http2HeaderDecoder.hasError())
        {
            if (http2HeaderDecoder.connectionError != null)
            {
                doHttp2ConnectionError(http2HeaderDecoder.connectionError);
            }
            else if (http2HeaderDecoder.streamError != null)
            {
                doHttp2StreamError(streamId, http2HeaderDecoder.streamError);
            }
            http2Writer.flush();
            return null;
        }

        Http2ClientStream stream = http2Streams.get(streamId);
        if (stream == null)
        {
            return null;
        }
        stream.contentLength = http2HeaderDecoder.contentLength;
        return httpBeginEx;
    }

    private void handleConnectThrottle(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
            case WindowFW.TYPE_ID:
                factory.windowRO.wrap(buffer, index, index + length);
                int update = factory.windowRO.credit();
                int padding = factory.windowRO.padding();

                http2Writer.updateNukleusWindow(update, padding);
                break;
            case ResetFW.TYPE_ID:
                factory.resetRO.wrap(buffer, index, index + length);
                cleanupRequests();
                http2Writer.doCleanup();
                break;
            default:
                // ignore
                break;
        }
    }

    // handles http2 window update on the connect reply stream by sending a window frame on accept stream
    public void updateAcceptStreamWindow(int http2StreamId, int windowUpdateSize)
    {
        if (windowUpdateSize <= 0)
        {
            return;
        }

        Http2ClientStream clientStream = http2Streams.get(http2StreamId);
        if (clientStream == null)
        {
            return;
        }

        clientStream.http2Window += windowUpdateSize;

        factory.doWindow(clientStream.acceptThrottle, clientStream.acceptStreamId, windowUpdateSize, 0);
    }

    public void handleAcceptReplyThrottle(Http2ClientStream clientStream, int update, int padding)
    {
        clientStream.httpWindow += update;

        http2Writer.doWindowUpdate(0, update);
        http2Writer.doWindowUpdate(clientStream.http2StreamId, update);
        http2Writer.flush();
    }

    @Override
    public String toString()
    {
        return String.format("Http2ClientConnection[streamId=%s activeStreams=%s]",
                nukleusStreamId, noClientStreams);
    }
}
