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

import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;

class Http2ClientConnectionManager
{
    private Long2ObjectHashMap<Http2ClientConnection> http2Connections = new Long2ObjectHashMap<>();

    private final ClientStreamFactory factory;

    Http2ClientConnectionManager(ClientStreamFactory factory)
    {
        this.factory = factory;
    }

    /**
     *  Allocates a new stream for this http request. If there is no connection available, one is created
     *  and initiated (preface and settings sent to server). Headers are sent on the new stream.
     * @param httpBegin The received http begin frame
     * @param acceptThrottle The accept throttle message consumer, used for sending window frames
     * @return an available http2 stream or null if a new connection could not be initialized
     */
    public Http2ClientStreamId newStream(BeginFW httpBegin, MessageConsumer acceptThrottle)
    {
        for (Http2ClientConnection connection:http2Connections.values())
        {
            int streamId = connection.newStreamId();
            if (streamId != -1)
            {
                //init http2 stream
                connection.sendHttp2Headers(streamId, httpBegin, acceptThrottle);

                return new Http2ClientStreamId(connection.nukleusStreamId, streamId);
            }
        }

        // there is no stream available in existing connections, create a new connection
        Http2ClientConnection connection = new Http2ClientConnection(factory);
        if (! connection.initConnection(httpBegin))
        { // if connection could not be created, reset
            factory.doReset(acceptThrottle, httpBegin.streamId());
            return null;
        }

        http2Connections.put(connection.nukleusStreamId, connection);
        int streamId =  connection.newStreamId();

        //init http2 stream
        connection.sendHttp2Headers(streamId, httpBegin, acceptThrottle);

        return new Http2ClientStreamId(connection.nukleusStreamId, streamId);
    }

    public Http2ClientConnection getConnection(long nukleusStreamId)
    {
        return http2Connections.get(nukleusStreamId);
    }

    public Http2ClientConnection removeConnection(long nukleusStreamId)
    {
        return http2Connections.remove(nukleusStreamId);
    }
}
