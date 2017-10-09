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
import org.reaktivity.nukleus.http2.internal.types.stream.BeginFW;

class Http2ClientConnectionManager
{
    private Long2ObjectHashMap<Http2ClientConnection> http2Connections = new Long2ObjectHashMap<>();

    private final ClientStreamFactory factory;

    Http2ClientConnectionManager(ClientStreamFactory factory)
    {
        this.factory = factory;
    }

    // returns an available http2 stream
    // if there is no connection available, one is created and initiated (pre and settings sent to server)
    public Http2ClientStreamId newStream(BeginFW httpBegin)
    {
        for (Http2ClientConnection connection:http2Connections.values())
        {
            int streamId = connection.newStreamId();
            if (streamId != -1)
            {
                return new Http2ClientStreamId(connection.nukleusStreamId, streamId);
            }
        }

        // there is no stream available in existing connections, create a new connection
        Http2ClientConnection connection = new Http2ClientConnection(factory);
        connection.initConnection(httpBegin);

        http2Connections.put(connection.nukleusStreamId, connection);
        int streamId =  connection.newStreamId();

        //init http2 stream
        connection.sendHttp2Headers(streamId, httpBegin);

        return new Http2ClientStreamId(connection.nukleusStreamId, streamId);
    }

    public Http2ClientConnection getConnection(long nukleusStreamId)
    {
        return http2Connections.get(nukleusStreamId);
    }
}
