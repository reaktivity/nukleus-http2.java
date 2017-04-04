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
package org.reaktivity.nukleus.http2.internal.routable;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.function.IntSupplier;

import org.reaktivity.nukleus.http2.internal.routable.stream.PushHandler;
import org.reaktivity.nukleus.http2.internal.router.RouteKind;

public class Correlation
{
    private final String source;
    private final PushHandler pushHandler;
    private final long id;
    private final int http2StreamId;
    private final IntSupplier promisedStreamIds;
    private final RouteKind established;
    private final long sourceOutputEstId;

    public Correlation(
        long id,
        long sourceOutputEstId,
        PushHandler pushHandler,
        int http2StreamId,
        IntSupplier promisedStreamIds,
        String source,
        RouteKind established)
    {
        this.id = id;
        this.sourceOutputEstId = sourceOutputEstId;
        this.pushHandler = pushHandler;
        this.http2StreamId = http2StreamId;
        this.promisedStreamIds = promisedStreamIds;
        this.source = requireNonNull(source, "source");
        this.established = requireNonNull(established, "established");
    }

    public String source()
    {
        return source;
    }

    public long id()
    {
        return id;
    }

    public long getSourceOutputEstId()
    {
        return sourceOutputEstId;
    }

    public int http2StreamId()
    {
        return http2StreamId;
    }

    public RouteKind established()
    {
        return established;
    }

    public PushHandler pushHandler()
    {
        return pushHandler;
    }

    public IntSupplier promisedStreamIds()
    {
        return promisedStreamIds;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, sourceOutputEstId, http2StreamId, source, established);
    }

    @Override
    public boolean equals(
        Object obj)
    {
        if (!(obj instanceof Correlation))
        {
            return false;
        }

        Correlation that = (Correlation) obj;
        return this.id == that.id &&
                this.sourceOutputEstId == that.sourceOutputEstId &&
                this.http2StreamId == that.http2StreamId &&
                this.established == that.established &&
                Objects.equals(this.source, that.source);
    }

    @Override
    public String toString()
    {
        return String.format("[id=%s, sourceOutputEstId=%s http2StreamId=%s source=\"%s\", established=%s]",
                id, sourceOutputEstId, http2StreamId, source, established);
    }
}
