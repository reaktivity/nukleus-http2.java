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
package org.reaktivity.nukleus.http2.internal.types.stream;

import java.util.HashMap;
import java.util.Map;

public enum Http2FrameType
{
    DATA(0),
    HEADERS(1),
    PRIORITY(2),
    RST_STREAM(3),
    SETTINGS(4),
    PUSH_PROMISE(5),
    PING(6),
    GO_AWAY(7),
    WINDOW_UPDATE(8),
    CONTINUATION(9);

    private final byte type;

    Http2FrameType(int type)
    {
        this.type = (byte) type;
        Types.TYPES.put(this.type, this);
    }

    public byte getType()
    {
        return type;
    }

    private static class Types
    {
        private static final Map<Byte, Http2FrameType> TYPES = new HashMap<>();
    }

    public static Http2FrameType from(byte type)
    {
        return Types.TYPES.get(type);
    }
}
