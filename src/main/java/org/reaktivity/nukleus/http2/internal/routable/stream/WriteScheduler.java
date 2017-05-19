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

package org.reaktivity.nukleus.http2.internal.routable.stream;

import org.reaktivity.nukleus.http2.internal.types.Flyweight;

/*
 Strategy to send data to replyTarget/Source. When there is replyWindow available, there are many
 ways to send the data to replyTarget.

 For e.g some strategies are:
 * Select a HTTP2 stream (that has pending data to be sent) sequentially in available streams.
 * Select HTTP2 streams (that has pending data to be sent) based on the priority
 * Select a HTTP2 stream (that has pending data to be sent) in a round-robin way in available streams.
 ...
 */
interface WriteScheduler
{
    void doHttp2(int length, int http2StreamId, Flyweight.Builder.Visitor visitor);

    void doEnd();

    void flush(int windowUpdate);

}
