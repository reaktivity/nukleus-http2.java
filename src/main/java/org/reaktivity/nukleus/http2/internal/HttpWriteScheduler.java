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
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2DataFW;
import org.reaktivity.nukleus.http2.internal.types.stream.RegionFW;

class HttpWriteScheduler
{
    private final HttpWriter target;
    private final long targetId;
    private final MessageConsumer applicationTarget;

    private Http2Stream stream;
    private boolean end;
    private boolean endSent;
    private int applicationBudget;
    private int applicationPadding;

    private int totalRead;
    private int totalWritten;

    HttpWriteScheduler(MessageConsumer applicationTarget, HttpWriter target, long targetId,
                       Http2Stream stream)
    {
        this.applicationTarget = applicationTarget;
        this.target = target;
        this.targetId = targetId;
        this.stream = stream;
    }

    /*
     * @return true if the data is written or stored
     *         false if there are no slots or no space in the buffer
     */
    boolean onData(Http2DataFW http2DataRO, RegionFW region)
    {
//        totalRead += http2DataRO.dataLength();
//        end = http2DataRO.endStream();
//        //target.doHttpData(applicationTarget, targetId, applicationPadding, buffer, offset, length);
//
//        factory.doWrite();
//
//        // HTTP2 connection-level flow-control
//        stream.connection.writeScheduler.windowUpdate(0, (int) applicationCredit);
//
//        // HTTP2 stream-level flow-control
//        stream.connection.writeScheduler.windowUpdate(stream.http2StreamId, (int) applicationCredit);
//
//        // since there is no data is pending, we can send END frame
//        if (end && !endSent)
//        {
//            endSent = true;
//            factory.doEnd(applicationTarget, targetId);
//        }

        return true;
    }

    void onWindow(int credit, int padding, long groupId)
    {
        applicationBudget += credit;

    }

    void onReset()
    {

    }

    void doAbort()
    {
        //factory.doAbort(applicationTarget, targetId);
    }

}
