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
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http2.internal.types.stream.AckFW;
import org.reaktivity.nukleus.http2.internal.types.stream.Http2ErrorCode;
import org.reaktivity.nukleus.http2.internal.types.stream.TransferFW;

import java.io.Closeable;
import java.util.Deque;
import java.util.LinkedList;

class Http2Stream implements Closeable
{
    private static final int FIN = 0x01;
    private static final int RST = 0x02;

    final Http2Connection connection;
    final int http2StreamId;
    final long targetId;
    final long correlationId;
    private final MessageConsumer applicationTarget;
    Http2Connection.State state;
    long http2OutWindow;
    long http2InWindow;

    long contentLength;
    long totalData;

    Deque<WriteScheduler.Entry> replyQueue = new LinkedList<>();
    boolean endStream;

    long totalOutData;
    private ServerStreamFactory factory;

    MessageConsumer applicationReplyThrottle;
    long applicationReplyId;

    int responseBytes;
    int ackedResponseBytes;
    boolean completeResponseReceived;
    boolean sentApplicationReplyAck;
    boolean recvApplicationAck;

    boolean requestTransferRst;

    boolean closed;
    private boolean pendingApplicationReplyAckRst;
    private boolean networkTransferRst;
    private boolean networkTransferFin;
    private boolean networkReplyAckFin;
    private boolean networkReplyAckRst;

    Http2Stream(ServerStreamFactory factory, Http2Connection connection, int http2StreamId, Http2Connection.State state,
                MessageConsumer applicationTarget, HttpWriter httpWriter)
    {
        this.factory = factory;
        this.connection = connection;
        this.http2StreamId = http2StreamId;
        this.targetId = factory.supplyStreamId.getAsLong();
        this.correlationId = factory.supplyCorrelationId.getAsLong();
        this.http2InWindow = connection.localSettings.initialWindowSize;
        this.applicationTarget = applicationTarget;

        this.http2OutWindow = connection.remoteSettings.initialWindowSize;
        this.state = state;
    }

    boolean isClientInitiated()
    {
        return http2StreamId%2 == 1;
    }

    void onApplicationReplyFin()
    {
        connection.writeScheduler.dataEos(http2StreamId);
        completeResponseReceived = true;
        doApplicationReplyAckFin();
        if (recvApplicationAck && sentApplicationReplyAck)
        {
            connection.closeStream(this);
        }
    }

    void onApplicationReplyTransferRst()
    {
        // more request data to be sent, so send TRANSFER(RST)
        if (state != Http2Connection.State.HALF_CLOSED_REMOTE)
        {
            doApplicationTransferRst();
        }

        if (!(networkTransferRst || networkReplyAckRst))
        {
            connection.writeScheduler.rst(http2StreamId, Http2ErrorCode.CONNECT_ERROR);
        }

        doApplicationReplyAckRst();

        if (recvApplicationAck && sentApplicationReplyAck)
        {
            connection.closeStream(this);
        }
    }

    void onRstStream()
    {
        // more request data to be sent, so send TRANSFER(RST)
        if (state != Http2Connection.State.HALF_CLOSED_REMOTE)
        {
            doApplicationTransferRst();
        }
    }

    void doRstStream(Http2ErrorCode errorCode)
    {
        connection.writeScheduler.rst(http2StreamId, errorCode);
/*
        // Send ACK for buffered regions
        if (!replyQueue.isEmpty())
        {
            try {
                factory.ackRW.wrap(factory.writeBuffer, 0, factory.writeBuffer.capacity())
                             .streamId(applicationReplyId);
                replyQueue.stream().filter(e -> e instanceof WriteScheduler.DataEntry).forEach(e ->
                {
                    WriteScheduler.DataEntry de = (WriteScheduler.DataEntry) e;
                    ackedResponseBytes += de.length();
                    factory.ackRW.regionsItem(r -> r.address(de.regionAddress()).length(de.length()).streamId(de.streamId()));
                });
                AckFW ack = factory.ackRW.build();
                factory.doAck(applicationReplyThrottle, ack);

                replyQueue.clear();
            } catch (Exception e)
            {
                e.printStackTrace();
            }
        }
        */
    }

    void onRequestData()
    {
        boolean end = factory.http2DataRO.endStream();

        factory.transferRW.wrap(factory.writeBuffer, 0, factory.writeBuffer.capacity())
                          .streamId(targetId)
                          .flags(end ? FIN : 0);
        Http2Decoder.transferForData(connection.regionsRW.build(), factory.http2RO, factory.http2DataRO, factory.transferRW);
        TransferFW transfer = factory.transferRW.build();
        factory.doTransfer(applicationTarget, transfer);

        if (factory.http2DataRO.dataLength() > 0)
        {
            http2InWindow += factory.http2DataRO.dataLength();
            connection.http2InWindow += factory.http2DataRO.dataLength();

            // HTTP2 connection-level flow-control
            connection.writeScheduler.windowUpdate(0, factory.http2DataRO.dataLength());

            // HTTP2 stream-level flow-control
            connection.writeScheduler.windowUpdate(http2StreamId, factory.http2DataRO.dataLength());
        }
    }

    void onResponseData(TransferFW data)
    {
        data.regions().forEach(r -> responseBytes += r.length());
        connection.writeScheduler.data(http2StreamId, data);
    }

    void onNetworkTransferRst()
    {
        networkTransferRst = true;

        // more request data to be sent, so send TRANSFER(RST)
        if (state != Http2Connection.State.HALF_CLOSED_REMOTE)
        {
            doApplicationTransferRst();
        }

        // reset the response (if all the application reply regions are acked)
        doApplicationReplyAckRst();
        if (recvApplicationAck && sentApplicationReplyAck)
        {
            connection.closeStream(this);
        }
    }

    void onNetworkReplyAckFin()
    {
        networkReplyAckFin = true;

        // more request data to be sent, so send TRANSFER(RST)
        if (state != Http2Connection.State.HALF_CLOSED_REMOTE)
        {
            doApplicationTransferRst();
        }

        // reset the response (if all the application reply regions are acked)
        pendingApplicationReplyAckRst = doApplicationReplyAckFin();

        if (recvApplicationAck && sentApplicationReplyAck)
        {
            connection.closeStream(this);
        }
    }

    void onNetworkReplyAckRst()
    {
        networkReplyAckRst = true;

        // more request data to be sent, so send TRANSFER(RST)
        if (state != Http2Connection.State.HALF_CLOSED_REMOTE)
        {
            doApplicationTransferRst();
        }

        // reset the response (if all the application reply regions are acked)
        pendingApplicationReplyAckRst = doApplicationReplyAckRst();

        if (recvApplicationAck && sentApplicationReplyAck)
        {
            connection.closeStream(this);
        }
    }

    void onNetworkTransferFin()
    {
        networkTransferFin = true;

        // more request data to be sent, so send TRANSFER(RST)
        if (state != Http2Connection.State.HALF_CLOSED_REMOTE)
        {
            doApplicationTransferRst();
        }
        if (recvApplicationAck && sentApplicationReplyAck)
        {
            connection.closeStream(this);
        }
    }

    void onThrottle(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
            case AckFW.TYPE_ID:
                final AckFW ack = factory.ackRO.wrap(buffer, index, index + length);
                connection.onApplicationAck(ack);
                if ((ack.flags() & FIN) == FIN)
                {
                    onApplicationAckFin();
                }
                if ((ack.flags() & RST) == RST)
                {
                    onApplicationAckRst();
                }
                break;
            default:
                // ignore
                break;
        }
    }

    private void onApplicationAckFin()
    {
        recvApplicationAck = true;

        if (recvApplicationAck && sentApplicationReplyAck)
        {
            connection.closeStream(this);
        }

        connection.onApplicationAckFin();
    }

    private void onApplicationAckRst()
    {
        recvApplicationAck = true;

        if (!(networkTransferRst || networkReplyAckRst))
        {
            // send RST_STREAM on HTTP2 stream
            doRstStream(Http2ErrorCode.CONNECT_ERROR);
        }

        // reset the response (if all the application reply regions are acked)
        doApplicationReplyAckRst();

        if (recvApplicationAck && sentApplicationReplyAck)
        {
            connection.closeStream(this);
        }

        connection.onApplicationAckRst();
    }

    @Override
    public void close()
    {
        if (!closed)
        {
            closed = true;
        }
    }

    void onNetworkReplyAck(int tflags, long address, int length, long streamId)
    {
        ackedResponseBytes += length;

        AckFW ack = factory.ackRW.wrap(factory.writeBuffer, 0, factory.writeBuffer.capacity())
                                 .streamId(applicationReplyId)
                                 .regionsItem(r -> r.address(address).length(length).streamId(streamId))
                                 .build();

        factory.doAck(applicationReplyThrottle, ack);

        if (pendingApplicationReplyAckRst)
        {
            doApplicationReplyAckRst();
        }
        else
        {
            doApplicationReplyAckFin();
        }
    }

    private boolean doApplicationReplyAckFin()
    {
        if (!sentApplicationReplyAck && completeResponseReceived && responseBytes == ackedResponseBytes)
        {
            sentApplicationReplyAck = true;
            AckFW ack = factory.ackRW.wrap(factory.writeBuffer, 0, factory.writeBuffer.capacity())
                                     .streamId(applicationReplyId)
                                     .flags(FIN)
                                     .build();

            factory.doAck(applicationReplyThrottle, ack);
            return true;
        }

        return false;
    }

    private boolean doApplicationReplyAckRst()
    {
        int buffered[] = new int[1];
        replyQueue.stream()
                  .filter(e -> e instanceof WriteScheduler.DataEntry)
                  .forEach(e -> buffered[0] += ((WriteScheduler.DataEntry) e).length());

        if (!sentApplicationReplyAck && applicationReplyThrottle != null && responseBytes == buffered[0] + ackedResponseBytes)
        {
            sentApplicationReplyAck = true;
            factory.ackRW.wrap(factory.writeBuffer, 0, factory.writeBuffer.capacity())
                                     .streamId(applicationReplyId)
                                     .flags(RST);

            replyQueue.stream().filter(e -> e instanceof WriteScheduler.DataEntry).forEach(e ->
            {
                WriteScheduler.DataEntry de = (WriteScheduler.DataEntry) e;
                ackedResponseBytes += de.length();
                factory.ackRW.regionsItem(r -> r.address(de.regionAddress()).length(de.length()).streamId(de.streamId()));
            });
            AckFW ack = factory.ackRW.build();

            factory.doAck(applicationReplyThrottle, ack);
            replyQueue.clear();

            return true;
        }

        return false;
    }

    private void doApplicationTransferRst()
    {
        if (!requestTransferRst)
        {
            requestTransferRst = true;
            TransferFW transfer = factory.transferRW.wrap(factory.writeBuffer, 0, factory.writeBuffer.capacity())
                                                    .streamId(targetId)
                                                    .flags(RST)
                                                    .build();

            factory.doTransfer(applicationTarget, transfer);
        }
    }

}
