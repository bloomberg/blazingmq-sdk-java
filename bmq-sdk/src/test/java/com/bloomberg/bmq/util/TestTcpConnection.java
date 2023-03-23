/*
 * Copyright 2022 Bloomberg Finance L.P.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bloomberg.bmq.util;

import com.bloomberg.bmq.impl.infr.net.ConnectionOptions;
import com.bloomberg.bmq.impl.infr.net.intf.ChannelStatusHandler;
import com.bloomberg.bmq.impl.infr.net.intf.ChannelStatusHandler.ChannelStatus;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.ReadCallback.ReadCompletionStatus;
import com.bloomberg.bmq.impl.infr.util.Argument;
import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TestTcpConnection implements TcpConnection {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private ChannelStatusHandler channelStatusHandler;
    private ReadCallback readCallback;
    private ReadCompletionStatus readBytesStatus;
    private ConnectCallback connectCallback;
    private DisconnectCallback disconnectCallback;

    private static final int WAIT_TO_WRITE_TIMEOUT = 10;

    private final Semaphore writeSemaphore = new Semaphore(0);
    private WriteStatus writeStatus = WriteStatus.SUCCESS;

    private LinkedBlockingQueue<ByteBuffer[]> clientRequests =
            new LinkedBlockingQueue<ByteBuffer[]>();

    public static TestTcpConnection createInstance() {
        return new TestTcpConnection();
    }

    private TestTcpConnection() {}

    @Override
    public int connect(
            ConnectionOptions options,
            ConnectCallback connectCb,
            ReadCallback readCb,
            int initialMinNumBytes) {
        logger.info("Called connect");

        if (channelStatusHandler == null) {
            throw new IllegalStateException("Channel status handler not set");
        }

        Argument.expectPositive(initialMinNumBytes, "initialMinNumBytes");
        Argument.expectNonNull(options, "options");
        Argument.expectNonNull(options.brokerUri().getHost(), "host");
        Argument.expectPositive(options.brokerUri().getPort(), "port");

        connectCallback = Argument.expectNonNull(connectCb, "connectCb");
        readCallback = Argument.expectNonNull(readCb, "readCb");
        readBytesStatus = new ReadCompletionStatus();
        readBytesStatus.setNumNeeded(initialMinNumBytes);

        // Although we have 'connectCallback' sending just
        // ConnectStatus.SUCCESS won't affect the session FSM.
        // It waits for ChannelStatus.CHANNEL_UP event to proceed
        // to the negotiation phase.
        // TODO: maybe we don't need ConnectCallback at all?

        connectCallback.handleConnectCb(ConnectStatus.SUCCESS); // useless
        channelStatusHandler.handleChannelStatus(ChannelStatus.CHANNEL_UP);

        return 0;
    }

    @Override
    public int disconnect(DisconnectCallback disconnectCb) {
        disconnectCallback = Argument.expectNonNull(disconnectCb, "disconnectCb");
        disconnectCallback.handleDisconnectCb(DisconnectStatus.SUCCESS);
        return 0;
    }

    @Override
    public int linger() {
        return 0;
    }

    @Override
    public boolean isConnected() {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void setChannelStatusHandler(ChannelStatusHandler handler) {
        logger.info("Called setChannelStatusHandler");
        channelStatusHandler = handler;
    }

    @Override
    public WriteStatus write(ByteBuffer[] data) {
        logger.info("Called write");
        try {
            clientRequests.put(data);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Unsupported interruption.");
        }
        return writeStatus;
    }

    @Override
    public boolean isWritable() {
        boolean isWritable = writeStatus == WriteStatus.SUCCESS;
        logger.info("Channel is writable: {}", isWritable);

        return isWritable;
    }

    @Override
    public void waitUntilWritable() {
        logger.info("Wait for channel to become writable");

        TestHelpers.acquireSema(writeSemaphore, WAIT_TO_WRITE_TIMEOUT);
    }

    @Override
    public InetSocketAddress localAddress() {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public InetSocketAddress remoteAddress() {
        throw new IllegalStateException("Not implemented");
    }

    public ByteBuffer[] nextWriteRequest() {
        return nextWriteRequest(5);
    }

    public ByteBuffer[] nextWriteRequest(int sec) {
        Argument.expectPositive(sec, "sec");

        ByteBuffer[] res;
        try {
            res = clientRequests.poll(sec, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Unsupported interruption.");
        }
        return res;
    }

    public void sendResponse(ByteBuffer[] data) {
        readCallback.handleReadCb(readBytesStatus, Argument.expectNonNull(data, "data"));
    }

    public void setWriteStatus(WriteStatus writeStatus) {
        logger.info("Change status from {} to {}", this.writeStatus, writeStatus);

        this.writeStatus = writeStatus;
    }

    public void resetWriteStatus() {
        writeStatus = WriteStatus.SUCCESS;
        logger.info("Reset status to {}", writeStatus);
    }

    public void setWritable() {
        if (writeStatus != WriteStatus.WRITE_BUFFER_FULL) {
            throw new IllegalStateException("Write buffer must be full");
        }

        logger.info("Channel becomes writeable");

        writeStatus = WriteStatus.SUCCESS;
        writeSemaphore.release();
    }

    public void setChannelStatus(ChannelStatus status) {
        if (channelStatusHandler == null) {
            throw new IllegalStateException("Channel status handler not set");
        }
        channelStatusHandler.handleChannelStatus(status);
    }
}
