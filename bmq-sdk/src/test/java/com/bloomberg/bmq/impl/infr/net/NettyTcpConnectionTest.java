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
package com.bloomberg.bmq.impl.infr.net;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.impl.infr.net.intf.ChannelStatusHandler;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.ConnectCallback;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.ConnectStatus;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.DisconnectCallback;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.DisconnectStatus;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.ReadCallback;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.WriteStatus;
import com.bloomberg.bmq.util.TestHelpers;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.embedded.EmbeddedChannel;
import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyTcpConnectionTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private class ConnectionEventHandler
            implements ChannelStatusHandler, ConnectCallback, DisconnectCallback, ReadCallback {
        private ArrayList<String> receivedPayloads;
        private ArrayList<ByteBuffer[]> receivedBuffers;
        private int[] nextMsgLens;

        public ConnectionEventHandler() {
            nextMsgLens = null;
            receivedPayloads = new ArrayList<>();
            receivedBuffers = new ArrayList<>();
        }

        @Override
        public void handleChannelStatus(ChannelStatus status) {
            logger.info("ChannelStatus: {}", status);
            switch (status) {
                case CHANNEL_UP:
                    NettyTcpConnectionTest.this.channelUpSema.release();
                    break;
                case CHANNEL_WRITABLE:
                    NettyTcpConnectionTest.this.channelWritableSema.release();
                    break;
                case CHANNEL_DOWN:
                    NettyTcpConnectionTest.this.channelDownSema.release();
                    break;
                default:
                    fail();
                    break;
            }
        }

        @Override
        public void handleConnectCb(ConnectStatus status) {
            logger.info("ConnectCallbackStatus: {}", status);
            NettyTcpConnectionTest.this.connectSema.release();
        }

        @Override
        public void handleDisconnectCb(DisconnectStatus status) {
            logger.info("DisconnectCbStatus: {}", status);
            NettyTcpConnectionTest.this.disconnectSema.release();
        }

        @Override
        public void handleReadCb(ReadCompletionStatus readStatus, ByteBuffer[] data) {
            String strData = byteBuffersToString(data);
            logger.info("Received data [{}]", strData);

            receivedPayloads.add(strData);
            receivedBuffers.add(data);
            if (nextMsgLens != null) {
                // More messages to be received.

                if (receivedPayloads.size() < nextMsgLens.length) {
                    readStatus.setNumNeeded(nextMsgLens[receivedPayloads.size() - 1]);
                } else {
                    readStatus.setNumNeeded(1); // zero ok?
                }
                NettyTcpConnectionTest.this.readSema.release();
            }
        }

        public void setNextReadMessageLengths(int[] nextMsgLens) {
            this.nextMsgLens = nextMsgLens;
        }

        public ArrayList<String> receivedPayloads() {
            return receivedPayloads;
        }

        public ArrayList<ByteBuffer[]> receivedBuffers() {
            return receivedBuffers;
        }
    }

    private String byteBuffersToString(ByteBuffer[] data) {
        StringBuilder sb = new StringBuilder();
        for (ByteBuffer b : data) {
            byte[] array = new byte[b.limit()];
            b.get(array);
            b.rewind();
            sb.append(new String(array, StandardCharsets.US_ASCII));
        }
        return sb.toString();
    }

    private static void acquireSema(Semaphore sema) {
        TestHelpers.acquireSema(sema, DEFAULT_CB_TIMEOUT);
    }

    private static void sleepForMillies(int millies) {
        try {
            Thread.sleep(millies);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final int MIN_NUM_READ_BYTES = 10;
    private static final int LOW_WATER_MARK = 5;
    private static final int HIGH_WATER_MARK = 20;
    private static final int START_TIMEOUT_MILLIES = 50;
    private static final int DEFAULT_CB_TIMEOUT = 8;
    private static final int LINGER_TIMEOUT_MILLIES = 1000;

    private Semaphore connectSema = new Semaphore(0);
    private Semaphore disconnectSema = new Semaphore(0);
    private Semaphore channelUpSema = new Semaphore(0);
    private Semaphore channelDownSema = new Semaphore(0);
    private Semaphore channelWritableSema = new Semaphore(0);
    private Semaphore writeSema = new Semaphore(0);
    private Semaphore readSema = new Semaphore(0);

    private EmbeddedChannel channel;
    private ConnectionEventHandler eventHandler = new ConnectionEventHandler();

    private int configCounter = 0;
    private int connectCounter = 0;
    private int resetCounter = 0;

    private ExecutorService IOExecutor;
    private Boolean isAsync = true;
    private AtomicBoolean isServerEnabled = new AtomicBoolean(true);
    private AtomicBoolean isServerReadEnabled = new AtomicBoolean(true);
    private ConnectionOptions connectionOption = new ConnectionOptions();

    public NettyTcpConnectionTest() {
        // Note that startRetryInterval must be smaller than 'DEFAULT_CB_TIMEOUT'
        // to gracefully handle the scenario where client is brought up before
        // the server.
        connectionOption.setStartRetryInterval(Duration.ofSeconds(2L));
        connectionOption.setStartAttemptTimeout(Duration.ofSeconds(10L));
        connectionOption.setStartNumRetries(5);
    }

    private NettyTcpConnection.ClientChannelAdapter adapter =
            new NettyTcpConnection.ClientChannelAdapter() {
                private NettyTcpConnection connection = null;

                private void submit(Runnable job) {
                    assertNotNull(job);
                    if (IOExecutor != null) {
                        IOExecutor.submit(job);
                    } else {
                        job.run();
                    }
                };

                @Override
                public void configure(NettyTcpConnection conn, EventLoopGroup loop) {
                    logger.debug("Test channel adapter: configuration");
                    connection = conn;
                    channel = new EmbeddedChannel(connection);
                    channel.config()
                            .setWriteBufferWaterMark(
                                    new WriteBufferWaterMark(
                                            connectionOption.writeBufferWaterMark().lowWaterMark(),
                                            connectionOption
                                                    .writeBufferWaterMark()
                                                    .highWaterMark()));
                    if (isAsync) {
                        IOExecutor = loop;
                    } else {
                        IOExecutor = null;
                    }
                    ++configCounter;
                }

                @Override
                public ChannelFuture connect() {
                    logger.debug("Test channel adapter: connection");
                    ++connectCounter;
                    ChannelPromise promise = channel.newPromise();
                    Runnable ioJob =
                            () -> {
                                logger.debug("Test channel adapter: connection in IO thread");
                                boolean res = false;
                                try {
                                    if (isServerEnabled.get()) {
                                        res =
                                                channel.connect(new InetSocketAddress(0))
                                                        .sync()
                                                        .isSuccess();
                                    }
                                } catch (Exception ex) {
                                    promise.setFailure(ex);
                                }
                                if (res) {
                                    promise.setSuccess();
                                } else {
                                    promise.setFailure(new Exception("Connect failed"));
                                }
                            };
                    submit(ioJob);
                    return promise;
                }

                @Override
                public void reset() {
                    logger.debug("Test channel adapter: reset");
                    connection = null;
                    channel = null;
                    ++resetCounter;
                }

                @Override
                public void write(ChannelHandlerContext ctx, ByteBuffer[] data) {
                    logger.debug("Test channel adapter: write");
                    Runnable ioJob =
                            () -> {
                                logger.debug("Test channel adapter: write in IO thread");
                                if (!isServerEnabled.get()) return;
                                ctx.writeAndFlush(data);
                                if (!isServerReadEnabled.get()) return;
                                ByteBuffer[] dataRecorded =
                                        ((EmbeddedChannel) ctx.channel()).readOutbound();
                                int length = 0;
                                for (ByteBuffer unit : dataRecorded) {
                                    length += unit.limit();
                                }
                                ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(length);
                                for (ByteBuffer unit : dataRecorded) {
                                    buf.writeBytes(unit);
                                }
                                // NOTE: it is simple simulation of echo server
                                Runnable serverJob =
                                        () -> {
                                            channel.writeInbound(buf);
                                        };
                                submit(serverJob);
                                NettyTcpConnectionTest.this.writeSema.release();
                            };
                    submit(ioJob);
                }
            };

    public void readWriteTest(Boolean isAsync) {
        // 1) Invoke 'connect' and ensure that it succeeds.
        // 2) Write 4 messages to the server.
        // 3) Ensure that same 4 messages are received.
        // 4) Invoke 'disconnect' and ensure that it succeeds.

        this.isAsync = isAsync;
        // Prepare list of messages which will be sent and echoed back.
        String[] messages =
                new String[] {
                    "ABCDEFGHIJ",
                    "1234567890987654321",
                    "abcdefghijklmnopqrstuvwxyz",
                    "Payload for NettyTcpConnection integration test"
                };

        int[] nextMsgLens = new int[messages.length];
        for (int i = 1; i < messages.length; ++i) {
            nextMsgLens[i - 1] = messages[i].length();
        }
        eventHandler.setNextReadMessageLengths(nextMsgLens);

        TcpConnection obj =
                NettyTcpConnection.createTestedInstance(adapter, LINGER_TIMEOUT_MILLIES);
        obj.setChannelStatusHandler(eventHandler);

        assertEquals(0, configCounter);
        assertEquals(0, connectCounter);
        assertEquals(0, resetCounter);
        // 1) Invoke 'connect' and ensure that it succeeds.
        int rc = obj.connect(connectionOption, eventHandler, eventHandler, MIN_NUM_READ_BYTES);

        assertEquals(0, rc);
        logger.info("Waiting for connection...");
        acquireSema(connectSema);
        assertTrue(obj.isConnected()); // Since server is running.
        acquireSema(channelUpSema);

        assertEquals(1, configCounter);
        assertEquals(1, connectCounter);
        assertEquals(1, resetCounter);

        // 2) Write 4 messages to the server.
        for (String message : messages) {
            ByteBuffer packet = ByteBuffer.wrap(message.getBytes(StandardCharsets.US_ASCII));
            ByteBuffer[] data = new ByteBuffer[] {packet};
            WriteStatus writeRc = obj.write(data);
            assertSame(WriteStatus.SUCCESS, writeRc);
            acquireSema(writeSema);
            acquireSema(readSema);
        }

        // 3) Ensure that same 4 messages are received.
        ArrayList<String> recvdPayloads = eventHandler.receivedPayloads();
        ArrayList<ByteBuffer[]> recvdBuffers = eventHandler.receivedBuffers();
        assertEquals(recvdPayloads.size(), messages.length);
        assertEquals(recvdBuffers.size(), messages.length);

        for (int i = 0; i < messages.length; ++i) {
            String expectedStr = messages[i];
            String copiedStr = recvdPayloads.get(i);
            String rawStr = byteBuffersToString(recvdBuffers.get(i));

            logger.info("Expected: {}", expectedStr);
            logger.info("Copied  : {}", copiedStr);
            logger.info("Raw     : {}", rawStr);

            assertArrayEquals("copied is expected", expectedStr.getBytes(), copiedStr.getBytes());
            assertArrayEquals("raw is expected", expectedStr.getBytes(), rawStr.getBytes());
        }

        // 4) Invoke 'disconnect' and ensure that it succeeds.
        logger.info("Disconnecting...");
        rc = obj.disconnect(eventHandler);
        assertEquals(0, rc);

        logger.info("Waiting for disconnection...");
        acquireSema(disconnectSema);
        assertFalse(obj.isConnected());
        acquireSema(channelDownSema);

        if (this.isAsync) {
            assertEquals(0, obj.linger());
        }
    }

    @Test
    public void testReadWriteSync() {
        logger.info("=======================================================");
        logger.info("Testing NettyTcpConnection read/write in sync mode.");
        logger.info("=======================================================");
        readWriteTest(false);
    }

    @Test
    public void testReadWriteAsync() {
        logger.info("=======================================================");
        logger.info("Testing NettyTcpConnection read/write in async mode.");
        logger.info("=======================================================");
        readWriteTest(true);
    }

    public void testConnectDisconnectNoServer(Boolean isAsync) {
        // 1) Invoke 'connect' without bringing up the server.
        // 2) Ensure that 'connect' fails, and no channel up event is fired.
        // 3) Invoke 'disconnect' and ensure that it fails and no channel down
        //    event is fired.
        this.isAsync = isAsync;
        isServerEnabled.set(false);

        TcpConnection obj =
                NettyTcpConnection.createTestedInstance(adapter, LINGER_TIMEOUT_MILLIES);

        obj.setChannelStatusHandler(eventHandler);

        // 1) Invoke 'connect' without bringing up the server.
        logger.info("Initiating connection...");
        final int NUM_RETRIES = 1;
        ConnectionOptions opts = new ConnectionOptions();
        opts.setStartNumRetries(NUM_RETRIES);
        int rc = obj.connect(opts, eventHandler, eventHandler, MIN_NUM_READ_BYTES);

        assertEquals(0, rc);
        logger.info("Waiting for connection...");
        long timeout =
                opts.startAttemptTimeout()
                        .plus(opts.startRetryInterval().multipliedBy(NUM_RETRIES))
                        .getSeconds();
        TestHelpers.acquireSema(connectSema, (int) timeout + 1);

        // 2) Ensure that 'connect' fails, and no channel up event is fired.
        assertFalse(obj.isConnected());

        // 3) Invoke 'disconnect' and ensure that it fails and no channel down
        //    event is fired.
        logger.info("Disconnecting...");

        rc = obj.disconnect(eventHandler);

        assertNotEquals(0, rc); // Since not connected
        assertFalse(obj.isConnected());
        assertEquals(0, channelDownSema.availablePermits());
        assertEquals(0, disconnectSema.availablePermits());

        logger.info("Already disconnected.");

        // No need to wait on 'disconnectSema' since we are not connected.
        if (isAsync) {
            assertEquals(0, obj.linger());
        }
    }

    @Test
    public void testConnectDisconnectNoServerAsync() {
        logger.info("============================================================");
        logger.info("Testing NettyTcpConnection without server in async mode.");
        logger.info("============================================================");
        testConnectDisconnectNoServer(true);
    }

    @Test
    public void testConnectDisconnectNoServerSync() {
        logger.info("===========================================================");
        logger.info("Testing NettyTcpConnection without server in sync mode.");
        logger.info("===========================================================");
        testConnectDisconnectNoServer(false);
    }

    public void testConnectDisconnectWithServer(Boolean isAsync) {
        // 1) Invoke 'connect'.  Ensure that it succeeds, and channel up event
        //    is fired.
        // 2) Invoke 'disconnect' and ensure that it succeeds and channel down
        //    event is fired.
        this.isAsync = isAsync;
        TcpConnection obj =
                NettyTcpConnection.createTestedInstance(adapter, LINGER_TIMEOUT_MILLIES);

        obj.setChannelStatusHandler(eventHandler);

        // 1) Invoke 'connect'.  Ensure that it succeeds, and channel up event
        //    is fired.
        logger.info("Initiating connection...");
        int rc = obj.connect(connectionOption, eventHandler, eventHandler, MIN_NUM_READ_BYTES);

        assertEquals(0, rc);
        logger.info("Waiting for connection...");
        acquireSema(connectSema);
        assertTrue(obj.isConnected()); // Since server is running.
        acquireSema(channelUpSema);

        logger.info(
                "Client (self) address: [{}], server address: [{}].",
                obj.localAddress(),
                obj.remoteAddress());

        // 2) Invoke 'disconnect' and ensure that it succeeds and channel down
        //    event is fired.
        logger.info("Disconnecting...");
        rc = obj.disconnect(eventHandler);

        assertEquals(0, rc);

        logger.info("Waiting for disconnection...");
        acquireSema(disconnectSema);
        assertFalse(obj.isConnected());
        acquireSema(channelDownSema);

        if (isAsync) {
            assertEquals(0, obj.linger());
        }
    }

    @Test
    public void testConnectDisconnectWithServerAsync() {
        logger.info("=========================================================");
        logger.info("Testing NettyTcpConnection with server in async mode.");
        logger.info("=========================================================");
        this.testConnectDisconnectWithServer(true);
    }

    @Test
    public void testConnectDisconnectWithServerSync() {
        logger.info("========================================================");
        logger.info("Testing NettyTcpConnection with server in sync mode.");
        logger.info("========================================================");
        this.testConnectDisconnectWithServer(false);
    }

    @Test
    public void testConnectDisconnectWithIntermittentServer() {
        logger.info("=======================================================");
        logger.info("Testing NettyTcpConnection with intermittent server.");
        logger.info("=======================================================");

        // 1) Invoke 'connect' without bringing up the server.
        // 2) Start the server.
        // 3) Ensure that 'connect' succeeds, and channel up event is received.
        // 4) Disconnect and ensure that it succeeds and channel down event is
        //    received.

        isServerEnabled.set(false);
        TcpConnection obj =
                NettyTcpConnection.createTestedInstance(adapter, LINGER_TIMEOUT_MILLIES);

        obj.setChannelStatusHandler(eventHandler);

        logger.info("Initiating connection...");

        // 1) Invoke 'connect' without bringing up the server.
        int rc = obj.connect(connectionOption, eventHandler, eventHandler, MIN_NUM_READ_BYTES);

        assertEquals(0, rc);
        logger.info("Waiting for connection...");
        sleepForMillies(100);
        // 2) Start the server.
        isServerEnabled.set(true);

        // 3) Ensure that 'connect' succeeds, and channel up event is received.
        acquireSema(connectSema);
        assertTrue(obj.isConnected()); // Since server is running.
        acquireSema(channelUpSema);

        logger.info(
                "Client (self) address: [{}], server address: [{}].",
                obj.localAddress(),
                obj.remoteAddress());

        // 4) Disconnect and ensure that it succeeds and channel down event is
        //    received.
        logger.info("Disconnecting...");
        rc = obj.disconnect(eventHandler);

        assertEquals(0, rc);

        logger.info("Waiting for disconnection...");
        acquireSema(disconnectSema);
        assertFalse(obj.isConnected());

        assertEquals(0, obj.linger());
    }

    @Test
    public void testDisconnectWhileConnectionInProgress() {

        logger.info("==========================================");
        logger.info("Testing NettyTcpConnection disconnect.");
        logger.info("==========================================");

        // 1) Invoke 'connect' without bringing up the server.
        // 2) Sleep for number of milliseconds (before 'connect' finishes).
        // 3) Invoke 'disconnect' and ensure that connection callback is fired
        //    with 'CANCELLED' status, followed by disconnection callback with
        //    'SUCCESS' status.  Also ensure that no channel up/down events are
        //    fired.

        isServerEnabled.set(false);

        TcpConnection obj =
                NettyTcpConnection.createTestedInstance(adapter, LINGER_TIMEOUT_MILLIES);

        obj.setChannelStatusHandler(eventHandler);

        logger.info("Initiating connection...");

        // 1) Invoke 'connect' without bringing up the server.
        int rc = obj.connect(connectionOption, eventHandler, eventHandler, MIN_NUM_READ_BYTES);

        assertEquals(0, rc);
        logger.info("Waiting for connection...");

        // 2) Sleep for number of milliseconds (before 'connect' finishes).
        sleepForMillies(START_TIMEOUT_MILLIES / 2);
        assertEquals(0, connectSema.availablePermits());
        assertFalse(obj.isConnected());

        // 3) Invoke 'disconnect' and ensure that connection callback is fired
        //    with 'CANCELLED' status, followed by disconnection callback with
        //    'SUCCESS' status.  Also ensure that no channel up/down events are
        //    fired.
        logger.info("Disconnecting...");
        rc = obj.disconnect(eventHandler);
        assertEquals(0, rc);

        // Since connection is in progress, connection callback will be fired
        // first, followed by disconnection callback.
        acquireSema(connectSema);
        logger.info("Connection callback fired.");
        acquireSema(disconnectSema);
        logger.info("Disconnection callback fired.");
        assertEquals(0, channelDownSema.availablePermits());
        assertFalse(obj.isConnected());

        assertEquals(0, obj.linger());
    }

    // @Test
    public void testChannelWaterMark() {

        // ====================================================================
        // DISABLED_TEST
        // This test has been disabled because it fails occasionally.  The
        // assert in step (4) "assertTrue(obj.isWritable());" fires.  This is
        // probably related to the fact that an embedded channel is being used
        // in this test.  Note that the corresponding integration test passes
        // everytime.
        // ====================================================================

        logger.info("==================================================");
        logger.info("Testing NettyTcpConnection channel water mark.");
        logger.info("==================================================");

        // 1) Invoke 'connect' with small lwm & hwm values, and ensure that
        //    'connect' succeeds.
        // 2) Write messages to the server until 'write' returns
        //    'WRITE_BUFFER_FULL'.
        // 3) Invoke 'waitUntilWritable'.
        // 4) Ensure that channel is writable.
        // 5) Ensure that 'write' succeeds after 'waitUntilWritable' returns.
        // 6) Repeat steps 3-6.
        // 7) Invoke 'disconnect' and ensure that it succeeds.

        connectionOption.setWriteBufferWaterMark(
                new SessionOptions.WriteBufferWaterMark(LOW_WATER_MARK, HIGH_WATER_MARK));
        final String message = "Payload for NettyTcpConnection integration test";

        TcpConnection obj = NettyTcpConnection.createTestedInstance(adapter, 0);

        obj.setChannelStatusHandler(eventHandler);

        // 1) Invoke 'connect' with small lwm & hwm values, and ensure that
        //    'connect' succeeds.
        logger.info("Initiating connection...");
        int rc = obj.connect(connectionOption, eventHandler, eventHandler, MIN_NUM_READ_BYTES);

        assertEquals(0, rc);
        logger.info("Waiting for connection...");
        acquireSema(connectSema);
        logger.info("Waiting for connection2...");
        assertTrue(obj.isConnected()); // Since server is running.
        acquireSema(channelUpSema);

        logger.info(
                "Client (self) address: [{}], server address: [{}].",
                obj.localAddress(),
                obj.remoteAddress());

        // 2) Write messages to the server until 'write' returns
        //    'WRITE_BUFFER_FULL'.
        ByteBuffer packet = ByteBuffer.wrap(message.getBytes(StandardCharsets.US_ASCII));
        ByteBuffer[] data = new ByteBuffer[] {packet};
        WriteStatus writeRc;
        while (true) {
            writeRc = obj.write(data);
            if (writeRc != WriteStatus.SUCCESS) {
                break;
            }
        }

        assertEquals(WriteStatus.WRITE_BUFFER_FULL, writeRc);

        // Note that we can't assert here that channel is not writable (i.e.,
        // 'obj.isWritable() == false', because channel may have become
        // writable by this time.

        // 3) Invoke 'waitUntilWritable'.
        obj.waitUntilWritable();

        // 4) Ensure that channel is writable.
        assertTrue(obj.isWritable());

        // 5) Ensure that 'write' succeeds after 'waitUntilWritable' returns.
        writeRc = obj.write(data);
        assertEquals(WriteStatus.SUCCESS, writeRc);

        // 6) Repeat steps 2-5.
        while (true) {
            writeRc = obj.write(data);
            if (writeRc != WriteStatus.SUCCESS) {
                break;
            }
        }

        assertSame(WriteStatus.WRITE_BUFFER_FULL, writeRc);
        obj.waitUntilWritable();
        assertTrue(obj.isWritable());
        writeRc = obj.write(data);
        assertEquals(WriteStatus.SUCCESS, writeRc);

        // 7) Invoke 'disconnect' and ensure that it succeeds.
        logger.info("Disconnecting...");
        rc = obj.disconnect(eventHandler);
        assertEquals(0, rc);

        logger.info("Waiting for disconnection...");
        acquireSema(disconnectSema);
        assertFalse(obj.isConnected());
        acquireSema(channelDownSema);

        assertEquals(0, obj.linger());
    }

    // @Test
    public void testChannelWritable() {

        // ====================================================================
        // DISABLED_TEST
        // This test has been disabled because it fails occasionally.  The
        // assert in step (4) "assertTrue(obj.isWritable());" fires.  This is
        // probably related to the fact that an embedded channel is being used
        // in this test.  Note that the corresponding integration test passes
        // everytime.
        // ====================================================================

        logger.info("=======================================================");
        logger.info("Testing NettyTcpConnection channel writable status.");
        logger.info("=======================================================");

        // 1) Invoke 'connect' with small lwm & hwm values, and ensure that
        //    'connect' succeeds.
        // 2) Write messages to the server until 'write' returns
        //    'WRITE_BUFFER_FULL'.
        // 3) Wait for CHANNEL_WRITABLE status.
        // 4) Ensure that channel is writable and 'write' succeeds.
        // 5) Invoke 'disconnect' and ensure that it succeeds.

        connectionOption.setWriteBufferWaterMark(
                new SessionOptions.WriteBufferWaterMark(LOW_WATER_MARK, HIGH_WATER_MARK));
        final String message = "Payload for NettyTcpConnection integration test";

        TcpConnection obj = NettyTcpConnection.createTestedInstance(adapter, 0);

        obj.setChannelStatusHandler(eventHandler);

        // 1) Invoke 'connect' with small lwm & hwm values, and ensure that
        //    'connect' succeeds.
        logger.info("Initiating connection...");
        int rc = obj.connect(connectionOption, eventHandler, eventHandler, MIN_NUM_READ_BYTES);

        assertEquals(0, rc);
        logger.info("Waiting for connection...");
        acquireSema(connectSema);
        logger.info("Waiting for connection2...");
        assertTrue(obj.isConnected()); // Since server is running.
        acquireSema(channelUpSema);

        logger.info(
                "Client (self) address: [{}], server address: [{}].",
                obj.localAddress(),
                obj.remoteAddress());

        // 2) Write messages to the server until 'write' returns
        //    'WRITE_BUFFER_FULL'.
        ByteBuffer packet = ByteBuffer.wrap(message.getBytes(StandardCharsets.US_ASCII));
        ByteBuffer[] data = new ByteBuffer[] {packet};
        WriteStatus writeRc;
        while (true) {
            writeRc = obj.write(data);
            if (writeRc != WriteStatus.SUCCESS) {
                break;
            }
        }

        assertEquals(WriteStatus.WRITE_BUFFER_FULL, writeRc);

        // Note that we can't assert here that channel is not writable (i.e.,
        // 'obj.isWritable() == false', because channel may have become
        // writable by this time.

        // 3) Wait for CHANNEL_WRITABLE status.
        acquireSema(channelWritableSema);

        // 4) Ensure that channel is writable and 'write' succeeds.
        assertTrue(obj.isWritable());

        writeRc = obj.write(data);
        assertEquals(WriteStatus.SUCCESS, writeRc);

        // 5) Invoke 'disconnect' and ensure that it succeeds.
        logger.info("Disconnecting...");
        rc = obj.disconnect(eventHandler);
        assertEquals(0, rc);

        logger.info("Waiting for disconnection...");
        acquireSema(disconnectSema);
        assertFalse(obj.isConnected());
        acquireSema(channelDownSema);

        assertEquals(0, obj.linger());
    }

    // @Test
    public void testChannelWaterMarkSlowServer() {

        // ====================================================================
        // DISABLED_TEST
        // This test has been disabled because it fails occasionally.  The
        // assert in step (6) "assertTrue(obj.isWritable());" fires.  This is
        // probably related to the fact that an embedded channel is being used
        // in this test.  Note that the corresponding integration test passes
        // everytime.
        // ====================================================================

        logger.info("==============================================================");
        logger.info("Testing NettyTcpConnection channel water mark slow server.");
        logger.info("==============================================================");

        // This test case is different from 'testChannelWaterMark' in that
        // channel high water mark was raised in that test by providing
        // unrealistically low values for the (lwm, hwm) pair -- only a few
        // bytes.  In this test case, we will provide realistic values for the
        // pair, but disable reads from the server to raise a high water mark.

        // 1) Invoke 'connect' with realistic (default) lwm & hwm values, and
        //    ensure that 'connect' succeeds.
        // 2) Disable reads in the server for the client's channel.
        // 3) Write messages to the server until 'write' returns
        //    'WRITE_BUFFER_FULL'.
        // 4) Enable reads in the server.
        // 5) Invoke 'waitUntilWritable'.
        // 6) Ensure that channel is writable.
        // 7) Invoke 'disconnect' and ensure that it succeeds.

        isServerEnabled.set(true);
        connectionOption.setWriteBufferWaterMark(
                new SessionOptions.WriteBufferWaterMark(LOW_WATER_MARK, HIGH_WATER_MARK));
        final String message = "Payload for NettyTcpConnection integration test";

        TcpConnection obj = NettyTcpConnection.createTestedInstance(adapter, 0);

        obj.setChannelStatusHandler(eventHandler);

        // 1) Invoke 'connect' with realistic (default) values, and ensure that
        //    'connect' succeeds.
        logger.info("Initiating connection...");

        int rc = obj.connect(connectionOption, eventHandler, eventHandler, MIN_NUM_READ_BYTES);

        assertEquals(0, rc);
        logger.info("Waiting for connection...");
        acquireSema(connectSema);
        assertTrue(obj.isConnected()); // Since server is running.
        acquireSema(channelUpSema);

        logger.info(
                "Client (self) address: [{}], server address: [{}].",
                obj.localAddress(),
                obj.remoteAddress());

        // 2) Disable reads in the server for the client's channel.
        isServerReadEnabled.set(false);

        // 3) Write messages to the server until 'write' returns
        //    'WRITE_BUFFER_FULL'.
        ByteBuffer packet = ByteBuffer.wrap(message.getBytes(StandardCharsets.US_ASCII));
        ByteBuffer[] data = new ByteBuffer[] {packet};
        WriteStatus writeRc;
        int i = 0;
        while (true) {
            i++;
            writeRc = obj.write(data);
            if (writeRc != WriteStatus.SUCCESS) {
                break;
            }
        }
        logger.info("Iterations: {}", i);
        assertEquals(WriteStatus.WRITE_BUFFER_FULL, writeRc);

        // Note that we can't assert here that channel is not writable (i.e.,
        // 'obj.isWritable() == false', because channel may have become
        // writable by this time.

        // 4) Enable reads in the server.
        isServerReadEnabled.set(true);

        // 5) Invoke 'waitUntilWritable'.
        obj.waitUntilWritable();

        // 6) Ensure that channel is writable.
        assertTrue(obj.isWritable());

        // 7) Invoke 'disconnect' and ensure that it succeeds.
        logger.info("Disconnecting...");
        rc = obj.disconnect(eventHandler);
        assertEquals(0, rc);

        logger.info("Waiting for disconnection...");
        acquireSema(disconnectSema);
        assertFalse(obj.isConnected());
        acquireSema(channelDownSema);

        assertEquals(0, obj.linger());
    }

    @Test
    public void testCompositeBuffer() {
        // 1) In a child thread put 4 strigns into netty ByteBufs and add them into
        // CompositeByteBuf.
        // 2) Store interal NIO buffers, clean related ByteBufs
        // 3) In a main thread check content of the NIO buffers.

        logger.info("=============================================");
        logger.info("BEGIN NettyTcpConnection testCompositeBuffer.");
        logger.info("=============================================");

        String[] messages =
                new String[] {
                    "ABCDEFGHIJ",
                    "1234567890987654321",
                    "abcdefghijklmnopqrstuvwxyz",
                    "Payload for NettyTcpConnection integration test"
                };
        ArrayList<String> receivedPayloads = new ArrayList<>();
        ArrayList<ByteBuffer[]> receivedBuffers = new ArrayList<>();

        CompositeByteBuf readBuffer = Unpooled.compositeBuffer();
        Runnable task =
                () -> {
                    for (String msg : messages) {
                        // Create ByteBuf without baked array
                        byte[] baked = msg.getBytes(StandardCharsets.US_ASCII);
                        ByteBuffer notBaked = ByteBuffer.allocateDirect(baked.length);
                        notBaked.put(baked);
                        notBaked.rewind();
                        logger.info("Wrapping");
                        ByteBuf b = Unpooled.wrappedBuffer(notBaked);
                        assertFalse(b.hasArray());
                        assertTrue(b.isDirect());

                        // Fill CompositeByteBuf and get internal ByteBuffer[]
                        readBuffer.addComponent(true, b);
                        ByteBuffer[] bbuf = readBuffer.nioBuffers();

                        receivedPayloads.add(byteBuffersToString(bbuf));
                        receivedBuffers.add(bbuf);

                        readBuffer.readerIndex(readBuffer.readableBytes());
                        readBuffer.discardReadComponents();
                    }
                };
        Thread thread = new Thread(task);
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            logger.error("Interrupted: ", e);
            Thread.currentThread().interrupt();
        }

        assertEquals(receivedPayloads.size(), messages.length);
        assertEquals(receivedBuffers.size(), messages.length);

        for (int i = 0; i < messages.length; ++i) {
            String expectedStr = messages[i];
            String copiedStr = receivedPayloads.get(i);
            String rawStr = byteBuffersToString(receivedBuffers.get(i));

            assertEquals(expectedStr, copiedStr);
            assertEquals(expectedStr, rawStr);
        }

        logger.info("===========================================");
        logger.info("END NettyTcpConnection testCompositeBuffer.");
        logger.info("===========================================");
    }
}
