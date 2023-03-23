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
package com.bloomberg.bmq.it;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.SessionOptions.WriteBufferWaterMark;
import com.bloomberg.bmq.impl.infr.net.ConnectionOptions;
import com.bloomberg.bmq.impl.infr.net.NettyTcpConnection;
import com.bloomberg.bmq.impl.infr.net.intf.ChannelStatusHandler;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.ConnectCallback;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.ConnectStatus;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.DisconnectCallback;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.DisconnectStatus;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.ReadCallback;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.WriteStatus;
import com.bloomberg.bmq.impl.infr.util.SystemUtil;
import com.bloomberg.bmq.it.util.BmqBroker;
import com.bloomberg.bmq.it.util.BmqBrokerSimulator;
import com.bloomberg.bmq.it.util.BmqBrokerSimulator.Mode;
import com.bloomberg.bmq.it.util.TestTcpServer;
import com.bloomberg.bmq.it.util.TestTools;
import io.netty.util.ResourceLeakDetector;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyTcpConnectionImplIT {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int MIN_NUM_READ_BYTES = 10;

    private class ConnectionEventHandler
            implements ChannelStatusHandler, ConnectCallback, DisconnectCallback, ReadCallback {

        private final ArrayList<String> receivedPayloads;
        private int[] nextMsgLens;

        public ConnectionEventHandler() {
            receivedPayloads = new ArrayList<>();
            nextMsgLens = null;
        }

        @Override
        public void handleChannelStatus(ChannelStatus status) {
            logger.info("ChannelStatus: {}", status);
            switch (status) {
                case CHANNEL_UP:
                    NettyTcpConnectionImplIT.this.channelUpSema.release();
                    break;
                case CHANNEL_WRITABLE:
                    NettyTcpConnectionImplIT.this.channelWritableSema.release();
                    break;
                case CHANNEL_DOWN:
                    NettyTcpConnectionImplIT.this.channelDownSema.release();
                    break;
                default:
                    fail();
                    break;
            }
        }

        @Override
        public void handleConnectCb(ConnectStatus status) {
            logger.info("ConnectCallbackStatus: {}", status);
            NettyTcpConnectionImplIT.this.connectSema.release();
        }

        @Override
        public void handleDisconnectCb(DisconnectStatus status) {
            logger.info("DisconnectCbStatus: {}", status);
            NettyTcpConnectionImplIT.this.disconnectSema.release();
        }

        @Override
        public void handleReadCb(ReadCompletionStatus readStatus, ByteBuffer[] data) {
            StringBuilder sb = new StringBuilder();
            for (ByteBuffer b : data) {
                byte[] array = new byte[b.limit()];
                b.get(array);
                b.rewind();
                sb.append(new String(array, StandardCharsets.US_ASCII));
            }
            String strData = sb.toString();
            logger.info("Received data [{}]", strData);

            receivedPayloads.add(strData);

            if (nextMsgLens != null) {
                // More messages to be received.

                if (receivedPayloads.size() < nextMsgLens.length) {
                    readStatus.setNumNeeded(nextMsgLens[receivedPayloads.size() - 1]);
                } else {
                    readStatus.setNumNeeded(1); // zero ok?
                }
            }
        }

        public void setNextReadMessageLengths(int[] nextMsgLens) {
            this.nextMsgLens = nextMsgLens;
        }

        public ArrayList<String> receivedPayloads() {
            return receivedPayloads;
        }
    }

    private final ConnectionEventHandler eventHandler;

    private Semaphore connectSema;
    private Semaphore disconnectSema;
    private Semaphore channelUpSema;
    private Semaphore channelDownSema;
    private Semaphore channelWritableSema;

    private static URI getServerUri() {
        return URI.create("tcp://localhost:" + SystemUtil.getEphemeralPort());
    }

    public NettyTcpConnectionImplIT() {
        // Setting netty's resource leak detector level to advanced by default.
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);

        eventHandler = new ConnectionEventHandler();
    }

    void init() {
        connectSema = new Semaphore(0);
        disconnectSema = new Semaphore(0);
        channelUpSema = new Semaphore(0);
        channelDownSema = new Semaphore(0);
        channelWritableSema = new Semaphore(0);
    }

    TestTcpServer getServer(int port, boolean echoMode) {
        Mode mode = echoMode ? Mode.ECHO_MODE : Mode.SILENT_MODE;
        return new BmqBrokerSimulator(port, mode);
    }

    @Test
    public void testConnectDisconnectNoServer() {

        logger.info("================================================");
        logger.info("BEGIN Testing NettyTcpConnection without server.");
        logger.info("================================================");

        // 1) Invoke 'connect' without bringing up the server.
        // 2) Ensure that 'connect' fails, and no channel up event is fired.
        // 3) Invoke 'disconnect' and ensure that it fails and no channel down
        //    event is fired.
        init();

        ConnectionOptions co = new ConnectionOptions();
        URI uri = URI.create("tcp://www.bloomberg.com:" + SystemUtil.getEphemeralPort());
        co.setBrokerUri(uri);
        co.setStartNumRetries(3).setStartRetryInterval(Duration.ofSeconds(2));

        TcpConnection impl = NettyTcpConnection.createInstance();

        impl.setChannelStatusHandler(eventHandler);

        // 1) Invoke 'connect' without bringing up the server.
        logger.info("Initiating connection...");
        int rc = impl.connect(co, eventHandler, eventHandler, MIN_NUM_READ_BYTES);

        assertEquals(0, rc);
        logger.info("Waiting for connection...");
        TestTools.acquireSema(connectSema);

        // 2) Ensure that 'connect' fails, and no channel up event is fired.
        assertFalse(impl.isConnected());
        assertEquals(0, channelUpSema.availablePermits());

        // 3) Invoke 'disconnect' and ensure that it fails and no channel down
        //    event is fired.
        logger.info("Disconnecting...");

        rc = impl.disconnect(eventHandler);

        assertNotEquals(0, rc); // Since not connected
        assertFalse(impl.isConnected());
        assertEquals(0, channelDownSema.availablePermits());
        assertEquals(0, disconnectSema.availablePermits());

        logger.info("Already disconnected.");

        // No need to wait on 'disconnectSema' since we are not connected.

        assertEquals(0, impl.linger());

        logger.info("==============================================");
        logger.info("END Testing NettyTcpConnection without server.");
        logger.info("==============================================");
    }

    @Test
    public void testConnectDisconnectWithServer() {

        logger.info("=============================================");
        logger.info("BEGIN Testing NettyTcpConnection with server.");
        logger.info("=============================================");

        // 1) Bring up the server.
        // 2) Invoke 'connect'.  Ensure that it succeeds, and channel up event
        //    is fired.
        // 3) Invoke 'disconnect' and ensure that it succeeds and channel down
        //    event is fired.
        // 4) Stop the server.

        init();

        final int NUM_RECONNECTIONS = 2;

        SessionOptions so = SessionOptions.builder().setBrokerUri(getServerUri()).build();

        TestTcpServer server = getServer(so.brokerUri().getPort(), false);
        // 1) Bring up the server.
        server.start();

        TcpConnection impl = NettyTcpConnection.createInstance();

        impl.setChannelStatusHandler(eventHandler);

        for (int i = 0; i < NUM_RECONNECTIONS; i++) {
            // 2) Invoke 'connect'.  Ensure that it succeeds, and channel up event
            //    is fired.
            logger.info("Initiating connection...");
            int rc =
                    impl.connect(
                            new ConnectionOptions(so),
                            eventHandler,
                            eventHandler,
                            MIN_NUM_READ_BYTES);

            assertEquals(0, rc);
            logger.info("Waiting for connection...");
            TestTools.acquireSema(connectSema);
            assertTrue(impl.isConnected()); // Since server is running.
            TestTools.acquireSema(channelUpSema);

            logger.info(
                    "Client (self) address: [{}], server address: [{}].",
                    impl.localAddress(),
                    impl.remoteAddress());

            // 3) Invoke 'disconnect' and ensure that it succeeds and channel down
            //    event is fired.
            logger.info("Disconnecting...");
            rc = impl.disconnect(eventHandler);

            assertEquals(0, rc);

            logger.info("Waiting for disconnection...");
            TestTools.acquireSema(disconnectSema);
            assertFalse(impl.isConnected());
            TestTools.acquireSema(channelDownSema);
        }

        assertEquals(0, impl.linger());

        // 4) Stop the server.
        server.stop();

        logger.info("===========================================");
        logger.info("END Testing NettyTcpConnection with server.");
        logger.info("===========================================");
    }

    @Test
    public void testConnectDropReconnect() {

        logger.info("========================================================");
        logger.info("BEGIN Testing NettyTcpConnection Connect-Drop-Reconnect.");
        logger.info("======================================================= ");

        // 1) Bring up the server.
        // 2) Invoke 'connect'.  Ensure that it succeeds, and channel up event
        //    is fired.
        // 3) Invoke 'disconnect' without callback, ensure that it succeeds
        //    and channel down event is fired.
        // 4) Ensure that the connection is restored and channel up event is
        //    fired
        // 5) Invoke 'disconnect', ensure that it succeeds and channel down
        //    event is fired.
        // 6) Stop the server.

        init();

        SessionOptions so = SessionOptions.builder().setBrokerUri(getServerUri()).build();

        TestTcpServer server = getServer(so.brokerUri().getPort(), false);
        // 1) Bring up the server.
        server.start();

        TcpConnection impl = NettyTcpConnection.createInstance();

        impl.setChannelStatusHandler(eventHandler);

        // 2) Invoke 'connect'.  Ensure that it succeeds, and channel up event
        //    is fired.
        logger.info("Initiating connection...");
        int rc =
                impl.connect(
                        new ConnectionOptions(so), eventHandler, eventHandler, MIN_NUM_READ_BYTES);

        assertEquals(0, rc);
        logger.info("Waiting for connection...");
        TestTools.acquireSema(connectSema);
        assertTrue(impl.isConnected()); // Since server is running.
        TestTools.acquireSema(channelUpSema);

        logger.info(
                "Client (self) address: [{}], server address: [{}].",
                impl.localAddress(),
                impl.remoteAddress());

        // 3) Invoke 'disconnect' without callback, ensure that it succeeds
        //    and channel down event is fired.
        logger.info("Disconnecting...");
        rc = impl.disconnect(null);

        assertEquals(0, rc);

        logger.info("Waiting for disconnection...");
        TestTools.acquireSema(channelDownSema);
        assertFalse(impl.isConnected());

        // Since there is no disconnect callback, we should fail to acquire
        // disconnect semaphore.
        assertFalse(disconnectSema.tryAcquire());

        // 4) Ensure that the connection is restored and channel up event is
        //    fired
        logger.info("Waiting for connection...");
        TestTools.acquireSema(channelUpSema);
        assertTrue(impl.isConnected()); // Since server is running.

        logger.info(
                "Client (self) address: [{}], server address: [{}].",
                impl.localAddress(),
                impl.remoteAddress());

        // Since this is reconnecting, connection callback should not be called.
        assertFalse(connectSema.tryAcquire());

        // 5) Invoke 'disconnect', ensure that it succeeds and channel down
        //    event is fired.
        logger.info("Disconnecting...");
        rc = impl.disconnect(eventHandler);

        assertEquals(0, rc);

        logger.info("Waiting for disconnection...");
        TestTools.acquireSema(disconnectSema);
        assertFalse(impl.isConnected());
        TestTools.acquireSema(channelDownSema);

        assertEquals(0, impl.linger());

        // 6) Stop the server.
        server.stop();

        logger.info("======================================================");
        logger.info("END Testing NettyTcpConnection Connect-Drop-Reconnect.");
        logger.info("======================================================");
    }

    @Test
    public void testConnectDisconnectWithIntermittentServer() {

        logger.info("==========================================================");
        logger.info("BEGIN Testing NettyTcpConnection with intermittent server.");
        logger.info("==========================================================");

        // 1) Invoke 'connect' without bringing up the server.
        // 2) Sleep for a few seconds (before 'connect' finishes), and then
        //    start the server.
        // 3) Ensure that 'connect' succeeds, and channel up event is received.
        // 4) Stop the server.  Ensure that channel down event is received.
        // 5) Start the server.  Ensure that channel up event is received.
        // 6) Disconnect and ensure that it succeeds and channel down event is
        //    received.

        init();

        SessionOptions so = SessionOptions.builder().setBrokerUri(getServerUri()).build();

        TcpConnection impl = NettyTcpConnection.createInstance();
        impl.setChannelStatusHandler(eventHandler);

        logger.info("Initiating connection...");

        // 1) Invoke 'connect' without bringing up the server.
        int rc =
                impl.connect(
                        new ConnectionOptions(so), eventHandler, eventHandler, MIN_NUM_READ_BYTES);

        assertEquals(0, rc);
        logger.info("Waiting for connection...");

        // 2) Sleep for a few seconds (before 'connect' finishes), and then
        //    start the server.
        TestTools.sleepForSeconds(2);
        assertEquals(0, channelUpSema.availablePermits());

        TestTcpServer server = getServer(so.brokerUri().getPort(), false);
        server.start();

        // 3) Ensure that 'connect' succeeds, and channel up event is received.
        TestTools.acquireSema(connectSema);
        assertTrue(impl.isConnected()); // Since server is running.
        TestTools.acquireSema(channelUpSema);

        logger.info(
                "Client (self) address: [{}], server address: [{}].",
                impl.localAddress(),
                impl.remoteAddress());

        // 4) Stop the server.  Ensure that channel down event is received.
        TestTools.sleepForSeconds(2);
        server.stop();
        TestTools.acquireSema(channelDownSema);
        assertFalse(impl.isConnected()); // Since server is not running.

        // 5) Start the server.  Ensure that channel up event is received.
        TestTools.sleepForSeconds(2);
        server.start();

        TestTools.acquireSema(channelUpSema);
        assertTrue(impl.isConnected());

        // 6) Disconnect and ensure that it succeeds and channel down event is
        //    received.
        logger.info("Disconnecting...");
        TestTools.sleepForSeconds(2);
        rc = impl.disconnect(eventHandler);

        assertEquals(0, rc);

        logger.info("Waiting for disconnection...");
        TestTools.acquireSema(channelDownSema);
        TestTools.acquireSema(disconnectSema);
        assertFalse(impl.isConnected());

        assertEquals(0, impl.linger());

        server.stop();

        logger.info("========================================================");
        logger.info("END Testing NettyTcpConnection with intermittent server.");
        logger.info("========================================================");
    }

    @Test
    public void testDisconnectWhileConnectionInProgress() {

        logger.info("============================================");
        logger.info("BEGIN Testing NettyTcpConnection disconnect.");
        logger.info("============================================");

        // 1) Invoke 'connect' without bringing up the server.
        // 2) Sleep for a few seconds (before 'connect' finishes).
        // 3) Invoke 'disconnect' and ensure that connection callback is fired
        //    with 'CANCELLED' status, followed by disconnection callback with
        //    'SUCCESS' status.  Also ensure that no channel up/down events are
        //    fired.

        init();

        SessionOptions so = SessionOptions.builder().setBrokerUri(getServerUri()).build();

        TcpConnection impl = NettyTcpConnection.createInstance();
        impl.setChannelStatusHandler(eventHandler);

        logger.info("Initiating connection...");

        // 1) Invoke 'connect' without bringing up the server.
        int rc =
                impl.connect(
                        new ConnectionOptions(so), eventHandler, eventHandler, MIN_NUM_READ_BYTES);

        assertEquals(0, rc);
        logger.info("Waiting for connection...");

        // 2) Sleep for a few seconds (before 'connect' finishes).
        TestTools.sleepForSeconds(3);
        assertEquals(0, channelUpSema.availablePermits());
        assertEquals(0, connectSema.availablePermits());
        assertFalse(impl.isConnected());

        // 3) Invoke 'disconnect' and ensure that connection callback is fired
        //    with 'CANCELLED' status, followed by disconnection callback with
        //    'SUCCESS' status.  Also ensure that no channel up/down events are
        //    fired.
        logger.info("Disconnecting...");
        rc = impl.disconnect(eventHandler);
        assertEquals(0, rc);

        // Since connection is in progress, connection callback will be fired
        // first, followed by disconnection callback.
        TestTools.acquireSema(connectSema);
        logger.info("Connection callback fired.");
        TestTools.acquireSema(disconnectSema);
        logger.info("Disconnection callback fired.");
        assertEquals(0, channelUpSema.availablePermits());
        assertEquals(0, channelDownSema.availablePermits());
        assertFalse(impl.isConnected());

        assertEquals(0, impl.linger());

        logger.info("==========================================");
        logger.info("END Testing NettyTcpConnection disconnect.");
        logger.info("==========================================");
    }

    @Test
    public void testReadWrite() {

        logger.info("============================================");
        logger.info("BEGIN Testing NettyTcpConnection read/write.");
        logger.info("============================================");

        // 1) Bring up the server.
        // 2) Invoke 'connect' and ensure that it succeeds.
        // 3) Write 4 messages to the server.
        // 4) Ensure that same 4 messages are received.
        // 5) Invoke 'disconnect' and ensure that it succeeds.
        // 6) Stop the server.

        init();

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

        SessionOptions so = SessionOptions.builder().setBrokerUri(getServerUri()).build();

        // 1) Bring up the server.
        TestTcpServer server = getServer(so.brokerUri().getPort(), true);
        server.start();

        TcpConnection impl = NettyTcpConnection.createInstance();

        impl.setChannelStatusHandler(eventHandler);

        // 2) Invoke 'connect'.  Ensure that it succeeds, and channel up event
        //    is fired.
        logger.info("Initiating connection...");
        int rc =
                impl.connect(
                        new ConnectionOptions(so),
                        eventHandler,
                        eventHandler,
                        messages[0].length()); // Length of 1st message

        assertEquals(0, rc);
        logger.info("Waiting for connection...");
        TestTools.acquireSema(connectSema);
        assertTrue(impl.isConnected()); // Since server is running.
        TestTools.acquireSema(channelUpSema);

        logger.info(
                "Client (self) address: [{}], server address: [{}].",
                impl.localAddress(),
                impl.remoteAddress());

        TestTools.sleepForSeconds(2);

        // 3) Write 4 messages to the server.
        for (String message : messages) {
            ByteBuffer packet = ByteBuffer.wrap(message.getBytes(StandardCharsets.US_ASCII));
            ByteBuffer[] data = new ByteBuffer[] {packet};
            WriteStatus writeRc = impl.write(data);
            assertSame(WriteStatus.SUCCESS, writeRc);
            TestTools.sleepForSeconds(2);
        }

        // 4) Ensure that same 4 messages are received.
        ArrayList<String> recvdPayloads = eventHandler.receivedPayloads();
        assertEquals(messages.length, recvdPayloads.size());

        for (int i = 0; i < messages.length; ++i) {
            assertEquals(messages[i], recvdPayloads.get(i));
        }

        // 5) Invoke 'disconnect' and ensure that it succeeds.
        logger.info("Disconnecting...");
        rc = impl.disconnect(eventHandler);
        assertEquals(0, rc);

        logger.info("Waiting for disconnection...");
        TestTools.acquireSema(disconnectSema);
        assertFalse(impl.isConnected());
        TestTools.acquireSema(channelDownSema);

        assertEquals(0, impl.linger());

        // 6) Stop the server.
        server.stop();

        logger.info("==========================================");
        logger.info("END Testing NettyTcpConnection read/write.");
        logger.info("==========================================");
    }

    @Test
    public void testChannelWaterMark() {

        logger.info("====================================================");
        logger.info("BEGIN Testing NettyTcpConnection channel water mark.");
        logger.info("====================================================");

        // 1) Bring up the server.
        // 2) Invoke 'connect' with small lwm & hwm values, and ensure that
        //    'connect' succeeds.
        // 3) Write messages to the server until 'write' returns
        //    'WRITE_BUFFER_FULL'.
        // 4) Invoke 'waitUntilWritable'.
        // 5) Ensure that channel is writable.
        // 6) Ensure that 'write' succeeds after 'waitUntilWritable' returns.
        // 7) Repeat steps 3-6.
        // 8) Invoke 'disconnect' and ensure that it succeeds.
        // 9) Stop the server.

        init();

        final String message = "Payload for NettyTcpConnection integration test";

        SessionOptions so =
                SessionOptions.builder()
                        .setBrokerUri(getServerUri())
                        .setWriteBufferWaterMark(new WriteBufferWaterMark(5, 200))
                        .build();

        // 1) Bring up the server.
        TestTcpServer server = getServer(so.brokerUri().getPort(), false);
        server.start();

        TcpConnection impl = NettyTcpConnection.createInstance();

        impl.setChannelStatusHandler(eventHandler);

        // 2) Invoke 'connect' with small lwm & hwm values, and ensure that
        //    'connect' succeeds.
        logger.info("Initiating connection...");
        int rc =
                impl.connect(
                        new ConnectionOptions(so), eventHandler, eventHandler, MIN_NUM_READ_BYTES);

        assertEquals(0, rc);
        logger.info("Waiting for connection...");
        TestTools.acquireSema(connectSema);
        assertTrue(impl.isConnected()); // Since server is running.
        TestTools.acquireSema(channelUpSema);

        logger.info(
                "Client (self) address: [{}], server address: [{}].",
                impl.localAddress(),
                impl.remoteAddress());

        TestTools.sleepForSeconds(2);

        // 3) Write messages to the server until 'write' returns
        //    'WRITE_BUFFER_FULL'.
        ByteBuffer packet = ByteBuffer.wrap(message.getBytes(StandardCharsets.US_ASCII));
        ByteBuffer[] data = new ByteBuffer[] {packet};
        WriteStatus writeRc;
        while (true) {
            writeRc = impl.write(data);
            if (writeRc != WriteStatus.SUCCESS) {
                break;
            }
        }

        assertEquals(WriteStatus.WRITE_BUFFER_FULL, writeRc);

        // Note that we can't assert here that channel is not writable (i.e.,
        // 'impl.isWritable() == false', because channel may have become
        // writable by this time.

        // 4) Invoke 'waitUntilWritable'.
        impl.waitUntilWritable();

        // 5) Ensure that channel is writable.
        assertTrue(impl.isWritable());

        // 6) Ensure that 'write' succeeds after 'waitUntilWritable' returns.
        writeRc = impl.write(data);
        assertEquals(WriteStatus.SUCCESS, writeRc);

        // 7) Repeat steps 3-6.
        while (true) {
            writeRc = impl.write(data);
            if (writeRc != WriteStatus.SUCCESS) {
                break;
            }
        }
        assertSame(WriteStatus.WRITE_BUFFER_FULL, writeRc);
        impl.waitUntilWritable();
        assertTrue(impl.isWritable());
        writeRc = impl.write(data);
        assertEquals(WriteStatus.SUCCESS, writeRc);

        TestTools.sleepForSeconds(2);

        // 8) Invoke 'disconnect' and ensure that it succeeds.
        logger.info("Disconnecting...");
        rc = impl.disconnect(eventHandler);
        assertEquals(0, rc);

        logger.info("Waiting for disconnection...");
        TestTools.acquireSema(disconnectSema);
        assertFalse(impl.isConnected());
        TestTools.acquireSema(channelDownSema);

        assertEquals(0, impl.linger());

        // 9) Stop the server.
        server.stop();

        logger.info("==================================================");
        logger.info("END Testing NettyTcpConnection channel water mark.");
        logger.info("==================================================");
    }

    @Test
    public void testChannelWritable() {

        logger.info("=========================================================");
        logger.info("BEGIN Testing NettyTcpConnection channel writable status.");
        logger.info("=========================================================");

        // 1) Bring up the server.
        // 2) Invoke 'connect' with small lwm & hwm values, and ensure that
        //    'connect' succeeds.
        // 3) Write messages to the server until 'write' returns
        //    'WRITE_BUFFER_FULL'.
        // 4) Wait for CHANNEL_WRITABLE status.
        // 5) Ensure that channel is writable and 'write' succeeds.
        // 6) Invoke 'disconnect' and ensure that it succeeds.
        // 7) Stop the server.

        init();

        final String message = "Payload for NettyTcpConnection integration test";

        SessionOptions so =
                SessionOptions.builder()
                        .setBrokerUri(getServerUri())
                        .setWriteBufferWaterMark(new WriteBufferWaterMark(5, 200))
                        .build();

        // 1) Bring up the server.
        TestTcpServer server = getServer(so.brokerUri().getPort(), false);
        server.start();

        TcpConnection impl = NettyTcpConnection.createInstance();

        impl.setChannelStatusHandler(eventHandler);

        // 2) Invoke 'connect' with small lwm & hwm values, and ensure that
        //    'connect' succeeds.
        logger.info("Initiating connection...");
        int rc =
                impl.connect(
                        new ConnectionOptions(so), eventHandler, eventHandler, MIN_NUM_READ_BYTES);

        assertEquals(0, rc);
        logger.info("Waiting for connection...");
        TestTools.acquireSema(connectSema);
        assertTrue(impl.isConnected()); // Since server is running.
        TestTools.acquireSema(channelUpSema);

        logger.info(
                "Client (self) address: [{}], server address: [{}].",
                impl.localAddress(),
                impl.remoteAddress());

        TestTools.sleepForSeconds(2);

        // 3) Write messages to the server until 'write' returns
        //    'WRITE_BUFFER_FULL'.
        ByteBuffer packet = ByteBuffer.wrap(message.getBytes(StandardCharsets.US_ASCII));
        ByteBuffer[] data = new ByteBuffer[] {packet};
        WriteStatus writeRc;
        while (true) {
            writeRc = impl.write(data);
            if (writeRc != WriteStatus.SUCCESS) {
                break;
            }
        }

        assertEquals(WriteStatus.WRITE_BUFFER_FULL, writeRc);

        // Note that we can't assert here that channel is not writable (i.e.,
        // 'impl.isWritable() == false', because channel may have become
        // writable by this time.

        // 4) Wait for CHANNEL_WRITABLE status
        TestTools.acquireSema(channelWritableSema);

        // 5) Ensure that channel is writable and 'write' succeeds
        assertTrue(impl.isWritable());
        writeRc = impl.write(data);
        assertEquals(WriteStatus.SUCCESS, writeRc);

        // 6) Invoke 'disconnect' and ensure that it succeeds.
        logger.info("Disconnecting...");
        rc = impl.disconnect(eventHandler);
        assertEquals(0, rc);

        logger.info("Waiting for disconnection...");
        TestTools.acquireSema(disconnectSema);
        assertFalse(impl.isConnected());
        TestTools.acquireSema(channelDownSema);

        assertEquals(0, impl.linger());

        // 7) Stop the server.
        server.stop();

        logger.info("=======================================================");
        logger.info("END Testing NettyTcpConnection channel writable status.");
        logger.info("=======================================================");
    }

    @Test
    public void testChannelWaterMarkSlowServer() {

        logger.info("================================================================");
        logger.info("BEGIN Testing NettyTcpConnection channel water mark slow server.");
        logger.info("================================================================");

        // This test case is different from 'testChannelWaterMark' in that
        // channel high water mark was raised in that test by providing
        // unrealistically low values for the (lwm, hwm) pair -- only a few
        // bytes.  In this test case, we will provide realistic values for the
        // pair, but disable reads from the server to raise a high water mark.

        // 1) Bring up the server.
        // 2) Invoke 'connect' with realistic (default) lwm & hwm values, and
        //    ensure that 'connect' succeeds.
        // 3) Disable reads in the server for the client's channel.
        // 4) Write messages to the server until 'write' returns
        //    'WRITE_BUFFER_FULL'.
        // 5) Ensure that channel is not writable.
        // 6) Enable reads in the server.
        // 7) Invoke 'waitUntilWritable'.
        // 8) Ensure that channel is writable.
        // 9) Invoke 'disconnect' and ensure that it succeeds.
        // 10) Stop the server.

        init();

        final String message = "Payload for NettyTcpConnection integration test";

        SessionOptions so = SessionOptions.builder().setBrokerUri(getServerUri()).build();

        // 1) Bring up the server (only netty-based server support disabling
        //    and enabling reads).
        logger.info("Using netty-based server.");
        TestTcpServer server = new BmqBrokerSimulator(so.brokerUri().getPort(), Mode.SILENT_MODE);
        server.start();

        TcpConnection impl = NettyTcpConnection.createInstance();

        impl.setChannelStatusHandler(eventHandler);

        // 2) Invoke 'connect' with realistic (default) values, and ensure that
        //    'connect' succeeds.
        logger.info("Initiating connection...");
        int rc =
                impl.connect(
                        new ConnectionOptions(so), eventHandler, eventHandler, MIN_NUM_READ_BYTES);

        assertEquals(0, rc);
        logger.info("Waiting for connection...");
        TestTools.acquireSema(connectSema);
        assertTrue(impl.isConnected()); // Since server is running.
        TestTools.acquireSema(channelUpSema);

        logger.info(
                "Client (self) address: [{}], server address: [{}].",
                impl.localAddress(),
                impl.remoteAddress());

        TestTools.sleepForSeconds(2);

        // 3) Disable reads in the server for the client's channel.
        server.disableRead();

        // 4) Write messages to the server until 'write' returns
        //    'WRITE_BUFFER_FULL'.
        ByteBuffer packet = ByteBuffer.wrap(message.getBytes(StandardCharsets.US_ASCII));
        ByteBuffer[] data = new ByteBuffer[] {packet};
        WriteStatus writeRc;
        while (true) {
            writeRc = impl.write(data);
            if (writeRc != WriteStatus.SUCCESS) {
                break;
            }
        }
        assertEquals(WriteStatus.WRITE_BUFFER_FULL, writeRc);

        // 5) Ensure that channel is not writable.
        assertFalse(impl.isWritable());

        TestTools.sleepForSeconds(1);

        // 6) Enable reads in the server.
        server.enableRead();

        // 7) Invoke 'waitUntilWritable'.
        impl.waitUntilWritable();

        // 8) Ensure that channel is writable.
        assertTrue(impl.isWritable());

        TestTools.sleepForSeconds(2);

        // 9) Invoke 'disconnect' and ensure that it succeeds.
        logger.info("Disconnecting...");
        rc = impl.disconnect(eventHandler);
        assertEquals(0, rc);

        logger.info("Waiting for disconnection...");
        TestTools.acquireSema(disconnectSema);
        assertFalse(impl.isConnected());
        TestTools.acquireSema(channelDownSema);

        assertEquals(0, impl.linger());

        // 10) Stop the server.
        server.stop();

        logger.info("==============================================================");
        logger.info("END Testing NettyTcpConnection channel water mark slow server.");
        logger.info("==============================================================");
    }

    @Test
    public void testBmqServer() throws IOException {
        logger.info("======================================================================");
        logger.info("BEGIN Testing NettyTcpConnection channel and connection to BMQ Broker.");
        logger.info("======================================================================");

        // 1) Bring up the server.
        // 2) Invoke 'connect', and ensure that 'connect' succeeds.
        // 3) Invoke 'disconnect' and ensure that it succeeds.
        // 4) Repeat steps 2-3 for several times.

        final int NUM_OF_RESTARTS = 10;

        connectSema = new Semaphore(0);
        disconnectSema = new Semaphore(0);
        channelUpSema = new Semaphore(0);
        channelDownSema = new Semaphore(0);

        // 1) Bring up the server
        logger.info("Using bmq broker.");
        try (BmqBroker broker = BmqBroker.createStartedBroker()) {
            for (int i = 0; i < NUM_OF_RESTARTS; i++) {
                logger.info("=====================================================");
                logger.info("             Iteration #{}", i + 1);
                logger.info("=====================================================");

                TcpConnection impl = NettyTcpConnection.createInstance();

                impl.setChannelStatusHandler(eventHandler);

                // 2) Invoke 'connect', and ensure that 'connect' succeeds.
                logger.info("Initiating connection...");
                int rc =
                        impl.connect(
                                new ConnectionOptions(broker.sessionOptions()),
                                eventHandler,
                                eventHandler,
                                MIN_NUM_READ_BYTES);

                assertEquals(0, rc);
                logger.info("Waiting for connection...");
                TestTools.acquireSema(connectSema);
                assertTrue(impl.isConnected()); // Since server is running.
                TestTools.acquireSema(channelUpSema);

                logger.info(
                        "Client (self) address: [{}], server address: [{}].",
                        impl.localAddress(),
                        impl.remoteAddress());

                TestTools.sleepForSeconds(1);

                // 3) Invoke 'disconnect' and ensure that it succeeds.
                logger.info("Disconnecting...");
                rc = impl.disconnect(eventHandler);
                assertEquals(0, rc);

                logger.info("Waiting for disconnection...");
                TestTools.acquireSema(disconnectSema);
                assertFalse(impl.isConnected());
                TestTools.acquireSema(channelDownSema);

                assertEquals(0, impl.linger());

                // 4) Repeat in cycle
            }

            broker.setDropTmpFolder();
            broker.setDumpBrokerOutput();
        }

        logger.info("====================================================================");
        logger.info("END Testing NettyTcpConnection channel and connection to BMQ Broker.");
        logger.info("====================================================================");
    }
}
