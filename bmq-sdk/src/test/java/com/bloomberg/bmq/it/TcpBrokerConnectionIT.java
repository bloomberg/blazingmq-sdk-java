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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.bloomberg.bmq.ResultCodes.GenericResult;
import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.impl.TcpBrokerConnection;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.msg.StatusCategory;
import com.bloomberg.bmq.impl.infr.net.ConnectionOptions;
import com.bloomberg.bmq.impl.infr.net.NettyTcpConnectionFactory;
import com.bloomberg.bmq.impl.infr.proto.AckMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.ConfirmEventImpl;
import com.bloomberg.bmq.impl.infr.proto.ControlEventImpl;
import com.bloomberg.bmq.impl.infr.proto.EventHeader;
import com.bloomberg.bmq.impl.infr.proto.EventImpl;
import com.bloomberg.bmq.impl.infr.proto.EventType;
import com.bloomberg.bmq.impl.infr.proto.MessagePropertiesImpl;
import com.bloomberg.bmq.impl.infr.proto.Protocol;
import com.bloomberg.bmq.impl.infr.proto.ProtocolUtil;
import com.bloomberg.bmq.impl.infr.proto.PushHeader;
import com.bloomberg.bmq.impl.infr.proto.PushMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.PutEventImpl;
import com.bloomberg.bmq.impl.infr.proto.RequestManager;
import com.bloomberg.bmq.impl.infr.stat.EventsStats;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.infr.util.SystemUtil;
import com.bloomberg.bmq.impl.intf.BrokerConnection;
import com.bloomberg.bmq.impl.intf.BrokerConnection.StartStatus;
import com.bloomberg.bmq.impl.intf.BrokerConnection.StopStatus;
import com.bloomberg.bmq.impl.intf.SessionEventHandler;
import com.bloomberg.bmq.impl.intf.SessionStatusHandler;
import com.bloomberg.bmq.impl.intf.SessionStatusHandler.SessionStatus;
import com.bloomberg.bmq.it.util.BmqBroker;
import com.bloomberg.bmq.it.util.BmqBrokerContainer;
import com.bloomberg.bmq.it.util.BmqBrokerSimulator;
import com.bloomberg.bmq.it.util.BmqBrokerSimulator.Mode;
//import com.bloomberg.bmq.it.util.BmqBrokerTestServer;
import com.bloomberg.bmq.it.util.TestTcpServer;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Exchanger;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpBrokerConnectionIT {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static final int DEFAULT_CALLBACK_STATUS_EXCHANGE_TIMEOUT = 60;
    static final Duration STOP_TIMEOUT = Duration.ofSeconds(30);
    static final int NO_CLIENT_REQUEST_TIMEOUT = 1; // sec

    private static void sleepForSeconds(int seconds) {
        try {
            Thread.sleep(seconds * 1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static URI getServerUri() {
        return URI.create("tcp://localhost:" + SystemUtil.getEphemeralPort());
    }

    static class TestSession {
        private final SessionHandler sessionHandler;
        private final BrokerConnection channel;
        private final RequestManager requestManager;
        private final Exchanger<StartStatus> startExchanger;
        private final Exchanger<StopStatus> stopExchanger;
        private final Exchanger<SessionStatus> sessionExchanger;
        private final Exchanger<Object> eventExchanger;

        private class SessionHandler
                implements SessionStatusHandler,
                        SessionEventHandler,
                        BrokerConnection.StartCallback,
                        BrokerConnection.StopCallback {

            final Logger cbLogger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

            public SessionHandler() {}

            @Override
            public void handleSessionStatus(SessionStatus status) {
                cbLogger.info("SessionStatus: {}", status);
                exchangeSessionStatus(status, DEFAULT_CALLBACK_STATUS_EXCHANGE_TIMEOUT);
            }

            @Override
            public void handleStartCb(StartStatus status) {
                logger.info("handleStartCb: {}", status);
                exchangeStartStatus(status, DEFAULT_CALLBACK_STATUS_EXCHANGE_TIMEOUT);
            }

            @Override
            public void handleStopCb(BrokerConnection.StopStatus status) {
                logger.info("handleStopCb: {}", status);
                exchangeStopStatus(status, DEFAULT_CALLBACK_STATUS_EXCHANGE_TIMEOUT);
            }

            @Override
            public void handleControlEvent(ControlEventImpl controlEvent) {
                logger.info("Got Control EventImpl: {}", controlEvent);

                try {
                    int result = requestManager.processResponse(controlEvent.controlChoice());

                    logger.info("Request manager process status: {}", result);
                } catch (JsonSyntaxException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void handleAckMessage(AckMessageImpl ackMsg) {
                logger.info("Got Ack message: {}", ackMsg);
            }

            @Override
            public void handlePushMessage(PushMessageImpl pushMsg) {
                logger.info("Got Push message: {}", pushMsg);
                exchangePushMessage(pushMsg, DEFAULT_CALLBACK_STATUS_EXCHANGE_TIMEOUT);
            }

            @Override
            public void handlePutEvent(PutEventImpl putEvent) {
                logger.info("Got Put EventImpl: {}", putEvent);
            }

            @Override
            public void handleConfirmEvent(ConfirmEventImpl confirmEvent) {
                logger.info("Got Confirm  EventImpl: {}", confirmEvent);
            }
        }

        public TestSession(ConnectionOptions opts) {
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            requestManager = new RequestManager(scheduler);
            sessionHandler = new SessionHandler();
            channel =
                    TcpBrokerConnection.createInstance(
                            opts,
                            new NettyTcpConnectionFactory(),
                            scheduler,
                            requestManager,
                            new EventsStats(),
                            sessionHandler, // SessionEventHandler
                            sessionHandler); // SessionStatusHandler

            startExchanger = new Exchanger<>();
            stopExchanger = new Exchanger<>();
            sessionExchanger = new Exchanger<>();
            eventExchanger = new Exchanger<>();
        }

        public void start() {
            channel.start(sessionHandler);
        }

        public void stop() {
            stop(STOP_TIMEOUT);
        }

        public void stop(Duration timeout) {
            channel.stop(sessionHandler, timeout);
        }

        public void drop() {
            channel.drop();
        }

        public GenericResult linger() {
            return channel.linger();
        }

        StartStatus startStatus() {
            return startStatus(DEFAULT_CALLBACK_STATUS_EXCHANGE_TIMEOUT);
        }

        StartStatus startStatus(int sec) {
            StartStatus startStatus = null;
            return exchangeStartStatus(startStatus, sec);
        }

        StopStatus stopStatus() {
            StopStatus stopStatus = null;
            return exchangeStopStatus(stopStatus, DEFAULT_CALLBACK_STATUS_EXCHANGE_TIMEOUT);
        }

        SessionStatus sessionStatus() {
            SessionStatus sessionStatus = null;
            return exchangeSessionStatus(sessionStatus, DEFAULT_CALLBACK_STATUS_EXCHANGE_TIMEOUT);
        }

        EventImpl getEvent() {
            EventImpl ev = null;
            return exchangeEvent(ev, DEFAULT_CALLBACK_STATUS_EXCHANGE_TIMEOUT);
        }

        PushMessageImpl getPush() {
            return getPush(DEFAULT_CALLBACK_STATUS_EXCHANGE_TIMEOUT);
        }

        PushMessageImpl getPush(int sec) {
            PushMessageImpl m = null;
            return exchangePushMessage(m, sec);
        }

        // TODO: think about some common exchange method, e.g.
        // Enum exchange(Enum v, int sec)
        private StartStatus exchangeStartStatus(StartStatus st, int sec) {
            StartStatus res = null;
            try {
                res =
                        startExchanger.exchange(
                                st, Argument.expectPositive(sec, "sec"), TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                logger.info("Exchange timeout");
                throw new IllegalStateException("Missed start status");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail();
            }
            return res;
        }

        private StopStatus exchangeStopStatus(StopStatus st, int sec) {
            StopStatus res = null;
            try {
                res =
                        stopExchanger.exchange(
                                st, Argument.expectPositive(sec, "sec"), TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                logger.info("Exchange timeout");
                throw new IllegalStateException("Missed stop status");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail();
            }
            return res;
        }

        private SessionStatus exchangeSessionStatus(SessionStatus st, int sec) {
            SessionStatus res = null;
            try {
                res =
                        sessionExchanger.exchange(
                                st, Argument.expectPositive(sec, "sec"), TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                logger.info("Exchange timeout");
                throw new IllegalStateException("Missed session status");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail();
            }
            return res;
        }

        private EventImpl exchangeEvent(EventImpl ev, int sec) {
            return (EventImpl) exchangeObject(ev, sec);
        }

        private PushMessageImpl exchangePushMessage(PushMessageImpl msg, int sec) {
            return (PushMessageImpl) exchangeObject(msg, sec);
        }

        private Object exchangeObject(Object ev, int sec) {
            Object res = null;
            try {
                res =
                        eventExchanger.exchange(
                                ev, Argument.expectPositive(sec, "sec"), TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                logger.info("Exchange timeout");
                throw new IllegalStateException("Missed event");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail();
            }
            return res;
        }
    }

    @Test
    public void testConnectionDown() throws Exception {

        logger.info("===========================================================");
        logger.info("BEGIN Testing TcpBrokerConnectionIT connection down.");
        logger.info("===========================================================");

        // 1) Bring up the broker.
        // 2) Invoke channel 'start'.
        // 3) Wait for start and SESSION_UP events.
        // 4) Stop the broker.
        // 5) Wait for SESSION_DOWN event.

        // 1) Bring up the broker.
        BmqBroker broker = BmqBroker.createStartedBroker();
        ConnectionOptions opts = new ConnectionOptions(broker.sessionOptions());
        TestSession session = new TestSession(opts);

        CompletableFuture<Void> stopFuture = null;
        try {
            // 2) Invoke channel 'start'.
            logger.info("Starting channel...");
            session.start();

            // 3) Wait for start and SESSION_UP events. Order matters.
            assertEquals(StartStatus.SUCCESS, session.startStatus());
            assertEquals(SessionStatus.SESSION_UP, session.sessionStatus());

            // 4) Stop the broker.
            stopFuture = broker.stopAsync();

            // 5) Wait for SESSION_DOWN event.
            assertEquals(SessionStatus.SESSION_DOWN, session.sessionStatus());
            logger.info("Channel is down");

            broker.setDropTmpFolder();
        } finally {
            session.stop();
            assertEquals(StopStatus.SUCCESS, session.stopStatus());
            assertEquals(GenericResult.SUCCESS, session.linger());

            // Give some time for the broker process to die
            if (stopFuture != null) {
                stopFuture.get();
            }

            broker.close();

            logger.info("=========================================================");
            logger.info("END Testing TcpBrokerConnectionIT connection down.");
            logger.info("=========================================================");
        }
    }

    @Test
    public void testReconnection() throws Exception {

        logger.info("=========================================================");
        logger.info("BEGIN Testing TcpBrokerConnectionIT re-connection.");
        logger.info("=========================================================");

        // 1) Bring up the broker.
        // 2) Invoke channel 'start'.
        // 3) Wait for start status callback.
        // 4) Wait for SESSION_UP event.
        // 5) Stop the broker.
        // 6) Wait for SESSION_DOWN event.
        // 7) Start the broker again.
        // 8) Wait for SESSION_UP event.
        // 9) Stop the broker.

        // 1) Bring up the broker
        BmqBroker broker = BmqBroker.createStartedBroker();
        ConnectionOptions opts = new ConnectionOptions(broker.sessionOptions());
        TestSession session = new TestSession(opts);

        CompletableFuture<Void> finalStopFuture = null;
        try {
            // 2) Invoke channel 'start'.
            logger.info("Starting channel...");
            session.start();

            // 3) Wait for start status callback.
            logger.info("Waiting start status...");
            assertEquals(StartStatus.SUCCESS, session.startStatus());

            // 4) Wait for SESSION_UP event.
            logger.info("Waiting session status...");
            assertEquals(SessionStatus.SESSION_UP, session.sessionStatus());

            // 5) Stop the broker.
            CompletableFuture<Void> stopFuture = broker.stopAsync();

            // 6) Wait for SESSION_DOWN event
            logger.info("Waiting session down status...");
            assertEquals(SessionStatus.SESSION_DOWN, session.sessionStatus());

            // 7) Start the broker again.
            stopFuture.get();
            broker.start();

            // 8) Wait for SESSION_UP event
            logger.info("Waiting session up status...");
            assertEquals(SessionStatus.SESSION_UP, session.sessionStatus());

            // 9) Stop the broker.
            finalStopFuture = broker.stopAsync();

            assertEquals(SessionStatus.SESSION_DOWN, session.sessionStatus());

            broker.setDropTmpFolder();
        } finally {
            session.stop();
            assertEquals(StopStatus.SUCCESS, session.stopStatus());
            assertEquals(GenericResult.SUCCESS, session.linger());

            // Give some time for the broker process to die
            if (finalStopFuture != null) {
                finalStopFuture.get();
            }

            broker.close();

            logger.info("=======================================================");
            logger.info("END Testing TcpBrokerConnectionIT re-connection.");
            logger.info("=======================================================");
        }
    }

    @Test
    public void testDropAndReconnection() throws Exception {

        logger.info("===========================================================");
        logger.info("BEGIN Testing TcpBrokerConnectionIT drop and re-connection.");
        logger.info("===========================================================");

        // 1) Bring up the broker.
        // 2) Invoke channel 'start'.
        // 3) Wait for start status callback.
        // 4) Wait for SESSION_UP event.
        // 5) Drop the connection and initiate reconnecting.
        // 6) Wait for SESSION_DOWN event.
        // 7) Wait for SESSION_UP event.
        // 8) Stop the broker.

        // 1) Bring up the broker
        BmqBroker broker = BmqBroker.createStartedBroker();
        ConnectionOptions opts = new ConnectionOptions(broker.sessionOptions());
        TestSession session = new TestSession(opts);

        CompletableFuture<Void> finalStopFuture = null;
        try {
            // 2) Invoke channel 'start'.
            logger.info("Starting channel...");
            session.start();

            // 3) Wait for start status callback.
            logger.info("Waiting start status...");
            assertEquals(StartStatus.SUCCESS, session.startStatus());

            // 4) Wait for SESSION_UP event.
            logger.info("Waiting session status...");
            assertEquals(SessionStatus.SESSION_UP, session.sessionStatus());

            // 5) Drop the connection and initiate reconnecting.
            session.drop();

            // 6) Wait for SESSION_DOWN event
            logger.info("Waiting session down status...");
            assertEquals(SessionStatus.SESSION_DOWN, session.sessionStatus());

            // 7) Wait for SESSION_UP event
            logger.info("Waiting session up status...");
            assertEquals(SessionStatus.SESSION_UP, session.sessionStatus());

            // 8) Stop the broker.
            finalStopFuture = broker.stopAsync();

            assertEquals(SessionStatus.SESSION_DOWN, session.sessionStatus());

            broker.setDropTmpFolder();
        } finally {
            session.stop();
            assertEquals(StopStatus.SUCCESS, session.stopStatus());
            assertEquals(GenericResult.SUCCESS, session.linger());

            // Give some time for the broker process to die
            if (finalStopFuture != null) {
                finalStopFuture.get();
            }

            broker.close();

            logger.info("=========================================================");
            logger.info("END Testing TcpBrokerConnectionIT drop and re-connection.");
            logger.info("=========================================================");
        }
    }

    @Test
    public void testRestart() throws Exception {

        logger.info("===================================================");
        logger.info("BEGIN Testing TcpBrokerConnectionIT restart.");
        logger.info("===================================================");

        // 1) Bring up the broker.
        // 2) Invoke channel 'start'.
        // 3) Invoke channel 'stop' and ensure that the start operation
        //    in progress gets cancelled and 'stop' succeeds.
        // 4) Wait for start status callback.
        // 5) Wait for SESSION_UP event.
        // 6) Stop the channel.
        // 7) Wait for SESSION_DOWN event.
        // 8) Wait for stop status callback.
        // 9) Start the channel again.
        // 10) Wait for start status callback.
        // 11) Wait for SESSION_UP event.
        // 12) Stop the broker.

        logger.info(" 1) Bring up the broker");
        BmqBroker broker = BmqBroker.createStartedBroker();
        ConnectionOptions opts = new ConnectionOptions(broker.sessionOptions());
        TestSession session = new TestSession(opts);

        CompletableFuture<Void> stopFuture = null;
        try {
            logger.info(" 2) Invoke channel 'start'.");
            session.start();

            logger.info(
                    " 3) Invoke channel 'stop' and ensure that the start operation in progress gets cancelled and 'stop' succeeds.");
            session.stop();
            assertEquals(StartStatus.CANCELLED, session.startStatus());
            assertEquals(StopStatus.SUCCESS, session.stopStatus());

            session.start();

            logger.info(" 4) Wait for start status callback.");
            assertEquals(StartStatus.SUCCESS, session.startStatus());

            logger.info(" 5) Wait for SESSION_UP event.");
            assertEquals(SessionStatus.SESSION_UP, session.sessionStatus());

            logger.info(" 6) Stop the channel.");
            session.stop();

            logger.info(" 7) Wait for SESSION_DOWN event.");
            assertEquals(SessionStatus.SESSION_DOWN, session.sessionStatus());

            logger.info(" 8) Wait for stop status callback.");
            assertEquals(StopStatus.SUCCESS, session.stopStatus());

            logger.info(" 9) Start the channel again.");
            session.start();

            logger.info(" 10) Wait for start status callback.");
            assertEquals(StartStatus.SUCCESS, session.startStatus());

            logger.info(" 11) Wait for SESSION_UP event.");
            assertEquals(SessionStatus.SESSION_UP, session.sessionStatus());

            logger.info(" 12) Stop the broker.");
            stopFuture = broker.stopAsync();

            assertEquals(SessionStatus.SESSION_DOWN, session.sessionStatus());

            broker.setDropTmpFolder();
        } finally {
            session.stop();
            assertEquals(StopStatus.SUCCESS, session.stopStatus());
            assertEquals(GenericResult.SUCCESS, session.linger());

            // Give some time for the broker process to die
            if (stopFuture != null) {
                stopFuture.get();
            }

            broker.close();

            logger.info("=================================================");
            logger.info("END Testing TcpBrokerConnectionIT restart.");
            logger.info("=================================================");
        }
    }

    @Test
    public void testNegotiationMpsEx() throws IOException {
        final ConnectionOptions opts = new ConnectionOptions();
        opts.setBrokerUri(getServerUri());

        final TestTcpServer[] servers =
                new TestTcpServer[] {
                    new BmqBrokerSimulator(opts.brokerUri().getPort(), Mode.BMQ_AUTO_MODE),
                    BmqBrokerContainer.createContainer(opts.brokerUri().getPort()),
                    // BmqBrokerTestServer.createStoppedBroker(opts.brokerUri().getPort())
                };

        assertFalse(servers[0].isOldStyleMessageProperties());
        assertFalse(servers[1].isOldStyleMessageProperties());
        //assertFalse(servers[2].isOldStyleMessageProperties());

        for (TestTcpServer server : servers) {
            TestSession session = new TestSession(opts);

            // 1) Bring up the the server
            // 2) Invoke channel 'start' and ensure that it succeeds.
            // 3) Wait for start status callback
            // 4) Check that the "broker" supports new style message properties
            // 5) Linger client session.
            // 6) Stop the server.

            logger.info("Start the server.");
            server.start();

            sleepForSeconds(1);

            try {
                // 2) Invoke channel 'start' and ensure that it succeeds.
                logger.info("Starting channel...");

                session.start();

                final int timeout = (int) opts.startAttemptTimeout().getSeconds();

                // 3) Wait for start status callback.
                assertEquals(StartStatus.SUCCESS, session.startStatus(timeout));
                assertEquals(SessionStatus.SESSION_UP, session.sessionStatus());

                // 4) Check the connection for broker response
                logger.info(
                        "Server: {}, old style properties: {}",
                        server,
                        server.isOldStyleMessageProperties());

                assertEquals(
                        server.isOldStyleMessageProperties(),
                        session.channel.isOldStyleMessageProperties());

                if (server instanceof BmqBroker) {
                    ((BmqBroker) server).setDropTmpFolder();
                }
            } finally {
                // 5) Stop client session.
                session.stop();
                assertEquals(SessionStatus.SESSION_DOWN, session.sessionStatus());

                assertEquals(StopStatus.SUCCESS, session.stopStatus());
                assertEquals(GenericResult.SUCCESS, session.linger());

                // 6) Close the server.
                server.close();
            }
        }
    }

    private void testNegotiationFailed(StatusCategory status) {
        final int NUM_RETRIES = 1;

        sleepForSeconds(10);
        ConnectionOptions opts = new ConnectionOptions();
        opts.setStartNumRetries(NUM_RETRIES).setBrokerUri(getServerUri());

        TestSession session = new TestSession(opts);

        // 1) Bring up the netty server, which supports connection but
        //    wont send negotiation response.
        // 2) Invoke channel 'start' and ensure that it succeeds.
        // 3) Wait for start status callback and ensure it reports error status.
        // 4) Stop the server.
        // 5) Linger client session.

        logger.info("Using netty-based server.");
        // To emulate negotiation timeout start the server in a non-echo mode
        // and to emulate wrong negotiation response start in an echo mode
        BmqBrokerSimulator server =
                new BmqBrokerSimulator(opts.brokerUri().getPort(), Mode.BMQ_MANUAL_MODE);
        server.start();

        server.pushItem(status);

        sleepForSeconds(1);

        try {
            // 2) Invoke channel 'start' and ensure that it succeeds.
            logger.info("Starting channel...");

            session.start();

            long timeout =
                    opts.startAttemptTimeout()
                            .plus(opts.startRetryInterval().multipliedBy(NUM_RETRIES))
                            .getSeconds();

            // 3) Wait for start status callback and ensure it reports error status.
            assertEquals(session.startStatus((int) timeout + 1), StartStatus.NEGOTIATION_FAILURE);
        } finally {
            // 4) Stop the server.
            server.stop();

            // 5) Linger client session.
            session.stop();
            assertEquals(StopStatus.SUCCESS, session.stopStatus());
            assertEquals(GenericResult.SUCCESS, session.linger());
        }
    }

    @Test
    public void testNegotiationTimeout() {
        logger.info("===============================================================");
        logger.info("BEGIN Testing TcpBrokerConnectionIT negotiation timeout.");
        logger.info("===============================================================");

        // Server should not answer to the negotiation message
        testNegotiationFailed(StatusCategory.E_TIMEOUT);

        logger.info("=============================================================");
        logger.info("END Testing TcpBrokerConnectionIT negotiation timeout.");
        logger.info("=============================================================");
    }

    @Test
    public void testNegotiationError() {
        logger.info("=============================================================");
        logger.info("BEGIN Testing TcpBrokerConnectionIT negotiation error.");
        logger.info("=============================================================");

        // Server should answer with an error negotiation message
        testNegotiationFailed(StatusCategory.E_REFUSED);

        logger.info("===========================================================");
        logger.info("END Testing TcpBrokerConnectionIT negotiation error.");
        logger.info("===========================================================");
    }

    @Test
    public void testNegotiationInterrupted() {
        logger.info("===================================================================");
        logger.info("BEGIN Testing TcpBrokerConnectionIT negotiation interrupted.");
        logger.info("===================================================================");

        // Server should goes down after receiving the negotiation message
        // without sending any answer
        testNegotiationFailed(StatusCategory.E_NOT_READY);

        logger.info("=================================================================");
        logger.info("END Testing TcpBrokerConnectionIT negotiation interrupted.");
        logger.info("=================================================================");
    }

    @Test
    public void testConnectionTimeout() {
        logger.info("==============================================================");
        logger.info("BEGIN Testing TcpBrokerConnectionIT connection timeout.");
        logger.info("==============================================================");

        // 1) Without server invoke channel 'start'.
        // 2) Wait for start status callback and ensure it reports error status.
        // 3) Linger client session.

        final int NUM_RETRIES = 1;
        ConnectionOptions opts = new ConnectionOptions();
        opts.setStartNumRetries(NUM_RETRIES).setBrokerUri(getServerUri());

        TestSession session = new TestSession(opts);

        // 1) Invoke channel 'start'.
        logger.info("Starting channel...");

        try {
            session.start();

            // 2) Wait for start status callback and ensure it reports error status.
            long timeout =
                    opts.startAttemptTimeout()
                            .plus(opts.startRetryInterval().multipliedBy(NUM_RETRIES))
                            .getSeconds();

            assertEquals(StartStatus.CONNECT_FAILURE, session.startStatus((int) timeout + 1));
        } finally {
            // 3) Linger client session.
            session.stop();
            assertEquals(StopStatus.SUCCESS, session.stopStatus());
            assertEquals(GenericResult.SUCCESS, session.linger());

            logger.info("============================================================");
            logger.info("END Testing TcpBrokerConnectionIT connection timeout.");
            logger.info("============================================================");
        }
    }

    @Test
    public void testConnectionDelay() throws IOException {
        logger.info("============================================================");
        logger.info("BEGIN Testing TcpBrokerConnectionIT connection delay.");
        logger.info("============================================================");

        // 1) Without server invoke channel 'start'.
        // 2) Wait some time, the session should try to reconnect in the background.
        // 3) Bring up the broker
        // 4) Wait for successful start and SESSION_UP statuses.
        // 5) Linger client session.
        // 6) Stop the broker.

        BmqBroker broker = BmqBroker.createStoppedBroker();
        SessionOptions opts = broker.sessionOptions();

        TestSession session = new TestSession(new ConnectionOptions(opts));

        try {
            // 1) Invoke channel 'start'.
            logger.info("Starting channel...");

            session.start();

            // 2) Wait some time, the session should try to reconnect in the background.
            int timeout = (int) opts.startTimeout().getSeconds();
            sleepForSeconds(timeout / 2);

            // 3) Bring up the broker
            CompletableFuture<Void> startFuture = broker.startAsync();

            // 4) Wait for successful start and SESSION_UP statuses.
            assertEquals(StartStatus.SUCCESS, session.startStatus());
            assertEquals(SessionStatus.SESSION_UP, session.sessionStatus());

            startFuture.get();

            broker.setDropTmpFolder();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            // 5) Linger client session.
            session.stop();

            assertEquals(SessionStatus.SESSION_DOWN, session.sessionStatus());
            assertEquals(StopStatus.SUCCESS, session.stopStatus());
            assertEquals(GenericResult.SUCCESS, session.linger());

            // 6) Stop the broker
            broker.close();

            logger.info("==========================================================");
            logger.info("END Testing TcpBrokerConnectionIT connection delay.");
            logger.info("==========================================================");
        }
    }

    @Test
    public void testMultipleStarts() throws IOException {
        logger.info("===========================================================");
        logger.info("BEGIN Testing TcpBrokerConnectionIT multiple starts.");
        logger.info("===========================================================");

        // 1) Bring up the broker
        // 2) Invoke and wait for statuses:
        //    a) stop  => SUCCESS (already stopped)
        //    b) start => ...
        //    c) start => NOT_SUPPORTED
        //    d) stop  => ...
        //    ...   b) => CANCELLED
        //    ...   d) => SUCCESS
        // 3) Wait for successful start and SESSION_UP statuses.
        // 4) Stop the session.
        // 5) Stop the broker.

        // 1) Bring up the broker
        BmqBroker broker = BmqBroker.createStartedBroker();
        ConnectionOptions opts = new ConnectionOptions(broker.sessionOptions());
        TestSession session = new TestSession(opts);

        try {
            // 2) Invoke and wait for statuses:
            //    a) stop  => SUCCESS (already stopped)
            //    b) start => ...
            //    c) start => NOT_SUPPORTED
            //    d) stop  => ...
            //    ...   b) => CANCELLED
            //    ...   d) => SUCCESS (stopped)
            session.stop();
            assertEquals(StopStatus.SUCCESS, session.stopStatus());
            session.start();
            session.start();
            assertEquals(StartStatus.NOT_SUPPORTED, session.startStatus());
            session.stop();
            assertEquals(StartStatus.CANCELLED, session.startStatus());
            assertEquals(StopStatus.SUCCESS, session.stopStatus());

            // 3) Start session and wait for successful start and SESSION_UP statuses.
            session.start();
            assertEquals(StartStatus.SUCCESS, session.startStatus());
            assertEquals(SessionStatus.SESSION_UP, session.sessionStatus());

            broker.setDropTmpFolder();
        } finally {
            // 4) Stop the session
            session.stop();
            assertEquals(SessionStatus.SESSION_DOWN, session.sessionStatus());
            assertEquals(StopStatus.SUCCESS, session.stopStatus());
            assertEquals(GenericResult.SUCCESS, session.linger());

            // 5) Stop the broker.
            broker.close();

            logger.info("=========================================================");
            logger.info("END Testing TcpBrokerConnectionIT multiple starts.");
            logger.info("=========================================================");
        }
    }

    @Test
    public void testMultipleRestarts() throws IOException {
        logger.info("=============================================================");
        logger.info("BEGIN Testing TcpBrokerConnectionIT multiple restarts.");
        logger.info("=============================================================");

        // 1) Bring up the broker
        // 2) Start session and wait for successful start and SESSION_UP statuses.
        // 3) Invoke and wait for statuses:
        //    a) start => SUCCESS (already started)
        //    b) stop  => ...
        //    c) start => ...
        //    d) stop  => ...
        //    ...      => SESSION_DOWN
        //    ...   c) => NOT_SUPPORTED
        //    ...   d) => SUCCESS (stopped)
        // 4) Start session and wait for successful start and SESSION_UP statuses.
        // 5) Stop the session.
        // 6) Stop the broker.

        // 1) Bring up the broker
        BmqBroker broker = BmqBroker.createStartedBroker();
        ConnectionOptions opts = new ConnectionOptions(broker.sessionOptions());
        TestSession session = new TestSession(opts);

        try {
            // 2) Start session and wait for successful start and SESSION_UP statuses.
            session.start();
            assertEquals(StartStatus.SUCCESS, session.startStatus());
            assertEquals(SessionStatus.SESSION_UP, session.sessionStatus());

            // 3) Invoke and wait for statuses:
            //    a) start  => SUCCESS (already started)
            //    b) stop  => ...
            //    c) start => ...
            //    d) stop  => ...
            //    ...      => SESSION_DOWN
            //    ...   c) => NOT_SUPPORTED
            //    ...   d) => SUCCESS (stopped)
            session.start();
            assertEquals(StartStatus.SUCCESS, session.startStatus());
            session.stop();
            session.start();
            assertEquals(SessionStatus.SESSION_DOWN, session.sessionStatus());
            assertEquals(StartStatus.NOT_SUPPORTED, session.startStatus());
            assertEquals(StopStatus.SUCCESS, session.stopStatus());

            // 4) Start session and wait for successful start and SESSION_UP statuses.
            session.start();
            assertEquals(StartStatus.SUCCESS, session.startStatus());
            assertEquals(SessionStatus.SESSION_UP, session.sessionStatus());

            broker.setDropTmpFolder();
        } finally {
            // 5) Stop the session
            session.stop();
            assertEquals(SessionStatus.SESSION_DOWN, session.sessionStatus());
            assertEquals(StopStatus.SUCCESS, session.stopStatus());
            assertEquals(GenericResult.SUCCESS, session.linger());

            // 6) Stop the broker.
            broker.close();

            logger.info("===========================================================");
            logger.info("END Testing TcpBrokerConnectionIT multiple restarts.");
            logger.info("===========================================================");
        }
    }

    @Test
    public void testLinger() {
        logger.info("=============================================================");
        logger.info("BEGIN Testing TcpBrokerConnectionIT multiple restarts.");
        logger.info("=============================================================");

        // 1) Create session and linger it.
        // 2) Verify that start and stop calls throw IllegalStateException

        // 1) Create session and linger it.
        SessionOptions opts = SessionOptions.createDefault();
        TestSession session = new TestSession(new ConnectionOptions(opts));
        assertEquals(GenericResult.SUCCESS, session.linger());

        // 2) Verify that start and stop calls throw IllegalStateException
        try {
            session.start();
            fail();
        } catch (IllegalStateException e) {
            // OK
        }
        try {
            session.stop();
            fail();
        } catch (IllegalStateException e) {
            // OK
        }

        logger.info("===========================================================");
        logger.info("END Testing TcpBrokerConnectionIT multiple restarts.");
        logger.info("===========================================================");
    }

    @Test
    public void testPartialPayload() throws IOException {
        logger.info("===========================================================");
        logger.info("BEGIN Testing TcpBrokerConnectionIT partial payload.");
        logger.info("===========================================================");
        // Check that after recieving a partial event content and if the server goes
        // down and up again the client session will operate normally inspite of the
        // payload loss.
        // Steps:
        // 1) Bring up the netty server that supports negotiation.
        // 2) Invoke session 'start' and ensure that it succeeds.
        // 3) From the server send PUSH event and receive it on the client side.
        // 4) From the server send partial PUSH event (only EventHeader)
        // 5) Stop the server, then start it again.
        // 6) Verify that session goes down then up again.
        // 7) Stop the session and the server.

        final int NUM_RETRIES = 1;

        ConnectionOptions opts = new ConnectionOptions();
        opts.setStartNumRetries(NUM_RETRIES).setBrokerUri(getServerUri());

        TestSession session = new TestSession(opts);

        // 1) Bring up the netty server that supports negotiation.

        BmqBrokerSimulator server =
                new BmqBrokerSimulator(opts.brokerUri().getPort(), Mode.BMQ_AUTO_MODE);
        server.start();

        sleepForSeconds(1);

        try {
            // 2) Invoke session 'start' and ensure that it succeeds.

            logger.info("Starting channel...");

            session.start();

            assertEquals(StartStatus.SUCCESS, session.startStatus(15));
            assertEquals(SessionStatus.SESSION_UP, session.sessionStatus());

            // 3) From the server send PUSH event and receive it on the client side.

            server.writePushRequest(1);

            PushMessageImpl pm = session.getPush();
            assertNotNull(pm);

            ByteBufferOutputStream bbos = new ByteBufferOutputStream();
            try {
                pm.header().streamOut(bbos);
            } catch (IOException e) {
                logger.error("Failed to stream out event header: ", e);
                fail();
            }

            // 4) From the server send partial PUSH event (only EventHeader)
            server.write(bbos.reset());

            // 5) Stop the server, then start it again.
            server.stop();
            assertEquals(SessionStatus.SESSION_DOWN, session.sessionStatus());
            server.start();

            // 6) Verify that session goes down then up again.
            assertEquals(SessionStatus.SESSION_UP, session.sessionStatus());
        } finally {
            // 7) Stop the session and the server.
            session.stop();
            assertEquals(SessionStatus.SESSION_DOWN, session.sessionStatus());
            assertEquals(StopStatus.SUCCESS, session.stopStatus());
            assertEquals(GenericResult.SUCCESS, session.linger());

            server.stop();

            logger.info("=========================================================");
            logger.info("END Testing TcpBrokerConnectionIT partial payload.");
            logger.info("=========================================================");
        }
    }

    @Test
    public void testUnresolvedUri() throws Exception {
        logger.info("======================================================");
        logger.info("BEGIN Testing TcpBrokerConnectionIT testUnresolvedUri.");
        logger.info("======================================================");

        // 1) Create session with a valid but unresolved broker URI
        // 2) Verify that start call returns CONNECT_FAILURE status
        // 3) Stop and linger the session

        // 1) Create session with a valid but unresolved broker URI
        SessionOptions opts =
                SessionOptions.builder().setBrokerUri(new URI("http://abc:1")).build();
        TestSession session = new TestSession(new ConnectionOptions(opts));

        try {
            // 2) Verify that start call returns CONNECT_FAILURE status
            session.start();

            assertEquals(StartStatus.CONNECT_FAILURE, session.startStatus());

        } finally {
            // 3) Stop and linger the session
            session.stop();
            assertEquals(StopStatus.SUCCESS, session.stopStatus());
            assertEquals(GenericResult.SUCCESS, session.linger());
        }

        logger.info("====================================================");
        logger.info("END Testing TcpBrokerConnectionIT testUnresolvedUri.");
        logger.info("====================================================");
    }

    @Test
    public void testUnknownCompressionType() throws IOException {
        logger.info("=============================================================");
        logger.info("BEGIN Testing TcpBrokerConnectionIT unknown compression type.");
        logger.info("=============================================================");
        // Check that after recieving a push event containing a message with
        // unknown compression type only first 'k-1' messages are dispatched
        // where 'k' is the index of invalid message
        // Steps:
        // 1) Bring up the netty server that supports negotiation.
        // 2) Invoke session 'start' and ensure that it succeeds.
        // 3) Generate PUSH event with N messages with invalid message on K index,
        // 4) From the server send PUSH event
        // 5) Ensure that only K-2 messages are dispatched.
        //    Other messages are discarded due to exception
        //    (see PushEventImpl and PushMessageIterator for details)
        // 6) Stop the session and the server.

        final int NUM_RETRIES = 1;

        ConnectionOptions opts = new ConnectionOptions();
        opts.setStartNumRetries(NUM_RETRIES).setBrokerUri(getServerUri());

        TestSession session = new TestSession(opts);

        // 1) Bring up the netty server that supports negotiation.

        BmqBrokerSimulator server =
                new BmqBrokerSimulator(opts.brokerUri().getPort(), Mode.BMQ_AUTO_MODE);
        server.start();

        sleepForSeconds(1);

        try {
            // 2) Invoke session 'start' and ensure that it succeeds.

            logger.info("Starting channel...");

            session.start();

            assertEquals(StartStatus.SUCCESS, session.startStatus(15));
            assertEquals(SessionStatus.SESSION_UP, session.sessionStatus());

            // 3) Generate PUSH event with N messages with invalid message on K index,

            byte[] bytes = new byte[Protocol.COMPRESSION_MIN_APPDATA_SIZE + 1];

            bytes[0] = 1;
            bytes[Protocol.COMPRESSION_MIN_APPDATA_SIZE - 1] = 1;

            final int N = 15, K = 10;

            logger.info("Generate event with {} message, where message #{} is invalid", N, K);

            final MessagePropertiesImpl props = new MessagePropertiesImpl();
            props.setPropertyAsInt32("routingId", 42);
            props.setPropertyAsInt64("timestamp", 1234567890L);

            ByteBufferOutputStream bbos = new ByteBufferOutputStream();

            EventHeader header = new EventHeader();
            header.setType(EventType.PUSH);

            final int unpackedSize = props.totalSize() + bytes.length;
            final int numPaddingBytes = ProtocolUtil.calculatePadding(unpackedSize);

            header.setLength(
                    EventHeader.HEADER_SIZE
                            + (PushHeader.HEADER_SIZE_FOR_SCHEMA_ID
                                            + unpackedSize
                                            + numPaddingBytes)
                                    * N);

            header.streamOut(bbos);

            // to avoid confusion, start loop from i = 1
            for (int i = 1; i <= N; i++) {
                PushMessageImpl pushMsg = new PushMessageImpl();

                pushMsg.appData().setPayload(ByteBuffer.wrap(bytes));

                pushMsg.appData().setProperties(props);
                pushMsg.appData().setIsOldStyleProperties(server.isOldStyleMessageProperties());

                pushMsg.compressData();

                // override compression type to unknown value (by this moment)
                // for K-th message
                if (i == K) {
                    pushMsg.header().setCompressionType(7);
                }

                assertEquals(unpackedSize, pushMsg.appData().unpackedSize());
                assertEquals(numPaddingBytes, pushMsg.appData().numPaddingBytes());

                pushMsg.streamOut(bbos);
            }

            // 4) From the server send PUSH event
            server.write(bbos.reset());

            // 5) Ensure that only K-2 messages are dispatched

            // Check first K-2 messages
            for (int i = 1; i <= K - 2; i++) {
                PushMessageImpl pm = session.getPush();
                assertNotNull(pm);

                assertArrayEquals(
                        new ByteBuffer[] {ByteBuffer.wrap(bytes)}, pm.appData().payload());
            }

            logger.info("Successfully dispatched {} messages", K - 2);

            // We should not get 'K-1'-th message.
            // Use smaller timeout because the event has been already received
            // from the broker
            try {
                PushMessageImpl pm = session.getPush(1);
                logger.info("PUSH message: {}", pm);
                fail();
            } catch (IllegalStateException e) {
                assertEquals("Missed event", e.getMessage());
                logger.info("[EXPECTED] Failed to get message #{}", K - 1);
            }

        } finally {
            // 6) Stop the session and the server.
            session.stop();
            assertEquals(SessionStatus.SESSION_DOWN, session.sessionStatus());
            assertEquals(StopStatus.SUCCESS, session.stopStatus());
            assertEquals(GenericResult.SUCCESS, session.linger());

            server.stop();

            logger.info("===========================================================");
            logger.info("END Testing TcpBrokerConnectionIT unknown compression type.");
            logger.info("===========================================================");
        }
    }

    @Test
    public void testStopConnection() {
        logger.info("===========================================================");
        logger.info("BEGIN Testing TcpBrokerConnectionIT stop connection.");
        logger.info("===========================================================");
        // Check connection stop method
        // Steps:
        // 1) Bring up the netty server that supports negotiation.
        // 2) Invoke session 'start' and ensure that it succeeds.
        // 3) Call stop with null timeout
        // 4) Call stop with negative timeout
        // 5) Call stop with zero timeout
        // 6) Call stop with proper timeout
        // 7) Linger the connection and call stop again
        // 7) Stop the server.

        final int NUM_RETRIES = 1;

        ConnectionOptions opts = new ConnectionOptions();
        opts.setStartNumRetries(NUM_RETRIES).setBrokerUri(getServerUri());

        TestSession session = new TestSession(opts);

        // 1) Bring up the netty server that supports negotiation.

        BmqBrokerSimulator server =
                new BmqBrokerSimulator(opts.brokerUri().getPort(), Mode.BMQ_AUTO_MODE);
        server.start();

        try {
            // 2) Invoke session 'start' and ensure that it succeeds.

            logger.info("Starting channel...");

            session.start();

            assertEquals(StartStatus.SUCCESS, session.startStatus(15));
            assertEquals(SessionStatus.SESSION_UP, session.sessionStatus());

            // 3) Call stop with null timeout
            try {
                session.stop(null);
                fail(); // should not get here
            } catch (IllegalArgumentException e) {
                assertEquals("'timeout' must be non-null", e.getMessage());
            }

            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // 4) Call stop with negative timeout
            try {
                session.stop(Duration.ofSeconds(-1));
                fail(); // should not get here
            } catch (IllegalArgumentException e) {
                assertEquals("'timeout' must be positive", e.getMessage());
            }

            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // 5) Call stop with zero timeout
            try {
                session.stop(Duration.ofSeconds(0));
                fail(); // should not get here
            } catch (IllegalArgumentException e) {
                assertEquals("'timeout' must be positive", e.getMessage());
            }

            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // 6) Call stop with proper timeout
            session.stop(Duration.ofSeconds(5));
            assertEquals(SessionStatus.SESSION_DOWN, session.sessionStatus());
            assertEquals(StopStatus.SUCCESS, session.stopStatus());

            assertNotNull(server.nextClientRequest());

            // 7) Linger the connection and call stop again
            assertEquals(GenericResult.SUCCESS, session.linger());

            try {
                session.stop();
                fail();
            } catch (IllegalStateException e) {
                assertEquals("The connection has been lingered", e.getMessage());
            }
        } finally {
            server.stop();

            logger.info("=========================================================");
            logger.info("END Testing TcpBrokerConnectionIT stop connection.");
            logger.info("=========================================================");
        }
    }
}
