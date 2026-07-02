/*
 * Copyright 2026 Bloomberg Finance L.P.
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
package com.bloomberg.bmq.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bloomberg.bmq.AuthnCredential;
import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.SessionOptions.AuthnCredentialCb;
import com.bloomberg.bmq.impl.infr.msg.AuthenticationMessage;
import com.bloomberg.bmq.impl.infr.msg.BrokerResponse;
import com.bloomberg.bmq.impl.infr.msg.ClientIdentity;
import com.bloomberg.bmq.impl.infr.msg.NegotiationMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.Status;
import com.bloomberg.bmq.impl.infr.msg.StatusCategory;
import com.bloomberg.bmq.impl.infr.net.ConnectionOptions;
import com.bloomberg.bmq.impl.infr.proto.AuthenticationEventBuilder;
import com.bloomberg.bmq.impl.infr.proto.Protocol;
import com.bloomberg.bmq.impl.infr.proto.RequestManager;
import com.bloomberg.bmq.impl.infr.proto.SchemaEventBuilder;
import com.bloomberg.bmq.impl.infr.stat.EventsStats;
import com.bloomberg.bmq.impl.intf.BrokerConnection.StartStatus;
import com.bloomberg.bmq.impl.intf.SessionEventHandler;
import com.bloomberg.bmq.impl.intf.SessionStatusHandler;
import com.bloomberg.bmq.util.TestHelpers;
import com.bloomberg.bmq.util.TestTcpConnection;
import com.bloomberg.bmq.util.TestTcpConnectionFactory;
import com.google.gson.Gson;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TcpBrokerConnectionReauthTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private ScheduledExecutorService scheduler;
    private TestTcpConnectionFactory connectionFactory;
    private RequestManager requestManager;
    private EventsStats eventsStats;
    private SessionEventHandler sessionEventHandler;
    private SessionStatusHandler sessionStatusHandler;

    @BeforeEach
    void setUp() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        connectionFactory = new TestTcpConnectionFactory();
        requestManager = new RequestManager(scheduler);
        eventsStats = new EventsStats();
        sessionEventHandler =
                new SessionEventHandler() {
                    public void handleControlEvent(
                            com.bloomberg.bmq.impl.infr.proto.ControlEventImpl e) {}

                    public void handleAckMessage(
                            com.bloomberg.bmq.impl.infr.proto.AckMessageImpl e) {}

                    public void handlePushMessage(
                            com.bloomberg.bmq.impl.infr.proto.PushMessageImpl e) {}

                    public void handlePutEvent(com.bloomberg.bmq.impl.infr.proto.PutEventImpl e) {}

                    public void handleConfirmEvent(
                            com.bloomberg.bmq.impl.infr.proto.ConfirmEventImpl e) {}
                };
        sessionStatusHandler = status -> {};
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    private TcpBrokerConnection createConnection(AuthnCredentialCb authnCb) {
        SessionOptions.Builder builder = SessionOptions.builder();
        if (authnCb != null) {
            builder.setAuthnCredentialCb(authnCb);
        }
        SessionOptions sessionOptions = builder.build();
        ConnectionOptions options = new ConnectionOptions(sessionOptions);

        return new TcpBrokerConnection(
                options,
                connectionFactory,
                scheduler,
                requestManager,
                eventsStats,
                sessionEventHandler,
                sessionStatusHandler);
    }

    private ByteBuffer[] buildAuthResponse(StatusCategory category, Integer lifetimeMs)
            throws IOException {
        String lifetimeField = lifetimeMs != null ? ",\"lifetimeMs\":" + lifetimeMs : "";
        String json =
                "{\"authenticationResponse\":"
                        + "{\"status\":{\"category\":\""
                        + category.name()
                        + "\",\"code\":0,\"message\":\"\"}"
                        + lifetimeField
                        + "}}";
        AuthenticationMessage msg = new Gson().fromJson(json, AuthenticationMessage.class);
        AuthenticationEventBuilder builder = new AuthenticationEventBuilder();
        builder.setMessage(msg);
        return builder.build();
    }

    private void sendNegotiationResponse(TestTcpConnection conn) throws IOException {
        ByteBuffer[] negoRequest = conn.nextWriteRequest();
        assertNotNull(negoRequest);

        NegotiationMessageChoice negMsg = new NegotiationMessageChoice();
        negMsg.makeBrokerResponse();
        BrokerResponse brokerResponse = negMsg.brokerResponse();
        brokerResponse.setBrokerVersion(1);
        brokerResponse.setIsDeprecatedSdk(false);
        brokerResponse.setProtocolversion(Protocol.VERSION);
        Status status = new Status();
        status.setCategory(StatusCategory.E_SUCCESS);
        brokerResponse.setResult(status);
        ClientIdentity clientIdentity = new ClientIdentity();
        clientIdentity.setFeatures("MPS:MESSAGE_PROPERTIES_EX");
        brokerResponse.setOriginalRequest(clientIdentity);

        SchemaEventBuilder builder = new SchemaEventBuilder();
        builder.setMessage(negMsg);
        conn.sendResponse(builder.build());
    }

    private CompletableFuture<StartStatus> startConnection(TcpBrokerConnection connection) {
        CompletableFuture<StartStatus> startFuture = new CompletableFuture<>();
        connection.start(startFuture::complete);
        return startFuture;
    }

    private void startWithAuth(
            TcpBrokerConnection connection, TestTcpConnection testConn, int lifetimeMs)
            throws Exception {
        CompletableFuture<StartStatus> startFuture = startConnection(connection);

        // Consume the authentication request
        ByteBuffer[] authnRequest = testConn.nextWriteRequest();
        assertNotNull(authnRequest);

        // Send auth response with lifetime
        testConn.sendResponse(buildAuthResponse(StatusCategory.E_SUCCESS, lifetimeMs));

        // Wait a bit for FSM to process auth response and transition to NEGOTIATING
        Thread.sleep(200);

        // Send negotiation response
        sendNegotiationResponse(testConn);

        // Wait for CONNECTED
        StartStatus status = startFuture.get(5, TimeUnit.SECONDS);
        assertEquals(StartStatus.SUCCESS, status);
    }

    private ScheduledFuture<?> getReauthFuture(TcpBrokerConnection connection) {
        return (ScheduledFuture<?>)
                TestHelpers.getInternalState(connection, "reauthenticationFuture");
    }

    private long getCredentialLifetimeMs(TcpBrokerConnection connection) {
        return (long) TestHelpers.getInternalState(connection, "credentialLifetimeMs");
    }

    private boolean getIsReauthenticating(TcpBrokerConnection connection) {
        return (boolean) TestHelpers.getInternalState(connection, "isReauthenticating");
    }

    @Test
    void testReauthScheduledAfterSuccessfulAuth() throws Exception {
        AuthnCredentialCb cb =
                () ->
                        AuthnCredential.builder()
                                .setMechanism("OAUTH2")
                                .setData("token".getBytes())
                                .build();
        TcpBrokerConnection connection = createConnection(cb);
        TestTcpConnection testConn = connectionFactory.getTestConnection();

        startWithAuth(connection, testConn, 10000);

        assertEquals(10000, getCredentialLifetimeMs(connection));
        ScheduledFuture<?> reauthFuture = getReauthFuture(connection);
        assertNotNull(reauthFuture);
        assertFalse(reauthFuture.isDone());

        // 80% of 10000ms = 8000ms, so delay should be around 8000ms
        long delayMs = reauthFuture.getDelay(TimeUnit.MILLISECONDS);
        assertTrue(delayMs > 6000 && delayMs <= 8000, "Expected delay ~8000ms, got " + delayMs);
    }

    @Test
    void testNoReauthScheduledWithoutLifetime() throws Exception {
        AuthnCredentialCb cb =
                () ->
                        AuthnCredential.builder()
                                .setMechanism("OAUTH2")
                                .setData("token".getBytes())
                                .build();
        TcpBrokerConnection connection = createConnection(cb);
        TestTcpConnection testConn = connectionFactory.getTestConnection();

        CompletableFuture<StartStatus> startFuture = startConnection(connection);

        ByteBuffer[] authnRequest = testConn.nextWriteRequest();
        assertNotNull(authnRequest);

        // Send auth response WITHOUT lifetime
        testConn.sendResponse(buildAuthResponse(StatusCategory.E_SUCCESS, null));
        Thread.sleep(200);

        sendNegotiationResponse(testConn);

        StartStatus status = startFuture.get(5, TimeUnit.SECONDS);
        assertEquals(StartStatus.SUCCESS, status);

        assertNull(getReauthFuture(connection));
    }

    @Test
    void testNoReauthWithoutAuthCallback() throws Exception {
        TcpBrokerConnection connection = createConnection(null);
        TestTcpConnection testConn = connectionFactory.getTestConnection();

        CompletableFuture<StartStatus> startFuture = startConnection(connection);

        // Without authn callback, it skips auth and goes straight to negotiation
        sendNegotiationResponse(testConn);

        StartStatus status = startFuture.get(5, TimeUnit.SECONDS);
        assertEquals(StartStatus.SUCCESS, status);

        assertNull(getReauthFuture(connection));
    }

    @Test
    void testReauthSuccessReschedules() throws Exception {
        AuthnCredentialCb cb =
                () ->
                        AuthnCredential.builder()
                                .setMechanism("OAUTH2")
                                .setData("token".getBytes())
                                .build();
        TcpBrokerConnection connection = createConnection(cb);
        TestTcpConnection testConn = connectionFactory.getTestConnection();

        // Use a short lifetime so reauthentication fires quickly
        startWithAuth(connection, testConn, 500);

        ScheduledFuture<?> firstReauthFuture = getReauthFuture(connection);
        assertNotNull(firstReauthFuture);

        // Wait for reauthentication to fire (80% of 500ms = 400ms)
        Thread.sleep(600);

        // Consume the reauthentication request
        ByteBuffer[] reauthRequest = testConn.nextWriteRequest(2);
        assertNotNull(reauthRequest);

        // Send successful reauthentication response with new lifetime
        testConn.sendResponse(buildAuthResponse(StatusCategory.E_SUCCESS, 2000));
        Thread.sleep(200);

        // Verify new lifetime and rescheduled future
        assertEquals(2000, getCredentialLifetimeMs(connection));
        ScheduledFuture<?> secondReauthFuture = getReauthFuture(connection);
        assertNotNull(secondReauthFuture);
        assertFalse(secondReauthFuture.isDone());

        long delayMs = secondReauthFuture.getDelay(TimeUnit.MILLISECONDS);
        assertTrue(delayMs > 1000 && delayMs <= 1600, "Expected delay ~1600ms, got " + delayMs);
    }

    @Test
    void testReauthFailureRetriesAtFixedInterval() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);
        AuthnCredentialCb cb =
                () -> {
                    callCount.incrementAndGet();
                    return AuthnCredential.builder()
                            .setMechanism("OAUTH2")
                            .setData("token".getBytes())
                            .build();
                };
        TcpBrokerConnection connection = createConnection(cb);
        TestTcpConnection testConn = connectionFactory.getTestConnection();

        // Use a short lifetime
        startWithAuth(connection, testConn, 500);

        // Wait for reauthentication to fire
        Thread.sleep(600);

        ByteBuffer[] reauthRequest = testConn.nextWriteRequest(2);
        assertNotNull(reauthRequest);

        // Send a failed reauthentication response
        testConn.sendResponse(buildAuthResponse(StatusCategory.E_REFUSED, null));
        Thread.sleep(200);

        assertFalse(getIsReauthenticating(connection));

        // Verify retry scheduled at ~10s
        ScheduledFuture<?> retryFuture = getReauthFuture(connection);
        assertNotNull(retryFuture);
        assertFalse(retryFuture.isDone());

        long delayMs = retryFuture.getDelay(TimeUnit.MILLISECONDS);
        assertTrue(
                delayMs > 8000 && delayMs <= TcpBrokerConnection.REAUTH_RETRY_INTERVAL_MS,
                "Expected delay ~10000ms, got " + delayMs);
    }

    @Test
    void testReauthCallbackFailureRetriesAtFixedInterval() throws Exception {
        AtomicInteger callCount = new AtomicInteger(0);
        AuthnCredentialCb cb =
                () -> {
                    if (callCount.incrementAndGet() > 1) {
                        throw new RuntimeException("token expired");
                    }
                    return AuthnCredential.builder()
                            .setMechanism("OAUTH2")
                            .setData("token".getBytes())
                            .build();
                };
        TcpBrokerConnection connection = createConnection(cb);
        TestTcpConnection testConn = connectionFactory.getTestConnection();

        startWithAuth(connection, testConn, 500);

        // Wait for reauthentication to fire — callback will return error
        Thread.sleep(600);

        assertFalse(getIsReauthenticating(connection));

        ScheduledFuture<?> retryFuture = getReauthFuture(connection);
        assertNotNull(retryFuture);
        assertFalse(retryFuture.isDone());

        long delayMs = retryFuture.getDelay(TimeUnit.MILLISECONDS);
        assertTrue(
                delayMs > 8000 && delayMs <= TcpBrokerConnection.REAUTH_RETRY_INTERVAL_MS,
                "Expected delay ~10000ms, got " + delayMs);
    }

    @Test
    void testReauthCancelledOnChannelDown() throws Exception {
        AuthnCredentialCb cb =
                () ->
                        AuthnCredential.builder()
                                .setMechanism("OAUTH2")
                                .setData("token".getBytes())
                                .build();
        TcpBrokerConnection connection = createConnection(cb);
        TestTcpConnection testConn = connectionFactory.getTestConnection();

        startWithAuth(connection, testConn, 60000);

        ScheduledFuture<?> reauthFuture = getReauthFuture(connection);
        assertNotNull(reauthFuture);
        assertFalse(reauthFuture.isDone());

        // Simulate channel down
        testConn.setChannelStatus(
                com.bloomberg.bmq.impl.infr.net.intf.ChannelStatusHandler.ChannelStatus
                        .CHANNEL_DOWN);
        Thread.sleep(200);

        assertTrue(reauthFuture.isCancelled());
    }
}
