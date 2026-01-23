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
package com.bloomberg.bmq.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bloomberg.bmq.HostHealthMonitor;
import com.bloomberg.bmq.HostHealthState;
import com.bloomberg.bmq.QueueFlags;
import com.bloomberg.bmq.QueueOptions;
import com.bloomberg.bmq.ResultCodes.CloseQueueCode;
import com.bloomberg.bmq.ResultCodes.CloseQueueResult;
import com.bloomberg.bmq.ResultCodes.ConfigureQueueCode;
import com.bloomberg.bmq.ResultCodes.ConfigureQueueResult;
import com.bloomberg.bmq.ResultCodes.GenericCode;
import com.bloomberg.bmq.ResultCodes.GenericResult;
import com.bloomberg.bmq.ResultCodes.OpenQueueCode;
import com.bloomberg.bmq.ResultCodes.OpenQueueResult;
import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.Uri;
import com.bloomberg.bmq.impl.events.BrokerSessionEvent;
import com.bloomberg.bmq.impl.events.Event;
import com.bloomberg.bmq.impl.events.QueueControlEvent;
import com.bloomberg.bmq.impl.infr.msg.BrokerResponse;
import com.bloomberg.bmq.impl.infr.msg.ClientIdentity;
import com.bloomberg.bmq.impl.infr.msg.ConsumerInfo;
import com.bloomberg.bmq.impl.infr.msg.ControlMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.NegotiationMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.Status;
import com.bloomberg.bmq.impl.infr.msg.StatusCategory;
import com.bloomberg.bmq.impl.infr.msg.StreamParameters;
import com.bloomberg.bmq.impl.infr.net.intf.ChannelStatusHandler.ChannelStatus;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.WriteStatus;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnectionFactory;
import com.bloomberg.bmq.impl.infr.proto.ControlEventImpl;
import com.bloomberg.bmq.impl.infr.proto.Protocol;
import com.bloomberg.bmq.impl.infr.proto.SchemaEventBuilder;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.intf.EventHandler;
import com.bloomberg.bmq.impl.intf.QueueState;
import com.bloomberg.bmq.util.TestHelpers;
import com.bloomberg.bmq.util.TestTcpConnection;
import com.bloomberg.bmq.util.TestTcpConnectionFactory;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BrokerSessionTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Duration SEQUENCE_TIMEOUT = Duration.ofMillis(500);
    private static final Duration FUTURE_TIMEOUT = Duration.ofSeconds(1);

    private static class TestSession {

        enum QueueTestStep {
            OPEN_OPENING, // pending open queue request
            OPEN_CONFIGURING, // pending config queue request
            OPEN_OPENED, // no pending requests
            REOPEN_OPENING, // pending open queue request
            REOPEN_CONFIGURING, // pending config queue request
            REOPEN_REOPENED, // no pending requests
            CONFIGURING, // pending config queue request
            CONFIGURING_RECONFIGURING, // pending config queue request
            CONFIGURED, // no pending requests
            CLOSE_CONFIGURING, // pending config queue request
            CLOSE_CLOSING, // pending close queue request
            CLOSE_CLOSED // no pending requests
        }

        private final ScheduledExecutorService service;
        private final BrokerSession session;
        private final TestTcpConnection connection;

        private final LinkedBlockingQueue<Event> eventQueue;

        private final QueueStateManager queueManager;

        public TestSession() {
            this(SessionOptions.createDefault());
        }

        public TestSession(SessionOptions options) {

            eventQueue = new LinkedBlockingQueue<>();

            TestTcpConnectionFactory connectionFactory = new TestTcpConnectionFactory();

            service = Executors.newSingleThreadScheduledExecutor();

            EventHandler eventHandler =
                    event -> {
                        try {
                            eventQueue.put(event);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    };

            session =
                    BrokerSession.createInstance(options, connectionFactory, service, eventHandler);

            connection = connectionFactory.getTestConnection();

            queueManager =
                    (QueueStateManager) TestHelpers.getInternalState(session, "queueStateManager");
            assertNotNull(queueManager);
        }

        public BrokerSession session() {
            return session;
        }

        public TestTcpConnection connection() {
            return connection;
        }

        public QueueStateManager queueManager() {
            return queueManager;
        }

        public void start() {
            start(Duration.ofSeconds(3));
        }

        public void start(Duration timeout) {
            session.startAsync(Argument.expectNonNull(timeout, "timeout"));

            sendNegotiationResponse(verifyNegotiationRequest(), StatusCategory.E_SUCCESS);

            verifySessionEvent(BrokerSessionEvent.Type.e_CONNECTED);
        }

        public void stop() {
            stop(true, Duration.ofSeconds(3));
        }

        public void stop(boolean waitDisconnectEvent, Duration timeout) {
            session.stopAsync(Argument.expectNonNull(timeout, "timeout"));

            sendDisconnectResponse(verifyDisconnectRequest());

            if (waitDisconnectEvent) {
                verifySessionEvent(BrokerSessionEvent.Type.e_DISCONNECTED);
            }
        }

        public void verifySessionEvent(BrokerSessionEvent.Type type) {
            Argument.expectNonNull(type, "type");

            Event event = nextEvent();
            assertNotNull(event);

            logger.info("Verify session event: {}", event);

            assertInstanceOf(BrokerSessionEvent.class, event);

            assertEquals(type, ((BrokerSessionEvent) event).getEventType());
        }

        public void sendBrokerResponse(Object rsp) {
            SchemaEventBuilder builder = new SchemaEventBuilder();
            try {
                builder.setMessage(Argument.expectNonNull(rsp, "rsp"));
                connection.sendResponse(builder.build());
            } catch (IOException ex) {
                throw new RuntimeException("Bad broker message");
            }
        }

        public ClientIdentity verifyNegotiationRequest() {
            ByteBuffer[] data = connection.nextWriteRequest();
            assertNotNull(data);

            ControlEventImpl controlEvent = new ControlEventImpl(data);

            logger.info("Verify negotiation request: {}", controlEvent);

            NegotiationMessageChoice negotiationMessageChoice = controlEvent.tryNegotiationChoice();

            assertNotNull(negotiationMessageChoice);
            assertTrue(negotiationMessageChoice.isClientIdentityValue());

            final String features = "PROTOCOL_ENCODING:JSON;MPS:MESSAGE_PROPERTIES_EX";
            assertEquals(features, negotiationMessageChoice.clientIdentity().features());

            return negotiationMessageChoice.clientIdentity();
        }

        public void sendNegotiationResponse(
                ClientIdentity clientIdentity, StatusCategory category) {
            Argument.expectNonNull(clientIdentity, "clientIdentity");
            Argument.expectNonNull(category, "category");

            NegotiationMessageChoice negMsg = new NegotiationMessageChoice();
            negMsg.makeBrokerResponse();
            BrokerResponse brokerResponse = negMsg.brokerResponse();
            brokerResponse.setBrokerVersion(1);
            brokerResponse.setIsDeprecatedSdk(false);
            brokerResponse.setProtocolversion(Protocol.VERSION);
            Status status = new Status();
            status.setCategory(category);
            brokerResponse.setResult(status);
            brokerResponse.setOriginalRequest(clientIdentity);

            sendBrokerResponse(negMsg);
        }

        public ControlMessageChoice verifyDisconnectRequest() {
            ByteBuffer[] data = connection.nextWriteRequest();

            assertNotNull(data);

            ControlEventImpl controlEvent = new ControlEventImpl(data);

            logger.info("Verify disconnect request: {}", controlEvent);

            ControlMessageChoice controlMessageChoice = controlEvent.tryControlChoice();

            assertNotNull(controlMessageChoice);
            assertTrue(controlMessageChoice.isDisconnectValue());

            // Reset the event so it will rewind the data to be used again
            try {
                controlEvent.setPosition(0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return controlMessageChoice;
        }

        public void sendDisconnectResponse(ControlMessageChoice request) {
            Argument.expectNonNull(request, "request");
            Argument.expectCondition(request.isDisconnectValue(), "Expected disconnect request");

            ControlMessageChoice response = new ControlMessageChoice();
            response.setId(request.id());
            response.makeDisconnectResponse();

            sendBrokerResponse(response);
        }

        public ControlMessageChoice verifyOpenQueueRequest() {
            ByteBuffer[] data = connection.nextWriteRequest();

            assertNotNull(data);

            ControlEventImpl controlEvent = new ControlEventImpl(data);

            logger.info("Verify open queue request: {}", controlEvent);

            ControlMessageChoice controlMessageChoice = controlEvent.tryControlChoice();

            assertNotNull(controlMessageChoice);
            assertTrue(controlMessageChoice.isOpenQueueValue());

            // Reset the event so it will rewind the data to be used again
            try {
                controlEvent.setPosition(0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return controlMessageChoice;
        }

        public ControlMessageChoice verifyConfigureStreamRequest() {
            ByteBuffer[] data = connection.nextWriteRequest();

            assertNotNull(data);

            ControlEventImpl controlEvent = new ControlEventImpl(data);

            logger.info("Verify configure stream request: {}", controlEvent);

            ControlMessageChoice controlMessageChoice = controlEvent.tryControlChoice();

            assertNotNull(controlMessageChoice);
            assertTrue(controlMessageChoice.isConfigureStreamValue());

            // Reset the event so it will rewind the data to be used again
            try {
                controlEvent.setPosition(0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return controlMessageChoice;
        }

        public void sendConfigureStreamResponse(ControlMessageChoice request) {
            Argument.expectNonNull(request, "request");
            Argument.expectCondition(
                    request.isConfigureStreamValue(), "Expected configure stream request");

            ControlMessageChoice response = new ControlMessageChoice();
            response.setId(request.id());
            response.makeConfigureStreamResponse();

            response.configureStreamResponse().setOriginalRequest(request.configureStream());

            sendBrokerResponse(response);
        }

        public void sendOpenQueueResponse(ControlMessageChoice request) {
            Argument.expectNonNull(request, "request");
            Argument.expectCondition(request.isOpenQueueValue(), "Expected open queue request");

            ControlMessageChoice response = new ControlMessageChoice();
            response.setId(request.id());
            response.makeOpenQueueResponse();

            sendBrokerResponse(response);
        }

        public void sendCloseQueueResponse(ControlMessageChoice request) {
            Argument.expectNonNull(request, "request");
            Argument.expectCondition(request.isCloseQueueValue(), "Expected close queue request");

            ControlMessageChoice response = new ControlMessageChoice();
            response.setId(request.id());
            response.makeCloseQueueResponse();

            sendBrokerResponse(response);
        }

        public QueueImpl createQueue(Uri uri, long flags) {
            return new QueueImpl(session, uri, flags, null, null, null);
        }

        public void openQueue(QueueImpl queue, QueueOptions queueOptions) throws TimeoutException {

            arriveAtStep(queue, queueOptions, QueueTestStep.OPEN_OPENED);
        }

        public void reopenQueue(QueueImpl queue) {
            sendOpenQueueResponse(verifyOpenQueueRequest());
            sendConfigureStreamResponse(verifyConfigureStreamRequest());
            verifyQueueControlEvent(
                    QueueControlEvent.Type.e_QUEUE_REOPEN_RESULT, OpenQueueResult.SUCCESS);
            assertEquals(QueueState.e_OPENED, queue.getState());
        }

        public void closeQueue(QueueImpl queue) {
            assertEquals(QueueState.e_OPENED, queue.getState());
            queue.closeAsync(SEQUENCE_TIMEOUT);
            sendConfigureStreamResponse(verifyConfigureStreamRequest());
            sendCloseQueueResponse(verifyCloseQueueRequest(true));
            verifyQueueControlEvent(
                    QueueControlEvent.Type.e_QUEUE_CLOSE_RESULT, CloseQueueResult.SUCCESS);
            assertEquals(QueueState.e_CLOSED, queue.getState());
        }

        public ControlMessageChoice verifyCloseQueueRequest(boolean isFinal) {
            ByteBuffer[] data = connection.nextWriteRequest();

            assertNotNull(data);

            ControlEventImpl controlEvent = new ControlEventImpl(data);

            logger.info("Verify close queue request: {}", controlEvent);

            ControlMessageChoice controlMessageChoice = controlEvent.tryControlChoice();

            assertNotNull(controlMessageChoice);
            assertTrue(controlMessageChoice.isCloseQueueValue());
            assertEquals(isFinal, controlMessageChoice.closeQueue().isFinal());

            // Reset the event so it will rewind the data to be used again
            try {
                controlEvent.setPosition(0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return controlMessageChoice;
        }

        public void verifyQueueControlEvent(QueueControlEvent.Type type, GenericCode status) {
            Argument.expectNonNull(type, "type");
            Argument.expectNonNull(status, "status");

            Event event = nextEvent();
            assertNotNull(event);

            logger.info("Verify queue control event: {}", event);

            assertTrue(event instanceof QueueControlEvent);

            QueueControlEvent queueEvent = (QueueControlEvent) event;
            assertEquals(type, queueEvent.getEventType());
            assertEquals(status, queueEvent.getStatus());
        }

        public void arriveAtStep(QueueImpl queue, QueueOptions queueOptions, QueueTestStep step)
                throws TimeoutException {

            Argument.expectNonNull(queue, "queue");
            Argument.expectNonNull(queueOptions, "queueOptions");
            Argument.expectNonNull(step, "step");

            final Duration futureTimeout = Duration.ofSeconds(5);
            final Duration noEventTimeout = Duration.ofMillis(200);

            ControlMessageChoice curRequest = null;
            BmqFuture<OpenQueueCode> openFuture = null;
            BmqFuture<ConfigureQueueCode> configFuture = null;
            BmqFuture<CloseQueueCode> closeFuture = null;

            for (QueueTestStep curStep : QueueTestStep.values()) {
                switch (curStep) {
                    case OPEN_OPENING:
                        {
                            // Open the queue
                            assertEquals(QueueState.e_CLOSED, queue.getState());
                            // Check substreamcount == 0
                            assertEquals(0, queueManager.getSubStreamCount(queue));

                            openFuture = queue.openAsync(queueOptions, futureTimeout);

                            // Check open queue request is sent
                            curRequest = verifyOpenQueueRequest();

                            // Check there are no events
                            assertNull(nextEvent(noEventTimeout));

                            assertEquals(QueueState.e_OPENING, queue.getState());
                            assertEquals(1, queueManager.getSubStreamCount(queue));
                        }
                        break;
                    case OPEN_CONFIGURING: // fallthrough
                    case REOPEN_CONFIGURING:
                        {
                            assert curRequest != null;

                            // send back open queue response
                            sendOpenQueueResponse(curRequest);

                            // check configure queue request is sent
                            curRequest = verifyConfigureStreamRequest();

                            // Check there are no events
                            assertNull(nextEvent(noEventTimeout));

                            assertEquals(QueueState.e_OPENING, queue.getState());
                            assertEquals(1, queueManager.getSubStreamCount(queue));
                        }
                        break;
                    case OPEN_OPENED:
                        {
                            assert curRequest != null;

                            // Send back open configure queue response
                            sendConfigureStreamResponse(curRequest);

                            assertEquals(OpenQueueResult.SUCCESS, openFuture.get(futureTimeout));

                            // Check substreamcount == 1
                            assertEquals(1, queueManager.getSubStreamCount(queue));

                            // Check event
                            verifyQueueControlEvent(
                                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT,
                                    OpenQueueResult.SUCCESS);

                            // Check there are no events
                            assertNull(nextEvent(noEventTimeout));

                            assertEquals(QueueState.e_OPENED, queue.getState());
                        }
                        break;
                    case REOPEN_OPENING:
                        {
                            // emulate network channel drop
                            connection.setChannelStatus(ChannelStatus.CHANNEL_DOWN);

                            // Wait CONNECTION_LOST event
                            verifySessionEvent(BrokerSessionEvent.Type.e_CONNECTION_LOST);

                            // Queue is still opened
                            assertEquals(QueueState.e_OPENED, queue.getState());
                            assertEquals(1, queueManager.getSubStreamCount(queue));

                            // Check there are no events
                            assertNull(nextEvent(noEventTimeout));

                            // Restore the channel
                            connection.setChannelStatus(ChannelStatus.CHANNEL_UP);

                            // Negotiate
                            sendNegotiationResponse(
                                    verifyNegotiationRequest(), StatusCategory.E_SUCCESS);

                            // Wait RECONNECTED event
                            verifySessionEvent(BrokerSessionEvent.Type.e_RECONNECTED);

                            // Check open part of open request is sent
                            curRequest = verifyOpenQueueRequest();

                            // Check there are no events
                            assertNull(nextEvent(noEventTimeout));

                            assertEquals(QueueState.e_OPENING, queue.getState());
                            assertEquals(1, queueManager.getSubStreamCount(queue));
                        }
                        break;
                    case REOPEN_REOPENED:
                        {
                            assert curRequest != null;

                            // Send back open configure queue response
                            sendConfigureStreamResponse(curRequest);

                            // Check reopen queue result event
                            verifyQueueControlEvent(
                                    QueueControlEvent.Type.e_QUEUE_REOPEN_RESULT,
                                    OpenQueueResult.SUCCESS);

                            // Check STATE_RESTORED event
                            verifySessionEvent(BrokerSessionEvent.Type.e_STATE_RESTORED);

                            // Check there are no events
                            assertNull(nextEvent(noEventTimeout));

                            assertEquals(QueueState.e_OPENED, queue.getState());
                            assertEquals(1, queueManager.getSubStreamCount(queue));
                        }
                        break;
                    case CONFIGURING:
                        {
                            configFuture = queue.configureAsync(queueOptions, futureTimeout);

                            // check configure queue request is sent
                            curRequest = verifyConfigureStreamRequest();

                            // Check there are no events
                            assertNull(nextEvent(noEventTimeout));

                            assertEquals(QueueState.e_OPENED, queue.getState());
                            assertEquals(1, queueManager.getSubStreamCount(queue));
                        }
                        break;
                    case CONFIGURING_RECONFIGURING:
                        {
                            assert curRequest != null;

                            // Wait configure request timeout
                            assertEquals(
                                    ConfigureQueueResult.TIMEOUT, configFuture.get(futureTimeout));

                            // Check configure timeout event
                            verifyQueueControlEvent(
                                    QueueControlEvent.Type.e_QUEUE_CONFIGURE_RESULT,
                                    ConfigureQueueResult.TIMEOUT);

                            // Modify stream parameters to trigger reconfigure request
                            StreamParameters params =
                                    Argument.expectNonNull(curRequest, "curRequest")
                                            .configureStream()
                                            .streamParameters();
                            assertEquals(1, params.subscriptions().length);
                            assertEquals(1, params.subscriptions()[0].consumers().length);

                            ConsumerInfo info = params.subscriptions()[0].consumers()[0];
                            info.setMaxUnconfirmedBytes(info.maxUnconfirmedBytes() - 1);

                            // Send late configure queue response
                            sendConfigureStreamResponse(curRequest);

                            // check reconfigure queue request is sent
                            curRequest = verifyConfigureStreamRequest();

                            // Check there are no events
                            assertNull(nextEvent(noEventTimeout));

                            assertEquals(QueueState.e_OPENED, queue.getState());
                            assertEquals(1, queueManager.getSubStreamCount(queue));
                        }
                        break;
                    case CONFIGURED:
                        {
                            assert curRequest != null;

                            // Send configure queue response
                            sendConfigureStreamResponse(curRequest);

                            // Check there are no events
                            assertNull(nextEvent(noEventTimeout));

                            assertEquals(QueueState.e_OPENED, queue.getState());
                            assertEquals(1, queueManager.getSubStreamCount(queue));
                        }
                        break;
                    case CLOSE_CONFIGURING:
                        {
                            closeFuture = queue.closeAsync(futureTimeout);

                            // check close-configure queue request is sent
                            curRequest = verifyConfigureStreamRequest();

                            // Check there are no events
                            assertNull(nextEvent(noEventTimeout));

                            assertEquals(QueueState.e_CLOSING, queue.getState());
                            assertEquals(1, queueManager.getSubStreamCount(queue));
                        }
                        break;
                    case CLOSE_CLOSING:
                        {
                            assert curRequest != null;

                            // Send configure queue response
                            sendConfigureStreamResponse(curRequest);

                            // Check close queue request is sent with isFinal=true
                            verifyCloseQueueRequest(true);

                            // Check there are no events
                            assertNull(nextEvent(noEventTimeout));

                            assertEquals(QueueState.e_CLOSING, queue.getState());
                            assertEquals(0, queueManager.getSubStreamCount(queue));
                        }
                        break;
                    case CLOSE_CLOSED:
                        {
                            assert curRequest != null;

                            // Send close queue response
                            sendCloseQueueResponse(curRequest);

                            // Check close queue result event
                            verifyQueueControlEvent(
                                    QueueControlEvent.Type.e_QUEUE_CLOSE_RESULT,
                                    CloseQueueResult.SUCCESS);

                            // Check there are no events
                            assertNull(nextEvent(noEventTimeout));

                            assertEquals(QueueState.e_CLOSED, queue.getState());
                            assertEquals(0, queueManager.getSubStreamCount(queue));
                        }
                        break;
                    default:
                        {
                            throw new IllegalStateException("Unexpected test step: " + curStep);
                        }
                }
                if (curStep == step) {
                    break;
                }
            }
        }

        public QueueImpl createQueueOnStep(QueueOptions queueOptions, QueueTestStep step)
                throws TimeoutException {
            long flags = 0;
            flags = QueueFlags.setReader(flags);

            QueueImpl queue = createQueue(createUri(), flags);

            arriveAtStep(queue, queueOptions, Argument.expectNonNull(step, "step"));
            return queue;
        }

        public Event nextEvent(Duration timeout) {
            return TestHelpers.poll(eventQueue, timeout);
        }

        public Event nextEvent() {
            return nextEvent(FUTURE_TIMEOUT);
        }
    }

    private static Uri createUri() {
        return new Uri("bmq://bmq.training.myapp/my.queue." + UUID.randomUUID().toString());
    }

    /** Basic test that creates BrokerSession with a test channel */
    @Test
    void breathingTest() {
        TcpConnectionFactory connectionFactory = new TestTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

        BrokerSession session =
                BrokerSession.createInstance(
                        SessionOptions.createDefault(),
                        connectionFactory,
                        service,
                        null); // event handler

        try {
            assertEquals(GenericResult.TIMEOUT, session.start(Duration.ofMillis(100)));

        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        }
    }

    /** Check that BrokerSession with a test channel can be started and stopped asynchronously */
    @Test
    void checkStartStopAsyncTest() {
        TestSession obj = new TestSession();

        try {
            // Check startAsync timeout
            obj.session().startAsync(Duration.ofMillis(100));

            obj.verifyNegotiationRequest();

            obj.verifySessionEvent(BrokerSessionEvent.Type.e_CONNECTION_TIMEOUT);

            // TODO: in case of start timeout not only e_CONNECTION_TIMEOUT
            // event is delivered but also e_CANCELED (as a result of start
            // status callback). Not sure it is an expected behavior.
            obj.verifySessionEvent(BrokerSessionEvent.Type.e_CANCELED);

            // Check startAsync OK
            obj.session().startAsync(Duration.ofSeconds(1));

            obj.sendNegotiationResponse(obj.verifyNegotiationRequest(), StatusCategory.E_SUCCESS);

            obj.verifySessionEvent(BrokerSessionEvent.Type.e_CONNECTED);

            // Check stopAsync timeout
            obj.session().stopAsync(Duration.ofMillis(100));

            obj.verifySessionEvent(BrokerSessionEvent.Type.e_DISCONNECTION_TIMEOUT);

            obj.verifySessionEvent(BrokerSessionEvent.Type.e_DISCONNECTED);

            // Check there are no more events
            assertNull(obj.nextEvent());
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            obj.stop();
            obj.session().linger();
        }
    }

    /**
     * Check that BrokerSession with a host health monitor and a test channel can be started and
     * stopped asynchronously
     */
    @Test
    void checkStartStopAsyncWithMonitorTest() {
        HostHealthMonitor monitor = mock(HostHealthMonitor.class);
        SessionOptions options = SessionOptions.builder().setHostHealthMonitor(monitor).build();

        TestSession obj = new TestSession(options);

        // Setup monitor to return unknown state
        when(monitor.hostHealthState()).thenReturn(HostHealthState.Unknown);

        // Setup add/remove monitor handler methods:
        // When call first time return true
        // When call second time return false
        // For subsequent calls throw an exception
        when(monitor.addHandler(obj.session()))
                .thenReturn(true)
                .thenReturn(false)
                .thenThrow(NullPointerException.class);

        when(monitor.removeHandler(obj.session()))
                .thenReturn(true)
                .thenReturn(false)
                .thenThrow(NullPointerException.class);

        try {
            // Call startAsync first time
            obj.session().startAsync(Duration.ofSeconds(1));

            obj.sendNegotiationResponse(obj.verifyNegotiationRequest(), StatusCategory.E_SUCCESS);

            // Since monitor returns Unknown state, HOST_UNHEALTHY event
            // should be issued
            obj.verifySessionEvent(BrokerSessionEvent.Type.e_HOST_UNHEALTHY);
            obj.verifySessionEvent(BrokerSessionEvent.Type.e_CONNECTED);

            // Call startAsync second time
            obj.session().startAsync(Duration.ofMillis(100));
            obj.verifySessionEvent(BrokerSessionEvent.Type.e_CONNECTED);

            // Call startAsync one more time
            obj.session().startAsync(Duration.ofMillis(100));
            obj.verifySessionEvent(BrokerSessionEvent.Type.e_CONNECTED);

            // Call stopAsync first time
            obj.stop();

            // Call stopAsync second time
            obj.session().stopAsync(Duration.ofMillis(100));
            obj.verifySessionEvent(BrokerSessionEvent.Type.e_DISCONNECTED);

            // Call stopAsync one more time
            obj.session().stopAsync(Duration.ofMillis(100));
            obj.verifySessionEvent(BrokerSessionEvent.Type.e_DISCONNECTED);

            // Check there are no more events
            assertNull(obj.nextEvent());

            // Verify monitor methods
            verify(monitor, times(1)).hostHealthState();
            verify(monitor, times(3)).addHandler(obj.session());
            verify(monitor, times(3)).removeHandler(obj.session());
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            obj.session().linger();
        }
    }

    /**
     * Check that BrokerSession with a host health monitor and a test channel can be started and
     * stopped synchronously
     */
    @Test
    void checkStartStopSyncWithMonitorTest() {
        HostHealthMonitor monitor = mock(HostHealthMonitor.class);
        SessionOptions options = SessionOptions.builder().setHostHealthMonitor(monitor).build();

        TestSession obj = new TestSession(options);

        // Setup monitor to return unknown state
        when(monitor.hostHealthState()).thenReturn(HostHealthState.Unknown);

        // Setup add/remove monitor handler methods:
        // When call first time return true
        // When call second time return false
        // For subsequent calls throw an exception
        when(monitor.addHandler(obj.session()))
                .thenReturn(true)
                .thenReturn(false)
                .thenThrow(NullPointerException.class);

        when(monitor.removeHandler(obj.session()))
                .thenReturn(true)
                .thenReturn(false)
                .thenThrow(NullPointerException.class);

        try {
            // Call start first time
            CompletableFuture.runAsync(
                    () -> {
                        obj.sendNegotiationResponse(
                                obj.verifyNegotiationRequest(), StatusCategory.E_SUCCESS);
                    });
            obj.session().start(Duration.ofSeconds(1));

            // Since monitor returns Unknown state, HOST_UNHEALTHY event
            // should be issued
            obj.verifySessionEvent(BrokerSessionEvent.Type.e_HOST_UNHEALTHY);

            // Call startAsync second time
            obj.session().start(Duration.ofMillis(100));

            // Call startAsync one more time
            obj.session().start(Duration.ofMillis(100));

            // Call stopAsync first time
            CompletableFuture.runAsync(
                    () -> {
                        obj.sendDisconnectResponse(obj.verifyDisconnectRequest());
                    });
            obj.session().stop(Duration.ofSeconds(1));

            // Call stopAsync second time
            obj.session().stop(Duration.ofMillis(100));

            // Call stopAsync one more time
            obj.session().stop(Duration.ofMillis(100));

            // Check there are no more events
            assertNull(obj.nextEvent());

            // Verify monitor methods
            verify(monitor, times(1)).hostHealthState();
            verify(monitor, times(3)).addHandler(obj.session());
            verify(monitor, times(3)).removeHandler(obj.session());
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            obj.session().linger();
        }
    }

    /**
     * Check that BrokerSession with a test channel can be restarted after session stop and channel
     * drop
     */
    @Test
    void checkRestartTest() {
        TestSession obj = new TestSession();

        try {
            for (int i = 1; i <= 3; i++) {
                obj.start();

                obj.stop();

                obj.start();

                // Trigger channel drop
                obj.connection().setChannelStatus(ChannelStatus.CHANNEL_DOWN);

                obj.verifySessionEvent(BrokerSessionEvent.Type.e_CONNECTION_LOST);

                // Restore the channel
                obj.connection().setChannelStatus(ChannelStatus.CHANNEL_UP);

                // Negotiate
                obj.sendNegotiationResponse(
                        obj.verifyNegotiationRequest(), StatusCategory.E_SUCCESS);

                obj.verifySessionEvent(BrokerSessionEvent.Type.e_RECONNECTED);

                obj.verifySessionEvent(BrokerSessionEvent.Type.e_STATE_RESTORED);

                // Check there are no more events
                assertNull(obj.nextEvent());

                obj.stop();
            }
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            obj.session().stop(Duration.ofMillis(100));
            obj.session().linger();
        }
    }

    /** Check substreamcount when broker session fails to send open queue request */
    @Test
    void openQueueRequestFailsTest() {
        TestSession obj = new TestSession();

        try {
            obj.start();

            QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();

            long flags = 0;
            flags = QueueFlags.setReader(flags);

            QueueImpl queue = obj.createQueue(createUri(), flags);

            assertEquals(QueueState.e_CLOSED, queue.getState());
            // Check substreamcount == 0
            assertEquals(0, obj.queueManager().getSubStreamCount(queue));

            // Verify openQueueAsync when write status is CLOSED
            obj.connection().setWriteStatus(WriteStatus.CLOSED);
            assertEquals(
                    OpenQueueResult.NOT_CONNECTED,
                    queue.openAsync(queueOptions, SEQUENCE_TIMEOUT).get(FUTURE_TIMEOUT));

            // Check open request
            obj.verifyOpenQueueRequest();
            // No more requests
            assertNull(obj.connection().nextWriteRequest(1));

            // Check substreamcount == 0
            assertEquals(0, obj.queueManager().getSubStreamCount(queue));

            // Check event
            obj.verifyQueueControlEvent(
                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT, OpenQueueResult.NOT_CONNECTED);

            assertEquals(QueueState.e_CLOSED, queue.getState());

            // Verify openQueueAsync when write status is WRITE_BUFFER_FULL.
            obj.connection().setWriteStatus(WriteStatus.WRITE_BUFFER_FULL);
            BmqFuture<OpenQueueCode> openFuture = queue.openAsync(queueOptions, SEQUENCE_TIMEOUT);

            // Check there was an attempt to send open request
            obj.verifyOpenQueueRequest();

            // Check substreamcount == 0
            assertEquals(0, obj.queueManager().getSubStreamCount(queue));

            // TcpBrokerConnection will be waiting until the channel
            // becomes writable
            obj.connection().setWritable();

            // Open request should be sent but there will be no response
            assertEquals(OpenQueueResult.TIMEOUT, openFuture.get(FUTURE_TIMEOUT));

            // Check open request has been sent
            obj.verifyOpenQueueRequest();
            // No more requests
            assertNull(obj.connection().nextWriteRequest(1));

            // Check substreamcount == 1 (open request has been sent)
            assertEquals(1, obj.queueManager().getSubStreamCount(queue));

            // Check event
            obj.verifyQueueControlEvent(
                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT, OpenQueueResult.TIMEOUT);

            assertEquals(QueueState.e_CLOSED, queue.getState());
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            obj.stop();
            obj.session().linger();
        }
    }

    /** Check substreamcount when broker session fails to send open configure queue request */
    @Test
    void openConfigureQueueRequestFailsTest() {
        TestSession obj = new TestSession();

        try {
            obj.start();

            QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();

            long flags = 0;
            flags = QueueFlags.setReader(flags);

            QueueImpl queue = obj.createQueue(createUri(), flags);

            assertEquals(QueueState.e_CLOSED, queue.getState());
            // Check substreamcount == 0
            assertEquals(0, obj.queueManager().getSubStreamCount(queue));

            // Verify openQueueAsync when sending configure request and
            // write status is CLOSED
            BmqFuture<OpenQueueCode> openFuture = queue.openAsync(queueOptions, SEQUENCE_TIMEOUT);

            // Check open request
            ControlMessageChoice openRequest = obj.verifyOpenQueueRequest();

            // Send back open queue response
            obj.connection().setWriteStatus(WriteStatus.CLOSED);
            obj.sendOpenQueueResponse(openRequest);

            assertEquals(OpenQueueResult.NOT_CONNECTED, openFuture.get(FUTURE_TIMEOUT));

            // Check configure request
            obj.verifyConfigureStreamRequest();
            // No more requests
            assertNull(obj.connection().nextWriteRequest(1));

            // Check substreamcount == 0
            assertEquals(0, obj.queueManager().getSubStreamCount(queue));

            // Check event
            obj.verifyQueueControlEvent(
                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT, OpenQueueResult.NOT_CONNECTED);

            assertEquals(QueueState.e_CLOSED, queue.getState());

            // Verify openQueueAsync when sending configure request and
            // write status is WRITE_BUFFER_FULL.
            obj.connection().resetWriteStatus(); // SUCCESS
            openFuture = queue.openAsync(queueOptions, SEQUENCE_TIMEOUT);

            // Check open request
            openRequest = obj.verifyOpenQueueRequest();

            // Send back open queue response
            obj.connection().setWriteStatus(WriteStatus.WRITE_BUFFER_FULL);
            obj.sendOpenQueueResponse(openRequest);

            // Check there was an attempt to send configure request
            obj.verifyConfigureStreamRequest();

            // TcpBrokerConnection will be waiting until the channel
            // becomes writable
            obj.connection().setWritable();

            // Configure request should be sent but there will be no response
            assertEquals(OpenQueueResult.TIMEOUT, openFuture.get(FUTURE_TIMEOUT));

            // Check open request has been sent
            obj.verifyConfigureStreamRequest();
            // No more requests
            assertNull(obj.connection().nextWriteRequest(1));

            // Check substreamcount == 1 (open request has been sent)
            assertEquals(1, obj.queueManager().getSubStreamCount(queue));

            // Check event
            obj.verifyQueueControlEvent(
                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT, OpenQueueResult.TIMEOUT);

            assertEquals(QueueState.e_CLOSED, queue.getState());
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            obj.stop();
            obj.session().linger();
        }
    }

    /** Check substreamcount when broker session fails to send close configure queue request */
    @Test
    void closeConfigureQueueRequestFailsTest() {
        TestSession obj = new TestSession();

        try {
            obj.start();

            QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();

            long flags = 0;
            flags = QueueFlags.setReader(flags);

            QueueImpl queue = obj.createQueue(createUri(), flags);

            // Open the queue
            obj.openQueue(queue, queueOptions);

            // Verify closeQueueAsync when sending configure request and
            // write status is CLOSED
            obj.connection().setWriteStatus(WriteStatus.CLOSED);
            BmqFuture<CloseQueueCode> closeFuture = queue.closeAsync(SEQUENCE_TIMEOUT);

            assertEquals(CloseQueueResult.SUCCESS, closeFuture.get(FUTURE_TIMEOUT));

            // Check configure request
            obj.verifyConfigureStreamRequest();
            // No more requests
            assertNull(obj.connection().nextWriteRequest(1));

            // Check substreamcount == 0
            assertEquals(0, obj.queueManager().getSubStreamCount(queue));

            // Check event
            obj.verifyQueueControlEvent(
                    QueueControlEvent.Type.e_QUEUE_CLOSE_RESULT, CloseQueueResult.SUCCESS);

            assertEquals(QueueState.e_CLOSED, queue.getState());

            // Open the queue
            obj.connection().resetWriteStatus(); // SUCCESS
            obj.openQueue(queue, queueOptions);

            // Verify closeQueueAsync when sending configure request and
            // write status is WRITE_BUFFER_FULL.
            obj.connection().setWriteStatus(WriteStatus.WRITE_BUFFER_FULL);
            closeFuture = queue.closeAsync(SEQUENCE_TIMEOUT);

            // Check there was an attempt to send configure request
            obj.verifyConfigureStreamRequest();

            // TcpBrokerConnection will be waiting until the channel
            // becomes writable
            obj.connection().setWritable();

            // Configure request should be sent but there will be no response
            assertEquals(CloseQueueResult.TIMEOUT, closeFuture.get(FUTURE_TIMEOUT));

            // Check configure request has been sent
            obj.verifyConfigureStreamRequest();
            // No more requests
            assertNull(obj.connection().nextWriteRequest(1));

            // Check substreamcount == 1 (close request has not been sent)
            assertEquals(1, obj.queueManager().getSubStreamCount(queue));

            // Check event
            obj.verifyQueueControlEvent(
                    QueueControlEvent.Type.e_QUEUE_CLOSE_RESULT, CloseQueueResult.TIMEOUT);

            assertEquals(QueueState.e_CLOSED, queue.getState());
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            obj.stop();
            obj.session().linger();
        }
    }

    /**
     * Check substreamcount when broker session fails to send close queue request after configure
     * response
     */
    @Test
    void closeQueueRequestFailsTest() {
        TestSession obj = new TestSession();

        try {
            obj.start();

            QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();

            long flags = 0;
            flags = QueueFlags.setReader(flags);

            QueueImpl queue = obj.createQueue(createUri(), flags);

            // Open the queue
            obj.openQueue(queue, queueOptions);

            // Verify closeQueueAsync when sending close request and
            // write status is CLOSED
            BmqFuture<CloseQueueCode> closeFuture = queue.closeAsync(SEQUENCE_TIMEOUT);

            // Send back configure response
            ControlMessageChoice configureRequest = obj.verifyConfigureStreamRequest();

            obj.connection().setWriteStatus(WriteStatus.CLOSED);

            obj.sendConfigureStreamResponse(configureRequest);

            assertEquals(CloseQueueResult.NOT_CONNECTED, closeFuture.get(FUTURE_TIMEOUT));

            // Check close request
            obj.verifyCloseQueueRequest(true);
            // No more requests
            assertNull(obj.connection().nextWriteRequest(1));

            // Check substreamcount == 0
            assertEquals(0, obj.queueManager().getSubStreamCount(queue));

            // Check event
            obj.verifyQueueControlEvent(
                    QueueControlEvent.Type.e_QUEUE_CLOSE_RESULT, CloseQueueResult.NOT_CONNECTED);

            assertEquals(QueueState.e_CLOSED, queue.getState());

            // Open the queue
            obj.connection().resetWriteStatus(); // SUCCESS

            // Due to the bug, queue is not closed fully, so we cannot reopen it
            logger.info("[BUG] Current queue cannot be reopened");
            assertEquals(
                    OpenQueueResult.QUEUE_ID_NOT_UNIQUE,
                    queue.open(queueOptions, SEQUENCE_TIMEOUT));

            // Create and open another queue
            queue = obj.createQueue(createUri(), flags);

            obj.openQueue(queue, queueOptions);

            // Verify closeQueueAsync when sending close request and
            // write status is WRITE_BUFFER_FULL.
            closeFuture = queue.closeAsync(SEQUENCE_TIMEOUT);

            // Send back configure response
            configureRequest = obj.verifyConfigureStreamRequest();

            obj.connection().setWriteStatus(WriteStatus.WRITE_BUFFER_FULL);

            obj.sendConfigureStreamResponse(configureRequest);

            // Check there was an attempt to send close request
            obj.verifyCloseQueueRequest(true);

            // TcpBrokerConnection will be waiting until the channel
            // becomes writable
            obj.connection().setWritable();

            // Close request should be sent but there will be no response
            assertEquals(CloseQueueResult.TIMEOUT, closeFuture.get(FUTURE_TIMEOUT));

            // Check close request has been sent
            obj.verifyCloseQueueRequest(true);
            // No more requests
            assertNull(obj.connection().nextWriteRequest(1));

            // Check substreamcount == 0
            assertEquals(0, obj.queueManager().getSubStreamCount(queue));

            // Check event
            obj.verifyQueueControlEvent(
                    QueueControlEvent.Type.e_QUEUE_CLOSE_RESULT, CloseQueueResult.TIMEOUT);

            assertEquals(QueueState.e_CLOSED, queue.getState());
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            obj.stop();
            obj.session().linger();
        }
    }

    @Test
    void cancelRequestTest() {

        class Test {
            TestSession.QueueTestStep queueTestStep;
            QueueControlEvent.Type eventType;
            GenericCode expectedOperationResult;
            boolean waitStateRestoredOnDisconnect;
            QueueState queueStateAfterDisconnect;

            Test(
                    TestSession.QueueTestStep step,
                    QueueControlEvent.Type eventType,
                    GenericCode operationResult,
                    boolean waitStateRestoredOnDisconnect,
                    QueueState queueStateAfterDisconnect) {

                queueTestStep = step;
                this.eventType = eventType;
                expectedOperationResult = operationResult;
                this.waitStateRestoredOnDisconnect = waitStateRestoredOnDisconnect;
                this.queueStateAfterDisconnect = queueStateAfterDisconnect;
            }
        }
        Test[] tests = {
            new Test(
                    TestSession.QueueTestStep.OPEN_OPENING,
                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT,
                    OpenQueueResult.CANCELED,
                    false, // no STATE_RESTORED event after disconnect
                    QueueState.e_CLOSED),
            new Test(
                    TestSession.QueueTestStep.OPEN_CONFIGURING,
                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT,
                    OpenQueueResult.CANCELED,
                    false, // no STATE_RESTORED event after disconnect
                    QueueState.e_CLOSED),
            new Test(
                    TestSession.QueueTestStep.REOPEN_OPENING,
                    QueueControlEvent.Type.e_QUEUE_REOPEN_RESULT,
                    OpenQueueResult.CANCELED,
                    true, // expect STATE_RESTORED event after disconnect
                    QueueState.e_CLOSED),
            new Test(
                    TestSession.QueueTestStep.REOPEN_CONFIGURING,
                    QueueControlEvent.Type.e_QUEUE_REOPEN_RESULT,
                    OpenQueueResult.CANCELED,
                    true, // expect STATE_RESTORED event after disconnect
                    QueueState.e_CLOSED),
            new Test(
                    TestSession.QueueTestStep.CONFIGURING,
                    QueueControlEvent.Type.e_QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.CANCELED,
                    false, // no STATE_RESTORED event after disconnect
                    QueueState.e_OPENED),
            new Test(
                    TestSession.QueueTestStep.CONFIGURING_RECONFIGURING,
                    QueueControlEvent.Type.e_QUEUE_CONFIGURE_RESULT,
                    null, // no queue operation event
                    false, // no STATE_RESTORED event after disconnect
                    QueueState.e_OPENED),
            new Test(
                    TestSession.QueueTestStep.CLOSE_CONFIGURING,
                    QueueControlEvent.Type.e_QUEUE_CLOSE_RESULT,
                    CloseQueueResult.CANCELED,
                    false, // no STATE_RESTORED event after disconnect
                    QueueState.e_CLOSED),
            new Test(
                    TestSession.QueueTestStep.CLOSE_CLOSING,
                    QueueControlEvent.Type.e_QUEUE_CLOSE_RESULT,
                    CloseQueueResult.CANCELED,
                    false, // no STATE_RESTORED event after disconnect
                    QueueState.e_CLOSED)
        };

        QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();

        TestSession obj = new TestSession();

        try {
            for (Test test : tests) {
                logger.info("Testing step {}", test.queueTestStep);

                // Starting session
                obj.start();

                // Create a queue
                QueueImpl queue = obj.createQueueOnStep(queueOptions, test.queueTestStep);

                // Trigger channel drop
                obj.connection().setChannelStatus(ChannelStatus.CHANNEL_DOWN);

                // TODO: native SDK cancels requests first then sends
                //       CONNECTION_LOST. Should we do the same in Java?
                obj.verifySessionEvent(BrokerSessionEvent.Type.e_CONNECTION_LOST);

                if (test.expectedOperationResult != null) {
                    obj.verifyQueueControlEvent(test.eventType, test.expectedOperationResult);
                }
                if (test.waitStateRestoredOnDisconnect) {
                    obj.verifySessionEvent(BrokerSessionEvent.Type.e_STATE_RESTORED);
                }

                // Verify no more events
                assertNull(obj.nextEvent());

                assertEquals(queue.getState(), test.queueStateAfterDisconnect);

                // Restore the channel
                obj.connection().setChannelStatus(ChannelStatus.CHANNEL_UP);

                // Negotiate
                obj.sendNegotiationResponse(
                        obj.verifyNegotiationRequest(), StatusCategory.E_SUCCESS);

                obj.verifySessionEvent(BrokerSessionEvent.Type.e_RECONNECTED);

                if (test.queueStateAfterDisconnect == QueueState.e_OPENED) {
                    // The queue is being reopening
                    obj.reopenQueue(queue);
                }

                obj.verifySessionEvent(BrokerSessionEvent.Type.e_STATE_RESTORED);

                // Verify no more events
                assertNull(obj.nextEvent());

                // Recreate a queue
                if (test.queueStateAfterDisconnect == QueueState.e_OPENED) {
                    obj.closeQueue(queue);
                    obj.arriveAtStep(queue, queueOptions, test.queueTestStep);
                } else {
                    queue = obj.createQueueOnStep(queueOptions, test.queueTestStep);
                }

                // Stop the session but don't wait for DISCONNECTED event
                obj.stop(false, Duration.ofSeconds(1));

                if (test.expectedOperationResult != null) {
                    obj.verifyQueueControlEvent(test.eventType, test.expectedOperationResult);
                }

                if (test.waitStateRestoredOnDisconnect) {
                    obj.verifySessionEvent(BrokerSessionEvent.Type.e_STATE_RESTORED);
                }

                obj.verifySessionEvent(BrokerSessionEvent.Type.e_DISCONNECTED);

                // Verify no more events
                assertNull(obj.nextEvent());

                assertEquals(QueueState.e_CLOSED, queue.getState());
            }
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            obj.session().stop(Duration.ofMillis(100));
            obj.session().linger();
        }
    }

    @Test
    void multipleStartAsync() {
        final Duration TIMEOUT = Duration.ofSeconds(1);
        final TestSession testSession = new TestSession();

        testSession.session().startAsync(TIMEOUT);
        testSession.sendNegotiationResponse(
                testSession.verifyNegotiationRequest(), StatusCategory.E_SUCCESS);
        testSession.verifySessionEvent(BrokerSessionEvent.Type.e_CONNECTED);

        // Ensure that after receiving CONNECTED event `startAsync` may be called again to receive
        // CONNECTED
        // event but not CONNECTION_IN_PROGRESS.
        for (int i = 0; i < 10000; ++i) {
            testSession.session().startAsync(TIMEOUT);
            testSession.verifySessionEvent(BrokerSessionEvent.Type.e_CONNECTED);
        }

        testSession.stop();
    }

    @Test
    void multipleStopAsync() {
        final Duration TIMEOUT = Duration.ofSeconds(1);
        final TestSession testSession = new TestSession();

        testSession.start();

        testSession.session.stopAsync(TIMEOUT);
        testSession.sendDisconnectResponse(testSession.verifyDisconnectRequest());
        testSession.verifySessionEvent(BrokerSessionEvent.Type.e_DISCONNECTED);

        // Ensure that after receiving DISCONNECTED event `stopAsync` may be called again to receive
        // DISCONNECTED
        // event but not DISCONNECTION_IN_PROGRESS.
        for (int i = 0; i < 10000; ++i) {
            testSession.session().stopAsync(TIMEOUT);
            testSession.verifySessionEvent(BrokerSessionEvent.Type.e_DISCONNECTED);
        }
    }
}
