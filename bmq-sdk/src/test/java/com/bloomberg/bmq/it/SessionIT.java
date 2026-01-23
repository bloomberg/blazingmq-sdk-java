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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.bloomberg.bmq.AbstractSession;
import com.bloomberg.bmq.AckMessage;
import com.bloomberg.bmq.AckMessageHandler;
import com.bloomberg.bmq.BMQException;
import com.bloomberg.bmq.CompressionAlgorithm;
import com.bloomberg.bmq.CorrelationId;
import com.bloomberg.bmq.Event;
import com.bloomberg.bmq.HostHealthMonitor;
import com.bloomberg.bmq.HostHealthState;
import com.bloomberg.bmq.MessageGUID;
import com.bloomberg.bmq.MessageProperties;
import com.bloomberg.bmq.MessageProperty;
import com.bloomberg.bmq.PushMessage;
import com.bloomberg.bmq.PushMessageHandler;
import com.bloomberg.bmq.PutMessage;
import com.bloomberg.bmq.Queue;
import com.bloomberg.bmq.QueueControlEvent;
import com.bloomberg.bmq.QueueEventHandler;
import com.bloomberg.bmq.QueueFlags;
import com.bloomberg.bmq.QueueOptions;
import com.bloomberg.bmq.ResultCodes;
import com.bloomberg.bmq.ResultCodes.AckResult;
import com.bloomberg.bmq.ResultCodes.CloseQueueResult;
import com.bloomberg.bmq.ResultCodes.ConfigureQueueResult;
import com.bloomberg.bmq.ResultCodes.GenericResult;
import com.bloomberg.bmq.ResultCodes.OpenQueueResult;
import com.bloomberg.bmq.Session;
import com.bloomberg.bmq.SessionEvent;
import com.bloomberg.bmq.SessionEventHandler;
import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.Uri;
import com.bloomberg.bmq.impl.infr.msg.ConsumerInfo;
import com.bloomberg.bmq.impl.infr.msg.ControlMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.StatusCategory;
import com.bloomberg.bmq.impl.infr.proto.Protocol;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.it.util.BmqBroker;
import com.bloomberg.bmq.it.util.BmqBrokerSimulator;
import com.bloomberg.bmq.it.util.TestTools;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionIT {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static final int DEFAULT_WAIT_TIMEOUT = 60; // sec
    static final int NO_CLIENT_REQUEST_TIMEOUT = 1; // sec

    static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(20);
    static final Duration SHORT_TIMEOUT = Duration.ofSeconds(5);
    static final QueueOptions QUEUE_OPTIONS = QueueOptions.builder().setConsumerPriority(2).build();

    static Uri createUniqueUri() {
        return BmqBroker.Domains.Priority.generateQueueUri();
    }

    static class TestSession {
        private final QueueHandler queueHandler;
        private final AbstractSession session;
        private final BlockingQueue<Event> eventQueue;

        // When set to true and event is added to queue, event handler will
        // try to acquire semaphore in order to wait before processing next events.
        private volatile boolean waitToProcessNextEvents = false;
        private final Semaphore waitToProcessSemaphore;

        private class SessionHandler implements SessionEventHandler {
            @Override
            public void handleSessionEvent(SessionEvent event) {
                logger.info("handle {} event", event.type());
                enqueueEvent(event);
            }
        }

        private class QueueHandler
                implements QueueEventHandler, AckMessageHandler, PushMessageHandler {

            @Override
            public void handleQueueEvent(QueueControlEvent event) {
                logger.info("handleQueueEvent: {}", event);
                enqueueEvent(event);
            }

            @Override
            public void handleAckMessage(AckMessage msg) {
                String ackStatus =
                        msg.status() == AckResult.SUCCESS
                                ? "ACK"
                                : "NACK with " + msg.status() + " status";

                logger.info("Got {} for correlationId: {}", ackStatus, msg.correlationId());

                enqueueEvent(msg);
            }

            @Override
            public void handlePushMessage(PushMessage msg) {
                logger.info("handlePushEvent: {}", msg);
                enqueueEvent(msg);
            }
        }

        public TestSession(SessionOptions opts) {
            queueHandler = new QueueHandler();
            session = new Session(opts, new SessionHandler());
            eventQueue = new LinkedBlockingQueue<>();
            waitToProcessSemaphore = new Semaphore(0);
        }

        public TestSession(int testPort) {
            this(
                    SessionOptions.builder()
                            .setBrokerUri(URI.create("tcp://localhost:" + testPort))
                            .build());
        }

        public void setWaitToProcessNextEvents() {
            if (waitToProcessNextEvents) {
                throw new IllegalStateException("Already waiting");
            }
            waitToProcessNextEvents = true;
        }

        public void processNextEvents() {
            if (!waitToProcessNextEvents) {
                throw new IllegalStateException("Already processing");
            }
            waitToProcessNextEvents = false;
            waitToProcessSemaphore.release();
        }

        public void start(Duration timeout) throws BMQException {
            session.start(timeout);
        }

        public void startAsync(Duration timeout) throws BMQException {
            session.startAsync(timeout);
        }

        public void stop(Duration timeout) throws BMQException {
            session.stop(timeout);
        }

        public void stopAsync(Duration timeout) throws BMQException {
            session.stopAsync(timeout);
        }

        public void linger() throws BMQException {
            session.linger();
        }

        public boolean isStarted() {
            return session.isStarted();
        }

        public Queue getReadWriteQueue(Uri uri, boolean withAck) throws BMQException {
            long flags = QueueFlags.setWriter(0);
            flags = QueueFlags.setReader(flags);
            if (withAck) {
                flags = QueueFlags.setAck(flags);
            }

            return session.getQueue(
                    uri,
                    flags,
                    queueHandler, // QueueEventHandler
                    queueHandler, // AckMessageHandler
                    queueHandler); // PushMessageHandler
        }

        public Queue getWriterQueue(Uri uri, boolean withAck) throws BMQException {
            long flags = QueueFlags.setWriter(0);
            if (withAck) {
                flags = QueueFlags.setAck(flags);
            }
            return session.getQueue(
                    uri,
                    flags,
                    queueHandler, // QueueEventHandler
                    queueHandler, // AckMessageHandler
                    null); // PushMessageHandler
        }

        public Queue getReaderQueue(Uri uri) throws BMQException {
            return getReaderQueue(uri, queueHandler);
        }

        public Queue getReaderQueue(Uri uri, QueueEventHandler queueHandler) throws BMQException {
            return session.getQueue(
                    uri,
                    QueueFlags.setReader(0),
                    queueHandler, // QueueEventHandler
                    null, // AckMessageHandler
                    this.queueHandler); // PushMessageHandler
        }

        private void enqueueEvent(Event event) {
            eventQueue.add(event);

            if (waitToProcessNextEvents) {
                try {
                    if (!waitToProcessSemaphore.tryAcquire(
                            DEFAULT_WAIT_TIMEOUT, TimeUnit.SECONDS)) {
                        throw new RuntimeException("Timeout while waiting for event confirmation");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    fail("Interrupted");
                }
            }
        }

        private QueueControlEvent waitQueueEvent() {
            return nextEvent(QueueControlEvent.class);
        }

        public AckMessage waitAckMessage() {
            return nextEvent(AckMessage.class);
        }

        public PushMessage waitPushMessage() {
            return nextEvent(PushMessage.class);
        }

        public SessionEvent.StartStatus waitStartSessionEvent() {
            return nextEvent(SessionEvent.StartStatus.class);
        }

        public SessionEvent.StopStatus waitStopSessionEvent() {
            return nextEvent(SessionEvent.StopStatus.class);
        }

        public SessionEvent.ConnectionLost waitConnectionLostEvent() {
            return nextEvent(SessionEvent.ConnectionLost.class);
        }

        public SessionEvent.ConnectionLost waitConnectionLostEvent(List<AckMessage> receivedAcks) {
            // Here it is expected that ACK messages may be received before
            // ConnectionLost event

            for (Event event; (event = waitAnyEvent()) != null; ) {
                if (event instanceof AckMessage) {
                    // Collect ACK messages
                    receivedAcks.add((AckMessage) event);
                } else {
                    // Return expected ConnectionLost event
                    logger.info(
                            "Received {} ACK messages before ConnectionLost event",
                            receivedAcks.size());

                    return (SessionEvent.ConnectionLost) event;
                }
            }

            logger.info("Didn't receive ConnectionLost event");
            return null;
        }

        public SessionEvent.Reconnected waitReconnectedEvent() {
            return nextEvent(SessionEvent.Reconnected.class);
        }

        public SessionEvent.StateRestored waitStateRestoredEvent() {
            return nextEvent(SessionEvent.StateRestored.class);
        }

        public SessionEvent.SlowConsumerHighWatermark waitHighWaterMarkEvent() {
            return nextEvent(SessionEvent.SlowConsumerHighWatermark.class);
        }

        public SessionEvent.SlowConsumerNormal waitLowWaterMarkEvent() {
            return nextEvent(SessionEvent.SlowConsumerNormal.class);
        }

        public SessionEvent.HostUnhealthy waitHostUnhealthyEvent() {
            return nextEvent(SessionEvent.HostUnhealthy.class);
        }

        public SessionEvent.HostHealthRestored waitHostHealthRestoredEvent() {
            return nextEvent(SessionEvent.HostHealthRestored.class);
        }

        public Event waitAnyEvent() {
            return waitAnyEvent(DEFAULT_WAIT_TIMEOUT);
        }

        public void checkNoEvent() {
            assertNull(waitAnyEvent(1));
        }

        public Event waitAnyEvent(int sec) {
            return nextEvent(Event.class, Argument.expectPositive(sec, "sec"), false);
        }

        private <T extends Event> T nextEvent(Class<T> eventClass) {
            return nextEvent(eventClass, DEFAULT_WAIT_TIMEOUT, true);
        }

        private <T extends Event> T nextEvent(Class<T> eventClass, int sec) {
            return nextEvent(eventClass, sec, true);
        }

        private <T extends Event> T nextEvent(
                Class<T> eventClass, int sec, boolean throwException) {
            Event event = null;
            try {
                event = eventQueue.poll(Argument.expectPositive(sec, "sec"), TimeUnit.SECONDS);

                if (event == null) {
                    String msg = "Timeout while waiting for event: " + eventClass.getName();
                    logger.info(msg);

                    if (throwException) {
                        throw new IllegalStateException(msg);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Exchange interrupted: ", e);
                fail();
            }

            return eventClass.cast(event);
        }
    }

    static class TestMonitor implements HostHealthMonitor {
        private final Set<HostHealthMonitor.Handler> handlers = new LinkedHashSet<>();
        private volatile HostHealthState state;

        TestMonitor() {
            this(HostHealthState.Healthy);
        }

        TestMonitor(HostHealthState initialState) {
            state = initialState;
        }

        @Override
        public boolean addHandler(HostHealthMonitor.Handler handler) {
            Argument.expectNonNull(handler, "handler");

            synchronized (handlers) {
                return handlers.add(handler);
            }
        }

        @Override
        public boolean removeHandler(HostHealthMonitor.Handler handler) {
            Argument.expectNonNull(handler, "handler");

            synchronized (handlers) {
                return handlers.remove(handler);
            }
        }

        @Override
        public HostHealthState hostHealthState() {
            return state;
        }

        void setState(HostHealthState state) {
            this.state = state;

            synchronized (handlers) {
                for (HostHealthMonitor.Handler handler : handlers) {
                    handler.onHostHealthStateChanged(this.state);
                }
            }
        }
    }

    static String createPayload(int msgSize, String filler) {
        StringBuilder sb = new StringBuilder(msgSize);
        while (sb.capacity() < msgSize) {
            sb.append("[").append(filler).append("]");
        }
        sb.setLength(msgSize);
        return sb.toString();
    }

    static PutMessage makePut(Queue queue, String payload) {
        return makePut(queue, payload, true);
    }

    static PutMessage makePut(Queue queue, String payload, boolean setCorrId) {
        Argument.expectNonNull(queue, "queue");
        Argument.expectNonNull(payload, "payload");

        PutMessage msg = queue.createPutMessage(ByteBuffer.wrap(payload.getBytes()));
        if (setCorrId) {
            msg.setCorrelationId();
        }

        MessageProperties mp = msg.messageProperties();

        mp.setPropertyAsString("routingId", "abcd-efgh-ijkl");
        mp.setPropertyAsInt64("timestamp", 123456789L);

        return msg;
    }

    static PutMessage makePut(Queue queue, byte[] payload) {
        Argument.expectNonNull(queue, "queue");
        Argument.expectNonNull(payload, "payload");

        PutMessage msg = queue.createPutMessage(ByteBuffer.wrap(payload));
        msg.setCorrelationId();

        MessageProperties mp = msg.messageProperties();

        mp.setPropertyAsString("routingId", "abcd-efgh-ijkl");
        mp.setPropertyAsInt64("timestamp", 123456789L);

        return msg;
    }

    private static void verifyOpenRequest(ControlMessageChoice request) {
        logger.info("Verify open request: {}", request);

        assertNotNull(request);
        assertTrue(request.isOpenQueueValue());
    }

    private static void verifyConfigureRequest(ControlMessageChoice request) {
        logger.info("Verify configure request: {}", request);

        assertNotNull(request);
        assertTrue(request.isConfigureStreamValue());
    }

    private static void verifyCloseRequest(ControlMessageChoice request) {
        verifyCloseRequest(request, true);
    }

    private static void verifyCloseRequest(ControlMessageChoice request, boolean isFinal) {
        logger.info("Verify close request: {}", request);

        assertNotNull(request);
        assertTrue(request.isCloseQueueValue());
        assertEquals(isFinal, request.closeQueue().isFinal());
    }

    private static void verifyQueueControlEvent(
            Event event,
            QueueControlEvent.Type eventType,
            ResultCodes.GenericCode result,
            Queue queue) {
        assertNotNull(event);

        QueueControlEvent queueControlEvent = assertInstanceOf(QueueControlEvent.class, event);
        assertEquals(queue, queueControlEvent.queue());
        assertEquals(eventType, queueControlEvent.type());
        assertEquals(result, queueControlEvent.result());
    }

    @Test
    void testConnectionDown() throws BMQException, IOException {

        logger.info("===========================================================");
        logger.info("BEGIN Testing SessionIT connection down.");
        logger.info("===========================================================");

        // 1) Bring up the broker.
        // 2) Invoke session 'start'.
        // 3) Check the session is started.
        // 4) Stop the broker.
        // 5) Wait for connection lost event.
        // 6) Start the broker again.
        // 7) Wait for reconnected and state restored events.
        // 8) Stop the session and the broker.

        // 1) Bring up the broker.
        BmqBroker broker = BmqBroker.createStartedBroker();
        TestSession session = new TestSession(broker.sessionOptions());

        try {
            // 2) Invoke session 'start'.
            logger.info("Starting session...");
            session.start(DEFAULT_TIMEOUT);

            // 3) Check the session is started.
            assertTrue(session.isStarted());

            // 4) Stop the broker.
            CompletableFuture<Void> stopFuture = broker.stopAsync();

            // 5) Wait for connection lost event.
            session.waitConnectionLostEvent();
            assertFalse(session.isStarted());

            // Give some time for the broker process to die
            stopFuture.get();

            // 6) Start the broker again.
            broker.start();

            // 7) Wait for reconnected and state restored events.
            session.waitReconnectedEvent();
            session.waitStateRestoredEvent();
            assertTrue(session.isStarted());

            broker.setDropTmpFolder();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            // 8) Stop the session and the broker.
            session.stop(DEFAULT_TIMEOUT);
            assertFalse(session.isStarted());
            session.linger();

            broker.close();

            logger.info("=========================================================");
            logger.info("END Testing SessionIT connection down.");
            logger.info("=========================================================");
        }
    }

    @Test
    void testAsyncConnection() throws BMQException, IOException {

        logger.info("================================================");
        logger.info("BEGIN Testing SessionIT async connection.");
        logger.info("================================================");

        // 1) Bring up the broker.
        // 2) Invoke session 'startAsync'.
        // 3) Wait for session started event and check the session is started.
        // 4) Invoke session 'stopAsync'.
        // 5) Wait for session stopped event and check the session is not started.
        // 6) Stop the broker.
        // 7) Invoke session 'startAsync'.
        // 8) Wait for start timeout and canceled events and check the session is not started.
        // 9) Stop the session and the broker.

        // 1) Bring up the broker.
        BmqBroker broker = BmqBroker.createStartedBroker();
        TestSession session = new TestSession(broker.sessionOptions());

        try {
            // 2) Invoke session 'start'.
            logger.info("Starting session async...");
            session.startAsync(DEFAULT_TIMEOUT);

            // 3) Wait for session started event and check the session is started.
            assertEquals(GenericResult.SUCCESS, session.waitStartSessionEvent().result());
            assertTrue(session.isStarted());

            // 4) Invoke session 'stopAsync'.
            session.stopAsync(DEFAULT_TIMEOUT);

            // 5) Wait for session stopped event and check the session is not started.
            assertEquals(GenericResult.SUCCESS, session.waitStopSessionEvent().result());

            assertFalse(session.isStarted());

            // 6) Stop the broker.
            broker.stop();

            // 7) Invoke session 'startAsync'.
            session.startAsync(DEFAULT_TIMEOUT);

            // 8) Wait for start timeout and canceled events and check the session is not started.
            assertEquals(GenericResult.TIMEOUT, session.waitStartSessionEvent().result());
            assertEquals(GenericResult.CANCELED, session.waitStartSessionEvent().result());
            assertFalse(session.isStarted());

            broker.setDropTmpFolder();

        } finally {
            // 9) Stop the session and the broker.
            session.linger();

            broker.close();

            logger.info("==============================================");
            logger.info("END Testing SessionIT async connection.");
            logger.info("==============================================");
        }
    }

    @Test
    void testQueueRestored() throws BMQException, IOException {

        logger.info("==============================================");
        logger.info("BEGIN Testing SessionIT queue restored.");
        logger.info("==============================================");

        // 1) Bring up the broker
        // 2) Without starting a session get a queue and try to open it expecting an exception
        // 3) Start the session
        // 4) Open the queue and post a PUT message
        // 5) Wait for an ACK event and verify it
        // 6) Stop the broker and wait for connection lost event
        // 7) Try to post to the queue expecting an exception
        // 8) Start the broker and wait for reconnected and state restored events
        // 9) Verify that the message can be posted to the queue without any exception
        // 10) Wait for an ACK event
        // 11) Close the queue, stop the session and the broker

        // 1) Bring up the broker.
        BmqBroker broker = BmqBroker.createStartedBroker();
        TestSession session = new TestSession(broker.sessionOptions());

        try {
            // 2) Without starting a session get a queue and try to open it expecting an exception
            Queue queue = session.getWriterQueue(createUniqueUri(), false);
            assertFalse(queue.isOpen());

            try {
                queue.open(QueueOptions.createDefault(), DEFAULT_TIMEOUT);
                throw new IllegalStateException("Opened via not started session");
            } catch (BMQException e) {
                assertFalse(queue.isOpen());
            }

            // 3) Start the session
            session.start(DEFAULT_TIMEOUT);

            // 4) Open the queue and post a PUT message
            queue.open(QueueOptions.createDefault(), DEFAULT_TIMEOUT);

            assertTrue(queue.isOpen());

            PutMessage putMsg = makePut(queue, "TEST");

            queue.post(putMsg);

            // 5) Wait for an ACK event and verify it
            verifyAckMessages(session, queue, putMsg);

            // 6) Stop the broker and wait for connection lost event
            CompletableFuture<Void> stopFuture = broker.stopAsync();

            session.waitConnectionLostEvent();

            // The Queue is still in open state
            assertTrue(queue.isOpen());

            // 7) Try to post to the queue expecting an exception
            try {
                queue.post(putMsg);
                throw new IllegalStateException("Post via not started session");
            } catch (BMQException e) {
                // OK
                logger.debug("Caught expected BMQException: ", e);
            }

            // 8) Start the broker and wait for reconnected and state restored events
            stopFuture.get();
            broker.start();

            session.waitReconnectedEvent();
            QueueControlEvent event = session.waitQueueEvent();
            assertEquals(QueueControlEvent.Type.QUEUE_REOPEN_RESULT, event.type());
            assertEquals(ResultCodes.OpenQueueResult.SUCCESS, event.result());
            session.waitStateRestoredEvent();

            // 9) Verify that the message can be posted to the queue without any exception
            assertTrue(queue.isOpen());
            queue.post(putMsg);

            // 10) Wait for an ACK event
            session.waitAckMessage();

            // 11) Close the queue, stop the session and the broker
            queue.close(DEFAULT_TIMEOUT);

            broker.setDropTmpFolder();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            session.stop(DEFAULT_TIMEOUT);
            session.linger();

            broker.close();

            logger.info("============================================");
            logger.info("END Testing SessionIT queue restored.");
            logger.info("============================================");
        }
    }

    @Test
    void testQueueReadAndConfirm() throws BMQException, IOException {

        logger.info("=======================================================");
        logger.info("BEGIN Testing SessionIT testQueueReadAndConfirm.");
        logger.info("=======================================================");

        // 1) Bring up the broker
        // 2) Start the session
        // 3) Open the queue for writing and post a PUT message
        // 4) Wait for an ACK event
        // 5) Close the queue and open another one for reading
        // 6) Wait for a PUSH message
        // 7) Close the queue and open it for reading again
        // 8) Wait for a PUSH message, confirm a PUSH message
        // 9) Close the queue and open it for reading again
        // 10) Verify that there is no more PUSH event
        // 11) Close the queue, stop the session and the broker

        logger.info("Step 1: Bring up the broker");
        BmqBroker broker = BmqBroker.createStartedBroker();
        TestSession session = new TestSession(broker.sessionOptions());

        try {
            logger.info("Step 2: Start the session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("Step 3: Open the queue for writing and post a PUT message");
            Uri uri = createUniqueUri();
            Queue queue = session.getWriterQueue(uri, false);
            queue.open(QueueOptions.createDefault(), DEFAULT_TIMEOUT);

            PutMessage putMsg = makePut(queue, "TEST");

            queue.post(putMsg);

            logger.info("Step 4: Wait for an ACK event");
            session.waitAckMessage();

            logger.info("Step 5: Close the queue and open another one for reading");
            queue.close(DEFAULT_TIMEOUT);

            queue = session.getReaderQueue(uri);
            queue.open(QueueOptions.createDefault(), DEFAULT_TIMEOUT);

            logger.info("Step 6: Wait for a PUSH message");
            PushMessage pushMsg = session.waitPushMessage();
            assertNotNull(pushMsg);

            logger.info("Step 7: Close the queue and open it for reading again");
            queue.close(DEFAULT_TIMEOUT);

            queue.open(QueueOptions.createDefault(), DEFAULT_TIMEOUT);

            logger.info("Step 8: Wait for a PUSH message, confirm a PUSH message");
            pushMsg = session.waitPushMessage();
            assertNotNull(pushMsg);

            assertEquals(GenericResult.SUCCESS, pushMsg.confirm());

            logger.info("Step 9: Close the queue and open it for reading again");
            queue.close(DEFAULT_TIMEOUT);
            pushMsg = null;

            queue.open(QueueOptions.createDefault(), DEFAULT_TIMEOUT);

            logger.info("Step 10: Verify that there is no more PUSH event");
            try {
                pushMsg = session.waitPushMessage();
                throw new IllegalStateException("Got confirmed message again");
            } catch (IllegalStateException e) {
                // Expected
                assertNull(pushMsg);
            }

            logger.info("Step 11: Close the queue, stop the session and the broker");
            queue.close(DEFAULT_TIMEOUT);

            broker.setDropTmpFolder();
        } finally {
            session.stop(DEFAULT_TIMEOUT);
            session.linger();

            broker.close();

            logger.info("=====================================================");
            logger.info("END Testing SessionIT testQueueReadAndConfirm.");
            logger.info("=====================================================");
        }
    }

    /**
     * Test for asynchronous operations under queue (open-configure-close);
     *
     * <p>This test is replica of corresponding BrokerSession test
     *
     * <ul>
     *   <li>start broker simulator in active mode
     *   <li>create session
     *   <li>start session
     *   <li>create queue
     *   <li>open queue asynchronously
     *   <li>check that proper event was handled by user-defined user handler
     *   <li>repeat previous 2 steps for configure queue and close queue
     *   <li>stop session
     * </ul>
     */
    @Test
    void openConfigureCloseQueueAsyncTest() throws BMQException {
        logger.info("=================================================================");
        logger.info("BEGIN Testing SessionIT openConfigureCloseQueueAsyncTest.");
        logger.info("=================================================================");

        logger.info("1. start broker simulator in auto mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_AUTO_MODE);
        server.start();

        logger.info("2. create session");
        TestSession session = new TestSession(server.getPort());

        try {
            logger.info("3. start session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. create queue");
            Queue queue = session.getReaderQueue(createUniqueUri());
            assertFalse(queue.isOpen());

            logger.info("5. open queue asynchronously");
            queue.openAsync(QUEUE_OPTIONS, DEFAULT_TIMEOUT);
            QueueControlEvent queueEvent = session.waitQueueEvent();

            logger.info("6. check that proper event was handled by user-defined user handler");
            assertEquals(QueueControlEvent.Type.QUEUE_OPEN_RESULT, queueEvent.type());
            assertEquals(ResultCodes.OpenQueueResult.SUCCESS, queueEvent.result());
            assertTrue(queue.isOpen());

            logger.info("7. configure queue asynchronously");
            queue.configureAsync(QUEUE_OPTIONS, DEFAULT_TIMEOUT);
            queueEvent = session.waitQueueEvent();

            logger.info("8. check that proper event was handled by user-defined user handler");
            assertEquals(QueueControlEvent.Type.QUEUE_CONFIGURE_RESULT, queueEvent.type());
            assertEquals(ResultCodes.ConfigureQueueResult.SUCCESS, queueEvent.result());

            logger.info("9. close queue asynchronously");
            queue.closeAsync(DEFAULT_TIMEOUT);
            queueEvent = session.waitQueueEvent();

            logger.info("10. check that proper event was handled by user-defined user handler");
            assertEquals(QueueControlEvent.Type.QUEUE_CLOSE_RESULT, queueEvent.type());
            assertEquals(ResultCodes.CloseQueueResult.SUCCESS, queueEvent.result());

            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());
            verifyCloseRequest(server.nextClientRequest());

        } finally {
            logger.info("11. stop session");
            session.stop(DEFAULT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("===============================================================");
            logger.info("END Testing SessionIT openConfigureCloseQueueAsyncTest.");
            logger.info("===============================================================");
        }
    }

    /**
     * Test for sending suspend/resume request after queue has been opened
     *
     * <ul>
     *   <li>start broker simulator in manual mode
     *   <li>create session
     *   <li>start session
     *   <li>create queue
     *   <li>open queue async
     *   <li>set host health to unhealthy
     *   <li>check if queue is opened (and suspended if configured)
     *   <li>close the queue
     *   <li>open queue async
     *   <li>set host health back to healthy
     *   <li>check if queue is opened (and resumed if configured)
     *   <li>close the queue
     *   <li>stop session
     * </ul>
     */
    private void openQueueHostHeathChangesTest(
            boolean isSensitive, boolean isOpenDelayed, boolean isConfigureDelayed)
            throws BMQException {
        logger.info("======================================================");
        logger.info("BEGIN Testing SessionIT openQueueHostHeathChangesTest.");
        logger.info("======================================================");

        logger.info(
                "Queue sensitive={}, openDelayed={}, configureDelayed={}",
                isSensitive,
                isOpenDelayed,
                isConfigureDelayed);

        logger.info("1. start broker simulator in manual mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_MANUAL_MODE);
        server.start();

        logger.info("2. create session");
        TestMonitor monitor = new TestMonitor();
        SessionOptions opts =
                SessionOptions.builder()
                        .setBrokerUri(server.getURI())
                        .setHostHealthMonitor(monitor)
                        .build();
        TestSession session = new TestSession(opts);

        try {
            server.pushSuccess(); // OK for nego
            logger.info("3. start session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. create queue");
            Queue queue = session.getReaderQueue(createUniqueUri());
            assertFalse(queue.isOpen());

            logger.info("5. open queue async");

            QueueOptions options =
                    QueueOptions.builder().setSuspendsOnBadHostHealth(isSensitive).build();

            final Duration DELAY = Duration.ofSeconds(1);
            assert DELAY.toMillis() < DEFAULT_TIMEOUT.toMillis();

            // Open request should be sent anyway
            if (isOpenDelayed) {
                server.pushItem(StatusCategory.E_SUCCESS, DELAY);
            } else {
                server.pushSuccess();
            }

            // Configure request will be sent if queue is not sensitive or if
            // open response comes before the host becomes unhealthy.
            boolean shouldSendConfigure = !isSensitive || !isOpenDelayed;
            logger.info("Should send configure: {}", shouldSendConfigure);
            if (shouldSendConfigure) {
                if (isConfigureDelayed) {
                    server.pushItem(StatusCategory.E_SUCCESS, DELAY);
                } else {
                    server.pushSuccess();
                }
            }

            // Open the queue
            queue.openAsync(options, DEFAULT_TIMEOUT);

            verifyOpenRequest(server.nextClientRequest()); // open request

            boolean isConfigureVerified = false;
            boolean isOpenEventVerified = false;
            if (!isOpenDelayed) {
                // If open response comes before unhealthy state then configure
                // request will be sent regardless of queue health settings. We
                // may check that configure request is sent here.

                verifyConfigureRequest(server.nextClientRequest()); // conf request
                isConfigureVerified = true;

                if (!isConfigureDelayed) {
                    // if configure response comes before unhealthy state then
                    // we may check that queue is opened.
                    verifyQueueControlEvent(
                            session.waitQueueEvent(),
                            QueueControlEvent.Type.QUEUE_OPEN_RESULT,
                            OpenQueueResult.SUCCESS,
                            queue);
                    isOpenEventVerified = true;
                }
            }

            logger.info(
                    "Is configure verified: {}, is open event verified: {}",
                    isConfigureVerified,
                    isOpenEventVerified);

            logger.info("6. set host health to unhealthy");
            // If queue is sensitive then there should be a suspend request.
            // If open response comes after the host becomes unhealthy then
            // the queue will go directly to suspended state without suspend
            // request.
            final boolean shouldSendSuspend = isSensitive && !isOpenDelayed;
            logger.info("Should send suspend: {}", shouldSendSuspend);
            if (shouldSendSuspend) {
                server.pushSuccess(); // for suspend request
            }

            monitor.setState(HostHealthState.Unhealthy);
            assertNotNull(session.waitHostUnhealthyEvent());

            logger.info("7. check if queue is opened{}", isSensitive ? " and suspended" : "");
            if (!isOpenEventVerified) {
                verifyQueueControlEvent(
                        session.waitQueueEvent(),
                        QueueControlEvent.Type.QUEUE_OPEN_RESULT,
                        OpenQueueResult.SUCCESS,
                        queue);
                logger.info("Open event verified");
            }

            assertTrue(queue.isOpen());

            if (shouldSendConfigure && !isConfigureVerified) {
                verifyConfigureRequest(server.nextClientRequest()); // conf request
                logger.info("Configure verified");
            }

            if (shouldSendSuspend) {
                verifyQueueControlEvent(
                        session.waitQueueEvent(),
                        QueueControlEvent.Type.QUEUE_SUSPENDED,
                        ConfigureQueueResult.SUCCESS,
                        queue);
                verifyConfigureRequest(server.nextClientRequest()); // suspend request
                logger.info("Suspend verified");
            }

            assertTrue(queue.isOpen());
            session.checkNoEvent();
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            logger.info("8. close the queue");
            server.pushSuccess(2);
            queue.close(DEFAULT_TIMEOUT);
            assertFalse(queue.isOpen());

            verifyConfigureRequest(server.nextClientRequest()); // deconfigure request
            verifyCloseRequest(server.nextClientRequest()); // close request

            session.checkNoEvent();
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            logger.info("9. open queue async");
            // Open request should be sent anyway
            if (isOpenDelayed) {
                server.pushItem(StatusCategory.E_SUCCESS, DELAY);
            } else {
                server.pushSuccess();
            }

            // We are in unhealthy state. If queue is not sensitive or open
            // response comes after the host health changes to healthy then the
            // queue will not be suspended and configure request will be sent.
            shouldSendConfigure = !isSensitive || isOpenDelayed;
            logger.info("Should send configure: {}", shouldSendConfigure);
            if (shouldSendConfigure) {
                if (isConfigureDelayed) {
                    server.pushItem(StatusCategory.E_SUCCESS, DELAY);
                } else {
                    server.pushSuccess();
                }
            }

            // open the queue
            queue.openAsync(options, DEFAULT_TIMEOUT);

            verifyOpenRequest(server.nextClientRequest()); // open request

            isConfigureVerified = false;
            isOpenEventVerified = false;
            if (!isOpenDelayed) {
                if (!isSensitive) {
                    // If queue is not sensitive and open response comes before
                    // healthy state then configure request will be sent and we may
                    // check it here.

                    verifyConfigureRequest(server.nextClientRequest()); // conf request
                    isConfigureVerified = true;

                    if (!isConfigureDelayed) {
                        // if configure response comes before healthy state then
                        // we may check that queue is opened.
                        verifyQueueControlEvent(
                                session.waitQueueEvent(),
                                QueueControlEvent.Type.QUEUE_OPEN_RESULT,
                                OpenQueueResult.SUCCESS,
                                queue);
                        isOpenEventVerified = true;
                    }
                } else {
                    // If queue is sensitive and open response comes before
                    // healthy state then configure request will not be sent
                    // and queue wil be opened.
                    verifyQueueControlEvent(
                            session.waitQueueEvent(),
                            QueueControlEvent.Type.QUEUE_OPEN_RESULT,
                            OpenQueueResult.SUCCESS,
                            queue);
                    isOpenEventVerified = true;
                }
            }

            logger.info(
                    "Is configure verified: {}, is open event verified: {}",
                    isConfigureVerified,
                    isOpenEventVerified);

            logger.info("10. set host health back to healthy");
            // If queue is sensitive then there should be a resume request.
            // If queue is opened before the host becomes healthy then a resume
            // request will be sent.
            final boolean shouldSendResume = isSensitive && !isOpenDelayed;
            logger.info("Should send resume: {}", shouldSendResume);
            if (shouldSendResume) {
                server.pushSuccess(); // for resume request
            }

            monitor.setState(HostHealthState.Healthy);

            logger.info("11. check if queue is opened{}", isSensitive ? " and resumed" : "");
            // HOST_HEALTH_RESTORED event should be issued immediately if queue
            // is not sensitive or open response was sent back after changing
            // host health.
            if (!isSensitive || isOpenDelayed) {
                assertNotNull(session.waitHostHealthRestoredEvent());
                logger.info("HOST_HEALTH_RESTORED event verified");
            }

            if (!isOpenEventVerified) {
                verifyQueueControlEvent(
                        session.waitQueueEvent(),
                        QueueControlEvent.Type.QUEUE_OPEN_RESULT,
                        OpenQueueResult.SUCCESS,
                        queue);
                logger.info("Open event verified");
            }

            assertTrue(queue.isOpen());

            if (shouldSendConfigure && !isConfigureVerified) {
                verifyConfigureRequest(server.nextClientRequest()); // conf request
                logger.info("Configure verified");
            }

            if (shouldSendResume) {
                verifyQueueControlEvent(
                        session.waitQueueEvent(),
                        QueueControlEvent.Type.QUEUE_RESUMED,
                        ConfigureQueueResult.SUCCESS,
                        queue);
                verifyConfigureRequest(server.nextClientRequest()); // suspend request
                logger.info("Resume verified");

                // HOST_HEALTH_RESTORED event should be issued after QUEUE_RESUMED
                assertNotNull(session.waitHostHealthRestoredEvent());
                logger.info("HOST_HEALTH_RESTORED event verified");
            }

            assertTrue(queue.isOpen());
            session.checkNoEvent();
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            logger.info("12. close the queue");
            server.pushSuccess(2);
            queue.close(DEFAULT_TIMEOUT);
            assertFalse(queue.isOpen());

            verifyConfigureRequest(server.nextClientRequest()); // deconfigure request
            verifyCloseRequest(server.nextClientRequest()); // close request

            session.checkNoEvent();
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

        } finally {
            logger.info("13. stop session");
            server.pushSuccess(); // for disconnect
            session.stop(DEFAULT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("====================================================");
            logger.info("END Testing SessionIT openQueueHostHeathChangedTest.");
            logger.info("====================================================");
        }
    }

    /** Tests for opening insensitive queue when host health changes */
    @Test
    void openInsensitiveQueueHostHeathChangesTest() throws BMQException {
        // For insensitive queues, there should be no suspend/resume requests
        // and QUEUE_SUSPENDED/QUEUE_RESUMED events.

        // Insensitive queue, open before changing host health
        openQueueHostHeathChangesTest(false, false, false);
        // Insensitive queue, open response comes after changing host health
        openQueueHostHeathChangesTest(false, true, false);
        // Insensitive queue, open configure response comes after changing host health
        openQueueHostHeathChangesTest(false, false, true);
        // Insensitive queue, open and configure responses come after changing host health
        openQueueHostHeathChangesTest(false, true, true);
    }

    /** Tests for opening sensitive queue when host health changes */
    @Test
    void openSensitiveQueueHostHeathChangesTest() throws BMQException {
        // Sensitive queue, open before changing host health.
        // When opening queue in healthy state, open and open configure
        // requests should be sent.
        // After host becomes unhealthy there should be a suspend request
        // initiated by host health handler.
        // When opening queue in unhealthy state, only open request should be sent.
        // After host becomes healthy there should be a resume request
        // initiated by host health handler.
        openQueueHostHeathChangesTest(true, false, false);

        // Sensitive queue, open response comes after changing host health.
        // When opening queue in healthy state, only open request should be sent.
        // After host becomes unhealthy there should be no suspend request.
        // When opening queue in unhealthy state, open and open configure
        // requests should be sent.
        // After host becomes healthy there should be no resume request.
        openQueueHostHeathChangesTest(true, true, false);

        // Sensitive queue, open configure response comes after changing host health.
        // When opening queue in healthy state, open and open configure
        // requests should be sent.
        // After host becomes unhealthy there should be a deferred suspend request
        // initiated by configure response handler.
        // When opening queue in unhealthy state, only open request should be sent.
        // After host becomes healthy there should be a resume request
        // initiated by host health handler.
        openQueueHostHeathChangesTest(true, false, true);

        // Sensitive queue, open and configure responses come after changing host health
        // When opening queue in healthy state, only open request should be sent.
        // After host becomes unhealthy there should be no suspend request.
        // When opening queue in unhealthy state, open and open configure
        // requests should be sent.
        // After host becomes healthy there should be no resume request.
        openQueueHostHeathChangesTest(true, true, true);
    }

    /**
     * Test for asynchronous operations failure under queue (open-configure-close) when queue event
     * handler is null.
     *
     * <p>This test is replica of corresponding BrokerSession test
     *
     * <ul>
     *   <li>start test server in active mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>create queue with null queue event handler
     *   <li>attempt to open queue asynchronously
     *   <li>open the queue synchronously
     *   <li>repeat previous 2 steps for configure queue and close queue
     *   <li>stop broker session
     * </ul>
     */
    @Test
    void openConfigureCloseQueueAsyncNullHandlerTest() {
        logger.info("====================================================================");
        logger.info("BEGIN Testing SessionIT openConfigureCloseQueueAsyncNullHandlerTest.");
        logger.info("====================================================================");

        logger.info("1. start broker simulator in auto mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_AUTO_MODE);
        server.start();

        logger.info("2. create session");
        TestSession session = new TestSession(server.getPort());

        try {
            logger.info("3. start session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. create queue with null queue event handler");
            Queue queue = session.getReaderQueue(createUniqueUri(), null);
            assertFalse(queue.isOpen());

            logger.info("5. attempt to open queue asynchronously");
            try {
                queue.openAsync(QUEUE_OPTIONS, DEFAULT_TIMEOUT);
                fail("Should not get here");
            } catch (BMQException e) {
                logger.debug("Expected BMQException", e);
                assertEquals("Queue event handler must be set.", e.getMessage());
                assertEquals(GenericResult.NOT_SUPPORTED, e.code());
            }

            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            session.checkNoEvent();
            assertFalse(queue.isOpen());

            logger.info("6. open the queue synchronously");
            queue.open(QUEUE_OPTIONS, DEFAULT_TIMEOUT);
            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            assertTrue(queue.isOpen());

            logger.info("7. attempt to configure queue asynchronously");
            QueueOptions newOptions = QueueOptions.builder().setMaxUnconfirmedMessages(500).build();
            try {
                queue.configureAsync(newOptions, DEFAULT_TIMEOUT);
                fail("Should not get here");
            } catch (BMQException e) {
                logger.debug("Expected BMQException", e);
                assertEquals("Queue event handler must be set.", e.getMessage());
                assertEquals(GenericResult.NOT_SUPPORTED, e.code());
            }

            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            session.checkNoEvent();
            assertTrue(queue.isOpen());

            logger.info("8. configure the queue synchronously");
            queue.configure(newOptions, DEFAULT_TIMEOUT);
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("9. attempt to close queue asynchronously");
            try {
                queue.closeAsync(DEFAULT_TIMEOUT);
                fail("Should not get here");
            } catch (BMQException e) {
                logger.debug("Expected BMQException", e);
                assertEquals("Queue event handler must be set.", e.getMessage());
                assertEquals(GenericResult.NOT_SUPPORTED, e.code());
            }

            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            session.checkNoEvent();
            assertTrue(queue.isOpen());

            logger.info("10. close the queue synchronously");
            queue.close(DEFAULT_TIMEOUT);
            verifyConfigureRequest(server.nextClientRequest());
            verifyCloseRequest(server.nextClientRequest());

            assertNull(server.nextClientRequest());
            session.checkNoEvent();
            assertFalse(queue.isOpen());
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            logger.info("11. stop session");
            session.stop(DEFAULT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("==================================================================");
            logger.info("END Testing SessionIT openConfigureCloseQueueAsyncNullHandlerTest.");
            logger.info("==================================================================");
        }
    }

    /**
     * Test for synchronous operations under queue (open-configure-close);
     *
     * <p>This test is replica of corresponding BrokerSession test
     *
     * <ul>
     *   <li>start test server in active mode
     *   <li>create session
     *   <li>start session
     *   <li>create queue
     *   <li>open queue synchronously
     *   <li>check that proper result code was returned
     *   <li>repeat previous 2 steps for configure queue and close queue
     *   <li>check that no single event was generated
     *   <li>stop session
     * </ul>
     */
    @Test
    void openConfigureCloseQueueTest() throws BMQException {
        logger.info("============================================================");
        logger.info("BEGIN Testing SessionIT openConfigureCloseQueueTest.");
        logger.info("============================================================");

        logger.info("1. start broker simulator in auto mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_AUTO_MODE);
        server.start();

        logger.info("2. create session");
        TestSession session = new TestSession(server.getPort());

        try {
            logger.info("3. start session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. create queue");
            Queue queue = session.getReaderQueue(createUniqueUri());

            assertFalse(queue.isOpen());
            logger.info("5. open queue synchronously");
            queue.open(QUEUE_OPTIONS, DEFAULT_TIMEOUT);
            assertTrue(queue.isOpen());

            logger.info("6. configure queue synchronously");
            queue.configure(QUEUE_OPTIONS, DEFAULT_TIMEOUT);
            assertTrue(queue.isOpen());

            logger.info("7. close queue synchronously");
            queue.close(DEFAULT_TIMEOUT);
            assertFalse(queue.isOpen());

            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());
            verifyCloseRequest(server.nextClientRequest());
        } finally {
            logger.info("8. stop session");
            session.stop(DEFAULT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("==========================================================");
            logger.info("END Testing SessionIT openConfigureCloseQueueTest.");
            logger.info("==========================================================");
        }
    }

    /**
     * Test for open operation failure under queue when queue is health sensitive and event handler
     * is null.
     *
     * <ul>
     *   <li>start test server in active mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>create queue with null queue event handler
     *   <li>attempt to open the queue synchronously with health sensitive options
     *   <li>open the queue synchronously with health insensitive options
     *   <li>attempt to reconfigure the queue synchronously as health sensitive
     *   <li>stop broker session
     * </ul>
     */
    @Test
    void openHealthSensitiveQueueNullHandlerTest() {
        logger.info("====================================================================");
        logger.info("BEGIN Testing SessionIT openConfigureCloseQueueAsyncNullHandlerTest.");
        logger.info("====================================================================");

        logger.info("1. start broker simulator in auto mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_AUTO_MODE);
        server.start();

        logger.info("2. create session");
        TestSession session = new TestSession(server.getPort());

        try {
            logger.info("3. start session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. create queue with null queue event handler");
            Queue queue = session.getReaderQueue(createUniqueUri(), null);
            assertFalse(queue.isOpen());

            logger.info("5. attempt to open the queue synchronously with health sensitive options");
            QueueOptions sensitiveOptions =
                    QueueOptions.builder().setSuspendsOnBadHostHealth(true).build();

            try {
                queue.open(sensitiveOptions, DEFAULT_TIMEOUT);
                fail("Should not get here");
            } catch (BMQException e) {
                logger.debug("Expected BMQException", e);
                assertEquals("Queue event handler must be set.", e.getMessage());
                assertEquals(GenericResult.NOT_SUPPORTED, e.code());
            }

            assertFalse(queue.isOpen());

            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            logger.info("6. open the queue synchronously with health insensitive options");
            QueueOptions insensitiveOptions =
                    QueueOptions.builder().setSuspendsOnBadHostHealth(false).build();

            queue.open(insensitiveOptions, DEFAULT_TIMEOUT);
            assertTrue(queue.isOpen());

            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("7. attempt to reconfigure the queue synchronously as health sensitive");
            try {
                queue.configure(sensitiveOptions, DEFAULT_TIMEOUT);
                fail("Should not get here");
            } catch (BMQException e) {
                logger.debug("Expected BMQException", e);
                assertEquals("Queue event handler must be set.", e.getMessage());
                assertEquals(GenericResult.NOT_SUPPORTED, e.code());
            }

            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            session.checkNoEvent();
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            logger.info("6. stop session");
            session.stop(DEFAULT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("==============================================================");
            logger.info("END Testing SessionIT openHealthSensitiveQueueNullHandlerTest.");
            logger.info("==============================================================");
        }
    }

    /**
     * Test for asynchronous open queue operation
     *
     * <p>This test is replica of corresponding BrokerSession test
     *
     * <ul>
     *   <li>start broker simulator in active mode
     *   <li>create session
     *   <li>start session
     *   <li>create queue
     *   <li>open queue asynchronously
     *   <li>send error broker response to the first (open) reqest
     *   <li>check that proper event was handled by user-defined user handler
     *   <li>open queue asynchronously again
     *   <li>send error broker response to the second (configure) reqest
     *   <li>check that proper event was handled by user-defined user handler
     *   <li>check that close request was sent to the broker
     *   <li>stop session
     * </ul>
     */
    @Test
    void queueOpenErrorTest() throws BMQException {
        logger.info("==========================================");
        logger.info("BEGIN Testing SessionIT queueOpenErrorTest");
        logger.info("==========================================");

        logger.info("Step 1. start broker simulator in manual mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_MANUAL_MODE);
        server.start();

        logger.info("Step 2. create session");
        TestSession session = new TestSession(server.getPort());
        server.pushItem(StatusCategory.E_SUCCESS); // Ok for nego request

        try {
            logger.info("Step 3. start session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("Step 4. create queue");
            Queue queue = session.getReaderQueue(createUniqueUri());
            assertFalse(queue.isOpen());

            logger.info("Step 5. open queue asynchronously");
            server.pushItem(StatusCategory.E_UNKNOWN); // error for open request
            queue.openAsync(QUEUE_OPTIONS, DEFAULT_TIMEOUT);

            logger.info("Step 6. check the only open request was sent");
            verifyOpenRequest(server.nextClientRequest());
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            logger.info("Step 7. check that open queue result event was emitted");
            QueueControlEvent queueEvent = session.waitQueueEvent();

            assertEquals(QueueControlEvent.Type.QUEUE_OPEN_RESULT, queueEvent.type());
            assertEquals(ResultCodes.OpenQueueResult.UNKNOWN, queueEvent.result());
            assertFalse(queue.isOpen());

            logger.info("Step 8. open queue asynchronously");
            server.pushItem(StatusCategory.E_SUCCESS); // ok for open request
            server.pushItem(StatusCategory.E_UNKNOWN); // error for config request
            server.pushItem(StatusCategory.E_SUCCESS); // ok for close request
            queue.openAsync(QUEUE_OPTIONS, DEFAULT_TIMEOUT);

            logger.info("Step 9. check open and config requests were sent");

            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("Step 10. check that open queue result event was emitted");
            queueEvent = session.waitQueueEvent();

            assertEquals(QueueControlEvent.Type.QUEUE_OPEN_RESULT, queueEvent.type());
            assertEquals(ResultCodes.OpenQueueResult.UNKNOWN, queueEvent.result());
            assertFalse(queue.isOpen());

            logger.info("Step 11. check close queue request was sent");
            verifyCloseRequest(server.nextClientRequest());

            assertFalse(queue.isOpen());

            logger.info("Step 12. check there are no more requests");
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

        } catch (Exception e) {
            logger.error("Exception: ", e);
        } finally {
            logger.info("12. stop session");
            server.pushItem(StatusCategory.E_SUCCESS); // Ok for disconnect request
            session.stop(DEFAULT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("=========================================");
            logger.info("END Testing SessionIT queueOpenErrorTest.");
            logger.info("=========================================");
        }
    }

    /**
     * Test for receiving PUSH message.
     *
     * <p>This test is replica of corresponding BrokerSession test
     *
     * <ul>
     *   <li>start test server in active mode
     *   <li>create session
     *   <li>start session
     *   <li>create queue
     *   <li>open queue synchronously
     *   <li>check that proper result code was returned
     *   <li>Send PUSH message from the server
     *   <li>Check that corresponding PUSH event was generated by session
     *   <li>stop session
     * </ul>
     */
    @Test
    @Disabled(
            "Disable this test for 2 reasons:\n"
                    + "- API misses convenient setters for Options, and it's not possible to override "
                    + "  the default subQueueId of the mocked PushMessageImpl\n"
                    + "- we don't expose subscription id in Queue, and we need it to mock PushMessageImpl")
    void pushMessageTest() throws BMQException, IOException {
        logger.info("===============================================");
        logger.info("BEGIN Testing SessionIT pushMessageTest.");
        logger.info("===============================================");

        logger.info("1. start broker simulator in auto mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_AUTO_MODE);
        server.start();

        logger.info("2. create session");
        TestSession session = new TestSession(server.getPort());
        try {
            logger.info("3. start session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. create queue");
            Queue queue = session.getReaderQueue(createUniqueUri());
            assertFalse(queue.isOpen());

            logger.info("5. open queue synchronously");
            queue.open(QUEUE_OPTIONS, DEFAULT_TIMEOUT);
            assertTrue(queue.isOpen());

            logger.info("6. send PUSH message from server");
            server.writePushRequest(0);

            logger.info("7. get and check PUSH message");
            PushMessage message = session.waitPushMessage();

            // TODO: consider rework payload API - fetch payload without padding
            assertEquals(
                    MessageGUID.fromHex("ABCDEF0123456789ABCDEF0123456789"), message.messageGUID());
            String PAYLOAD = "abcdefghijklmnopqrstuvwxyz";
            ByteBuffer buf = message.payload()[0];
            byte[] arr = new byte[PAYLOAD.length()];
            buf.get(arr);
            assertEquals(PAYLOAD, new String(arr));

            logger.info("8. close queue synchronously");
            queue.close(DEFAULT_TIMEOUT);
            assertFalse(queue.isOpen());
        } finally {
            logger.info("9. stop session");
            session.stop(DEFAULT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("=============================================");
            logger.info("END Testing SessionIT pushMessageTest.");
            logger.info("=============================================");
        }
    }

    /**
     * Critical test for queue fsm logic configure request in OPENING queue state
     *
     * <p>This test is replica of corresponding BrokerSession test
     *
     * <ul>
     *   <li>start test server in active mode
     *   <li>create session
     *   <li>start session
     *   <li>create queue
     *   <li>initiate open queue operation asynchronously
     *   <li>attempt to initiate configure queue operation asynchronously
     *   <li>check that return code was INVALID_QUEUE
     *   <li>stop session
     * </ul>
     */
    @Test
    void queueConfigureNotSupported() throws BMQException {
        logger.info("===========================================================");
        logger.info("BEGIN Testing SessionIT queueConfigureNotSupported.");
        logger.info("===========================================================");

        logger.info("1. start broker simulator in auto mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_AUTO_MODE);
        server.start();

        logger.info("2. create session");
        TestSession session = new TestSession(server.getPort());
        try {
            logger.info("3. start session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. create queue");
            Queue queue = session.getReaderQueue(createUniqueUri());
            assertFalse(queue.isOpen());

            logger.info("5. initiate open queue operation asynchronously");
            queue.openAsync(QUEUE_OPTIONS, DEFAULT_TIMEOUT);

            logger.info("6. attempt to initiate configure queue operation asynchronously");
            queue.configureAsync(QUEUE_OPTIONS, DEFAULT_TIMEOUT);

            logger.info("7. check that return code was INVALID_QUEUE");
            assertEquals(
                    ResultCodes.ConfigureQueueResult.INVALID_QUEUE,
                    session.waitQueueEvent().result());
        } finally {
            logger.info("8. stop session");
            session.stop(DEFAULT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("=========================================================");
            logger.info("END Testing SessionIT queueConfigureNotSupported.");
            logger.info("=========================================================");
        }
    }

    /**
     * Critical test for queue fsm logic open request in OPENING queue state
     *
     * <p>This test is replica of corresponding BrokerSession test
     *
     * <ul>
     *   <li>start test server in active mode
     *   <li>create session
     *   <li>start session
     *   <li>create queue
     *   <li>initiate open queue operation asynchronously
     *   <li>attempt to open configure queue operation asynchronously
     *   <li>check that return code was ALREADY_IN_PROGRESS
     *   <li>stop session
     * </ul>
     */
    @Test
    void queueOpenAlreadyInProgress() throws BMQException {
        logger.info("===========================================================");
        logger.info("BEGIN Testing SessionIT queueOpenAlreadyInProgress.");
        logger.info("===========================================================");

        logger.info("1. start broker simulator in auto mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_AUTO_MODE);
        server.start();

        logger.info("2. create session");
        TestSession session = new TestSession(server.getPort());
        try {
            logger.info("3. start session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. create queue");
            Queue queue = session.getReaderQueue(createUniqueUri());

            logger.info("5. initiate open queue operation asynchronously");
            queue.openAsync(QUEUE_OPTIONS, DEFAULT_TIMEOUT);

            logger.info("6. check that return code was ALREADY_IN_PROGRESS");
            queue.openAsync(QUEUE_OPTIONS, DEFAULT_TIMEOUT);

            assertEquals(
                    ResultCodes.OpenQueueResult.ALREADY_IN_PROGRESS,
                    session.waitQueueEvent().result());
        } finally {
            logger.info("7. stop session");
            session.stop(DEFAULT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("=========================================================");
            logger.info("END Testing SessionIT queueOpenAlreadyInProgress.");
            logger.info("=========================================================");
        }
    }

    /**
     * Critical test for queue fsm logic configure request in OPENING queue state.
     *
     * <p>This test is replica of corresponding BrokerSession test.
     *
     * <ul>
     *   <li>start test server in silent mode
     *   <li>create session
     *   <li>attempt to start session
     *   <li>check for proper return code (TIMEOUT)
     * </ul>
     */
    @Test
    void startSessionTimeoutTest() throws BMQException {

        logger.info("========================================================");
        logger.info("BEGIN Testing SessionIT startSessionTimeoutTest.");
        logger.info("========================================================");

        logger.info("1. start broker simulator in silent mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.SILENT_MODE);
        server.start();

        logger.info("2. create session");
        TestSession session = new TestSession(server.getPort());
        try {
            logger.info("3. attempt to start session");
            session.start(SHORT_TIMEOUT);
            fail();
        } catch (BMQException ex) {
            logger.info("4. check for proper return code (TIMEOUT)");
            assertEquals(ResultCodes.GenericResult.TIMEOUT, ex.code());
        } finally {
            logger.info("9. linger session");
            session.linger();
            server.stop();

            logger.info("======================================================");
            logger.info("END Testing SessionIT startSessionTimeoutTest.");
            logger.info("======================================================");
        }
    }

    /**
     * Critical test to verify timeout fsm logic for open request during opening queue state.
     *
     * <p>This test is replica of corresponding BrokerSession test.
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create session
     *   <li>start session
     *   <li>create queue
     *   <li>attempt to open queue
     *   <li>check for proper return code (TIMEOUT)
     * </ul>
     */
    @Test
    void openQueueTimeoutTest() throws BMQException {

        logger.info("=====================================================");
        logger.info("BEGIN Testing SessionIT openQueueTimeoutTest.");
        logger.info("=====================================================");

        logger.info("1. start broker simulator in manual mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_MANUAL_MODE);
        server.start();

        logger.info("2. create session");
        TestSession session = new TestSession(server.getPort());
        server.pushItem(StatusCategory.E_SUCCESS); // Ok for nego request

        try {
            logger.info("3. start session");
            session.start(SHORT_TIMEOUT);

            logger.info("4. create queue");
            Queue queue = session.getReaderQueue(createUniqueUri());
            assertFalse(queue.isOpen());

            logger.info("5. attempt to open queue");
            server.pushItem(
                    StatusCategory.E_SUCCESS, // openQueue out of time
                    SHORT_TIMEOUT.plusSeconds(1));
            try {
                queue.open(QUEUE_OPTIONS, SHORT_TIMEOUT);
                fail();
            } catch (BMQException ex) {
                logger.info("6. check for proper return code (TIMEOUT)");
                assertEquals(ResultCodes.OpenQueueResult.TIMEOUT, ex.code());
            }

            verifyOpenRequest(server.nextClientRequest());
            verifyCloseRequest(server.nextClientRequest());

            // Check there should be no QUEUE_CLOSE_RESULT event
            session.checkNoEvent();
        } finally {
            logger.info("7. stop session");
            server.pushItem(StatusCategory.E_SUCCESS);
            session.stop(SHORT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("===================================================");
            logger.info("END Testing SessionIT openQueueTimeoutTest.");
            logger.info("===================================================");
        }
    }

    /**
     * Critical test to verify that queue in write mode cannot be opened with app id.
     *
     * <ul>
     *   <li>start test server in auto mode
     *   <li>create session
     *   <li>start session
     *   <li>create write queue with app id
     *   <li>attempt to open queue sync
     *   <li>check for proper return code (INVALID_ARGUMENT)
     *   <li>attempt to open queue async
     *   <li>check for proper event (INVALID_ARGUMENT)
     * </ul>
     */
    @Test
    void openWriteQueueAppIdTest() throws BMQException {

        logger.info("================================================");
        logger.info("BEGIN Testing SessionIT openWriteQueueAppIdTest.");
        logger.info("================================================");

        logger.info("1. start broker simulator in auto mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_AUTO_MODE);
        server.start();

        logger.info("2. create session");
        TestSession session = new TestSession(server.getPort());

        try {
            logger.info("3. start session");
            session.start(SHORT_TIMEOUT);

            logger.info("4. create queue");
            // TODO: update the uri after merging of PR with new test domains.
            final Uri uri =
                    new Uri(
                            "bmq://bmq.regression.onetomany/java-it-"
                                    + UUID.randomUUID().toString()
                                    + "?id=foo");
            Queue queue = session.getWriterQueue(uri, true);
            assertFalse(queue.isOpen());

            logger.info("5. attempt to open queue sync");
            try {
                queue.open(QUEUE_OPTIONS, SHORT_TIMEOUT);
                fail();
            } catch (BMQException ex) {
                logger.info("6. check for proper return code (INVALID_ARGUMENT)");
                assertEquals(OpenQueueResult.INVALID_ARGUMENT, ex.code());
            }

            logger.info("7. attempt to open queue async");
            queue.openAsync(QUEUE_OPTIONS, SHORT_TIMEOUT);

            logger.info("8. check for proper event (INVALID_ARGUMENT)");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_OPEN_RESULT,
                    OpenQueueResult.INVALID_ARGUMENT,
                    queue);

            // Check there should be no open requests sent
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // Check there should be no other events
            session.checkNoEvent();
        } finally {
            logger.info("9. stop session");
            session.stop(SHORT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("==============================================");
            logger.info("END Testing SessionIT openWriteQueueAppIdTest.");
            logger.info("==============================================");
        }
    }

    /**
     * Critical test to verify timeout fsm logic for open configure request during opening queue
     * state.
     *
     * <p>This test is replica of corresponding BrokerSession test.
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create session
     *   <li>start session
     *   <li>create queue
     *   <li>attempt to open queue
     *   <li>check for proper return code (TIMEOUT)
     * </ul>
     */
    @Test
    void openQueueConfigureTimeoutTest() throws BMQException {

        logger.info("======================================================");
        logger.info("BEGIN Testing SessionIT openQueueConfigureTimeoutTest.");
        logger.info("======================================================");

        logger.info("1. start broker simulator in manual mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_MANUAL_MODE);
        server.start();

        logger.info("2. create session");
        TestSession session = new TestSession(server.getPort());
        server.pushItem(StatusCategory.E_SUCCESS); // Ok for nego request

        try {
            logger.info("3. start session");
            session.start(SHORT_TIMEOUT);

            logger.info("4. create queue");
            Queue queue = session.getReaderQueue(createUniqueUri());
            assertFalse(queue.isOpen());

            logger.info("5. attempt to open queue");
            server.pushSuccess(); // openQueue is Ok
            server.pushItem(
                    StatusCategory.E_SUCCESS, // configureQueue out of time
                    SHORT_TIMEOUT.plusSeconds(1));
            server.pushSuccess(); // close configureQueue is Ok
            server.pushSuccess(); // close closeQueue is Ok

            try {
                queue.open(QUEUE_OPTIONS, SHORT_TIMEOUT);
                fail();
            } catch (BMQException ex) {
                logger.info("6. check for proper return code (TIMEOUT)");
                assertEquals(ResultCodes.OpenQueueResult.TIMEOUT, ex.code());
            }

            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());
            verifyCloseRequest(server.nextClientRequest());

            // Check there should be no QUEUE_CLOSE_RESULT event
            session.checkNoEvent();
        } finally {
            logger.info("7. stop session");
            server.pushItem(StatusCategory.E_SUCCESS);
            session.stop(SHORT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("====================================================");
            logger.info("END Testing SessionIT openQueueConfigureTimeoutTest.");
            logger.info("====================================================");
        }
    }

    /**
     * Critical test to verify timeout fsm logic for configure request during closing queue state
     *
     * <p>This test is replica of corresponding BrokerSession test
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create session
     *   <li>start session
     *   <li>check queue is closed
     *   <li>check that look up method returns null
     *   <li>open queue
     *   <li>check queue state is open
     *   <li>attempt to close queue
     *   <li>check that single event was generated queue control event of E_TIMEOUT type
     *   <li>check queue is closed
     *   <li>stop session
     * </ul>
     */
    @Test
    void closeQueueConfigurationFailedTest() throws BMQException {
        logger.info("==================================================================");
        logger.info("BEGIN Testing SessionIT closeQueueConfigurationFailedTest.");
        logger.info("==================================================================");

        logger.info("1. start broker simulator in manual mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_MANUAL_MODE);
        server.start();

        logger.info("2. create session");
        TestSession session = new TestSession(server.getPort());
        server.pushItem(StatusCategory.E_SUCCESS); // Ok for nego request

        try {
            logger.info("3. start session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. create queue");
            Queue queue = session.getReaderQueue(createUniqueUri());

            logger.info("5. check queue is closed");
            assertFalse(queue.isOpen());

            logger.info("6. open queue");
            server.pushItem(StatusCategory.E_SUCCESS);
            server.pushItem(StatusCategory.E_SUCCESS);
            queue.open(QUEUE_OPTIONS, DEFAULT_TIMEOUT);

            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("7. check queue state is open");
            assertTrue(queue.isOpen());

            logger.info("8. attempt to close queue");
            server.pushItem(StatusCategory.E_TIMEOUT);
            queue.closeAsync(DEFAULT_TIMEOUT);

            logger.info(
                    "9. check that single event was generated queue control event of E_TIMEOUT type");
            QueueControlEvent event = session.waitQueueEvent();
            assertEquals(QueueControlEvent.Type.QUEUE_CLOSE_RESULT, event.type());
            assertEquals(ResultCodes.CloseQueueResult.TIMEOUT, event.result());

            logger.info("10. check queue is closed");
            assertFalse(queue.isOpen());

            verifyConfigureRequest(server.nextClientRequest());
            // There should be no close request until late broker response
            assertNull(server.nextClientRequest());
        } finally {
            logger.info("11. stop session");
            server.pushItem(StatusCategory.E_SUCCESS);
            session.stop(SHORT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("================================================================");
            logger.info("END Testing SessionIT closeQueueConfigurationFailedTest.");
            logger.info("================================================================");
        }
    }

    /**
     * Critical test to verify timeout fsm logic for configure request during closing queue state
     *
     * <p>This test is replica of corresponding BrokerSession test.
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>create queue
     *   <li>open queue
     *   <li>check queue state is closed
     *   <li>attempt to close queue
     *   <li>check that single queue control event was generated
     *   <li>check queue state is closed
     *   <li>stop broker session
     * </ul>
     */
    @Test
    void closeQueueConfigurationTimeoutTest() {
        logger.info("===========================================================");
        logger.info("BEGIN Testing SessionIT closeQueueConfigurationTimeoutTest.");
        logger.info("===========================================================");

        logger.info("1. start broker simulator in manual mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_MANUAL_MODE);
        server.start();

        logger.info("2. create session");
        TestSession session = new TestSession(server.getPort());
        server.pushItem(StatusCategory.E_SUCCESS); // Ok for nego request

        try {
            // The response delay for configure request during close queue sequence is 2 seconds
            final Duration REQUEST_DELAY = Duration.ofSeconds(2);
            // The whole sequence timeout is 1 seconds
            final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(1);

            logger.info("3. start session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. create queue");
            Queue queue = session.getReaderQueue(createUniqueUri());

            logger.info("5. check queue is closed");
            assertFalse(queue.isOpen());

            logger.info("6. open queue");
            server.pushItem(StatusCategory.E_SUCCESS);
            server.pushItem(StatusCategory.E_SUCCESS);
            queue.open(QUEUE_OPTIONS, DEFAULT_TIMEOUT);

            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            assertTrue(queue.isOpen());

            logger.info("7. attempt to close queue");
            // OK for configure request during closing queue with delay
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY);
            server.pushSuccess(); // ok for close
            queue.closeAsync(SEQUENCE_TIMEOUT);

            logger.info("8. check that single queue control event was generated");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CLOSE_RESULT,
                    ResultCodes.CloseQueueResult.TIMEOUT,
                    queue);

            // Verify there are no more events
            session.checkNoEvent();

            // Verify requests
            verifyConfigureRequest(server.nextClientRequest());
            verifyCloseRequest(server.nextClientRequest(), true);

            // Check no more requests
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            logger.info("9 check queue is closed");
            assertFalse(queue.isOpen());
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            logger.info("10. stop session");
            server.pushItem(StatusCategory.E_SUCCESS);
            session.stop(SHORT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("=========================================================");
            logger.info("END Testing SessionIT closeQueueConfigurationTimeoutTest.");
            logger.info("=========================================================");
        }
    }

    /**
     * Critical test to check opened queue can be closed if there is a pending configure request.
     *
     * <p>This test is replica of corresponding BrokerSession test
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>create queue
     *   <li>open the queue async
     *   <li>just after start of open queue sequence attempt to close given queue
     *   <li>check that close operation returned rc=ALREADY_IN_PROGRESS
     *   <li>wait until queue is opened
     *   <li>configure the queue async
     *   <li>just after sending the configure request attempt to close given queue
     *   <li>check that configure is canceled and close operation returned rc=SUCCESS
     *   <li>open the queue
     *   <li>set host health to unhealthy
     *   <li>just after sending the suspend request attempt to close given queue
     *   <li>check that suspension is canceled and close operation returned rc=SUCCESS
     *   <li>open the queue
     *   <li>set host health back to healthy
     *   <li>just after sending the resume request attempt to close given queue
     *   <li>check that resuming is canceled and close operation returned rc=SUCCESS
     *   <li>stop and linger broker session and stop server
     * </ul>
     */
    @Test
    @Disabled(
            "Temporarily disable this test until achieved more stable repeatability on slow hosts")
    void closeQueueOnPendingConfigureRequest() {
        logger.info("============================================================");
        logger.info("BEGIN Testing SessionIT closeQueueOnPendingConfigureRequest.");
        logger.info("============================================================");

        logger.info("1. start test server in manual mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_MANUAL_MODE);
        server.start();

        logger.info("2. create broker session");
        TestMonitor monitor = new TestMonitor();
        SessionOptions opts =
                SessionOptions.builder()
                        .setBrokerUri(server.getURI())
                        .setHostHealthMonitor(monitor)
                        .build();
        TestSession session = new TestSession(opts);

        server.pushSuccess(); // OK for nego
        try {
            logger.info("3. start broker session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. create queue");
            Queue queue = session.getReaderQueue(createUniqueUri());

            logger.info("*** Close queue when there is a pending standalone configure request ***");
            logger.info("5. open the queue async");
            server.pushSuccess(2);

            QueueOptions queueOptions =
                    QueueOptions.builder().setSuspendsOnBadHostHealth(true).build();

            queue.openAsync(queueOptions, DEFAULT_TIMEOUT);

            logger.info("6. just after start of open queue sequence attempt to close given queue");
            queue.closeAsync(DEFAULT_TIMEOUT);

            logger.info("7. check that close operation returns ALREADY_IN_PROGRESS result event.");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CLOSE_RESULT,
                    CloseQueueResult.ALREADY_IN_PROGRESS,
                    queue);

            logger.info("8. wait until queue is opened");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_OPEN_RESULT,
                    OpenQueueResult.SUCCESS,
                    queue);

            assertTrue(queue.isOpen());

            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("9. configure the queue async");
            QueueOptions newOptions = QueueOptions.builder().setConsumerPriority(2).build();

            queue.configureAsync(newOptions, DEFAULT_TIMEOUT);
            verifyConfigureRequest(server.nextClientRequest());

            logger.info(
                    "10. just after sending the configure request attempt to close given queue");
            server.pushSuccess(2);
            queue.closeAsync(DEFAULT_TIMEOUT);
            verifyConfigureRequest(server.nextClientRequest());
            verifyCloseRequest(server.nextClientRequest(), true);

            logger.info(
                    "11. check that configure is canceled and close operation returns SUCCESS result event");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.CANCELED,
                    queue);
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CLOSE_RESULT,
                    CloseQueueResult.SUCCESS,
                    queue);

            assertFalse(queue.isOpen());

            logger.info("*** Close queue when there is a pending suspend configure request ***");
            logger.info("12. open the queue async");
            server.pushSuccess(2);
            queue.openAsync(queueOptions, DEFAULT_TIMEOUT);

            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_OPEN_RESULT,
                    OpenQueueResult.SUCCESS,
                    queue);

            assertTrue(queue.isOpen());

            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("13. set host health to unhealthy");
            monitor.setState(HostHealthState.Unhealthy);
            assertNotNull(session.waitHostUnhealthyEvent());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("14. just after sending the suspend request attempt to close given queue");
            server.pushSuccess(2);
            queue.closeAsync(DEFAULT_TIMEOUT);
            verifyConfigureRequest(server.nextClientRequest());
            verifyCloseRequest(server.nextClientRequest(), true);

            logger.info(
                    "15. check that suspension is canceled and close operation returns SUCCESS result event");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CLOSE_RESULT,
                    CloseQueueResult.SUCCESS,
                    queue);

            assertFalse(queue.isOpen());

            session.checkNoEvent();

            logger.info("*** Close queue when there is a pending resume configure request ***");
            logger.info("16. open the queue async");
            server.pushSuccess();
            queue.openAsync(queueOptions, DEFAULT_TIMEOUT);

            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_OPEN_RESULT,
                    OpenQueueResult.SUCCESS,
                    queue);

            assertTrue(queue.isOpen());

            verifyOpenRequest(server.nextClientRequest());
            assertNull(server.nextClientRequest()); // no configure request

            logger.info("17. set host health to back to healthy");
            monitor.setState(HostHealthState.Healthy);
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("18. just after sending the resume request attempt to close given queue");
            server.pushSuccess(2);
            queue.closeAsync(DEFAULT_TIMEOUT);
            verifyConfigureRequest(server.nextClientRequest());
            verifyCloseRequest(server.nextClientRequest(), true);

            logger.info(
                    "19. check that resuming is canceled and close operation returns SUCCESS result event");
            assertNotNull(session.waitHostHealthRestoredEvent());

            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CLOSE_RESULT,
                    CloseQueueResult.SUCCESS,
                    queue);

            assertFalse(queue.isOpen());

            session.checkNoEvent();
            assertNull(server.nextClientRequest());
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            logger.info("20. stop session");
            server.pushItem(StatusCategory.E_SUCCESS);
            session.stop(SHORT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("==========================================================");
            logger.info("END Testing SessionIT closeQueueOnPendingConfigureRequest.");
            logger.info("==========================================================");
        }
    }

    /**
     * Critical test to check opened queue can be closed if there is a deferred resume request.
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>create queue
     *   <li>open the queue
     *   <li>set host health to unhealthy, do not send suspend response
     *   <li>set host health back to healthy, check that resuming is deferred
     *   <li>close the queue
     *   <li>check HOST_HEALTH_RESTORED event, no QUEUE_SUSPEND/QUEUE_RESUME
     *   <li>stop and linger broker session and stop server
     * </ul>
     */
    @Test
    void closeQueueOnDeferredResumeRequest() {
        logger.info("==========================================================");
        logger.info("BEGIN Testing SessionIT closeQueueOnDeferredResumeRequest.");
        logger.info("==========================================================");

        logger.info("1. start test server in manual mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_MANUAL_MODE);
        server.start();

        logger.info("2. create broker session");
        TestMonitor monitor = new TestMonitor();
        SessionOptions opts =
                SessionOptions.builder()
                        .setBrokerUri(server.getURI())
                        .setHostHealthMonitor(monitor)
                        .build();
        TestSession session = new TestSession(opts);

        server.pushSuccess(); // OK for nego
        try {
            logger.info("3. start broker session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. create queue");
            Queue queue = session.getReaderQueue(createUniqueUri());

            logger.info("5. open the queue");
            server.pushSuccess(2);
            QueueOptions options = QueueOptions.builder().setSuspendsOnBadHostHealth(true).build();
            queue.open(options, DEFAULT_TIMEOUT);

            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            // check generated events
            session.checkNoEvent();

            logger.info("6. set host health to unhealthy, do not send suspend response");
            monitor.setState(HostHealthState.Unhealthy);
            assertNotNull(session.waitHostUnhealthyEvent());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("7. set host health back to healthy");
            monitor.setState(HostHealthState.Healthy);

            logger.info("8. check that resuming is deferred");
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            logger.info("9. close the queue");
            server.pushSuccess(2);
            queue.close(DEFAULT_TIMEOUT);
            verifyConfigureRequest(server.nextClientRequest());
            verifyCloseRequest(server.nextClientRequest(), true);

            logger.info("10. check HOST_HEALTH_RESTORED event, no QUEUE_SUSPEND/QUEUE_RESUME");
            assertNotNull(session.waitHostHealthRestoredEvent());
            session.checkNoEvent();
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            logger.info("20. stop session");
            server.pushItem(StatusCategory.E_SUCCESS);
            session.stop(SHORT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("========================================================");
            logger.info("END Testing SessionIT closeQueueOnDeferredResumeRequest.");
            logger.info("========================================================");
        }
    }

    /**
     * Critical test of queue sequence timeout.
     *
     * <p>This test is replica of corresponding BrokerSession test
     *
     * <ul>
     *   <li>start test server in active mode;
     *   <li>create session;
     *   <li>start session;
     *   <li>create queue;
     *   <li>open queue synchronously with sequence timeout bigger than sum of delays for server
     *       responses;
     *   <li>close queue synchronously with sequence timeout smaller than sum of delays for server
     *       responses;
     *   <li>check that timeout result code was returned;
     *   <li>stop session.
     * </ul>
     */
    @Test
    void sequenceTimeoutTest() throws BMQException {
        logger.info("============================================================");
        logger.info("BEGIN Testing SessionIT sequenceTimeoutTest.");
        logger.info("============================================================");

        logger.info("1. start broker simulator in manual mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_MANUAL_MODE);
        server.start();

        logger.info("2. create session");
        TestSession session = new TestSession(server.getPort());
        server.pushItem(StatusCategory.E_SUCCESS); // Ok for nego request
        try {
            logger.info("3. start session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. create queue");
            Queue queue = session.getReaderQueue(createUniqueUri());

            logger.info(
                    "5. preset status code and delays for open and configure response from server");
            final Duration REQUEST_DELAY_SUCCESS = Duration.ofSeconds(2);
            final Duration REQUEST_DELAY_FAILURE = Duration.ofSeconds(4);
            final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(6);

            logger.info(
                    "6. open queue synchronously with sequence timeout bigger than sum of delays for server responses");
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY_SUCCESS);
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY_SUCCESS);
            queue.open(QUEUE_OPTIONS, SEQUENCE_TIMEOUT);

            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info(
                    "7. close queue synchronously with sequence timeout bigger than sum of delays for server responses");
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY_FAILURE);
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY_FAILURE);

            try {
                queue.close(SEQUENCE_TIMEOUT);
                fail(); // should not get here
            } catch (BMQException ex) {
                logger.info("8. check that timeout result code was returned");
                assertEquals(ResultCodes.CloseQueueResult.TIMEOUT, ex.code());
            }

            verifyConfigureRequest(server.nextClientRequest());
            verifyCloseRequest(server.nextClientRequest());
        } finally {
            logger.info("9. stop session");
            session.stop(DEFAULT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("============================================================");
            logger.info("END Testing SessionIT sequenceTimeoutTest.");
            logger.info("============================================================");
        }
    }

    @Test
    void testQueueFlush() throws BMQException, IOException {

        logger.info("==============================================");
        logger.info("BEGIN Testing SessionIT testQueueFlush.");
        logger.info("==============================================");

        // 1) Bring up the broker
        // 2) Start the session
        // 3) Open the queue for writing
        // 4) Pack two PUT messages
        // 5) Flush (send) the messages
        // 6) Wait for either ACK event with two ACK messages or two ACK events
        //    each with a single ACK message
        // 7) Close the queue, stop the session and the broker

        // 1) Bring up the broker and start the session

        BmqBroker broker = BmqBroker.createStartedBroker();

        // 2) Start the session
        SessionOptions opts =
                SessionOptions.builder().setBrokerUri(broker.sessionOptions().brokerUri()).build();

        TestSession session = new TestSession(opts);

        try {
            session.start(DEFAULT_TIMEOUT);

            // 3) Open the queue for writing
            Queue queue = session.getWriterQueue(createUniqueUri(), false);

            queue.open(QueueOptions.createDefault(), DEFAULT_TIMEOUT);

            assertTrue(queue.isOpen());

            final int PAYLOAD_SIZE = 150;
            final int NUM_MESSAGES = 2;

            // 4) Pack two PUT messages
            String payload = createPayload(PAYLOAD_SIZE, "a");

            PutMessage[] putMessages = new PutMessage[NUM_MESSAGES];
            for (int i = 0; i < NUM_MESSAGES; i++) {
                PutMessage putMsg = makePut(queue, payload);
                putMessages[i] = putMsg;
                queue.pack(putMsg);
            }

            // 5) Flush (send) the messages
            queue.flush();

            // 6) Wait for either ACK event with two ACK messages or two ACK events
            //    each with a single ACK message
            verifyAckMessages(session, queue, putMessages);

            // 7) Close the queue, stop the session and the broker
            queue.close(DEFAULT_TIMEOUT);

            broker.setDropTmpFolder();
        } catch (Exception e) {
            logger.error("Exception: ", e);
            fail();
        } finally {
            session.stop(DEFAULT_TIMEOUT);
            session.linger();

            broker.close();

            logger.info("============================================");
            logger.info("END Testing SessionIT testQueueFlush.");
            logger.info("============================================");
        }
    }

    void getPushAndConfirm(TestSession session, Uri uri) {
        Queue queue = session.getReaderQueue(uri);
        logger.info("Opening a reader queue");
        queue.open(QueueOptions.createDefault(), DEFAULT_TIMEOUT);

        logger.info("Waiting for a PUSH message");
        PushMessage pushMsg = session.waitPushMessage();
        assertNotNull(pushMsg);

        logger.info("Confirming the PUSH message");
        assertEquals(GenericResult.SUCCESS, pushMsg.confirm());

        logger.info("Closing the reader queue");
        queue.close(DEFAULT_TIMEOUT);
    }

    @Test
    void testQueueAck() throws BMQException, IOException {

        logger.info("=====================================");
        logger.info("BEGIN Testing SessionIT testQueueAck.");
        logger.info("=====================================");

        // 1) Bring up the broker
        // 2) Start the session
        // 3) Open the queue for writing without ACK flag and post a PUT message
        //    without CorrelationId. There should be no incoming ACK event.
        // 4) Close the queue, open a queue for reading, get and confirm PUSH message.
        // 5) Open the queue for writing with ACK flag.
        // 6) Verify that PUT message with zero CorrelationId cannot be posted.
        // 7) Set a valid CorrelationId, verify that the PUT message can be posted.
        // 8) Wait for ACK event.
        // 9) Close the queue, open a reader queue, get and confirm incoming PUSH message.
        // 10) Stop the session and the broker

        logger.info("Step 1: Bring up the broker");
        BmqBroker broker = BmqBroker.createStartedBroker();
        TestSession session = new TestSession(broker.sessionOptions());
        String payload = "TEST";
        ByteBuffer byteBuf = ByteBuffer.wrap(payload.getBytes());

        try {
            logger.info("Step 2: Start the session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("Step 3: Open the queue without ACK and post PUT wihout CorrelationId");
            Uri uri = createUniqueUri();
            Queue queue = session.getWriterQueue(uri, false);
            queue.open(QueueOptions.createDefault(), DEFAULT_TIMEOUT);

            PutMessage putMsg = queue.createPutMessage(byteBuf);
            assertNull(putMsg.correlationId());

            queue.post(putMsg);

            logger.info(
                    "Step 4: Close the queue, open a queue for reading, get and confirm PUSH message");
            queue.close(DEFAULT_TIMEOUT);

            getPushAndConfirm(session, uri);

            logger.info("Step 5: Open the queue for writing with ACK flag.");
            queue = session.getWriterQueue(uri, true);
            queue.open(QueueOptions.createDefault(), DEFAULT_TIMEOUT);

            logger.info(
                    "Step 6: Verify that PUT message with zero CorrelationId cannot be posted.");
            try {
                queue.post(putMsg);
                throw new IllegalStateException(
                        "Can post PUT with zero CorrelationID into ACK enabled queue");
            } catch (BMQException e) {
                // OK
            }

            logger.info(
                    "Step 7: Set a valid CorrelationId, verify that the PUT message can be posted.");

            putMsg = queue.createPutMessage(byteBuf);
            putMsg.setCorrelationId(this);

            queue.post(putMsg);

            logger.info("Step 8: Wait for an ACK event");
            AckMessage ackMsg = session.waitAckMessage();
            assertNotNull(ackMsg);
            assertEquals(AckResult.SUCCESS, ackMsg.status());
            assertEquals(session.session, ackMsg.session());
            assertEquals(queue, ackMsg.queue());
            assertEquals(this, ackMsg.correlationId().userData());

            logger.info(
                    "Step 9: Close the queue, open a reader queue, get and confirm incoming PUSH message.");
            queue.close(DEFAULT_TIMEOUT);

            getPushAndConfirm(session, uri);

            broker.setDropTmpFolder();

        } finally {
            logger.info("Step 10: Stop the session and the broker");

            session.stop(DEFAULT_TIMEOUT);
            session.linger();

            broker.close();

            logger.info("===================================");
            logger.info("END Testing SessionIT testQueueAck.");
            logger.info("===================================");
        }
    }

    @Test
    void testPushProperties() throws BMQException, IOException {

        logger.info("===========================================");
        logger.info("BEGIN Testing SessionIT testPushProperties.");
        logger.info("===========================================");

        // 1) Bring up the broker
        // 2) Start the session
        // 3) Open the queue for writing without ACK flag and post a PUT message
        //    without CorrelationId and properties.
        //    There should be no incoming ACK event.
        // 4) Open a queue for reading, get and confirm PUSH message
        // 5) Verify PUSH message has no properties
        // 6) Open the queue for writing without ACK flag and post a PUT message
        //    without CorrelationId but with properties.
        //    There should be no incoming ACK event.
        // 7) Open a queue for reading, get and confirm PUSH message
        // 8) Verify PUSH message has properties
        // 9) Stop the session and the broker

        try (BmqBroker broker = BmqBroker.createStoppedBroker()) {
            logger.info("Step 1: Bring up the broker");

            assertFalse(broker.isOldStyleMessageProperties());

            broker.start();

            TestSession session = new TestSession(broker.sessionOptions());
            String payload = "TEST";
            ByteBuffer byteBuf = ByteBuffer.wrap(payload.getBytes());

            try {
                logger.info("Step 2: Start the session");
                session.start(DEFAULT_TIMEOUT);

                logger.info("Step 3: Open the queue without ACK and post PUT");
                Uri uri = createUniqueUri();
                Queue queue = session.getWriterQueue(uri, false);
                queue.open(QueueOptions.createDefault(), DEFAULT_TIMEOUT);

                PutMessage putMsg = queue.createPutMessage(byteBuf);
                assertNull(putMsg.correlationId());

                queue.post(putMsg);

                logger.info("Step 4: Open the queue, get and confirm PUSH message");
                queue.close(DEFAULT_TIMEOUT);

                queue = session.getReaderQueue(uri);
                logger.info("Opening a reader queue");
                queue.open(QueueOptions.createDefault(), DEFAULT_TIMEOUT);

                logger.info("Waiting for a PUSH message");
                PushMessage pushMsg = session.waitPushMessage();
                assertNotNull(pushMsg);

                logger.info("Confirming the PUSH message");
                assertEquals(GenericResult.SUCCESS, pushMsg.confirm());

                logger.info("Closing the reader queue");
                queue.close(DEFAULT_TIMEOUT);

                logger.info("Step 5: Verify PUSH message has no properties");

                assertFalse(pushMsg.hasMessageProperties());
                try {
                    pushMsg.getProperty("property");
                    fail();
                } catch (IllegalStateException e) {
                    assertEquals("Message doesn't contain properties", e.getMessage());
                }

                logger.info("Step 6: Open the queue without ACK and post PUT");
                queue = session.getWriterQueue(uri, false);
                queue.open(QueueOptions.createDefault(), DEFAULT_TIMEOUT);

                putMsg = queue.createPutMessage(byteBuf);
                assertNull(putMsg.correlationId());

                MessageProperties mp = putMsg.messageProperties();
                mp.setPropertyAsBool("bool", true);
                mp.setPropertyAsByte("byte", Byte.MAX_VALUE);
                mp.setPropertyAsShort("short", Short.MAX_VALUE);
                mp.setPropertyAsInt32("int32", Integer.MAX_VALUE);
                mp.setPropertyAsInt64("int64", Long.MAX_VALUE);
                mp.setPropertyAsString("string", "some string");
                mp.setPropertyAsBinary("binary", new byte[] {80, 81, 82});
                // It's expected that all properties have been successfully added

                queue.post(putMsg);

                logger.info("Step 7: Open the queue, get and confirm PUSH message");
                queue.close(DEFAULT_TIMEOUT);

                queue = session.getReaderQueue(uri);
                logger.info("Opening a reader queue");
                queue.open(QueueOptions.createDefault(), DEFAULT_TIMEOUT);

                logger.info("Waiting for a PUSH message");
                pushMsg = session.waitPushMessage();
                assertNotNull(pushMsg);
                byteBuf.rewind();
                assertArrayEquals(new ByteBuffer[] {byteBuf}, pushMsg.payload());

                logger.info("Confirming the PUSH message");
                assertEquals(GenericResult.SUCCESS, pushMsg.confirm());

                logger.info("Closing the reader queue");
                queue.close(DEFAULT_TIMEOUT);

                logger.info("Step 8: Verify PUSH message has properties");

                assertTrue(pushMsg.hasMessageProperties());

                // The code below should be executed as part of unit tests,
                // but since there is no way create Session with mocked channel,
                // verification is done here

                boolean boolFound = false;
                boolean byteFound = false;
                boolean shortFound = false;
                boolean int32Found = false;
                boolean int64Found = false;
                boolean stringFound = false;
                boolean binaryFound = false;

                Iterator<MessageProperty> it = pushMsg.propertyIterator();

                while (it.hasNext()) {
                    MessageProperty p = it.next();

                    logger.info("Property: {}", p);

                    String name = p.name();
                    MessageProperty.Type type = p.type();
                    Object value = p.value();
                    String str = p.toString();

                    MessageProperty byName = pushMsg.getProperty(name);

                    assertEquals(type, byName.type());
                    assertEquals(value, byName.value());
                    assertEquals(str, byName.toString());

                    String strPattern =
                            String.format(
                                    "[ %s [ Type=%s Name=%s Value=%%s ] ]",
                                    "MessageProperty", type, name);

                    switch (name) {
                        case "bool":
                            boolFound = true;

                            assertEquals(MessageProperty.Type.BOOL, type);
                            assertTrue((Boolean) value);
                            assertEquals(String.format(strPattern, true), str);
                            break;
                        case "byte":
                            byteFound = true;

                            assertEquals(MessageProperty.Type.BYTE, type);
                            assertEquals(Byte.MAX_VALUE, value);
                            assertEquals(String.format(strPattern, Byte.MAX_VALUE), str);
                            break;
                        case "short":
                            shortFound = true;

                            assertEquals(MessageProperty.Type.SHORT, type);
                            assertEquals(Short.MAX_VALUE, value);
                            assertEquals(String.format(strPattern, Short.MAX_VALUE), str);
                            break;
                        case "int32":
                            int32Found = true;

                            assertEquals(MessageProperty.Type.INT32, type);
                            assertEquals(Integer.MAX_VALUE, value);
                            assertEquals(String.format(strPattern, Integer.MAX_VALUE), str);
                            break;
                        case "int64":
                            int64Found = true;

                            assertEquals(MessageProperty.Type.INT64, type);
                            assertEquals(Long.MAX_VALUE, value);
                            assertEquals(String.format(strPattern, Long.MAX_VALUE), str);
                            break;
                        case "string":
                            stringFound = true;

                            assertEquals(MessageProperty.Type.STRING, type);
                            assertEquals("some string", value);
                            assertEquals(String.format(strPattern, "\"some string\""), str);
                            break;
                        case "binary":
                            binaryFound = true;

                            assertEquals(MessageProperty.Type.BINARY, type);
                            assertArrayEquals(new byte[] {80, 81, 82}, (byte[]) value);
                            assertEquals(String.format(strPattern, "\"PQR\""), str);
                            break;
                        default:
                            fail();
                            break;
                    }
                }

                assertTrue(boolFound);
                assertTrue(byteFound);
                assertTrue(shortFound);
                assertTrue(int32Found);
                assertTrue(int64Found);
                assertTrue(stringFound);
                assertTrue(binaryFound);

                // Return null when property name is null
                assertNull(pushMsg.getProperty(null));

                // Return null when property is not found
                assertNull(pushMsg.getProperty("unknown_property"));

                broker.setDropTmpFolder();
            } finally {
                logger.info("Step 9: Stop the session and the broker");

                session.stop(DEFAULT_TIMEOUT);
                session.linger();
            }
        } // close broker resource here

        logger.info("===================================");
        logger.info("END Testing SessionIT testQueueAck.");
        logger.info("===================================");
    }

    @Test
    void testQueueCompression() throws BMQException, IOException {

        logger.info("=============================================");
        logger.info("BEGIN Testing SessionIT testQueueCompression.");
        logger.info("=============================================");

        // 1) Bring up the broker
        // 2) Start the session
        // 3) Open the queue for writing with ACK flag and reading
        // 5) Post incompressable PUT message with none compression, wait for ACK event and PUSH
        // message
        // 6) Post incompressable PUT message with Zlib compression, wait for ACK event and PUSH
        // message
        // 7) Post compressable PUT message with none compression, wait for ACK event and PUSH
        // message
        // 8) Post compressable PUT message with Zlib compression, wait for ACK event and PUSH
        // message
        // 9) Close the queue
        // 10) Stop the session and the broker

        try (BmqBroker broker = BmqBroker.createStoppedBroker()) {
            logger.info("Step 1: Bring up the broker");

            assertFalse(broker.isOldStyleMessageProperties());

            broker.start();

            TestSession session = new TestSession(broker.sessionOptions());

            Uri uri = createUniqueUri();

            try {
                logger.info("Step 2: Start the session");
                session.start(DEFAULT_TIMEOUT);

                logger.info("Step 3: Open the queue with ACK flag for writing and reading");
                Queue queue = session.getReadWriteQueue(uri, true);
                queue.open(QueueOptions.createDefault(), DEFAULT_TIMEOUT);

                logger.info(
                        "Step 4: Post incompressable PUT message with none compression, wait for ACK event and PUSH message");
                sendVerifyPut(
                        session,
                        queue,
                        CompressionAlgorithm.None,
                        Protocol.COMPRESSION_MIN_APPDATA_SIZE - 1);

                // The message will not be compressed in case message properties
                // are new style encoded.
                logger.info(
                        "Step 5: Post incompressable PUT message with Zlib compression, wait for ACK event and PUSH message");
                sendVerifyPut(
                        session,
                        queue,
                        CompressionAlgorithm.Zlib,
                        Protocol.COMPRESSION_MIN_APPDATA_SIZE - 1);

                logger.info(
                        "Step 6: Post compressable PUT message with none compression, wait for ACK event and PUSH message");
                sendVerifyPut(
                        session,
                        queue,
                        CompressionAlgorithm.None,
                        Protocol.COMPRESSION_MIN_APPDATA_SIZE);

                logger.info(
                        "Step 7: Post compressable PUT message with Zlib compression, wait for ACK event and PUSH message");
                sendVerifyPut(
                        session,
                        queue,
                        CompressionAlgorithm.Zlib,
                        Protocol.COMPRESSION_MIN_APPDATA_SIZE);

                logger.info("Step 8: Close the queue");
                queue.close(DEFAULT_TIMEOUT);

                broker.setDropTmpFolder();
            } finally {
                logger.info("Step 9: Stop the session and the broker");

                session.stop(DEFAULT_TIMEOUT);
                session.linger();
            }
        } // close broker resource here

        logger.info("===========================================");
        logger.info("END Testing SessionIT testQueueCompression.");
        logger.info("===========================================");
    }

    private void sendVerifyPut(
            TestSession session, Queue queue, CompressionAlgorithm algorithm, int payloadSize) {

        ByteBuffer bb = ByteBuffer.allocate(payloadSize);

        PutMessage putMsg = queue.createPutMessage(bb.duplicate());
        putMsg.setCorrelationId(this);
        putMsg.setCompressionAlgorithm(algorithm);

        putMsg.messageProperties().setPropertyAsInt32("id", 123);
        putMsg.messageProperties().setPropertyAsBinary("array", new byte[] {1, 2, 3});

        queue.post(putMsg);

        boolean isAckHandled = false, isPushHandled = false;

        for (int i = 0; i < 2; i++) {
            Event ev = session.waitAnyEvent(DEFAULT_WAIT_TIMEOUT);

            if (ev instanceof AckMessage) {
                AckMessage ackMsg = (AckMessage) ev;
                assertNotNull(ackMsg);
                assertEquals(AckResult.SUCCESS, ackMsg.status());
                assertEquals(session.session, ackMsg.session());
                assertEquals(queue, ackMsg.queue());
                assertEquals(this, ackMsg.correlationId().userData());

                isAckHandled = true;
            } else if (ev instanceof PushMessage) {
                PushMessage pushMsg = (PushMessage) ev;

                logger.info("Confirming the PUSH message");
                assertEquals(GenericResult.SUCCESS, pushMsg.confirm());

                assertArrayEquals(new ByteBuffer[] {bb}, pushMsg.payload());

                MessageProperty.Int32Property idProp =
                        (MessageProperty.Int32Property) pushMsg.getProperty("id");
                assertEquals((Integer) 123, idProp.value());
                MessageProperty.BinaryProperty arrayProp =
                        (MessageProperty.BinaryProperty) pushMsg.getProperty("array");
                assertArrayEquals(new byte[] {1, 2, 3}, arrayProp.value());

                isPushHandled = true;
            }
        }

        assertTrue(isAckHandled);
        assertTrue(isPushHandled);
    }

    private static SessionEvent getEvent(LinkedBlockingQueue<SessionEvent> queue) {
        SessionEvent event;
        try {
            event = queue.poll(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        if (event == null) {
            throw new RuntimeException("No incoming events");
        }
        return event;
    }

    @Test
    void testBadSessionEventHandler() throws Exception {
        // 1) Bring up the broker.
        // 2) Create a Session with a special SessionEventHandler that throws
        // 3) Invoke session 'startAsync' and verify incoming StartStatus event
        // 4) Stop the broker and verify incoming ConnectionLost event
        // 5) Start the broker again and verify incoming Reconnected and StateRestored events
        // 6) Invoke session 'stopAsync' and verify incoming StartStatus event
        // 7) Stop the session and the broker.

        logger.info("===================================================");
        logger.info("BEGIN Testing SessionIT testBadSessionEventHandler.");
        logger.info("===================================================");

        // 1) Bring up the broker.
        BmqBroker broker = BmqBroker.createStartedBroker();

        // 2) Create a Session with a special SessionEventHandler that throws
        LinkedBlockingQueue<SessionEvent> eventFIFO = new LinkedBlockingQueue<>();
        AbstractSession session =
                new Session(
                        broker.sessionOptions(),
                        (SessionEvent ev) -> {
                            try {
                                logger.info("Session event received {}", ev);
                                eventFIFO.put(ev);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException(e);
                            }
                            throw new RuntimeException("Poor handler");
                        });
        try {
            // 3) Invoke session 'startAsync' and verify incoming StartStatus event
            session.startAsync(DEFAULT_TIMEOUT);

            SessionEvent event = getEvent(eventFIFO);
            assertTrue(event instanceof SessionEvent.StartStatus);
            assertTrue(((SessionEvent.StartStatus) event).result().isSuccess());

            // 4) Stop the broker and verify incoming ConnectionLost event
            broker.stop();

            event = getEvent(eventFIFO);
            assertTrue(event instanceof SessionEvent.ConnectionLost);

            // 5) Start the broker again and verify incoming Reconnected and StateRestored events
            broker.start();

            event = getEvent(eventFIFO);
            assertTrue(event instanceof SessionEvent.Reconnected);

            event = getEvent(eventFIFO);
            assertTrue(event instanceof SessionEvent.StateRestored);

            // 6) Invoke session 'stopAsync' and verify incoming StartStatus event
            session.stopAsync(DEFAULT_TIMEOUT);
            event = getEvent(eventFIFO);
            assertTrue(event instanceof SessionEvent.StopStatus);
            assertTrue(((SessionEvent.StopStatus) event).result().isSuccess());

            broker.setDropTmpFolder();
        } finally {
            // 7) Stop the session and the broker.
            session.stop(DEFAULT_TIMEOUT);
            session.linger();

            broker.close();

            logger.info("=================================================");
            logger.info("END Testing SessionIT testBadSessionEventHandler.");
            logger.info("=================================================");
        }
    }

    @Test
    void testQueueFlushRestored() throws BMQException, IOException {

        logger.info("==============================================");
        logger.info("BEGIN Testing SessionIT queue flush restored.");
        logger.info("==============================================");

        // 1) Bring up the broker
        // 2) Start the session
        // 3) Open the queue
        // 4) Pack a number of PUT messages
        // 5) Flush the queue
        // 6) Wait for ACK events and verify them
        // 7) Pack a number of PUT messages
        // 8) Stop the broker and wait for connection lost event
        // 9) Try to flush the queue, expecting an exception
        // 10) Start the broker and wait for reconnected and state restored events
        // 11) Verify that the message can be posted to the queue without any exception
        // 12) Wait for ACK events and verify them
        // 13) Close the queue, stop the session and the broker

        // 1) Bring up the broker.
        BmqBroker broker = BmqBroker.createStartedBroker();
        TestSession session = new TestSession(broker.sessionOptions());

        final int NUM_MESSAGES = 10;

        try {
            // 2) Start the session
            session.start(DEFAULT_TIMEOUT);

            // 3) Open the queue
            Queue queue = session.getWriterQueue(createUniqueUri(), false);
            assertFalse(queue.isOpen());

            queue.open(QueueOptions.createDefault(), DEFAULT_TIMEOUT);
            assertTrue(queue.isOpen());

            // 4) Pack a number of PUT messages
            PutMessage[] putMessages = new PutMessage[NUM_MESSAGES];
            for (int i = 0; i < putMessages.length; i++) {
                PutMessage putMessage =
                        makePut(queue, String.format("Payload for message %d", i + 1));
                putMessages[i] = putMessage;
                queue.pack(putMessage);
            }

            // 5) Flush the queue
            queue.flush();

            // 6) Wait for ACK events and verify them
            verifyAckMessages(session, queue, putMessages);

            // 7) Pack a number of PUT messages
            putMessages = new PutMessage[NUM_MESSAGES];
            for (int i = 0; i < putMessages.length; i++) {
                PutMessage putMessage =
                        makePut(queue, String.format("Payload for message %d", i + 1));
                putMessages[i] = putMessage;
                queue.pack(putMessage);
            }

            // 8) Stop the broker and wait for connection lost event
            CompletableFuture<Void> stopFuture = broker.stopAsync();
            session.waitConnectionLostEvent();

            // The Queue is still in open state
            assertTrue(queue.isOpen());

            // 9) Try to flush the queue, expecting an exception
            try {
                queue.flush();
                throw new IllegalStateException("Flush via not started session");
            } catch (BMQException e) {
                // OK
                logger.debug("Caught expected BMQException: ", e);
            }

            // 10) Start the broker and wait for reconnected and state restored events
            stopFuture.get();
            broker.start();

            session.waitReconnectedEvent();
            QueueControlEvent event = session.waitQueueEvent();
            assertEquals(QueueControlEvent.Type.QUEUE_REOPEN_RESULT, event.type());
            assertEquals(ResultCodes.OpenQueueResult.SUCCESS, event.result());
            session.waitStateRestoredEvent();

            // 11) Verify that packed queue can be flushed without any exception
            assertTrue(queue.isOpen());

            // When connection is restored, the queue cannot be flushed again after failed attempt.
            // Need to pack messages again
            try {
                queue.flush();
                fail(); // we shouldn't get here
            } catch (IllegalArgumentException e) {
                // OK
                logger.debug("Caught expected IllegalArgumentException: ", e);
            }

            putMessages = new PutMessage[NUM_MESSAGES];
            for (int i = 0; i < putMessages.length; i++) {
                PutMessage putMessage =
                        makePut(queue, String.format("Payload for message %d", i + 1));
                putMessages[i] = putMessage;
                queue.pack(putMessage);
            }

            queue.flush();

            // 12) Wait for ACK events and verify them
            verifyAckMessages(session, queue, putMessages);

            // 13) Close the queue, stop the session and the broker
            queue.close(DEFAULT_TIMEOUT);

            broker.setDropTmpFolder();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            session.stop(DEFAULT_TIMEOUT);
            session.linger();

            broker.close();

            logger.info("============================================");
            logger.info("END Testing SessionIT queue flush restored.");
            logger.info("============================================");
        }
    }

    @Test
    void testQueueFlushDown() throws Exception {

        logger.info("==============================================");
        logger.info("BEGIN Testing SessionIT queue flush down.");
        logger.info("==============================================");

        // TODO: update comments

        // 1) Bring up the simulator
        // 2) Start the session
        // 3) Open the queue
        // 4) Pack a number of PUT messages
        // 5) Flush the queue
        // 6) Stop the simulator, wait connection lost event
        // 7) Wait for NAK events and verify them
        // 8) Start the broker and wait for reconnected and state restored events
        // 9) Verify that the message can be posted to the queue without any exception
        // 10) Close the queue, stop the session and the broker

        // 1) Bring up the simulator
        // Due to docker issue, we cannot kill or remove paused container.
        // See https://github.com/moby/moby/issues/28366.
        // The corresponding PR (https://github.com/moby/moby/pull/34027)
        // should be included in the Docker 17.07 release.
        // Now to kill the broker,  we need to unpause the container first.
        // But in this case broker manages to send back all 500 ACK messages
        // before getting killed.
        //
        // For that reason, we need to use BmqBrokerSimulator in the first part of the test.
        // When we migrate to Docker 17.07 or later, we could kill paused container e.g.
        //   broker.pause();
        //   queue.flush();
        //   broker.kill();
        //   session.waitConnectionLostEvent(); // There will be no ACK messages sent

        // Create, but don't start broker yet
        // Use broker in the second part of test where PUTs are sent
        // and corresponding ACKs are expected
        BmqBroker broker = BmqBroker.createStoppedBroker();

        // Use simulator in the first part of test where PUTs are sent
        // and there should be no ACKs
        BmqBrokerSimulator simulator =
                new BmqBrokerSimulator(
                        broker.sessionOptions().brokerUri().getPort(),
                        BmqBrokerSimulator.Mode.BMQ_MANUAL_MODE);
        simulator.start();

        TestSession session = new TestSession(broker.sessionOptions().brokerUri().getPort());
        simulator.pushSuccess(); // Ok for nego request

        final int NUM_MESSAGES = 500;
        final int PAYLOAD_SIZE = 1024;

        try {
            // 2) Start the session
            session.start(DEFAULT_TIMEOUT);

            // 3) Open the queue
            simulator.pushSuccess(); // Ok for open request
            simulator.pushSuccess(); // Ok for configure request
            Queue queue = session.getWriterQueue(createUniqueUri(), false);
            assertFalse(queue.isOpen());

            queue.open(QueueOptions.createDefault(), DEFAULT_TIMEOUT);
            assertTrue(queue.isOpen());

            // 4) Pack a number of PUT messages
            PutMessage[] putMessages = new PutMessage[NUM_MESSAGES];
            for (int i = 0; i < putMessages.length; i++) {
                byte[] payload = new byte[PAYLOAD_SIZE];
                payload[0] = (byte) i;
                PutMessage putMessage = makePut(queue, payload);
                putMessages[i] = putMessage;
                queue.pack(putMessage);
            }

            // 5) Flush the queue and answer nothing from simulator
            queue.flush();

            // 6) Stop the simulator, wait for connection lost event
            simulator.stop();

            List<AckMessage> receivedAcks = new ArrayList<>();
            assertNotNull(session.waitConnectionLostEvent(receivedAcks));

            assertEquals(0, receivedAcks.size());

            // 7) Wait for NAK events and verify them
            // Before that we packed 500 messages, where each message contained 4096 bytes payload.
            // Given the size of the data and the fact that we stopped the broker right after
            // flushing the queue,
            // here it is expected that the broker will not send ACK messages but only NAK messages.
            //
            // In the future, this test should be improved to handle more complicated situation when
            // the broker
            // manages to send ACKs for a part of sent messages before losing connection.
            verifyNakMessages(session, queue, receivedAcks, putMessages);

            // 8) Start the broker and wait for reconnected and state restored events
            broker.start();

            session.waitReconnectedEvent();
            QueueControlEvent event = session.waitQueueEvent();
            assertEquals(QueueControlEvent.Type.QUEUE_REOPEN_RESULT, event.type());
            assertEquals(ResultCodes.OpenQueueResult.SUCCESS, event.result());
            session.waitStateRestoredEvent();

            // 9) Verify that packed queue can be flushed without any exception
            assertTrue(queue.isOpen());

            // Pack messages
            putMessages = new PutMessage[NUM_MESSAGES];
            for (int i = 0; i < putMessages.length; i++) {
                byte[] payload = new byte[PAYLOAD_SIZE];
                payload[0] = (byte) i;
                PutMessage putMessage = makePut(queue, payload);
                putMessages[i] = putMessage;
                queue.pack(putMessage);
            }

            // Flush the queue
            queue.flush();

            // Wait for ACK events and verify them
            verifyAckMessages(session, queue, putMessages);

            // 10) Close the queue, stop the session and the broker
            queue.close(DEFAULT_TIMEOUT);

            broker.setDropTmpFolder();
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw new RuntimeException(e);
        } finally {
            session.stop(DEFAULT_TIMEOUT);
            session.linger();

            simulator.stop(); // should be already stopped
            broker.close();

            logger.info("============================================");
            logger.info("END Testing SessionIT queue flush down.");
            logger.info("============================================");
        }
    }

    private void verifyAckMessages(TestSession session, Queue queue, PutMessage... putMessages) {
        // Extract correlation Ids from put messages
        Set<CorrelationId> putCorrelationIds =
                Arrays.stream(putMessages)
                        .map(PutMessage::correlationId)
                        .collect(Collectors.toSet());

        // Find corresponding PUT message correlation Id for each ACK message
        while (!putCorrelationIds.isEmpty()) {
            // Get next Ack message
            AckMessage ackMsg = session.waitAckMessage();
            verifyAckMessage(session, queue, AckResult.SUCCESS, ackMsg, putCorrelationIds);
        }
    }

    private void verifyReceivedAckMessages(
            TestSession session,
            Queue queue,
            List<AckMessage> receivedAckMessages,
            PutMessage... putMessages) {
        // Extract correlation Ids from put messages
        Set<CorrelationId> putCorrelationIds =
                Arrays.stream(putMessages)
                        .map(PutMessage::correlationId)
                        .collect(Collectors.toSet());

        // Check received ACK messages
        for (AckMessage ackMsg : receivedAckMessages) {
            verifyAckMessage(session, queue, AckResult.SUCCESS, ackMsg, putCorrelationIds);
        }

        assertEquals(0, putCorrelationIds.size());
    }

    private void verifyAckMessage(
            TestSession session,
            Queue queue,
            AckResult ackResult,
            AckMessage ackMsg,
            Set<CorrelationId> putCorrelationIds) {

        CorrelationId cId = ackMsg.correlationId();

        assertEquals(ackResult, ackMsg.status());
        assertEquals(session.session, ackMsg.session());
        assertEquals(queue, ackMsg.queue());

        // Expect to find any matching correlationId
        assertTrue(putCorrelationIds.contains(cId));

        // Remove correlationId from the set
        putCorrelationIds.remove(cId);
    }

    private void verifyNakMessages(
            TestSession session,
            Queue queue,
            List<AckMessage> receivedAckMessages,
            PutMessage... putMessages) {
        // Extract correlation Ids from put messages
        Set<CorrelationId> putCorrelationIds =
                Arrays.stream(putMessages).map(m -> m.correlationId()).collect(Collectors.toSet());

        // First check already received ACK messages which could be received
        // before ConnectionLost event
        for (AckMessage ackMsg : receivedAckMessages) {
            verifyAckMessage(session, queue, AckResult.SUCCESS, ackMsg, putCorrelationIds);
        }

        // Find corresponding PUT message correlation Id for each NAK message
        while (!putCorrelationIds.isEmpty()) {
            // Get next Ack message
            AckMessage nackMsg = session.waitAckMessage();
            verifyAckMessage(session, queue, AckResult.UNKNOWN, nackMsg, putCorrelationIds);
        }
    }

    @Test
    void queueClose() throws BMQException, IOException {

        logger.info("=======================================");
        logger.info("BEGIN Testing SessionIT testQueueClose.");
        logger.info("=======================================");

        // 1) Bring up the broker.
        // 2) Start the session
        // 3) Open the queue
        // 4) Stop the broker and wait for connection lost event
        // 5) Close the queue
        // 6) Start the broker and wait for reconnected and state restored events
        // 7) Open the queue again
        // 8) Close the queue again
        // 9) Stop the session and the broker

        // 1) Bring up the broker.
        BmqBroker broker = BmqBroker.createStartedBroker();
        TestSession session = new TestSession(broker.sessionOptions());

        try {
            // 2) Start the session
            session.start(DEFAULT_TIMEOUT);

            // 3) Open the queue
            Queue queue = session.getWriterQueue(createUniqueUri(), false);
            assertFalse(queue.isOpen());

            queue.open(QueueOptions.createDefault(), DEFAULT_TIMEOUT);
            assertTrue(queue.isOpen());

            // 4) Stop the broker and wait for connection lost event
            CompletableFuture<Void> stopFuture = broker.stopAsync();
            session.waitConnectionLostEvent();

            // 5) Close the queue
            assertTrue(queue.isOpen());
            try {
                queue.close(DEFAULT_TIMEOUT);
                fail();
            } catch (BMQException e) {
                assertEquals(CloseQueueResult.NOT_CONNECTED, e.code());
            }

            // 6) Start the broker and wait for reconnected and state restored events
            stopFuture.get();
            broker.start();

            session.waitReconnectedEvent();
            session.waitStateRestoredEvent();

            // 7) Open another queue (we cannot open the same queue due to its invalid closing)
            queue = session.getWriterQueue(createUniqueUri(), false);
            queue.open(QueueOptions.createDefault(), DEFAULT_TIMEOUT);
            assertTrue(queue.isOpen());

            // 8) Close the queue again
            queue.close(DEFAULT_TIMEOUT);
            assertFalse(queue.isOpen());

            broker.setDropTmpFolder();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            // 9) Stop the session and the broker
            session.stop(DEFAULT_TIMEOUT);
            session.linger();

            broker.close();

            logger.info("=====================================");
            logger.info("END Testing SessionIT testQueueClose.");
            logger.info("=====================================");
        }
    }

    /**
     * Open and close multiple subqueues for the same uri
     *
     * <p>This test is replica of corresponding BrokerSession test
     *
     * <ul>
     *   <li>start broker simulator in auto mode
     *   <li>create session
     *   <li>start session
     *   <li>create and open subqueues asynchronously
     *   <li>check that proper events were handled by user-defined user handler
     *   <li>check that proper requests were sent
     *   <li>close subqueues asynchronously
     *   <li>check that proper events were handled by user-defined user handler
     *   <li>check that proper requests were sent
     *   <li>stop session
     * </ul>
     */
    @Test
    void openCloseMultipleSubqueuesTest() throws BMQException {
        logger.info("=======================================================");
        logger.info("BEGIN Testing SessionIT openCloseMultipleSubqueuesTest.");
        logger.info("=======================================================");

        logger.info("1. start broker simulator in auto mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_AUTO_MODE);
        server.start();

        logger.info("2. create session");
        TestSession session = new TestSession(server.getPort());

        try {
            logger.info("3. start session");
            session.start(DEFAULT_TIMEOUT);

            final int SUBQUEUE_NUM = 10;

            logger.info("4. create and open subqueues asyncronously");
            Uri uri = createUniqueUri();

            ArrayList<Queue> queues = new ArrayList<>(SUBQUEUE_NUM);

            for (int i = 0; i < SUBQUEUE_NUM; i++) {
                Uri subqueueUri = new Uri(uri.canonical() + "?id=app" + i);

                Queue queue = session.getReaderQueue(subqueueUri);
                assertFalse(queue.isOpen());

                queue.openAsync(QUEUE_OPTIONS, DEFAULT_TIMEOUT);
                queues.add(queue);
            }

            assertEquals(SUBQUEUE_NUM, queues.size());

            logger.info("6. check that proper events were handled by user-defined user handler");
            for (int i = 0; i < SUBQUEUE_NUM; i++) {
                QueueControlEvent queueEvent = session.waitQueueEvent();

                assertEquals(QueueControlEvent.Type.QUEUE_OPEN_RESULT, queueEvent.type());
                assertEquals(ResultCodes.OpenQueueResult.SUCCESS, queueEvent.result());
            }

            // All queues should be opened
            for (Queue queue : queues) {
                assertTrue(queue.isOpen());
            }

            logger.info("7. check that proper requests were sent");
            for (int i = 0; i < SUBQUEUE_NUM * 2; i++) {
                ControlMessageChoice request = server.nextClientRequest();

                if (request.isOpenQueueValue()) {
                    verifyOpenRequest(request);
                } else {
                    verifyConfigureRequest(request);
                }
            }

            // There should be no more requests
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            logger.info("8. close subqueues asynchronously");
            // Shuffle the list in order to send close requests
            // in some random order
            Collections.shuffle(queues);

            for (Queue queue : queues) {
                queue.closeAsync(DEFAULT_TIMEOUT);
            }

            logger.info("9. check that proper events were handled by user-defined user handler");
            for (int i = 0; i < SUBQUEUE_NUM; i++) {
                QueueControlEvent queueEvent = session.waitQueueEvent();

                assertEquals(QueueControlEvent.Type.QUEUE_CLOSE_RESULT, queueEvent.type());
                assertEquals(ResultCodes.CloseQueueResult.SUCCESS, queueEvent.result());
            }

            // All queues should be closed
            for (Queue queue : queues) {
                assertFalse(queue.isOpen());
            }

            logger.info("10. check that proper requests were sent");
            int numCloseRequests = 0;
            for (int i = 0; i < SUBQUEUE_NUM * 2; i++) {
                ControlMessageChoice request = server.nextClientRequest();

                if (request.isConfigureStreamValue()) {
                    verifyConfigureRequest(request);
                } else {
                    numCloseRequests++;
                    verifyCloseRequest(request, numCloseRequests == SUBQUEUE_NUM);
                }
            }

            // There should be no more requests
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

        } catch (AssertionError e) {
            logger.error("Exception: ", e);
        } finally {
            logger.info("11. stop session");
            session.stop(DEFAULT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("=====================================================");
            logger.info("END Testing SessionIT openCloseMultipleSubqueuesTest.");
            logger.info("=====================================================");
        }
    }

    /**
     * Test that queues are configured and events are issued properly during complete host health
     * state transitions.
     *
     * <ul>
     *   <li>start broker
     *   <li>create session with initial monitor state
     *   <li>start session
     *   <li>verify HOST_UNHEALTHY event if init state is not null and not HEALTHY
     *   <li>open 1 insensitive and 2 sensitive queues
     *   <li>change state to init value
     *   <li>verify no events
     *   <li>change state to unhealthy
     *   <li>verify sensitive queues suspend (if init state was null or healthy)
     *   <li>change state to unknown
     *   <li>verify no events
     *   <li>change state to null
     *   <li>verify no events
     *   <li>change state to healthy
     *   <li>verify sensitive queues resume
     *   <li>change state to null
     *   <li>verify no events
     *   <li>change state to unknown
     *   <li>verify sensitive queues suspend
     *   <li>change state to healthy
     *   <li>verify sensitive queues resume
     *   <li>close queues
     *   <li>change state to unhealthy
     *   <li>close session
     *   <li>change state to healthy
     *   <li>start session
     *   <li>verify no events
     * </ul>
     *
     * >
     *
     * @param initState initial host health state
     */
    private void testCompleteHealthTransitions(HostHealthState initState) throws IOException {
        logger.info("======================================================");
        logger.info("BEGIN Testing SessionIT testCompleteHealthTransitions.");
        logger.info("======================================================");
        logger.info("Initial health state: {}", initState);

        logger.info("1. start broker");
        BmqBroker broker = BmqBroker.createStartedBroker();

        logger.info("2. create session with initial monitor state");
        TestMonitor monitor = new TestMonitor(initState);
        SessionOptions opts =
                SessionOptions.builder()
                        .setBrokerUri(broker.sessionOptions().brokerUri())
                        .setHostHealthMonitor(monitor)
                        .build();
        TestSession session = new TestSession(opts);

        try {
            logger.info("3. start session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. verify HOST_UNHEALTHY event if init state is not null and not HEALTHY");
            if (initState != null && initState != HostHealthState.Healthy) {
                assertNotNull(session.waitHostUnhealthyEvent());
            }

            logger.info("5. open 1 insensitive and 2 sensitive queues");
            Queue queue1 = session.getReaderQueue(createUniqueUri());
            queue1.open(QUEUE_OPTIONS, DEFAULT_TIMEOUT);

            Queue queue2 = session.getReaderQueue(createUniqueUri());
            Queue queue3 = session.getReaderQueue(createUniqueUri());

            QueueOptions sensitiveOptions =
                    QueueOptions.builder()
                            .merge(QUEUE_OPTIONS)
                            .setSuspendsOnBadHostHealth(true)
                            .build();

            queue2.open(sensitiveOptions, DEFAULT_TIMEOUT);
            queue3.open(sensitiveOptions, DEFAULT_TIMEOUT);

            logger.info("6. change state to init value({})", initState);
            monitor.setState(initState);

            logger.info("7. verify no events");
            session.checkNoEvent();

            logger.info("8. change state to unhealthy");
            monitor.setState(HostHealthState.Unhealthy);

            logger.info("9. verify sensitive queues suspend (if init state was null or healthy)");
            if (initState == null || initState == HostHealthState.Healthy) {
                assertNotNull(session.waitHostUnhealthyEvent());

                verifyQueueControlEvent(
                        session.waitQueueEvent(),
                        QueueControlEvent.Type.QUEUE_SUSPENDED,
                        ConfigureQueueResult.SUCCESS,
                        queue2);
                verifyQueueControlEvent(
                        session.waitQueueEvent(),
                        QueueControlEvent.Type.QUEUE_SUSPENDED,
                        ConfigureQueueResult.SUCCESS,
                        queue3);
            } else {
                session.checkNoEvent();
            }

            logger.info("10. change state to unknown");
            monitor.setState(HostHealthState.Unknown);

            logger.info("11. verify no events");
            session.checkNoEvent();

            logger.info("12. change state to null");
            monitor.setState(null);

            logger.info("13. verify no events");
            session.checkNoEvent();

            logger.info("14. change state to healthy");
            monitor.setState(HostHealthState.Healthy);

            logger.info("15. verify sensitive queues resume");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.SUCCESS,
                    queue2);
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.SUCCESS,
                    queue3);
            assertNotNull(session.waitHostHealthRestoredEvent());

            logger.info("16. change state to null");
            monitor.setState(null);

            logger.info("17. verify no events");
            session.checkNoEvent();

            logger.info("18. change state to unknown");
            monitor.setState(HostHealthState.Unknown);
            assertNotNull(session.waitHostUnhealthyEvent());

            logger.info("19. verify sensitive queues suspend");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.SUCCESS,
                    queue2);
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.SUCCESS,
                    queue3);

            logger.info("20. change state to healthy");
            monitor.setState(HostHealthState.Healthy);

            logger.info("21. verify sensitive queues resume");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.SUCCESS,
                    queue2);
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.SUCCESS,
                    queue3);
            assertNotNull(session.waitHostHealthRestoredEvent());

            logger.info("22. close queues");
            queue1.close(DEFAULT_TIMEOUT);
            queue2.close(DEFAULT_TIMEOUT);
            queue3.close(DEFAULT_TIMEOUT);

            logger.info("23. change state to unhealthy");
            monitor.setState(HostHealthState.Unhealthy);
            assertNotNull(session.waitHostUnhealthyEvent());

            logger.info("24. close session");
            // This should reset session state to Healthy
            session.stop(DEFAULT_TIMEOUT);

            logger.info("25. change state to healthy");
            monitor.setState(HostHealthState.Healthy);

            logger.info("26. start session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("27. verify no events");
            session.checkNoEvent();

            broker.setDropTmpFolder();
        } catch (Exception e) {
            logger.error("Exception", e);
            throw e;
        } finally {
            logger.info("28. stop session");
            session.stop(DEFAULT_TIMEOUT);
            session.linger();

            broker.close();

            logger.info("====================================================");
            logger.info("END Testing SessionIT testCompleteHealthTransitions.");
            logger.info("====================================================");
        }
    }

    @Test
    void testCompleteHealthTransitions() throws IOException {
        // Test complete health transitions when monitor provides null state
        // during first session start. Since null values are ignored, default
        // health state (Healthy) should be used instead.
        testCompleteHealthTransitions(null);

        // Test complete health transitions when monitor provides Healthy
        // state during session start.
        testCompleteHealthTransitions(HostHealthState.Healthy);

        // Test complete health transitions when monitor provides Unhealthy
        // state during session start. It is expected that session goes to
        // unhealthy state and HOST_UNHEALTY event should be issued.
        testCompleteHealthTransitions(HostHealthState.Unhealthy);

        // Test complete health transitions when monitor provides Unknown
        // state during session start. Since Unknown state is interpreted as
        // unhealthy, session should go to unhealthy state and HOST_UNHEALTY
        // event should be issued.
        testCompleteHealthTransitions(HostHealthState.Unknown);
    }

    /**
     * Test that queues are configured and events are issued properly during incomplete host health
     * state transitions.
     *
     * <ul>
     *   <li>start server in manual mode
     *   <li>create session
     *   <li>start session
     *   <li>open insensitive queue
     *   <li>open sensitive queues 1 and 2
     * </ul>
     *
     * <p>Test resume request on suspend response
     *
     * <ul>
     *   <li>change state to unhealthy (delay queue 2 suspend response)
     *   <li>verify queue 1 suspends
     *   <li>change state to healthy
     *   <li>verify queue 2 suspends
     *   <li>verify queue 1 resumes
     *   <li>verify queue 2 resumes
     *   <li>change state to unhealthy
     *   <li>verify queue 1 suspends
     *   <li>verify queue 2 suspends
     * </ul>
     *
     * <p>Test suspend request on resume response
     *
     * <ul>
     *   <li>change state to healthy (delay queue 2 resume response)
     *   <li>verify queue 1 resumes
     *   <li>change state to unhealthy
     *   <li>verify queue 2 resumes
     *   <li>verify queue 1 suspends
     *   <li>verify queue 2 suspends
     * </ul>
     */
    @Test
    void testIncompleteHealthTransitions() {
        logger.info("========================================================");
        logger.info("BEGIN Testing SessionIT testIncompleteHealthTransitions.");
        logger.info("========================================================");

        final Duration DELAY = Duration.ofSeconds(1);

        logger.info("1. start test server in manual mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_MANUAL_MODE);
        server.start();

        logger.info("2. create session");
        TestMonitor monitor = new TestMonitor();
        SessionOptions opts =
                SessionOptions.builder()
                        .setBrokerUri(server.getURI())
                        .setHostHealthMonitor(monitor)
                        .build();
        TestSession session = new TestSession(opts);

        try {
            server.pushSuccess(); // OK for nego
            logger.info("3. start session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. open insensitive queue");
            server.pushSuccess(2);
            Queue queue = session.getReaderQueue(createUniqueUri());
            queue.open(QUEUE_OPTIONS, DEFAULT_TIMEOUT);
            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("5. open sensitive queues 1 and 2");
            Queue queue1 = session.getReaderQueue(createUniqueUri());
            Queue queue2 = session.getReaderQueue(createUniqueUri());

            QueueOptions sensitiveOptions =
                    QueueOptions.builder()
                            .merge(QUEUE_OPTIONS)
                            .setSuspendsOnBadHostHealth(true)
                            .build();

            server.pushSuccess(2);
            queue1.open(sensitiveOptions, DEFAULT_TIMEOUT);
            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            server.pushSuccess(2);
            queue2.open(sensitiveOptions, DEFAULT_TIMEOUT);
            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("*** Test resume request on suspend response");
            logger.info("6. change state to unhealthy (delay queue 2 suspend response)");
            server.pushSuccess(); // OK for queue 1 suspend
            // Delay queue 2 suspend response
            server.pushItem(StatusCategory.E_SUCCESS, DELAY);

            monitor.setState(HostHealthState.Unhealthy);
            assertNotNull(session.waitHostUnhealthyEvent());

            // Both queues should send suspend requests
            verifyConfigureRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("7. verify queue 1 suspends");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.SUCCESS,
                    queue1);

            logger.info("8. change state to healthy");
            server.pushSuccess(); // OK for queue 1 resume
            server.pushSuccess(); // OK for queue 2 resume

            monitor.setState(HostHealthState.Healthy);

            // Queue 1 should send resume request
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("9. verify queue 2 suspends");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.SUCCESS,
                    queue2);

            // Queue 2 should send resume request
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("10. verify queue 1 resumes");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.SUCCESS,
                    queue1);

            logger.info("11. verify queue 2 resumes");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.SUCCESS,
                    queue2);

            assertNotNull(session.waitHostHealthRestoredEvent());

            logger.info("12. change state to unhealthy");
            server.pushSuccess(); // OK for queue 1 suspend
            server.pushSuccess(); // OK for queue 2 suspend

            monitor.setState(HostHealthState.Unhealthy);
            assertNotNull(session.waitHostUnhealthyEvent());

            // Both queues should send suspend requests
            verifyConfigureRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("13. verify queue 1 suspends");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.SUCCESS,
                    queue1);

            logger.info("14. verify queue 2 suspends");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.SUCCESS,
                    queue2);

            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            session.checkNoEvent();

            logger.info("*** Test suspend request on resume response");
            logger.info("15. change state to healthy (delay queue 2 resume response)");
            server.pushSuccess(); // OK for queue 1 resume
            // Delay queue 2 resume response
            server.pushItem(StatusCategory.E_SUCCESS, DELAY);

            monitor.setState(HostHealthState.Healthy);

            // Both queues should send resume requests
            verifyConfigureRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("16. verify queue 1 resumes");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.SUCCESS,
                    queue1);

            logger.info("17. change state to unhealthy");
            server.pushSuccess(); // OK for queue 1 suspend
            server.pushSuccess(); // OK for queue 2 suspend

            monitor.setState(HostHealthState.Unhealthy);
            // Host wasn't stable healthy -> no HOST_UNHEALTHY event

            // Queue 1 should send suspend request
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("18. verify queue 2 resumes");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.SUCCESS,
                    queue2);

            // Queue 2 should send suspend request
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("19. verify queue 1 suspends");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.SUCCESS,
                    queue1);

            logger.info("20. verify queue 2 suspends");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.SUCCESS,
                    queue2);

            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            session.checkNoEvent();

        } catch (Exception e) {
            logger.error("Exception", e);
            throw e;
        } finally {
            logger.info("21. stop session");
            server.pushSuccess();
            session.stop(DEFAULT_TIMEOUT);
            session.linger();

            server.stop();

            logger.info("======================================================");
            logger.info("END Testing SessionIT testIncompleteHealthTransitions.");
            logger.info("======================================================");
        }
    }

    /**
     * Test that queues are not suspended/resumed if host health changes during connection lsot
     *
     * <ul>
     *   <li>start server in active mode
     *   <li>create session
     *   <li>start session
     *   <li>open insensitive queue
     *   <li>open sensitive queue
     * </ul>
     *
     * <p>Test host becomes unhealthy after connection lost
     *
     * <ul>
     *   <li>stop the server
     *   <li>change state to unhealthy
     *   <li>verify HOST_UNHEALTHY event
     *   <li>verify no QUEUE_SUSPENDED event
     *   <li>start the server
     *   <li>verify queues reopen
     * </ul>
     *
     * <p>Test host becomes healthy after connection lost
     *
     * <ul>
     *   <li>stop the server
     *   <li>change state to healthy
     *   <li>verify HOST_HEALTH_RESTORED event
     *   <li>verify no QUEUE_RESUMED event
     *   <li>start the server
     *   <li>verify queues reopen
     * </ul>
     */
    @Test
    void testHealthChangesOnConnectionLost() {
        logger.info("=========================================================");
        logger.info("BEGIN Testing SessionIT testHealthChangesOnConnectionLost");
        logger.info("=========================================================");

        logger.info("1. start test server in active mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_AUTO_MODE);
        server.start();

        logger.info("2. create session");
        TestMonitor monitor = new TestMonitor();
        SessionOptions opts =
                SessionOptions.builder()
                        .setBrokerUri(server.getURI())
                        .setHostHealthMonitor(monitor)
                        .build();
        TestSession session = new TestSession(opts);

        try {
            logger.info("3. start session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. open insensitive queue");
            Queue queue1 = session.getReaderQueue(createUniqueUri());
            queue1.open(QUEUE_OPTIONS, DEFAULT_TIMEOUT);
            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("5. open sensitive queue");
            Queue queue2 = session.getReaderQueue(createUniqueUri());

            QueueOptions sensitiveOptions =
                    QueueOptions.builder()
                            .merge(QUEUE_OPTIONS)
                            .setSuspendsOnBadHostHealth(true)
                            .build();

            queue2.open(sensitiveOptions, DEFAULT_TIMEOUT);
            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("*** Test host becomes unhealthy after connection lost");
            logger.info("6. stop the server");
            server.stop();
            session.waitConnectionLostEvent();

            logger.info("7. change state to unhealthy");
            monitor.setState(HostHealthState.Unhealthy);

            logger.info("8. verify HOST_UNHEALTHY event");
            assertNotNull(session.waitHostUnhealthyEvent());

            logger.info("9. verify no QUEUE_SUSPENDED event");
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            session.checkNoEvent();

            logger.info("10. start the server");
            server.start();

            assertNotNull(session.waitReconnectedEvent());

            logger.info("11. verify queues reopen");
            verifyOpenRequest(server.nextClientRequest());
            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            // Sensitive queue reopens first due to skipped configure request
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_REOPEN_RESULT,
                    OpenQueueResult.SUCCESS,
                    queue2);
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_REOPEN_RESULT,
                    OpenQueueResult.SUCCESS,
                    queue1);

            assertNotNull(session.waitStateRestoredEvent());

            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            session.checkNoEvent();

            logger.info("*** Test host becomes healthy after connection lost");
            logger.info("12. stop the server");
            server.stop();
            session.waitConnectionLostEvent();

            logger.info("13. change state to healthy");
            monitor.setState(HostHealthState.Healthy);

            logger.info("14. verify HOST_HEALTH_RESTORED event");
            assertNotNull(session.waitHostHealthRestoredEvent());

            logger.info("15. verify no QUEUE_RESUMED event");
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            session.checkNoEvent();

            logger.info("16. start the server");
            server.start();

            assertNotNull(session.waitReconnectedEvent());

            logger.info("17. verify queues reopen");
            verifyOpenRequest(server.nextClientRequest());
            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_REOPEN_RESULT,
                    OpenQueueResult.SUCCESS,
                    queue1);
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_REOPEN_RESULT,
                    OpenQueueResult.SUCCESS,
                    queue2);

            assertNotNull(session.waitStateRestoredEvent());

            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            session.checkNoEvent();
        } catch (Exception e) {
            logger.error("Exception", e);
            throw e;
        } finally {
            logger.info("18. stop session");
            session.stop(DEFAULT_TIMEOUT);
            session.linger();

            server.stop();

            logger.info("========================================================");
            logger.info("END Testing SessionIT testHealthChangesOnConnectionLost.");
            logger.info("========================================================");
        }
    }

    /**
     * Critical test to check that standalone configure request for suspended queue is sent only
     * when the queue resumes.
     *
     * <p>This test is replica of corresponding BrokerSession test.
     *
     * <ul>
     *   <li>start test server in active mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>create insensitive and sensitive to host health queues
     *   <li>open the queues
     *   <li>configure insensitive queue with unspecified host-health awareness
     *   <li>configure sensitive queue with unspecified host-health awareness
     *   <li>set host health to unhealthy
     *   <li>configure insensitive queue (configuration request is sent)
     *   <li>configure sensitive queue (configuration request is not sent)
     *   <li>set host health back to healthy
     *   <li>check that sensitive queue resume request with new options is sent
     *   <li>stop and linger broker session and stop server
     * </ul>
     */
    @Test
    void configureSuspendedQueue() {
        logger.info("================================================");
        logger.info("BEGIN Testing SessionIT configureSuspendedQueue.");
        logger.info("================================================");

        final int MAX_UNCONFIRMED_MESSAGES = 500;
        final int MAX_UNCONFIRMED_BYTES = 5000000;
        final int CONSUMER_PRIORITY = 100;
        final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(1);

        logger.info("1. start test server in active mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_AUTO_MODE);
        server.start();

        logger.info("2. create broker session");
        TestMonitor monitor = new TestMonitor();
        SessionOptions opts =
                SessionOptions.builder()
                        .setBrokerUri(server.getURI())
                        .setHostHealthMonitor(monitor)
                        .build();
        TestSession session = new TestSession(opts);

        try {
            logger.info("3. start broker session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. create queues");
            Queue queue1 = session.getReaderQueue(createUniqueUri());
            Queue queue2 = session.getReaderQueue(createUniqueUri());

            logger.info("5. open the queues");
            QueueOptions insensitiveOptions =
                    QueueOptions.builder().setMaxUnconfirmedBytes(MAX_UNCONFIRMED_BYTES).build();
            queue1.open(insensitiveOptions, SEQUENCE_TIMEOUT);

            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            QueueOptions sensitiveOptions =
                    QueueOptions.builder()
                            .setSuspendsOnBadHostHealth(true)
                            .setMaxUnconfirmedBytes(MAX_UNCONFIRMED_BYTES)
                            .build();
            queue2.open(sensitiveOptions, SEQUENCE_TIMEOUT);

            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("6. configure insensitive queue with unspecified host-health awareness");
            QueueOptions hostHealthUnspecifiedOptions =
                    QueueOptions.builder().setConsumerPriority(CONSUMER_PRIORITY).build();

            queue1.configureAsync(hostHealthUnspecifiedOptions, SEQUENCE_TIMEOUT);
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    queue1);
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("7. configure sensitive queue with unspecified host-health awareness");
            queue2.configure(hostHealthUnspecifiedOptions, SEQUENCE_TIMEOUT);
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("8. set host health to unhealthy");
            monitor.setState(HostHealthState.Unhealthy);
            assertNotNull(session.waitHostUnhealthyEvent());

            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ResultCodes.ConfigureQueueResult.SUCCESS,
                    queue2);

            verifyConfigureRequest(server.nextClientRequest());
            assertNull(server.nextClientRequest());

            logger.info("9. configure insensitive queue (configuration request is sent)");
            QueueOptions newOptions =
                    QueueOptions.builder()
                            .setMaxUnconfirmedMessages(MAX_UNCONFIRMED_MESSAGES)
                            .build();
            queue1.configure(newOptions, SEQUENCE_TIMEOUT);

            {
                ControlMessageChoice msg = server.nextClientRequest();
                verifyConfigureRequest(msg);

                ConsumerInfo info =
                        TestTools.getDefaultConsumerInfo(msg.configureStream().streamParameters());
                // 'maxUnconfirmedMessages' should be taken from 'newOptions'.
                assertEquals(newOptions.getMaxUnconfirmedMessages(), info.maxUnconfirmedMessages());
                // 'consumerPriority' should be taken from previous successful
                // configuration with 'hostHealthUnspecifiedOptions' options.
                assertEquals(
                        hostHealthUnspecifiedOptions.getConsumerPriority(),
                        info.consumerPriority());
                // 'maxUnconfirmedBytes' should be taken from 'insensitiveOptions'
                // options used to open the queue
                assertEquals(
                        insensitiveOptions.getMaxUnconfirmedBytes(), info.maxUnconfirmedBytes());
            }
            assertNull(server.nextClientRequest());

            logger.info("10. configure sensitive queue (configuration request is not sent)");
            queue2.configure(newOptions, SEQUENCE_TIMEOUT);
            assertNull(server.nextClientRequest());

            logger.info("11. set host health back to healthy");
            monitor.setState(HostHealthState.Healthy);

            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ResultCodes.ConfigureQueueResult.SUCCESS,
                    queue2);

            assertNotNull(session.waitHostHealthRestoredEvent());

            logger.info("12. check that sensitive queue resume request with new options is sent");
            {
                ControlMessageChoice msg = server.nextClientRequest();
                verifyConfigureRequest(msg);

                ConsumerInfo info =
                        TestTools.getDefaultConsumerInfo(msg.configureStream().streamParameters());
                // 'maxUnconfirmedMessages' should be taken from 'newOptions'.
                assertEquals(newOptions.getMaxUnconfirmedMessages(), info.maxUnconfirmedMessages());
                // 'consumerPriority' should be taken from previous successful
                // configuration with 'hostHealthUnspecifiedOptions' options.
                assertEquals(
                        hostHealthUnspecifiedOptions.getConsumerPriority(),
                        info.consumerPriority());
                // 'maxUnconfirmedBytes' should be taken from 'sensitiveOptions'
                // options used to open the queue
                assertEquals(sensitiveOptions.getMaxUnconfirmedBytes(), info.maxUnconfirmedBytes());
            }

            session.checkNoEvent();
            assertNull(server.nextClientRequest());
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            logger.info("13. stop session");
            session.stop(SHORT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("==============================================");
            logger.info("END Testing SessionIT configureSuspendedQueue.");
            logger.info("==============================================");
        }
    }

    /**
     * Critical test to check that queues are correctly suspended and resumed in response to
     * client-driven configure operations that modify the 'suspendsOnBadHostHealth' queue option
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>open two host health insensitive queues
     *   <li>configure each queue as host health sensitive
     *   <li>set host health to unhealthy
     *   <li>verify that queues suspend
     *   <li>configure each queue as no longer host health sensitive
     *   <li>verify that each queue resumes, with no HOST_HEALTH_RESTORED event
     *   <li>set host health back to healthy
     *   <li>verify HOST_HEALTH_RESTORED event is issued
     *   <li>set host health back to unhealthy
     *   <li>verify HOST_UNHEALTHY with no QUEUE_SUSPENDED events
     *   <li>configure each queue back to host health sensitive
     *   <li>verify that queues suspend
     *   <li>set host health back to healthy
     *   <li>verify that queues resume
     *   <li>verify HOST_HEALTH_RESTORED event is issued
     *   <li>configure each queue as no longer host health sensitive
     * </ul>
     */
    @Test
    void reconfigureHostHealthSensitivity() {
        logger.info("=========================================================");
        logger.info("BEGIN Testing SessionIT reconfigureHostHealthSensitivity.");
        logger.info("=========================================================");

        final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(2);

        logger.info("1. start test server in manual mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_MANUAL_MODE);
        server.start();

        logger.info("2. create broker session");
        TestMonitor monitor = new TestMonitor();
        SessionOptions opts =
                SessionOptions.builder()
                        .setBrokerUri(server.getURI())
                        .setHostHealthMonitor(monitor)
                        .build();
        TestSession session = new TestSession(opts);

        try {
            server.pushSuccess(); // OK for nego
            logger.info("3. start broker session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. create queues");
            Queue queue1 = session.getReaderQueue(createUniqueUri());
            Queue queue2 = session.getReaderQueue(createUniqueUri());

            logger.info("5. open two host health insensitive queues");
            QueueOptions insensitiveOptions =
                    QueueOptions.builder().setSuspendsOnBadHostHealth(false).build();

            server.pushSuccess(2);
            queue1.open(insensitiveOptions, SEQUENCE_TIMEOUT);
            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            server.pushSuccess(2);
            queue2.open(insensitiveOptions, SEQUENCE_TIMEOUT);
            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("6. configure each queue as host health sensitive");
            QueueOptions sensitiveOptions =
                    QueueOptions.builder().setSuspendsOnBadHostHealth(true).build();

            server.pushSuccess(2);
            queue1.configureAsync(sensitiveOptions, SEQUENCE_TIMEOUT);
            queue2.configureAsync(sensitiveOptions, SEQUENCE_TIMEOUT);

            verifyConfigureRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    queue1);

            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    queue2);

            logger.info("7. set host health to unhealthy");
            server.pushSuccess(2);
            monitor.setState(HostHealthState.Unhealthy);
            assertNotNull(session.waitHostUnhealthyEvent());

            logger.info("8. verify that queues suspend");
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ResultCodes.ConfigureQueueResult.SUCCESS,
                    queue1);
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ResultCodes.ConfigureQueueResult.SUCCESS,
                    queue2);
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            logger.info("9. configure each queue as no longer host health sensitive");
            server.pushSuccess(2);
            queue1.configure(insensitiveOptions, SEQUENCE_TIMEOUT);
            queue2.configureAsync(insensitiveOptions, SEQUENCE_TIMEOUT);

            logger.info("10. verify that each queue resumes, with no HOST_HEALTH_RESTORED event");
            verifyConfigureRequest(server.nextClientRequest());
            // Queue 1 is resumed synchronously

            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.SUCCESS,
                    queue2);
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    queue2);

            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            session.checkNoEvent();

            logger.info("11. set host health back to healthy");
            monitor.setState(HostHealthState.Healthy);

            logger.info("12. verify HOST_HEALTH_RESTORED event is issued");
            assertNotNull(session.waitHostHealthRestoredEvent());

            logger.info("13. set host health back to unhealthy");
            monitor.setState(HostHealthState.Unhealthy);

            logger.info("14. verify HOST_UNHEALTHY with no QUEUE_SUSPENDED events");
            assertNotNull(session.waitHostUnhealthyEvent());
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            session.checkNoEvent();

            logger.info("15. configure each queue back to host health sensitive");
            server.pushSuccess(2);
            queue1.configureAsync(sensitiveOptions, SEQUENCE_TIMEOUT);
            queue2.configure(sensitiveOptions, SEQUENCE_TIMEOUT);

            logger.info("16. verify that queues suspend");
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.SUCCESS,
                    queue1);
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    queue1);

            verifyConfigureRequest(server.nextClientRequest());
            // Queue 2 is suspended synchronously

            logger.info("17. set host health back to healthy");
            server.pushSuccess(2);
            monitor.setState(HostHealthState.Healthy);

            logger.info("18. verify that queues resume");
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.SUCCESS,
                    queue1);
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.SUCCESS,
                    queue2);

            logger.info("19. verify HOST_HEALTH_RESTORED event is issued");
            assertNotNull(session.waitHostHealthRestoredEvent());

            logger.info("20. configure each queue as no longer host health sensitive");
            server.pushSuccess(2);
            queue1.configureAsync(insensitiveOptions, SEQUENCE_TIMEOUT);
            queue2.configureAsync(insensitiveOptions, SEQUENCE_TIMEOUT);

            verifyConfigureRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    queue1);

            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    queue2);

            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            session.checkNoEvent();

        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            logger.info("21. stop session");
            server.pushSuccess(); // OK for disconnect
            session.stop(SHORT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("=======================================================");
            logger.info("END Testing SessionIT reconfigureHostHealthSensitivity.");
            logger.info("=======================================================");
        }
    }

    /**
     * Critical test to check that queues are correctly suspended and resumed in bad response to
     * client-driven configure operations that modify the 'suspendsOnBadHostHealth' queue option
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>open queue two insensitive queues
     *   <li>set host health to unhealthy
     *   <li>configure queues as host health sensitive (no response, bad response)
     *   <li>verify queue 2 suspends with bad status
     *   <li>verify CONNECTION_LOST, queue 1 suspends with cancel status
     *   <li>verify queue reopens
     *   <li>configure queues as host health sensitive
     *   <li>verify queues suspend
     *   <li>configure queue 2 as no longer host health sensitive with bad response
     *   <li>verify queue 2 resumes with bad status
     *   <li>verify queue 2 suspends with bad status
     *   <li>verify CONNECTION_LOST and RECONNECTED events
     *   <li>verify queue reopens
     *   <li>configure queue 2 as no longer host health insensitive (timeout)
     *   <li>set host health back to healthy
     *   <li>verify that queue 1 resumes
     *   <li>verify that queue 2 resumes by timeout
     *   <li>verify HOST_HEALTH_RESTORED event is issued
     *   <li>configure queue 2 with standalone request as health insensitive
     * </ul>
     */
    @Test
    void reconfigureHostHealthSensitivityErrors() {
        logger.info("===============================================================");
        logger.info("BEGIN Testing SessionIT reconfigureHostHealthSensitivityErrors.");
        logger.info("===============================================================");

        final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(2);

        logger.info("1. start test server in manual mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_MANUAL_MODE);
        server.start();

        logger.info("2. create broker session");
        TestMonitor monitor = new TestMonitor();
        SessionOptions opts =
                SessionOptions.builder()
                        .setBrokerUri(server.getURI())
                        .setHostHealthMonitor(monitor)
                        .build();
        TestSession session = new TestSession(opts);

        try {
            server.pushSuccess(); // OK for nego
            logger.info("3. start broker session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. open two insensitive queues");
            Queue queue1 = session.getReaderQueue(createUniqueUri());
            Queue queue2 = session.getReaderQueue(createUniqueUri());

            QueueOptions insensitiveOptions =
                    QueueOptions.builder().setSuspendsOnBadHostHealth(false).build();

            server.pushSuccess(2);
            queue1.open(insensitiveOptions, SEQUENCE_TIMEOUT);
            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            server.pushSuccess(2);
            queue2.open(insensitiveOptions, SEQUENCE_TIMEOUT);
            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("5. set host health to unhealthy");
            monitor.setState(HostHealthState.Unhealthy);
            assertNotNull(session.waitHostUnhealthyEvent());

            logger.info("6. configure queues as host health sensitive (no response, bad response)");
            QueueOptions sensitiveOptions =
                    QueueOptions.builder().setSuspendsOnBadHostHealth(true).build();

            server.pushItem(StatusCategory.E_TIMEOUT); // queue 1 no suspend response
            server.pushItem(StatusCategory.E_UNKNOWN); // queue 2 bad suspend response
            server.pushSuccess(); // OK for nego
            server.pushSuccess(4); // to reopen queues

            queue1.configureAsync(sensitiveOptions, SEQUENCE_TIMEOUT);
            queue2.configureAsync(sensitiveOptions, SEQUENCE_TIMEOUT);

            verifyConfigureRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("7. verify that queue 2 suspends with bad status");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.UNKNOWN,
                    queue2);
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.UNKNOWN,
                    queue2);

            logger.info("8. verify CONNECTION_LOST, queue 1 suspends with cancel status");
            assertNotNull(session.waitConnectionLostEvent());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.CANCELED,
                    queue1);
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.CANCELED,
                    queue1);

            logger.info("9. verify queue reopens");
            assertNotNull(session.waitReconnectedEvent());

            verifyOpenRequest(server.nextClientRequest());
            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_REOPEN_RESULT,
                    OpenQueueResult.SUCCESS,
                    queue1);

            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_REOPEN_RESULT,
                    OpenQueueResult.SUCCESS,
                    queue2);

            assertNotNull(session.waitStateRestoredEvent());

            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            session.checkNoEvent();

            logger.info("10. configure queues as host health sensitive");
            server.pushSuccess(2);
            queue1.configureAsync(sensitiveOptions, SEQUENCE_TIMEOUT);
            queue2.configureAsync(sensitiveOptions, SEQUENCE_TIMEOUT);

            verifyConfigureRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("11. verify queues suspend");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.SUCCESS,
                    queue1);
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    queue1);

            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.SUCCESS,
                    queue2);
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    queue2);

            logger.info(
                    "12. configure queue 2 as no longer host health sensitive with bad response");
            server.pushItem(StatusCategory.E_UNKNOWN); // queue 2 bad resume response
            server.pushItem(StatusCategory.E_UNKNOWN); // queue 2 bad suspend response
            server.pushSuccess(); // OK for nego
            server.pushSuccess(2); // to reopen queues

            queue2.configureAsync(insensitiveOptions, SEQUENCE_TIMEOUT);

            logger.info("13. verify queue 2 resumes with bad status");
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.UNKNOWN,
                    queue2);
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.UNKNOWN,
                    queue2);

            logger.info("14. verify that queue 2 suspend with bad status");
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.UNKNOWN,
                    queue2);

            logger.info("15. verify CONNECTION_LOST and RECONNECTED events");
            assertNotNull(session.waitConnectionLostEvent());
            assertNotNull(session.waitReconnectedEvent());

            logger.info("16. verify queue reopens");
            verifyOpenRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_REOPEN_RESULT,
                    OpenQueueResult.SUCCESS,
                    queue1);

            verifyOpenRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_REOPEN_RESULT,
                    OpenQueueResult.SUCCESS,
                    queue2);

            assertNotNull(session.waitStateRestoredEvent());
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            session.checkNoEvent();

            logger.info("17. configure queue 2 as no longer host health sensitive (timeout)");
            server.pushItem(StatusCategory.E_TIMEOUT);
            queue2.configureAsync(insensitiveOptions, SEQUENCE_TIMEOUT);

            logger.info("18. set host health back to healthy");
            server.pushSuccess(1);
            monitor.setState(HostHealthState.Healthy);

            logger.info("19. verify that queue 1 resumes");
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.SUCCESS,
                    queue1);

            logger.info("20. verify that queue 2 resumes by timeout");
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.TIMEOUT,
                    queue2);
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.TIMEOUT,
                    queue2);

            logger.info("21. verify HOST_HEALTH_RESTORED event is issued");
            assertNotNull(session.waitHostHealthRestoredEvent());

            logger.info("22. configure queue 2 with standalone request as health insensitive");
            server.pushSuccess();
            queue2.configureAsync(insensitiveOptions, SEQUENCE_TIMEOUT);
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    queue2);

            session.checkNoEvent();
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            logger.info("23. stop session");
            server.pushSuccess(); // OK for disconnect
            session.stop(SHORT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("=============================================================");
            logger.info("END Testing SessionIT reconfigureHostHealthSensitivityErrors.");
            logger.info("=============================================================");
        }
    }

    private void verifyPutsAreAllowed(TestSession session, Queue queue) {
        // Make PUT without correlation Id in order to not generate ACK message
        PutMessage msg = makePut(queue, "msg", false);
        queue.post(msg);

        queue.pack(msg);
        queue.flush();
    }

    private void verifyPutsAreBlocked(TestSession session, Queue queue) {
        PutMessage msg = makePut(queue, "msg");

        try {
            queue.post(msg);
            fail("Should not get here");
        } catch (BMQException e) {
            assertEquals(GenericResult.UNKNOWN, e.code());
            assertEquals("The queue is in suspended state", e.getMessage());
        }

        try {
            queue.pack(msg);
            fail("Should not get here");
        } catch (BMQException e) {
            assertEquals(GenericResult.UNKNOWN, e.code());
            assertEquals("The queue is in suspended state", e.getMessage());
        }
    }

    /**
     * Critical test to check that PUT messages are blocked to post for suspended queue and
     * unblocked for resumed one.
     *
     * <ul>
     *   <li>start test server
     *   <li>create broker session
     *   <li>start broker session
     *   <li>create sensitive to host health queue
     *   <li>open the queue
     *   <li>verify PUTs can be posted
     *   <li>set host health to unhealthy
     *   <li>verify HOST_UNHEALTHY event (PUTs still can be posted)
     *   <li>verify QUEUE_SUSPENDED event (PUTs cannot be posted)
     *   <li>configure queue as host-health insensitive (async resume)
     *   <li>verify QUEUE_RESUMED event (PUTs can be posted)
     *   <li>configure queue as host-health sensitive (sync suspend)
     *   <li>verify PUTs cannot be posted
     *   <li>configure queue as host-health insensitive (sync resume)
     *   <li>verify PUTs can be posted
     *   <li>configure queue as host-health sensitive (async suspend)
     *   <li>verify QUEUE_SUSPENDED event (PUTs cannot be posted)
     *   <li>set host health back to healthy
     *   <li>verify QUEUE_RESUMED event (PUTs can be posted)
     *   <li>verify HOST_HEALTH_RESTORED event
     *   <li>stop and linger broker session and stop server
     * </ul>
     */
    @Test
    void putBlockUnblock() throws IOException {
        logger.info("========================================");
        logger.info("BEGIN Testing SessionIT putBlockUnblock.");
        logger.info("========================================");

        logger.info("1. start test server");
        BmqBroker broker = BmqBroker.createStartedBroker();

        logger.info("2. create broker session");
        TestMonitor monitor = new TestMonitor();
        SessionOptions opts =
                SessionOptions.builder()
                        .setBrokerUri(broker.sessionOptions().brokerUri())
                        .setHostHealthMonitor(monitor)
                        .build();
        TestSession session = new TestSession(opts);

        try {
            logger.info("3. start broker session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. sensitive to host health queue");
            Queue queue = session.getWriterQueue(createUniqueUri(), false);

            logger.info("5. open the queue");
            QueueOptions sensitiveOptions =
                    QueueOptions.builder().setSuspendsOnBadHostHealth(true).build();
            queue.open(sensitiveOptions, DEFAULT_TIMEOUT);

            logger.info("6. verify PUTs can be posted");
            verifyPutsAreAllowed(session, queue);

            logger.info("7. set host health to unhealthy");
            // Make session event handler to wait before processing next events
            session.setWaitToProcessNextEvents();

            monitor.setState(HostHealthState.Unhealthy);

            logger.info("8. verify HOST_UNHEALTHY event (PUTs still can be posted)");
            assertNotNull(session.waitHostUnhealthyEvent());
            verifyPutsAreAllowed(session, queue);

            // Let session event handler to process next events
            session.processNextEvents();

            logger.info("9. verify QUEUE_SUSPENDED event (PUTs cannot be posted)");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.SUCCESS,
                    queue);

            verifyPutsAreBlocked(session, queue);

            logger.info("10. configure queue as host-health insensitive (async resume)");
            QueueOptions insensitiveOptions =
                    QueueOptions.builder().setSuspendsOnBadHostHealth(false).build();

            // Make session event handler to wait before processing next events
            session.setWaitToProcessNextEvents();

            queue.configureAsync(insensitiveOptions, DEFAULT_TIMEOUT);

            logger.info("11. verify QUEUE_RESUMED event (PUTs can be posted)");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.SUCCESS,
                    queue);
            verifyPutsAreAllowed(session, queue);

            // Let session event handler to process next events
            session.processNextEvents();

            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    queue);

            logger.info("12. configure queue as host-health sensitive (sync suspend)");
            queue.configure(sensitiveOptions, DEFAULT_TIMEOUT);

            logger.info("13. verify PUTs cannot be posted");
            verifyPutsAreBlocked(session, queue);

            logger.info("14. configure queue as host-health insensitive (sync resume)");
            queue.configure(insensitiveOptions, DEFAULT_TIMEOUT);

            logger.info("15. verify PUTs can be posted");
            verifyPutsAreAllowed(session, queue);

            logger.info("16. configure queue as host-health sensitive (async suspend)");
            // Make session event handler to wait before processing next events
            session.setWaitToProcessNextEvents();

            queue.configureAsync(sensitiveOptions, DEFAULT_TIMEOUT);

            logger.info("17. verify QUEUE_SUSPENDED event (PUTs cannot be posted)");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.SUCCESS,
                    queue);
            verifyPutsAreBlocked(session, queue);

            // Let session event handler to process next events
            session.processNextEvents();

            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    queue);

            logger.info("18. set host health back to healthy");
            verifyPutsAreBlocked(session, queue);

            // Make session event handler to wait before processing next events
            session.setWaitToProcessNextEvents();

            monitor.setState(HostHealthState.Healthy);

            logger.info("19. verify QUEUE_RESUMED event (PUTs can be posted)");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ResultCodes.ConfigureQueueResult.SUCCESS,
                    queue);
            verifyPutsAreAllowed(session, queue);

            // Let session event handler to process next events
            session.processNextEvents();

            logger.info("20. verify HOST_HEALTH_RESTORED event");
            assertNotNull(session.waitHostHealthRestoredEvent());

            session.checkNoEvent();

            broker.setDropTmpFolder();
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            logger.info("21. stop session");
            session.stop(SHORT_TIMEOUT);
            session.linger();

            broker.close();

            logger.info("======================================");
            logger.info("END Testing SessionIT putBlockUnblock.");
            logger.info("======================================");
        }
    }

    /**
     * Critical test to check that channel is dropped when getting bad suspend response and is not
     * dropped when getting bad resume response.
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>open three host health sensitive queues
     * </ul>
     *
     * <p>Verify that bad suspend response drops the connection
     *
     * <ul>
     *   <li>set host health to unhealthy (make queue 2 to suspend with bad response)
     *   <li>verify that queue 1 suspends
     *   <li>verify that queue 2 suspends with bad response
     *   <li>verify CONNECTION_LOST, RECONNECTING events are issues
     *   <li>verify all queues are reopened and STATE_RESTORED is issued
     *   <li>set host health back to healthy (make queue 2 to resume with bad response)
     *   <li>verify that queue 1 resumes
     *   <li>verify that queue 2 resumes with bad response
     *   <li>verify that queue 3 resumes
     *   <li>verify HOST_HEALTH_RESTORED event is issued
     * </ul>
     *
     * <p>Verify that timeout suspend response drops the connection
     *
     * <ul>
     *   <li>set host health to unhealthy (make queue 2 and 3 suspend with timeout)
     *   <li>verify that queue 1 suspends
     *   <li>verify that queue 2 suspends with timeout
     *   <li>verify that queue 3 suspends with timeout
     *   <li>verify CONNECTION_LOST, RECONNECTING events are issues
     *   <li>verify all queues are reopened and STATE_RESTORED is issued
     *   <li>set host health back to healthy (make queue 3 to resume with timeout)
     *   <li>verify that queue 1 resumes
     *   <li>verify that queue 2 resumes
     *   <li>verify that queue 3 resumes with timeout
     *   <li>verify HOST_HEALTH_RESTORED event is issued
     * </ul>
     */
    @Test
    void errorHealthResponse() {
        logger.info("============================================");
        logger.info("BEGIN Testing SessionIT errorHealthResponse.");
        logger.info("============================================");

        final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(2);

        logger.info("1. start test server in manual mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_MANUAL_MODE);
        server.start();

        logger.info("2. create broker session");
        TestMonitor monitor = new TestMonitor();
        SessionOptions opts =
                SessionOptions.builder()
                        .setBrokerUri(server.getURI())
                        .setHostHealthMonitor(monitor)
                        .setConfigureQueueTimeout(Duration.ofSeconds(1))
                        .build();
        TestSession session = new TestSession(opts);

        try {
            server.pushSuccess(); // OK for nego
            logger.info("3. start broker session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. open three host health sensitive queues");
            Queue queue1 = session.getReaderQueue(createUniqueUri());
            Queue queue2 = session.getReaderQueue(createUniqueUri());
            Queue queue3 = session.getReaderQueue(createUniqueUri());

            QueueOptions sensitiveOptions =
                    QueueOptions.builder().setSuspendsOnBadHostHealth(true).build();

            server.pushSuccess(2);
            queue1.open(sensitiveOptions, SEQUENCE_TIMEOUT);
            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            server.pushSuccess(2);
            queue2.open(sensitiveOptions, SEQUENCE_TIMEOUT);
            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            server.pushSuccess(2);
            queue3.open(sensitiveOptions, SEQUENCE_TIMEOUT);
            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("*** Verify that bad suspend response drops the connection *** ");
            logger.info(
                    "5. set host health to unhealthy (make queue 2 to suspend with bad response)");
            server.pushSuccess(); // OK for queue 1
            server.pushItem(StatusCategory.E_UNKNOWN); // BAD for queue 2

            // Do not send response for queue 3 so its suspend request will be
            // canceled channel down
            server.pushItem(StatusCategory.E_TIMEOUT);

            // OK for nego
            server.pushItem(StatusCategory.E_SUCCESS);

            server.pushSuccess(3); // to reopen 3 queues

            monitor.setState(HostHealthState.Unhealthy);
            assertNotNull(session.waitHostUnhealthyEvent());

            logger.info("7. verify that queue 1 suspends");
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.SUCCESS,
                    queue1);

            logger.info("8. verify that queue 2 to suspends with bad response");
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.UNKNOWN,
                    queue2);

            logger.info(
                    "9. verify CONNECTION_LOST, queue 3 SUSPENDED and RECONNECTING events are issues");
            assertNotNull(session.waitConnectionLostEvent());

            verifyConfigureRequest(server.nextClientRequest()); // suspend for queue 3
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.CANCELED,
                    queue3);

            assertNotNull(session.waitReconnectedEvent());

            logger.info("10. verify all queues are reopened and STATE_RESTORED is issued");
            verifyOpenRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_REOPEN_RESULT,
                    OpenQueueResult.SUCCESS,
                    queue1);
            verifyOpenRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_REOPEN_RESULT,
                    OpenQueueResult.SUCCESS,
                    queue2);
            verifyOpenRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_REOPEN_RESULT,
                    OpenQueueResult.SUCCESS,
                    queue3);
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            assertNotNull(session.waitStateRestoredEvent());

            logger.info(
                    "11. set host health back to healthy (make queue 2 to resume with bad response)");
            server.pushSuccess(); // OK for queue 1
            server.pushItem(StatusCategory.E_UNKNOWN); // BAD for queue 2
            server.pushSuccess(); // OK for queue 3

            monitor.setState(HostHealthState.Healthy);

            logger.info("12. verify that queue 1 resumes");
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.SUCCESS,
                    queue1);

            logger.info("13. verify that queue 2 resumes with bad response");
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.UNKNOWN,
                    queue2);

            logger.info("14. verify that queue 3 resumes");
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.SUCCESS,
                    queue3);

            logger.info("15. verify HOST_HEALTH_RESTORED event is issued");
            assertNotNull(session.waitHostHealthRestoredEvent());

            session.checkNoEvent();
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            logger.info("*** Verify that timeout suspend response drops the connection *** ");
            logger.info(
                    "16. set host health to unhealthy (make queue 2 and 3 to suspend with timeout)");
            server.pushSuccess(); // OK for queue 1
            server.pushItem(StatusCategory.E_TIMEOUT); // nothing for queue 2
            server.pushItem(StatusCategory.E_TIMEOUT); // nothing for queue 3

            // OK for nego
            server.pushItem(StatusCategory.E_SUCCESS);

            server.pushSuccess(3); // to reopen 3 queues

            monitor.setState(HostHealthState.Unhealthy);
            assertNotNull(session.waitHostUnhealthyEvent());

            logger.info("17. verify that queue 1 suspends");
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.SUCCESS,
                    queue1);

            logger.info("18. verify that queue 2 to suspends with timeout");
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.TIMEOUT,
                    queue2);

            logger.info("19. verify that queue 3 to suspends with timeout");
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.TIMEOUT,
                    queue3);

            logger.info("20. verify CONNECTION_LOST, RECONNECTING events are issues");
            assertNotNull(session.waitConnectionLostEvent());
            assertNotNull(session.waitReconnectedEvent());

            logger.info("21. verify all queues are reopened and STATE_RESTORED is issued");
            verifyOpenRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_REOPEN_RESULT,
                    OpenQueueResult.SUCCESS,
                    queue1);
            verifyOpenRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_REOPEN_RESULT,
                    OpenQueueResult.SUCCESS,
                    queue2);
            verifyOpenRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_REOPEN_RESULT,
                    OpenQueueResult.SUCCESS,
                    queue3);
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            assertNotNull(session.waitStateRestoredEvent());

            logger.info(
                    "22. set host health back to healthy (make queue 3 to resume with timeout)");
            server.pushSuccess(); // OK for queue 1
            server.pushSuccess(); // OK for queue 2
            server.pushItem(StatusCategory.E_TIMEOUT); // no response for queue 3

            monitor.setState(HostHealthState.Healthy);

            logger.info("23. verify that queue 1 resumes");
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.SUCCESS,
                    queue1);

            logger.info("24. verify that queue 2 resumes");
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.SUCCESS,
                    queue2);

            logger.info("25. verify that queue 3 resumes with timeout");
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.TIMEOUT,
                    queue3);

            logger.info("26. verify HOST_HEALTH_RESTORED event is issued");
            assertNotNull(session.waitHostHealthRestoredEvent());

            session.checkNoEvent();
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            logger.info("27. stop session");
            server.pushSuccess(); // OK for disconnect
            session.stop(SHORT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("==========================================");
            logger.info("END Testing SessionIT errorHealthResponse.");
            logger.info("==========================================");
        }
    }

    /**
     * Critical test to check that suspend and resume requests are deferred if there is a pending
     * standalone configure request.
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>open host health sensitive queue
     * </ul>
     *
     * <p>Verify suspend request sent after standalone response
     *
     * <ul>
     *   <li>configure async, delay response
     *   <li>set host health to unhealthy
     *   <li>verify queue defer to suspend
     *   <li>verify standalone response
     *   <li>verify queue suspends
     *   <li>set host health back to healthy
     *   <li>verify queue resumes
     *   <li>verify HOST_HEALTH_RESTORED event is issued
     * </ul>
     *
     * <p>Verify resume request is not sent after standalone response
     *
     * <ul>
     *   <li>configure async, delay response
     *   <li>set host health to unhealthy
     *   <li>set host health back to healthy
     *   <li>HOST_HEALTH_RESTORED event is issued
     *   <li>verify standalone response
     *   <li>verify queue doesn't send resume request
     * </ul>
     */
    @Test
    void deferHealthRequestOnPendingStandalone() {
        logger.info("==============================================================");
        logger.info("BEGIN Testing SessionIT deferHealthRequestOnPendingStandalone.");
        logger.info("==============================================================");

        final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(2);

        logger.info("1. start test server in manual mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_MANUAL_MODE);
        server.start();

        logger.info("2. create broker session");
        TestMonitor monitor = new TestMonitor();
        SessionOptions opts =
                SessionOptions.builder()
                        .setBrokerUri(server.getURI())
                        .setHostHealthMonitor(monitor)
                        .build();
        TestSession session = new TestSession(opts);

        try {
            server.pushSuccess(); // OK for nego
            logger.info("3. start broker session");
            session.start(DEFAULT_TIMEOUT);

            logger.info("4. open host health sensitive queue");
            Queue queue = session.getReaderQueue(createUniqueUri());

            QueueOptions sensitiveOptions =
                    QueueOptions.builder().setSuspendsOnBadHostHealth(true).build();

            server.pushSuccess(2);
            queue.open(sensitiveOptions, SEQUENCE_TIMEOUT);
            verifyOpenRequest(server.nextClientRequest());
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("*** Verify suspend request sent after standalone response *** ");
            logger.info("5. configure async, delay response");
            // Delay standalone configure response
            server.pushItem(StatusCategory.E_SUCCESS, Duration.ofSeconds(1));
            queue.configureAsync(sensitiveOptions, SEQUENCE_TIMEOUT);
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("6. set host health to unhealthy");
            monitor.setState(HostHealthState.Unhealthy);
            assertNotNull(session.waitHostUnhealthyEvent());

            logger.info("7. verify queue defer to suspend");
            server.pushSuccess(); // OK for queue suspend
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            logger.info("8. verify standalone response");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    queue);

            logger.info("9. verify queue suspends");
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_SUSPENDED,
                    ConfigureQueueResult.SUCCESS,
                    queue);

            logger.info("10. set host health back to healthy");
            server.pushSuccess(); // OK for queue resume
            monitor.setState(HostHealthState.Healthy);

            logger.info("11. verify queue resumes");
            verifyConfigureRequest(server.nextClientRequest());
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_RESUMED,
                    ConfigureQueueResult.SUCCESS,
                    queue);

            logger.info("12. verify HOST_HEALTH_RESTORED event is issued");
            assertNotNull(session.waitHostHealthRestoredEvent());

            session.checkNoEvent();
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            logger.info("*** Verify resume request is not sent after standalone response *** ");
            logger.info("13. configure async, delay response");
            // Delay standalone configure response
            server.pushItem(StatusCategory.E_SUCCESS, Duration.ofSeconds(1));
            queue.configureAsync(sensitiveOptions, SEQUENCE_TIMEOUT);
            verifyConfigureRequest(server.nextClientRequest());

            logger.info("14. set host health to unhealthy");
            monitor.setState(HostHealthState.Unhealthy);
            assertNotNull(session.waitHostUnhealthyEvent());

            logger.info("15. set host health back to healthy");
            server.pushSuccess(); // OK for queue resume
            monitor.setState(HostHealthState.Healthy);

            logger.info("16. verify HOST_HEALTH_RESTORED event is issued");
            assertNotNull(session.waitHostHealthRestoredEvent());

            logger.info("17. verify standalone response");
            verifyQueueControlEvent(
                    session.waitQueueEvent(),
                    QueueControlEvent.Type.QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    queue);

            logger.info("18. verify queue doesn't send resume request");
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            session.checkNoEvent();
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            logger.info("19. stop session");
            server.pushSuccess(); // OK for disconnect
            session.stop(SHORT_TIMEOUT);
            session.linger();
            server.stop();

            logger.info("============================================================");
            logger.info("END Testing SessionIT deferHealthRequestOnPendingStandalone.");
            logger.info("============================================================");
        }
    }
}
