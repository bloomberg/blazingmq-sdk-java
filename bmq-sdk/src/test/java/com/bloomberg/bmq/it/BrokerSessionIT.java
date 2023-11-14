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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bloomberg.bmq.BMQException;
import com.bloomberg.bmq.HostHealthMonitor;
import com.bloomberg.bmq.HostHealthState;
import com.bloomberg.bmq.MessageGUID;
import com.bloomberg.bmq.QueueFlags;
import com.bloomberg.bmq.QueueOptions;
import com.bloomberg.bmq.ResultCodes;
import com.bloomberg.bmq.ResultCodes.CloseQueueCode;
import com.bloomberg.bmq.ResultCodes.CloseQueueResult;
import com.bloomberg.bmq.ResultCodes.ConfigureQueueCode;
import com.bloomberg.bmq.ResultCodes.ConfigureQueueResult;
import com.bloomberg.bmq.ResultCodes.GenericResult;
import com.bloomberg.bmq.ResultCodes.OpenQueueCode;
import com.bloomberg.bmq.ResultCodes.OpenQueueResult;
import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.Uri;
import com.bloomberg.bmq.impl.BmqFuture;
import com.bloomberg.bmq.impl.BrokerSession;
import com.bloomberg.bmq.impl.CorrelationIdImpl;
import com.bloomberg.bmq.impl.QueueId;
import com.bloomberg.bmq.impl.QueueImpl;
import com.bloomberg.bmq.impl.QueueStateManager;
import com.bloomberg.bmq.impl.events.AckMessageEvent;
import com.bloomberg.bmq.impl.events.BrokerSessionEvent;
import com.bloomberg.bmq.impl.events.Event;
import com.bloomberg.bmq.impl.events.PushMessageEvent;
import com.bloomberg.bmq.impl.events.QueueControlEvent;
import com.bloomberg.bmq.impl.events.QueueControlEventHandler;
import com.bloomberg.bmq.impl.infr.msg.ConfigureStream;
import com.bloomberg.bmq.impl.infr.msg.ConsumerInfo;
import com.bloomberg.bmq.impl.infr.msg.ControlMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.OpenQueue;
import com.bloomberg.bmq.impl.infr.msg.QueueHandleParameters;
import com.bloomberg.bmq.impl.infr.msg.StatusCategory;
import com.bloomberg.bmq.impl.infr.msg.StreamParameters;
import com.bloomberg.bmq.impl.infr.net.NettyTcpConnectionFactory;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnectionFactory;
import com.bloomberg.bmq.impl.infr.proto.PushMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.RequestManager;
import com.bloomberg.bmq.impl.intf.EventHandler;
import com.bloomberg.bmq.impl.intf.QueueHandle;
import com.bloomberg.bmq.impl.intf.QueueState;
import com.bloomberg.bmq.it.util.BmqBroker;
import com.bloomberg.bmq.it.util.BmqBrokerSimulator;
import com.bloomberg.bmq.it.util.BmqBrokerSimulator.Mode;
import com.bloomberg.bmq.it.util.TestTools;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerSessionIT {
    static final Duration TEST_REQUEST_TIMEOUT = Duration.ofSeconds(15);
    static final Duration TEST_FUTURE_TIMEOUT = TEST_REQUEST_TIMEOUT.plus(TEST_REQUEST_TIMEOUT);
    static final int NO_CLIENT_REQUEST_TIMEOUT = 1; // sec

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static Uri createUri() {
        return BmqBroker.Domains.Priority.generateQueueUri();
    }

    private static QueueImpl createQueue(BrokerSession session, Uri uri, long flags) {
        return createQueue(session, uri, flags, null);
    }

    private static QueueImpl createQueue(
            BrokerSession session, Uri uri, long flags, QueueControlEventHandler handler) {
        return new QueueImpl(session, uri, flags, handler, null, null);
    }

    private static void verifyDisconnectRequest(int id, ControlMessageChoice request) {
        logger.info("Verify disconnect request: {}", request);

        assertNotNull(request);
        assertEquals(id, request.id());
        assertTrue(request.isDisconnectValue());
    }

    private static void verifyOpenRequest(int id, ControlMessageChoice request) {
        logger.info("Verify open request: {}", request);

        assertNotNull(request);
        assertEquals(id, request.id());
        assertTrue(request.isOpenQueueValue());

        // TODO: verify queue handle parameters
    }

    private static void verifyConfigureRequest(int id, ControlMessageChoice request) {
        verifyConfigureRequest(id, request, null);
    }

    private static void verifyConfigureRequest(
            int id, ControlMessageChoice request, QueueOptions options) {
        logger.info("Verify configure request: {}", request);

        assertNotNull(request);
        assertEquals(id, request.id());
        assertTrue(request.isConfigureStreamValue());

        // Verify stream parameters
        if (options != null) {
            ConsumerInfo info =
                    TestTools.getDefaultConsumerInfo(request.configureStream().streamParameters());

            QueueOptions.Builder builder = QueueOptions.builder().merge(options);

            // Default ConsumerInfo might be missing for deconfigure request, when there are no
            // subscriptions.
            if (info != null) {
                builder.setMaxUnconfirmedMessages(info.maxUnconfirmedMessages())
                        .setMaxUnconfirmedBytes(info.maxUnconfirmedBytes())
                        .setConsumerPriority(info.consumerPriority());
            }

            assertEquals(options, builder.build());
        }
    }

    private static void verifyCloseRequest(
            int id, ControlMessageChoice request, QueueHandleParameters parameters) {

        verifyCloseRequest(id, request, parameters, true);
    }

    private static void verifyCloseRequest(
            int id,
            ControlMessageChoice request,
            QueueHandleParameters parameters,
            boolean isFinal) {
        logger.info("Verify close request: {}", request);

        assertNotNull(request);
        assertEquals(id, request.id());
        assertTrue(request.isCloseQueueValue());
        assertEquals(isFinal, request.closeQueue().isFinal());

        QueueHandleParameters requestParameters = request.closeQueue().getHandleParameters();

        TestTools.assertQueueHandleParamsAreEqual(parameters, requestParameters);
    }

    private static void verifyQueueControlEvent(
            Event event,
            QueueControlEvent.Type eventType,
            ResultCodes.GenericCode status,
            QueueHandle queue) {
        assertNotNull(event);
        assertTrue(
                "Expected 'QueueControlEvent', received '"
                        + event.getClass().getName()
                        + "': "
                        + event,
                event instanceof QueueControlEvent);

        QueueControlEvent queueControlEvent = (QueueControlEvent) event;
        assertEquals(eventType, queueControlEvent.getEventType());
        assertEquals(status, queueControlEvent.getStatus());
        assertEquals(queue, queueControlEvent.getQueue());
    }

    private static void verifyBrokerSessionEvent(Event event, BrokerSessionEvent.Type eventType) {
        assertNotNull(event);
        assertTrue(
                "Expected 'BrokerSessionEvent', received '"
                        + event.getClass().getName()
                        + "': "
                        + event,
                event instanceof BrokerSessionEvent);

        BrokerSessionEvent brokerSessionEvent = (BrokerSessionEvent) event;
        assertEquals(eventType, brokerSessionEvent.getEventType());
    }

    private static QueueOptions mergeOptions(QueueOptions... optionsArray) {
        QueueOptions.Builder builder = QueueOptions.builder();

        for (QueueOptions options : optionsArray) {
            builder.merge(options);
        }

        return builder.build();
    }

    /**
     * Test for asynchronous operations under queue (open-configure-close);
     *
     * <ul>
     *   <li>start test server in active mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>create queue
     *   <li>open queue asynchronously
     *   <li>check that proper event was handled by user-defined user handler
     *   <li>repeat previous 2 steps for configure queue and close queue
     *   <li>stop broker session
     * </ul>
     */
    @Test
    public void openConfigureCloseQueueAsyncTest() {
        logger.info("=================================================================");
        logger.info("BEGIN Testing BrokerSessionIT openConfigureCloseQueueAsyncTest.");
        logger.info("=================================================================");

        final Semaphore testSema = new Semaphore(0);
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_AUTO_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        EventHandler eventHandler =
                (Event event) -> {
                    events.push(event);
                    testSema.release();
                };
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, eventHandler);

        QueueStateManager queueManager =
                (QueueStateManager) TestTools.getInternalState(session, "queueStateManager");
        assertNotNull(queueManager);

        int reqId = 0;
        try {
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));
            QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();

            long flags = 0;
            flags = QueueFlags.setReader(flags);

            QueueImpl queue = createQueue(session, createUri(), flags);

            assertEquals(QueueState.e_CLOSED, queue.getState());
            // Check substreamcount == 0
            assertEquals(0, queueManager.getSubStreamCount(queue));
            assertEquals(
                    OpenQueueResult.SUCCESS,
                    queue.openAsync(queueOptions, TEST_REQUEST_TIMEOUT).get(TEST_FUTURE_TIMEOUT));
            // Check substreamcount == 1 after sending open queue request
            assertEquals(1, queueManager.getSubStreamCount(queue));

            TestTools.acquireSema(testSema);
            assertEquals(QueueState.e_OPENED, queue.getState());
            assertEquals(1, events.size());
            Event event = events.pollLast();
            assertNotNull(event);
            assertEquals(event.getClass(), QueueControlEvent.class);
            QueueControlEvent queueControlEvent = (QueueControlEvent) event;
            assertEquals(
                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT, queueControlEvent.getEventType());
            assertEquals(OpenQueueResult.SUCCESS, queueControlEvent.getStatus());

            assertEquals(
                    ConfigureQueueResult.SUCCESS,
                    queue.configureAsync(queueOptions, TEST_REQUEST_TIMEOUT)
                            .get(TEST_FUTURE_TIMEOUT));
            // Check substreamcount hasn't changed
            assertEquals(1, queueManager.getSubStreamCount(queue));

            TestTools.acquireSema(testSema);
            assertEquals(1, events.size());
            event = events.pollLast();
            assertNotNull(event);
            assertEquals(event.getClass(), QueueControlEvent.class);
            queueControlEvent = (QueueControlEvent) event;
            assertEquals(
                    QueueControlEvent.Type.e_QUEUE_CONFIGURE_RESULT,
                    queueControlEvent.getEventType());
            assertEquals(ConfigureQueueResult.SUCCESS, queueControlEvent.getStatus());

            assertEquals(
                    CloseQueueResult.SUCCESS,
                    queue.closeAsync(TEST_REQUEST_TIMEOUT).get(TEST_FUTURE_TIMEOUT));
            // Substreamcount should decrement back to zero
            assertEquals(0, queueManager.getSubStreamCount(queue));

            TestTools.acquireSema(testSema);
            assertEquals(QueueState.e_CLOSED, queue.getState());
            assertEquals(1, events.size());
            event = events.pollLast();
            assertNotNull(event);
            assertEquals(event.getClass(), QueueControlEvent.class);
            queueControlEvent = (QueueControlEvent) event;
            assertEquals(
                    QueueControlEvent.Type.e_QUEUE_CLOSE_RESULT, queueControlEvent.getEventType());
            assertEquals(CloseQueueResult.SUCCESS, queueControlEvent.getStatus());

            // Verify requests
            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(++reqId, server.nextClientRequest(), queue.getParameters());
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());
            session.linger();
            server.stop();

            logger.info("===============================================================");
            logger.info("END Testing BrokerSessionIT openConfigureCloseQueueAsyncTest.");
            logger.info("===============================================================");
        }
    }

    /**
     * Test for synchronous operations under queue (open-configure-close);
     *
     * <ul>
     *   <li>start test server in active mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>create queue
     *   <li>open queue synchronously
     *   <li>check that proper result code was returned
     *   <li>repeat previous 2 steps for configure queue and close queue
     *   <li>check that no single event was generated
     *   <li>stop broker session
     * </ul>
     */
    @Test
    public void openConfigureCloseQueueTest() {
        logger.info("============================================================");
        logger.info("BEGIN Testing BrokerSessionIT openConfigureCloseQueueTest.");
        logger.info("============================================================");

        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_AUTO_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();

        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, events::push);
        QueueStateManager queueManager =
                (QueueStateManager) TestTools.getInternalState(session, "queueStateManager");
        assertNotNull(queueManager);

        int reqId = 0;

        try {
            assertEquals(GenericResult.SUCCESS, session.start(Duration.ofSeconds(5)));

            long flags = 0;
            flags = QueueFlags.setReader(flags);
            QueueImpl queue = createQueue(session, createUri(), flags);

            QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();

            assertEquals(QueueState.e_CLOSED, queue.getState());
            assertNull(session.lookupQueue(queue.getUri()));
            assertNull(session.lookupQueue(queue.getFullQueueId()));

            // Check substreamcount == 0
            assertEquals(0, queueManager.getSubStreamCount(queue));

            assertEquals(OpenQueueResult.SUCCESS, queue.open(queueOptions, TEST_REQUEST_TIMEOUT));
            // Check substreamcount == 1 after sending open queue request
            assertEquals(1, queueManager.getSubStreamCount(queue));

            QueueId queueKey = queue.getFullQueueId();
            assertEquals(QueueState.e_OPENED, queue.getState());
            assertEquals(queue, session.lookupQueue(queue.getUri()));
            assertEquals(queue, session.lookupQueue(queueKey));

            assertEquals(
                    ConfigureQueueResult.SUCCESS,
                    queue.configure(queueOptions, TEST_REQUEST_TIMEOUT));
            assertEquals(QueueState.e_OPENED, queue.getState());

            // Check substreamcount hasn't changed
            assertEquals(1, queueManager.getSubStreamCount(queue));

            assertEquals(CloseQueueResult.SUCCESS, queue.close(TEST_REQUEST_TIMEOUT));
            assertEquals(QueueState.e_CLOSED, queue.getState());
            assertNull(session.lookupQueue(queue.getUri()));
            assertNull(session.lookupQueue(queueKey));

            // Substreamcount should decrement back to zero
            assertEquals(0, queueManager.getSubStreamCount(queue));

            // Verify requests
            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(++reqId, server.nextClientRequest(), queue.getParameters());
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());
            session.linger();
            server.stop();

            logger.info("==========================================================");
            logger.info("END Testing BrokerSessionIT openConfigureCloseQueueTest.");
            logger.info("==========================================================");
        }
    }

    /**
     * Test for receiving PUSH message.
     *
     * <ul>
     *   <li>start test server in active mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>create queue
     *   <li>open queue synchronously
     *   <li>check that proper result code was returned
     *   <li>Send PUSH message from the server
     *   <li>Check that corresponding PUSH event was generated by Broker session
     *   <li>stop broker session
     * </ul>
     */
    @Test
    public void pushMessageTest() throws IOException {
        logger.info("============================================================");
        logger.info("BEGIN Testing BrokerSessionIT openConfigureCloseQueueTest.");
        logger.info("============================================================");

        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_AUTO_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();

        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, events::push);

        int reqId = 0;

        try {
            assertEquals(GenericResult.SUCCESS, session.start(Duration.ofSeconds(1)));
            long flags = 0;
            flags = QueueFlags.setReader(flags);
            QueueHandle queue = createQueue(session, createUri(), flags);

            QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();

            assertEquals(QueueState.e_CLOSED, queue.getState());
            assertEquals(OpenQueueResult.SUCCESS, queue.open(queueOptions, TEST_REQUEST_TIMEOUT));
            assertEquals(QueueState.e_OPENED, queue.getState());
            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            server.writePushRequest(1);

            assertEquals(CloseQueueResult.SUCCESS, queue.close(TEST_REQUEST_TIMEOUT));
            assertEquals(QueueState.e_CLOSED, queue.getState());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(++reqId, server.nextClientRequest(), queue.getParameters());

            assertEquals(1, events.size());
            Event event = events.poll();
            assertEquals(PushMessageEvent.TYPE_ID, event.getDispatchId());
            PushMessageEvent pushEvent = (PushMessageEvent) event;
            PushMessageImpl message = pushEvent.rawMessage();
            assertEquals(1, message.queueId());
            assertEquals(
                    MessageGUID.fromHex("ABCDEF0123456789ABCDEF0123456789"), message.messageGUID());
        } finally {
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());
            session.linger();
            server.stop();

            logger.info("==========================================================");
            logger.info("END Testing BrokerSessionIT openConfigureCloseQueueTest.");
            logger.info("==========================================================");
        }
    }

    /**
     * Critical test for queue fsm logic configure request in OPENING queue state
     *
     * <ul>
     *   <li>start test server in active mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>create queue
     *   <li>initiate open queue operation asynchronously
     *   <li>attempt to initiate configure queue operation asynchronously
     *   <li>check that return code was INVALID_QUEUE
     *   <li>stop broker session
     * </ul>
     */
    @Test
    public void queueConfigureNotSupported() {
        logger.info("===========================================================");
        logger.info("BEGIN Testing BrokerSessionIT queueConfigureNotSupported.");
        logger.info("===========================================================");

        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_AUTO_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();

        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, events::push);

        int reqId = 0;

        try {
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));
            QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();
            long flags = QueueFlags.setReader(0);
            QueueHandle queue = createQueue(session, createUri(), flags);

            assertEquals(QueueState.e_CLOSED, queue.getState());

            queue.openAsync(queueOptions, TEST_REQUEST_TIMEOUT);

            assertEquals(
                    ConfigureQueueResult.INVALID_QUEUE,
                    queue.configureAsync(queueOptions, TEST_REQUEST_TIMEOUT)
                            .get(TEST_FUTURE_TIMEOUT));

            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } finally {
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());
            session.linger();
            server.stop();

            logger.info("=========================================================");
            logger.info("END Testing BrokerSessionIT queueConfigureNotSupported.");
            logger.info("=========================================================");
        }
    }

    /**
     * Critical test for queue fsm logic open request in OPENING queue state
     *
     * <ul>
     *   <li>start test server in active mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>create queue
     *   <li>initiate open queue operation asynchronously
     *   <li>attempt to open configure queue operation asynchronously
     *   <li>check that return code was ALREADY_IN_PROGRESS
     *   <li>stop broker session
     * </ul>
     */
    @Test
    public void queueOpenAlreadyInProgress() {
        logger.info("===========================================================");
        logger.info("BEGIN Testing BrokerSessionIT queueOpenAlreadyInProgress.");
        logger.info("===========================================================");

        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_AUTO_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, events::push);

        int reqId = 0;

        try {
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));
            QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();
            long flags = QueueFlags.setReader(0);

            QueueHandle queue = createQueue(session, createUri(), flags);

            queue.openAsync(queueOptions, TEST_REQUEST_TIMEOUT);
            assertEquals(
                    OpenQueueResult.ALREADY_IN_PROGRESS,
                    queue.openAsync(queueOptions, TEST_REQUEST_TIMEOUT).get(TEST_FUTURE_TIMEOUT));

            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } finally {
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());
            session.linger();
            server.stop();

            logger.info("=========================================================");
            logger.info("END Testing BrokerSessionIT queueOpenAlreadyInProgress.");
            logger.info("=========================================================");
        }
    }

    /**
     * Critical test for queue fsm logic configure request in OPENING queue state
     *
     * <ul>
     *   <li>start test server in silent mode
     *   <li>create broker session
     *   <li>attempt to start broker session
     *   <li>check for proper return code (TIMEOUT)
     *   <li>check that no single event was generated
     * </ul>
     */
    @Test
    public void startSessionTimeoutTest() {

        logger.info("========================================================");
        logger.info("BEGIN Testing BrokerSessionIT startSessionTimeoutTest.");
        logger.info("========================================================");

        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.SILENT_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, events::push);
        try {
            assertEquals(GenericResult.TIMEOUT, session.start(Duration.ofSeconds(5)));

            assertEquals(0, events.size());
        } finally {
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            session.linger();
            server.stop();

            logger.info("======================================================");
            logger.info("END Testing BrokerSessionIT startSessionTimeoutTest.");
            logger.info("======================================================");
        }
    }

    /**
     * Critical test to verify timeout fsm logic for open request
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>attempt to open queue
     *   <li>check for proper return code (TIMEOUT)
     *   <li>check that no single event was generated
     * </ul>
     */
    @Test
    public void openQueueTimeoutTest() {

        logger.info("===================================================");
        logger.info("BEGIN Testing BrokerSessionIT openQueueTimeoutTest.");
        logger.info("===================================================");

        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

        LinkedList<Event> events = new LinkedList<>();
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, events::push);

        QueueStateManager queueManager =
                (QueueStateManager) TestTools.getInternalState(session, "queueStateManager");
        assertNotNull(queueManager);

        server.pushItem(StatusCategory.E_SUCCESS); // Ok for nego request

        int reqId = 0;

        try {
            assertEquals(GenericResult.SUCCESS, session.start(Duration.ofSeconds(5)));
            long flags = 0;
            flags = QueueFlags.setReader(flags);
            QueueImpl queue = createQueue(session, createUri(), flags);
            QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();

            final Duration REQUEST_DELAY = Duration.ofSeconds(2);
            final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(1);

            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY);
            // openQueue response is OK but out of time
            server.pushSuccess(); // OK for close queue request

            assertEquals(0, queueManager.getSubStreamCount(queue));
            assertEquals(QueueState.e_CLOSED, queue.getState());
            assertEquals(OpenQueueResult.TIMEOUT, queue.open(queueOptions, SEQUENCE_TIMEOUT));

            // === Test timeline: ~1 sec after start
            assertEquals(QueueState.e_CLOSED, queue.getState());
            assertEquals(0, events.size());

            // Check substreamcount == 1 after sending open request and has not
            // decremented after sequence timeout expired
            assertEquals(1, queueManager.getSubStreamCount(queue));

            // Wait for two seconds
            TestTools.sleepForSeconds(2);

            // === Test timeline: ~3 sec after start
            // Check substreamcount == 0 after handling late open queue
            // response and sending close queue request
            assertEquals(0, queueManager.getSubStreamCount(queue));

            // Verify requests
            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(++reqId, server.nextClientRequest(), queue.getParameters());

            // Verify there is no QUEUE_CLOSE_RESULT event
            assertEquals(0, events.size());
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            server.pushItem(StatusCategory.E_SUCCESS);
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());
            session.linger();
            server.stop();

            logger.info("=================================================");
            logger.info("END Testing BrokerSessionIT openQueueTimeoutTest.");
            logger.info("=================================================");
        }
    }

    /**
     * Critical test to verify timeout fsm logic for configure request during opening queue state
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>attempt to open queue
     *   <li>check for proper return code (TIMEOUT)
     *   <li>check that no single event was generated
     * </ul>
     */
    @Test
    public void openQueueConfigureTimeoutTest() {

        logger.info("============================================================");
        logger.info("BEGIN Testing BrokerSessionIT openQueueConfigureTimeoutTest.");
        logger.info("============================================================");

        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

        LinkedList<Event> events = new LinkedList<>();
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, events::push);

        QueueStateManager queueManager =
                (QueueStateManager) TestTools.getInternalState(session, "queueStateManager");
        assertNotNull(queueManager);

        server.pushItem(StatusCategory.E_SUCCESS); // Ok for nego request

        int reqId = 0;

        try {
            assertEquals(GenericResult.SUCCESS, session.start(Duration.ofSeconds(5)));
            long flags = 0;
            flags = QueueFlags.setReader(flags);
            QueueImpl queue = createQueue(session, createUri(), flags);
            QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();

            final Duration REQUEST_DELAY = Duration.ofSeconds(2);
            final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(1);

            server.pushItem(StatusCategory.E_SUCCESS); // openQueue is OK
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY);
            // configure response is OK but out of time
            server.pushSuccess(2);
            // OK for close configure and close queue requests

            assertEquals(0, queueManager.getSubStreamCount(queue));
            assertEquals(QueueState.e_CLOSED, queue.getState());
            assertEquals(OpenQueueResult.TIMEOUT, queue.open(queueOptions, SEQUENCE_TIMEOUT));

            // === Test timeline: ~1 sec after start
            assertEquals(QueueState.e_CLOSED, queue.getState());
            assertEquals(0, events.size());

            // Check substreamcount == 1 after sending open request and has not
            // decremented after sequence timeout expired
            assertEquals(1, queueManager.getSubStreamCount(queue));

            // Wait for two seconds
            TestTools.sleepForSeconds(2);

            // === Test timeline: ~3 sec after start
            // Check substreamcount == 0 after handling late open configure
            // response and sending close queue request
            assertEquals(0, queueManager.getSubStreamCount(queue));

            // Verify requests
            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(++reqId, server.nextClientRequest(), queue.getParameters());

            // Verify there are no events
            assertEquals(0, events.size());
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            server.pushItem(StatusCategory.E_SUCCESS);
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());
            session.linger();
            server.stop();

            logger.info("==========================================================");
            logger.info("END Testing BrokerSessionIT openQueueConfigureTimeoutTest.");
            logger.info("==========================================================");
        }
    }

    /**
     * Critical test to verify fsm logic when configure request is canceled during opening queue
     * state
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>attempt to open queue
     *   <li>ensure open and configure requests have been sent
     *   <li>cancel all pending requests
     *   <li>check for proper return code (CANCELED)
     *   <li>check queue state is closed
     *   <li>check that canceled event was generated
     *   <li>check close request hasn't been sent
     *   <li>stop the session and the server
     * </ul>
     */
    @Test
    public void openQueueConfigureCanceledTest() {

        logger.info("=============================================================");
        logger.info("BEGIN Testing BrokerSessionIT openQueueConfigureCanceledTest.");
        logger.info("=============================================================");

        final Semaphore testSema = new Semaphore(0);
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

        LinkedList<Event> events = new LinkedList<>();
        EventHandler eventHandler =
                (Event event) -> {
                    events.push(event);
                    testSema.release();
                };
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, eventHandler);

        QueueStateManager queueManager =
                (QueueStateManager) TestTools.getInternalState(session, "queueStateManager");
        assertNotNull(queueManager);
        RequestManager requestManager =
                (RequestManager) TestTools.getInternalState(session, "requestManager");
        assertNotNull(requestManager);

        server.pushItem(StatusCategory.E_SUCCESS); // Ok for nego request

        int reqId = 0;

        try {
            assertEquals(GenericResult.SUCCESS, session.start(Duration.ofSeconds(5)));
            long flags = 0;
            flags = QueueFlags.setReader(flags);
            QueueImpl queue = createQueue(session, createUri(), flags);
            QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();

            assertNull(session.lookupQueue(queue.getFullQueueId()));
            assertNull(session.lookupQueue(queue.getUri()));

            assertEquals(0, queueManager.getSubStreamCount(queue));
            assertEquals(QueueState.e_CLOSED, queue.getState());

            server.pushItem(StatusCategory.E_SUCCESS); // openQueue is OK
            // No response for configure request which will be canceled later

            BmqFuture<OpenQueueCode> openFuture =
                    queue.openAsync(queueOptions, TEST_REQUEST_TIMEOUT);

            // Verify requests
            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            // Check substreamcount == 1 after sending open request
            assertEquals(1, queueManager.getSubStreamCount(queue));

            // Cancel all request
            // NOTE: must be executed in the context of the scheduler
            service.execute(requestManager::cancelAllRequests);

            TestTools.acquireSema(testSema);

            // Queue should close
            assertEquals(QueueState.e_CLOSED, queue.getState());

            assertEquals(1, events.size());
            Event event = events.poll();
            assertEquals(QueueControlEvent.TYPE_ID, event.getDispatchId());
            QueueControlEvent queueControlEvent = (QueueControlEvent) event;
            assertEquals(
                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT, queueControlEvent.getEventType());
            assertEquals(OpenQueueResult.CANCELED, queueControlEvent.getStatus());

            assertEquals(OpenQueueResult.CANCELED, openFuture.get(TEST_FUTURE_TIMEOUT));

            // Substreamcount should decrement
            assertEquals(0, queueManager.getSubStreamCount(queue));

            // Close request shouldn't be sent
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            server.pushItem(StatusCategory.E_SUCCESS);
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());

            session.linger();
            server.stop();

            logger.info("===========================================================");
            logger.info("END Testing BrokerSessionIT openQueueConfigureCanceledTest.");
            logger.info("===========================================================");
        }
    }

    /**
     * Critical test to verify timeout fsm logic for configure request during closing queue state
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>check queue state is e_CLOSED
     *   <li>check that look up method returns null
     *   <li>open queue
     *   <li>check queue state is e_OPENED
     *   <li>check that look up method returns proper queue handle
     *   <li>attempt to close queue
     *   <li>check that single queue control event was generated
     *   <li>check queue state is e_CLOSED
     *   <li>stop broker session
     * </ul>
     */
    @Test
    public void closeQueueConfigurationFailedTest() {
        logger.info("==================================================================");
        logger.info("BEGIN Testing BrokerSessionIT closeQueueConfigurationFailedTest.");
        logger.info("==================================================================");

        final Semaphore testSema = new Semaphore(0);
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        EventHandler eventHandler =
                (Event event) -> {
                    events.push(event);
                    testSema.release();
                };

        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, eventHandler);

        QueueStateManager queueManager =
                (QueueStateManager) TestTools.getInternalState(session, "queueStateManager");
        assertNotNull(queueManager);

        server.pushItem(StatusCategory.E_SUCCESS);

        int reqId = 0;

        try {
            assertEquals(GenericResult.SUCCESS, session.start(Duration.ofSeconds(5)));

            QueueImpl queue = createQueue(session, createUri(), QueueFlags.setReader(0L));

            QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();

            assertNull(session.lookupQueue(queue.getFullQueueId()));
            assertNull(session.lookupQueue(queue.getUri()));

            assertEquals(QueueState.e_CLOSED, queue.getState());
            server.pushItem(StatusCategory.E_SUCCESS);
            server.pushItem(StatusCategory.E_SUCCESS);
            assertEquals(OpenQueueResult.SUCCESS, queue.open(queueOptions, TEST_REQUEST_TIMEOUT));
            // Substreamcount has incremented
            assertEquals(1, queueManager.getSubStreamCount(queue));
            QueueId queueKey = queue.getFullQueueId();
            assertEquals(QueueState.e_OPENED, queue.getState());
            assertEquals(queue, session.lookupQueue(queueKey));
            assertEquals(queue, session.lookupQueue(queue.getUri()));

            server.pushItem(StatusCategory.E_UNKNOWN);
            // bad response for configure request
            server.pushSuccess(); // ok for close
            queue.closeAsync(TEST_REQUEST_TIMEOUT);
            TestTools.acquireSema(testSema);
            assertEquals(1, events.size());
            Event event = events.poll();
            assertEquals(QueueControlEvent.TYPE_ID, event.getDispatchId());
            assertEquals(QueueState.e_CLOSED, queue.getState());
            assertNull(session.lookupQueue(queueKey));
            assertNull(session.lookupQueue(queue.getUri()));

            TestTools.sleepForSeconds(1);

            // Substreamcount has decremented after sending close request
            assertEquals(0, queueManager.getSubStreamCount(queue));

            // Verify requests
            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(++reqId, server.nextClientRequest(), queue.getParameters());
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            server.pushItem(StatusCategory.E_SUCCESS);
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());
            session.linger();
            server.stop();

            logger.info("================================================================");
            logger.info("END Testing BrokerSessionIT closeQueueConfigurationFailedTest.");
            logger.info("================================================================");
        }
    }

    /**
     * Critical test to verify timeout fsm logic for configure request during closing queue state
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>check queue state is e_CLOSED
     *   <li>check that look up method returns null
     *   <li>open queue
     *   <li>check queue state is e_OPENED
     *   <li>check that look up method returns proper queue handle
     *   <li>attempt to close queue
     *   <li>check that single queue control event was generated
     *   <li>check queue state is e_CLOSED
     *   <li>stop broker session
     * </ul>
     */
    @Test
    public void closeQueueConfigurationTimeoutTest() {
        logger.info("=================================================================");
        logger.info("BEGIN Testing BrokerSessionIT closeQueueConfigurationTimeoutTest.");
        logger.info("=================================================================");

        final Semaphore testSema = new Semaphore(0);
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        EventHandler eventHandler =
                (Event event) -> {
                    events.push(event);
                    testSema.release();
                };

        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, eventHandler);

        QueueStateManager queueManager =
                (QueueStateManager) TestTools.getInternalState(session, "queueStateManager");
        assertNotNull(queueManager);

        server.pushItem(StatusCategory.E_SUCCESS);

        int reqId = 0;

        try {
            // The response delay for configure request during close queue sequence is 2 seconds
            final Duration REQUEST_DELAY = Duration.ofSeconds(2);
            // The whole sequence timeout is 1 seconds
            final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(1);

            assertEquals(GenericResult.SUCCESS, session.start(Duration.ofSeconds(5)));

            QueueImpl queue = createQueue(session, createUri(), QueueFlags.setReader(0L));

            QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();

            assertNull(session.lookupQueue(queue.getFullQueueId()));
            assertNull(session.lookupQueue(queue.getUri()));

            assertEquals(QueueState.e_CLOSED, queue.getState());
            server.pushItem(StatusCategory.E_SUCCESS);
            server.pushItem(StatusCategory.E_SUCCESS);
            assertEquals(OpenQueueResult.SUCCESS, queue.open(queueOptions, SEQUENCE_TIMEOUT));
            // Substreamcount has incremented
            assertEquals(1, queueManager.getSubStreamCount(queue));
            QueueId queueKey = queue.getFullQueueId();
            assertEquals(QueueState.e_OPENED, queue.getState());
            assertEquals(queue, session.lookupQueue(queueKey));
            assertEquals(queue, session.lookupQueue(queue.getUri()));

            // OK for configure request during closing queue with delay
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY);
            server.pushSuccess(); // ok for close
            queue.closeAsync(SEQUENCE_TIMEOUT);
            TestTools.acquireSema(testSema);

            // Verify requests
            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(++reqId, server.nextClientRequest(), queue.getParameters());

            // Substreamcount has decremented after sending close request
            assertEquals(0, queueManager.getSubStreamCount(queue));

            // Check no more requests
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // Verify events
            assertEquals(1, events.size());
            verifyQueueControlEvent(
                    events.pollLast(),
                    QueueControlEvent.Type.e_QUEUE_CLOSE_RESULT,
                    CloseQueueResult.TIMEOUT,
                    queue);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            server.pushItem(StatusCategory.E_SUCCESS);
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));

            verifyDisconnectRequest(++reqId, server.nextClientRequest());
            session.linger();
            server.stop();

            logger.info("===============================================================");
            logger.info("END Testing BrokerSessionIT closeQueueConfigurationTimeoutTest.");
            logger.info("===============================================================");
        }
    }

    /**
     * Critical test to verify fsm logic when configure request is canceled during closing queue
     * state
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>check queue state is e_CLOSED
     *   <li>check that look up method returns null
     *   <li>open queue
     *   <li>check queue state is e_OPENED
     *   <li>check that look up method returns proper queue handle
     *   <li>close queue async
     *   <li>cancel all requests
     *   <li>check that single event was generated queue control event of E_CANCELED type
     *   <li>check queue state is e_CLOSED
     *   <li>check that look up method returns proper queue handle
     *   <li>stop broker session
     * </ul>
     */
    @Test
    public void closeQueueConfigurationCanceledTest() {
        logger.info("==================================================================");
        logger.info("BEGIN Testing BrokerSessionIT closeQueueConfigurationCanceledTest.");
        logger.info("==================================================================");

        final Semaphore testSema = new Semaphore(0);
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        EventHandler eventHandler =
                (Event event) -> {
                    events.push(event);
                    testSema.release();
                };

        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, eventHandler);

        QueueStateManager queueManager =
                (QueueStateManager) TestTools.getInternalState(session, "queueStateManager");
        assertNotNull(queueManager);

        RequestManager requestManager =
                (RequestManager) TestTools.getInternalState(session, "requestManager");
        assertNotNull(requestManager);

        server.pushItem(StatusCategory.E_SUCCESS);

        int reqId = 0;

        try {
            assertEquals(GenericResult.SUCCESS, session.start(Duration.ofSeconds(5)));

            QueueImpl queue = createQueue(session, createUri(), QueueFlags.setReader(0L));

            QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();

            assertNull(session.lookupQueue(queue.getFullQueueId()));
            assertNull(session.lookupQueue(queue.getUri()));

            assertEquals(QueueState.e_CLOSED, queue.getState());
            server.pushItem(StatusCategory.E_SUCCESS);
            server.pushItem(StatusCategory.E_SUCCESS);
            assertEquals(OpenQueueResult.SUCCESS, queue.open(queueOptions, TEST_REQUEST_TIMEOUT));
            // Substreamcount has incremented
            assertEquals(1, queueManager.getSubStreamCount(queue));
            QueueId queueKey = queue.getFullQueueId();
            assertEquals(QueueState.e_OPENED, queue.getState());
            assertEquals(queue, session.lookupQueue(queueKey));
            assertEquals(queue, session.lookupQueue(queue.getUri()));
            // Verify requests
            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            // Do not setup response for configure request
            BmqFuture<CloseQueueCode> closeFuture = queue.closeAsync(TEST_REQUEST_TIMEOUT);
            // Configure request should be sent
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            // Cancel all request
            // NOTE: must be executed in the context of the scheduler
            service.execute(requestManager::cancelAllRequests);

            TestTools.acquireSema(testSema);

            // Queue should close
            assertEquals(QueueState.e_CLOSED, queue.getState());

            assertEquals(1, events.size());
            Event event = events.poll();
            assertEquals(QueueControlEvent.TYPE_ID, event.getDispatchId());
            QueueControlEvent queueControlEvent = (QueueControlEvent) event;
            assertEquals(
                    QueueControlEvent.Type.e_QUEUE_CLOSE_RESULT, queueControlEvent.getEventType());
            assertEquals(CloseQueueResult.CANCELED, queueControlEvent.getStatus());

            assertEquals(CloseQueueResult.CANCELED, closeFuture.get(TEST_FUTURE_TIMEOUT));

            // Queue should be removed from active and expired lists
            assertNull(session.lookupQueue(queueKey));
            assertNull(session.lookupQueue(queue.getUri()));

            // Substreamcount should decrement
            assertEquals(0, queueManager.getSubStreamCount(queue));

            // Close request shouldn't be sent
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            server.pushItem(StatusCategory.E_SUCCESS);
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());

            session.linger();
            server.stop();

            logger.info("================================================================");
            logger.info("END Testing BrokerSessionIT closeQueueConfigurationCanceledTest.");
            logger.info("================================================================");
        }
    }

    /**
     * Critical test of queue sequence timeout.
     *
     * <ul>
     *   <li>start test server in active mode;
     *   <li>create broker session;
     *   <li>start broker session;
     *   <li>create queue;
     *   <li>preset status code and delays for open and configure response from server;
     *   <li>open queue synchronously with sequence timeout bigger than sum of delays for server
     *       responses;
     *   <li>check that success result code was returned;
     *   <li>preset status code and delays for configure and close response from server;
     *   <li>close queue synchronously with sequence timeout smaller than sum of delays for server
     *       responses;
     *   <li>check that timeout result code was returned;
     *   <li>stop broker session.
     * </ul>
     */
    @Test
    public void sequenceTimeoutTest() {
        logger.info("============================================================");
        logger.info("BEGIN Testing BrokerSessionIT sequenceTimeoutTest.");
        logger.info("============================================================");

        final Semaphore testSema = new Semaphore(0);
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();

        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, events::push);

        QueueStateManager queueManager =
                (QueueStateManager) TestTools.getInternalState(session, "queueStateManager");
        assertNotNull(queueManager);

        server.pushItem(StatusCategory.E_SUCCESS); // Ok for nego request

        int reqId = 0;

        try {
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));
            long flags = 0;
            flags = QueueFlags.setReader(flags);
            QueueImpl queue = createQueue(session, createUri(), flags);
            QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();

            final Duration REQUEST_DELAY_SUCCESS = Duration.ofSeconds(2);
            final Duration REQUEST_DELAY_FAILURE = Duration.ofSeconds(4);
            final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(6);
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY_SUCCESS);
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY_SUCCESS);
            // Check substreamcount == 0
            assertEquals(0, queueManager.getSubStreamCount(queue));

            assertEquals(OpenQueueResult.SUCCESS, queue.open(queueOptions, SEQUENCE_TIMEOUT));
            // Check substreamcount == 1 after sending open queue request
            assertEquals(1, queueManager.getSubStreamCount(queue));

            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY_FAILURE);
            server.pushItem(StatusCategory.E_SUCCESS, testSema, REQUEST_DELAY_FAILURE);
            assertEquals(CloseQueueResult.TIMEOUT, queue.close(SEQUENCE_TIMEOUT));
            assertEquals(0, events.size());

            TestTools.acquireSema(testSema);
            // Substreamcount should decrement back to zero
            assertEquals(0, queueManager.getSubStreamCount(queue));

            // Verify requests
            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(++reqId, server.nextClientRequest(), queue.getParameters());
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            server.pushItem(StatusCategory.E_SUCCESS);
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());
            session.linger();
            server.stop();

            logger.info("============================================================");
            logger.info("END Testing BrokerSessionIT sequenceTimeoutTest.");
            logger.info("============================================================");
        }
    }

    /**
     * Critical test of late open queue response.
     *
     * <p>NB: Given test method performs extracting QueueStateManager via reflection and gets around
     * encapsulation and any thread-safety guarantees. This test is strictly white-box and relies on
     * our current knowledge of BrokerSession, QueueManager and Queue internals. Can be broken if
     * implementation changes. Be aware about of fragility
     *
     * <ul>
     *   <li>start test server in active mode;
     *   <li>create broker session;
     *   <li>extract queue manager from broker session using reflection;
     *   <li>start broker session;
     *   <li>create queue;
     *   <li>preset status code and delay(3 sec) for open and configure response from server;
     *   <li>check that queue can not be found by correlation id in queue manager
     *   <li>open queue synchronously with sequence timeout less than delay for server open queue
     *       response(2 sec);
     *   <li>check that open queue request was sent successfully;
     *   <li>wait for 1 sec;
     *   <li>check that considered queue exists among active queues but not among expired queues;
     *   <li>wait until open queue sequence is finished;
     *   <li>check that considered queue exists among expired queues but not among active queues;
     *   <li>wait for 2 sec;
     *   <li>check that considered queue exists neither among active queues nor among expired
     *       queues;
     *   <li>check that the last request sent to the server has rId=2 and is close request and has
     *       proper queue handle parameters;
     *   <li>stop broker session.
     * </ul>
     */
    @Test
    public void lateOpenQueueResponseTest() {
        logger.info("============================================================");
        logger.info("BEGIN Testing BrokerSessionIT lateOpenQueueResponseTest.");
        logger.info("============================================================");

        final Semaphore openQueueSema = new Semaphore(0);
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

        LinkedList<Event> events = new LinkedList<>();
        EventHandler eventHandler =
                (Event event) -> {
                    events.push(event);
                    if (event.getDispatchId() == QueueControlEvent.TYPE_ID) {
                        QueueControlEvent queueEvent = (QueueControlEvent) event;
                        if (queueEvent.getEventType()
                                == QueueControlEvent.Type.e_QUEUE_OPEN_RESULT) {
                            openQueueSema.release();
                        }
                    }
                };
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, eventHandler);

        QueueStateManager queueManager =
                (QueueStateManager) TestTools.getInternalState(session, "queueStateManager");
        assertNotNull(queueManager);

        server.pushItem(StatusCategory.E_SUCCESS); // Ok response  for negotiation request

        int reqId = 0;

        try {
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));
            long flags = 0;
            flags = QueueFlags.setReader(flags);
            Uri uri = createUri();
            QueueHandle queueHandle = createQueue(session, uri, flags);
            QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();

            // The response delay for each request is 2 seconds
            final Duration REQUEST_DELAY = Duration.ofSeconds(3);
            // The whole sequence timeout is 3 seconds
            final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(2);
            // OK for open queue response, after 2 seconds
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY);
            // OK for further close queue request
            server.pushItem(StatusCategory.E_SUCCESS);
            // Before starting opening queue check if queue manager doesn't contain queue
            // with given queueId
            assertNull(queueManager.findByQueueId(queueHandle.getFullQueueId()));

            // Check substreamcount == 0
            assertEquals(0, queueManager.getSubStreamCount((QueueImpl) queueHandle));

            // === Test timeline: ~0 sec after start
            logger.info("Start opening the queue");
            queueHandle.openAsync(queueOptions, SEQUENCE_TIMEOUT);
            // Wait for one second
            TestTools.sleepForSeconds(1);

            // === Test timeline: ~1 sec after start
            // After a second after successful sending open queue request we are expecting
            // queue with given unique Id is stored in queue manager among active queues.
            QueueImpl queue = queueManager.findByQueueId(queueHandle.getFullQueueId());
            assertNotNull(queue);
            // Check that found queue has expected URI.
            assertEquals(uri, queue.getUri());
            // Get the queueId assigned to given queue by queue manager.
            QueueId queueId = queue.getFullQueueId();
            // Check if we can access given queue by queueId in queueManager.
            assertNotNull(queueManager.findByQueueId(queueId));
            assertEquals(queue, queueManager.findByQueueId(queueId));
            // Check substreamcount == 1 after sending open queue request
            assertEquals(1, queueManager.getSubStreamCount(queue));
            // Wait for timeout failure of the open queue sequence.
            TestTools.acquireSema(openQueueSema);
            logger.info("Open queue sequence finished");

            // === Test timeline: ~2 sec after start
            // Now we expect, that considered queue CAN NOT be found among active queues.
            assertNull(queueManager.findByQueueId(queueId));
            // We expect, that considered queue SHOULD be found among expired queues.
            QueueImpl expiredQueue = queueManager.findExpiredByQueueId(queueId);
            assertNotNull(expiredQueue);
            assertEquals(queue, expiredQueue);
            // Wait for 2 seconds
            TestTools.sleepForSeconds(2);

            // === Test timeline: ~4 sec after start
            logger.info("Ready to verify handling of late response");
            // At that moment we expect that onLateResponse method of QueueStateManager is called
            // and given queue CAN NOT be found among neither active nor expired queues.
            assertNull(queueManager.findByQueueId(queueId));
            assertNull(queueManager.findExpiredByQueueId(queueId));

            // Substreamcount should decrement back to zero
            assertEquals(0, queueManager.getSubStreamCount(queue));

            // Verify requests
            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(++reqId, server.nextClientRequest(), queueHandle.getParameters());

            // Verify OPEN_RESULT event
            assertEquals(1, events.size());
            verifyQueueControlEvent(
                    events.pollLast(),
                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT,
                    OpenQueueResult.TIMEOUT,
                    queueHandle);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            // Gently stop the session.
            server.pushItem(StatusCategory.E_SUCCESS);
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());
            session.linger();
            server.stop();

            logger.info("============================================================");
            logger.info("END Testing BrokerSessionIT lateOpenQueueResponseTest.");
            logger.info("============================================================");
        }
    }

    /**
     * Critical test of late configure queue response in open queue sequence.
     *
     * <p>NB: Given test method performs extracting QueueStateManager via reflection and gets around
     * encapsulation and any thread-safety guarantees. This test is strictly white-box and relies on
     * our current knowledge of BrokerSession, QueueManager and Queue internals. Can be broken if
     * implementation changes. Be aware of this fragility.
     *
     * <ul>
     *   <li>start test server in active mode;
     *   <li>create broker session;
     *   <li>extract queue manager from broker session using reflection;
     *   <li>start broker session;
     *   <li>create queue;
     *   <li>preset status code and delay(2 sec) for open and configure response from server;
     *   <li>also preset status code for configure and close queue responses during fututre close
     *       queue sequence;
     *   <li>check that queue can not be found by correlation id in queue manager
     *   <li>open queue asynchronously with sequence timeout less than delay for server open queue
     *       response(2 sec);
     *   <li>check that open queue request was sent successfully;
     *   <li>wait for 1 sec;
     *   <li>check that considered queue exists among active queues but not among expired queues;
     *   <li>wait until open queue sequence is finished;
     *   <li>check that considered queue exists among both active and expired queues;
     *   <li>wait for 2 sec;
     *   <li>check that considered queue exists neither among active queues nor among expired
     *       queues;
     *   <li>check that the last request sent to the server has rId=4 and is close request and has
     *       proper queue handle parameters;
     *   <li>stop broker session.
     * </ul>
     */
    @Test
    public void lateConfigureOpenQueueResponseTest() {
        logger.info("===================================================================");
        logger.info("BEGIN Testing BrokerSessionIT lateConfigureOpenQueueResponseTest.");
        logger.info("===================================================================");

        final Semaphore openQueueSema = new Semaphore(0);
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

        LinkedList<Event> events = new LinkedList<>();
        EventHandler eventHandler =
                (Event event) -> {
                    events.push(event);
                    if (event.getDispatchId() == QueueControlEvent.TYPE_ID) {
                        QueueControlEvent queueEvent = (QueueControlEvent) event;
                        if (queueEvent.getEventType()
                                == QueueControlEvent.Type.e_QUEUE_OPEN_RESULT) {
                            openQueueSema.release();
                        }
                    }
                };
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, eventHandler);

        QueueStateManager queueManager =
                (QueueStateManager) TestTools.getInternalState(session, "queueStateManager");
        assertNotNull(queueManager);

        server.pushItem(StatusCategory.E_SUCCESS); // Ok response  for negotiation request

        int reqId = 0;

        try {
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));
            long flags = 0;
            flags = QueueFlags.setReader(flags);
            Uri uri = createUri();
            QueueHandle queueHandle = createQueue(session, uri, flags);
            QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();

            // The response delay for each request is 2 seconds
            final Duration REQUEST_DELAY = Duration.ofSeconds(2);
            // The whole sequence timeout is 3 seconds
            final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(3);
            // OK for open queue response, after 2 seconds
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY);
            // OK for configure queue response, after 2 seconds
            // - arrives after open queue sequence timeout
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY);
            // OK for configure queue response during close queue sequence.
            server.pushItem(StatusCategory.E_SUCCESS);
            // OK for close queue response during close queue sequence.
            server.pushItem(StatusCategory.E_SUCCESS);
            // Before starting opening queue check if queue manager doesn't contain queue
            // with given QueueId
            assertNull(queueManager.findByQueueId(queueHandle.getFullQueueId()));

            // Check substreamcount == 0
            assertEquals(0, queueManager.getSubStreamCount((QueueImpl) queueHandle));

            // === Test timeline: ~0 sec after start
            logger.info("Start opening the queue");
            queueHandle.openAsync(queueOptions, SEQUENCE_TIMEOUT);
            // Wait for one second
            TestTools.sleepForSeconds(1);

            // === Test timeline: ~1 sec after start
            // After a second after successful sending open queue request we are expecting
            // queue with given queue Id is stored in queue manager among active queues.
            QueueImpl queue = queueManager.findByQueueId(queueHandle.getFullQueueId());
            assertNotNull(queue);
            // Check that found queue has expected URI.
            assertEquals(uri, queue.getUri());
            // Get the queueId assigned to given queue by queue manager.
            QueueId queueId = queue.getFullQueueId();
            // Check if we can access given queue by queueId in queueManager.
            assertNotNull(queueManager.findByQueueId(queueId));
            assertEquals(queue, queueManager.findByQueueId(queueId));
            // Check substreamcount == 1 after sending open queue request
            assertEquals(1, queueManager.getSubStreamCount(queue));
            // Wait for timeout failure of the open queue sequence.
            TestTools.acquireSema(openQueueSema);
            logger.info("Open queue sequence finished");

            // === Test timeline: ~3 sec after start
            // Now we expect, that considered queue SHOULD be found among expired queues.
            assertNull(queueManager.findByQueueId(queueId));
            QueueImpl expiredQueue = queueManager.findExpiredByQueueId(queueId);
            assertNotNull(expiredQueue);
            assertEquals(queue, expiredQueue);
            // Check substreamcount hasn't changed
            assertEquals(1, queueManager.getSubStreamCount(queue));
            // Wait for 2 seconds
            TestTools.sleepForSeconds(2);

            // === Test timeline: ~5 sec after start
            logger.info("Ready to verify handling of late response");
            // At that moment we expect that onLateResponse method of QueueStateManager is called
            // and given queue CAN NOT be found among neither active nor expired queues.
            assertNull(queueManager.findByQueueId(queueId));
            assertNull(queueManager.findExpiredByQueueId(queueId));
            // Substreamcount should decrement back to zero
            assertEquals(0, queueManager.getSubStreamCount(queue));

            // Verify requests
            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(++reqId, server.nextClientRequest(), queueHandle.getParameters());

            // Verify OPEN_RESULT event
            assertEquals(1, events.size());
            verifyQueueControlEvent(
                    events.pollLast(),
                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT,
                    OpenQueueResult.TIMEOUT,
                    queueHandle);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            // Gently stop the session.
            server.pushItem(StatusCategory.E_SUCCESS);
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());
            session.linger();
            server.stop();

            logger.info("=================================================================");
            logger.info("END Testing BrokerSessionIT lateConfigureOpenQueueResponseTest.");
            logger.info("=================================================================");
        }
    }

    /**
     * Critical test of late configure queue response in configure sequence.
     *
     * <ul>
     *   <li>start test server in active mode;
     *   <li>create broker session;
     *   <li>start broker session;
     *   <li>create queue;
     *   <li>preset status codes for open and configure response from server;
     *   <li>open queue asynchronously;
     *   <li>wait until queue is opened;
     *   <li>preset status success code and delay for configure response bigger than sequence
     *       timeout.
     *   <li>alos preset success code for further on-late-response sync configure request;
     *   <li>send configure request asynchronously;
     *   <li>wait while sequence is finished (due to timeout);
     *   <li>wait for 2 sec;
     *   <li>check that the last request sent to the server has rId=4 and options equal to those
     *       that was provided during open queue sequence;
     *   <li>stop broker session.
     * </ul>
     */
    @Test
    public void lateStandaloneConfigureResponseTest() {
        logger.info("===================================================================");
        logger.info("BEGIN Testing BrokerSessionIT lateStandaloneConfigureResponseTest.");
        logger.info("===================================================================");

        final Semaphore openQueueSema = new Semaphore(0);
        final Semaphore configureQueueSema = new Semaphore(0);
        final int MAX_UNCONFIRMED_BYTES = 10000;
        final int MAX_UNCONFIRMED_MESSAGES = 1000;
        final int INITIAL_CUSTOMER_PRIORITY = 2;
        final int FAILED_TO_APPLY_CUSTOMER_PRIORITY = 3;
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

        EventHandler eventHandler =
                (Event event) -> {
                    if (event.getDispatchId() == QueueControlEvent.TYPE_ID) {
                        QueueControlEvent queueEvent = (QueueControlEvent) event;

                        if (queueEvent.getEventType()
                                == QueueControlEvent.Type.e_QUEUE_OPEN_RESULT) {
                            openQueueSema.release();
                        } else if (queueEvent.getEventType()
                                == QueueControlEvent.Type.e_QUEUE_CONFIGURE_RESULT) {
                            configureQueueSema.release();
                        }
                    }
                };
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, eventHandler);

        QueueStateManager queueManager =
                (QueueStateManager) TestTools.getInternalState(session, "queueStateManager");
        assertNotNull(queueManager);

        // Ok response  for negotiation request
        server.pushItem(StatusCategory.E_SUCCESS);

        int reqId = 0;

        try {
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));
            long flags = 0;
            flags = QueueFlags.setReader(flags);
            QueueImpl queue = createQueue(session, createUri(), flags);
            QueueOptions queueOptions =
                    QueueOptions.builder()
                            .setMaxUnconfirmedBytes(MAX_UNCONFIRMED_BYTES)
                            .setMaxUnconfirmedMessages(MAX_UNCONFIRMED_MESSAGES)
                            .setConsumerPriority(INITIAL_CUSTOMER_PRIORITY)
                            .build();

            // The response delay for standalone configure request is 2 seconds
            final Duration REQUEST_DELAY = Duration.ofSeconds(2);
            // The whole sequence timeout is 1 seconds
            final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(1);
            // OK for open queue response
            server.pushItem(StatusCategory.E_SUCCESS);
            // OK for configure queue response
            server.pushItem(StatusCategory.E_SUCCESS);

            // === Test timeline: ~0 sec after start
            logger.info("Start opening the queue");
            assertEquals(
                    OpenQueueResult.SUCCESS,
                    queue.openAsync(queueOptions, SEQUENCE_TIMEOUT).get(TEST_FUTURE_TIMEOUT));
            TestTools.acquireSema(openQueueSema);
            // Check substreamcount == 1 after sending open queue request
            assertEquals(1, queueManager.getSubStreamCount(queue));

            // OK for standalone configure request with delay
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY);
            // OK for on late response configure request without delay
            server.pushItem(StatusCategory.E_SUCCESS);
            // To send recovery configure request to sync with a server we need
            // attempted QueueOptions be different from the applied one.
            // Here is different customer priority.
            QueueOptions failedOptions =
                    QueueOptions.builder()
                            .merge(queueOptions)
                            .setConsumerPriority(FAILED_TO_APPLY_CUSTOMER_PRIORITY)
                            .build();
            queue.configureAsync(failedOptions, SEQUENCE_TIMEOUT);
            TestTools.acquireSema(configureQueueSema);
            // === Test timeline: ~1 sec after start
            TestTools.sleepForSeconds(2);
            // === Test timeline: ~3 sec after start
            // Via test server backdoor we can verify that our Java SDK has sent
            // configure request with rId=4 and expected fields.

            // open queue request
            verifyOpenRequest(++reqId, server.nextClientRequest());
            // configure queue request during open queue sequence
            verifyConfigureRequest(++reqId, server.nextClientRequest(), queueOptions);
            // standalone configure queue request that was lately confirmed
            verifyConfigureRequest(++reqId, server.nextClientRequest(), failedOptions);
            // recovery on-late-response configure request to sync with a server
            verifyConfigureRequest(++reqId, server.nextClientRequest(), queueOptions);

        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } finally {
            // Gently stop the session.
            server.pushItem(StatusCategory.E_SUCCESS);
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());
            session.linger();
            server.stop();

            logger.info("==================================================================");
            logger.info("END Testing BrokerSessionIT lateStandaloneConfigureResponseTest.");
            logger.info("=================================================================");
        }
    }

    /**
     * Critical test of late configure queue response in close queue sequence.
     *
     * <ul>
     *   <li>start test server in active mode;
     *   <li>create broker session;
     *   <li>start broker session;
     *   <li>create queue;
     *   <li>preset status codes for open and configure response from server;
     *   <li>open queue asynchronously;
     *   <li>wait until queue is opened;
     *   <li>preset status success code and delay for configure response bigger than sequence
     *       timeout.
     *   <li>also preset success code for further close queue request;
     *   <li>close queue asynchronously;
     *   <li>wait while sequence is finished (due to timeout);
     *   <li>wait for 2 sec;
     *   <li>check that the last request sent to the server has rId=4 and is close request and has
     *       proper queue handle parameters;
     *   <li>stop broker session.
     * </ul>
     */
    @Test
    public void lateCloseQueueConfigureResponseTest() {
        logger.info("===================================================================");
        logger.info("BEGIN Testing BrokerSessionIT lateCloseQueueConfigureResponseTest.");
        logger.info("===================================================================");

        final Semaphore openQueueSema = new Semaphore(0);
        final Semaphore closeQueueSema = new Semaphore(0);
        final int MAX_UNCONFIRMED_BYTES = 10000;
        final int MAX_UNCONFIRMED_MESSAGES = 1000;
        final int INITIAL_CUSTOMER_PRIORITY = 2;

        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

        LinkedList<Event> events = new LinkedList<>();
        EventHandler eventHandler =
                (Event event) -> {
                    events.push(event);
                    if (event.getDispatchId() == QueueControlEvent.TYPE_ID) {
                        QueueControlEvent queueEvent = (QueueControlEvent) event;

                        if (queueEvent.getEventType()
                                == QueueControlEvent.Type.e_QUEUE_OPEN_RESULT) {
                            openQueueSema.release();
                        } else if (queueEvent.getEventType()
                                == QueueControlEvent.Type.e_QUEUE_CLOSE_RESULT) {
                            closeQueueSema.release();
                        }
                    }
                };
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, eventHandler);

        QueueStateManager queueManager =
                (QueueStateManager) TestTools.getInternalState(session, "queueStateManager");
        assertNotNull(queueManager);

        // Ok response  for negotiation request
        server.pushItem(StatusCategory.E_SUCCESS);

        int reqId = 0;

        try {
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));
            long flags = 0;
            flags = QueueFlags.setReader(flags);
            QueueImpl queue = createQueue(session, createUri(), flags);
            QueueOptions queueOptions =
                    QueueOptions.builder()
                            .setMaxUnconfirmedBytes(MAX_UNCONFIRMED_BYTES)
                            .setMaxUnconfirmedMessages(MAX_UNCONFIRMED_MESSAGES)
                            .setConsumerPriority(INITIAL_CUSTOMER_PRIORITY)
                            .build();

            // The response delay for configure request during close queue sequence is 2 seconds
            final Duration REQUEST_DELAY = Duration.ofSeconds(2);
            // The whole sequence timeout is 1 seconds
            final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(1);
            // OK for open queue response
            server.pushItem(StatusCategory.E_SUCCESS);
            // OK for configure queue response
            server.pushItem(StatusCategory.E_SUCCESS);

            // === Test timeline: ~0 sec after start
            logger.info("Start opening the queue");
            assertEquals(
                    OpenQueueResult.SUCCESS,
                    queue.openAsync(queueOptions, SEQUENCE_TIMEOUT).get(TEST_FUTURE_TIMEOUT));
            TestTools.acquireSema(openQueueSema);

            // OK for configure request during closing queue with delay
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY);

            // OK for on close request without delay
            server.pushItem(StatusCategory.E_SUCCESS, closeQueueSema);

            // Check substreamcount == 1 before sending close queue request
            assertEquals(1, queueManager.getSubStreamCount(queue));

            queue.closeAsync(SEQUENCE_TIMEOUT);

            TestTools.acquireSema(closeQueueSema);
            // === Test timeline: ~1 sec after start

            // Check substreamcount hasn't changed
            assertEquals(1, queueManager.getSubStreamCount(queue));

            TestTools.acquireSema(closeQueueSema);
            // === Test timeline: ~3 sec after start
            // Via test server backdoor we can verify that our Java SDK has sent
            // close request with rId=4 and expected fields.

            // After configure request local timeout,
            // close request should be sent and substreamcount
            // should decrement to zero
            assertEquals(0, queueManager.getSubStreamCount(queue));

            // Verify requests
            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(++reqId, server.nextClientRequest(), queue.getParameters());

            // Check no more requests
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // Verify events
            assertEquals(2, events.size());
            verifyQueueControlEvent(
                    events.pollLast(),
                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT,
                    OpenQueueResult.SUCCESS,
                    queue);
            verifyQueueControlEvent(
                    events.pollLast(),
                    QueueControlEvent.Type.e_QUEUE_CLOSE_RESULT,
                    CloseQueueResult.TIMEOUT,
                    queue);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            // Gently stop the session.
            server.pushItem(StatusCategory.E_SUCCESS);
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());
            session.linger();
            server.stop();

            logger.info("==================================================================");
            logger.info("END Testing BrokerSessionIT lateCloseQueueConfigureResponseTest.");
            logger.info("==================================================================");
        }
    }

    /**
     * Test of queue reopen logic of N queues during broker session reconnect.
     *
     * <ul>
     *   <li>start test server in auto mode;
     *   <li>create broker session;
     *   <li>start broker session;
     *   <li>create queues [1..N], open them synchronously
     *   <li>open queue N+1 synchronously;
     *   <li>close queue N+1 synchronously;
     *   <li>stop test server;
     *   <li>wait for 3 sec;
     *   <li>start test server;
     *   <li>wait for 3 sec;
     *   <li>check that the last N*2 requests sent to the server have the following properties:
     *   <li>rId of range [2N-1..4N], decreasing from 4N to 2N-1
     *   <li>all requests are either openQueueRequest or configureQueueRequest
     *   <li>QueueId extracted from request should exist among stored ids of open request
     *   <li>Handle and stream parameters should be extracted from requests should be equal to
     *       corresponding parameters extracted from stored QueueHandle objects with corresponding
     *       queueId.
     *   <li>ConfigureQueue request with some queueId should be sent after corresponding OpenQueue
     *       request with the same queueId
     *   <li>Next 2 requests sent to server should relate to closing sequence of manually closed
     *       queue;
     *   <li>Check the number of generated BlazingMQ events - it should be 3 + N
     *   <li>BlazingMQ event #1 should be: e_CONNECTION_LOST;
     *   <li>BlazingMQ event #2 should be: e_RECONNECTED;
     *   <li>BlazingMQ event #[3..N + 2] should be: e_QUEUE_REOPEN_RESULT(E_SUCCESS) with
     *       correlation id present among stored correlation ids of opened queue;
     *   <li>BlazingMQ event #(N + 3) should be: e_STATE_RESTORED
     *   <li>stop broker session.
     * </ul>
     */
    private void reOpenQueueTest(int numberOfQueuesOpened) {
        logger.info("BrokerSessionIT reOpenQueueTest for {} queues.", numberOfQueuesOpened);
        final int MAX_UNCONFIRMED_BYTES = 10000;
        final int MAX_UNCONFIRMED_MESSAGES = 1000;
        final int INITIAL_CUSTOMER_PRIORITY = 2;
        final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(1);
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_AUTO_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, events::push);

        QueueStateManager queueManager =
                (QueueStateManager) TestTools.getInternalState(session, "queueStateManager");
        assertNotNull(queueManager);

        try {
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));
            long flags = QueueFlags.setReader(0);
            QueueOptions queueOptions =
                    QueueOptions.builder()
                            .setMaxUnconfirmedBytes(MAX_UNCONFIRMED_BYTES)
                            .setMaxUnconfirmedMessages(MAX_UNCONFIRMED_MESSAGES)
                            .setConsumerPriority(INITIAL_CUSTOMER_PRIORITY)
                            .build();

            Map<Integer, Boolean> expectedIds = new TreeMap<>();
            Map<Integer, QueueHandle> queueIdMap = new TreeMap<>();
            Set<QueueId> queueIdSet = new HashSet<>();
            // === Test timeline: ~0 sec after start
            for (int i = 0; i < numberOfQueuesOpened; i++) {
                QueueHandle queueHandle = createQueue(session, createUri(), flags);
                assertNotNull(queueHandle);
                logger.info("Start opening the queue");
                assertEquals(
                        OpenQueueResult.SUCCESS, queueHandle.open(queueOptions, SEQUENCE_TIMEOUT));
                // Check substreamcount == 1 after sending open queue request
                assertEquals(1, queueManager.getSubStreamCount((QueueImpl) queueHandle));

                expectedIds.put(queueHandle.getQueueId(), false);
                queueIdMap.put(queueHandle.getQueueId(), queueHandle);
                queueIdSet.add(queueHandle.getFullQueueId());
            }
            // One extra queue that should be opened and closed manually to
            // verify that closed queues are not reopened after channel bouncing.
            QueueImpl openClosedQueue = createQueue(session, createUri(), flags);
            assertEquals(
                    OpenQueueResult.SUCCESS, openClosedQueue.open(queueOptions, SEQUENCE_TIMEOUT));
            // Check substreamcount == 1 before sending close queue request
            assertEquals(1, queueManager.getSubStreamCount(openClosedQueue));
            assertEquals(
                    GenericResult.SUCCESS,
                    openClosedQueue.close(SEQUENCE_TIMEOUT).getGeneralResult());
            // Substreamcount should decrement to zero
            assertEquals(0, queueManager.getSubStreamCount(openClosedQueue));

            // Connection bouncing, provoked by test server.
            server.stop();
            TestTools.sleepForSeconds(3);
            // === Test timeline: ~3 sec after start
            server.start();
            logger.error("Server started again");
            // === Test timeline: ~6 sec after start
            TestTools.sleepForSeconds(3);

            // rId = [1..N*2 - 1], open queue requests;
            // rId = [2..N*2],    configure queue requests during open queue sequence;
            // rId = [N*2 + 1], open queue request for extra queue (that will be manually closed);
            // rId = [N*2 + 2], configure queue request during open queue sequence for extra queue;
            // rId = [N*2 + 3], configure queue request during close queue sequence for extra queue;
            // rId = [N*2 + 4], close queue request during close queue sequence for extra queue;

            // rId = [N*2 + 5 - N*4 + 4], open queue requests during re-open sequences;;
            //                          , configure queue requests during re-open sequences;

            // Check first N*2 requests
            for (int i = 1; i <= numberOfQueuesOpened * 2; i++) {
                ControlMessageChoice clientRequest = server.nextClientRequest();
                if (i % 2 == 0) {
                    assertTrue(clientRequest.isConfigureStreamValue());
                } else {
                    assertTrue(clientRequest.isOpenQueueValue());
                }
            }
            // 4 requests for extra queue
            for (int i = 1; i <= 4; i++) {
                ControlMessageChoice clientRequest = server.nextClientRequest();
                assertNotNull(clientRequest);
                assertEquals(numberOfQueuesOpened * 2 + i, clientRequest.id());
                switch (i) {
                    case 1: // open queue request
                        assertTrue(clientRequest.isOpenQueueValue());
                        break;
                    case 2: // configure queue request during open queue sequence
                        assertTrue(clientRequest.isConfigureStreamValue());
                        break;
                    case 3:
                        { // configure queue request during close queue sequence
                            assertTrue(clientRequest.isConfigureStreamValue());
                            ConfigureStream configureCloseQueue = clientRequest.configureStream();
                            assertNotNull(configureCloseQueue);
                            StreamParameters streamParameters =
                                    configureCloseQueue.streamParameters();
                            assertNotNull(streamParameters);
                            assertEquals(
                                    openClosedQueue.getQueueId(),
                                    configureCloseQueue
                                            .id()); // todo qId moved to upper level structure
                            assertEquals(
                                    StreamParameters.DEFAULT_APP_ID,
                                    streamParameters
                                            .appId()); // todo no sub id info anymore, we have just
                            // app id here and subscription ids in nested
                            // subscriptions
                            // todo maybe delete this /\ check because no nullable field anymore
                            TestTools.assertCloseConfigurationStreamParameters(streamParameters);
                            break;
                        }
                    case 4:
                        { // close queue request
                            verifyCloseRequest(
                                    numberOfQueuesOpened * 2 + 4,
                                    clientRequest,
                                    openClosedQueue.getParameters());
                            break;
                        }
                    default:
                        throw new RuntimeException("Unexpected request: " + clientRequest);
                }
            }
            // check the last 2*N requests
            for (int i = 1; i <= numberOfQueuesOpened * 2; i++) {
                ControlMessageChoice clientRequest = server.nextClientRequest();
                assertEquals(numberOfQueuesOpened * 2 + 4 + i, clientRequest.id());
                ConfigureStream configureStream = clientRequest.configureStream();
                if (configureStream != null) {
                    StreamParameters streamParameters = configureStream.streamParameters();
                    assertNotNull(streamParameters);
                    Integer queueId = configureStream.id(); // todo qId moved to upper level
                    Boolean isOpenRequestAlreadyParsed = expectedIds.get(queueId);
                    assertNotNull(isOpenRequestAlreadyParsed);
                    assertTrue(isOpenRequestAlreadyParsed);
                    expectedIds.remove(queueId);

                    ConsumerInfo info = TestTools.getDefaultConsumerInfo(streamParameters);

                    assertEquals(MAX_UNCONFIRMED_MESSAGES, info.maxUnconfirmedMessages());
                    assertEquals(MAX_UNCONFIRMED_BYTES, info.maxUnconfirmedBytes());
                    assertEquals(INITIAL_CUSTOMER_PRIORITY, info.consumerPriority());
                    assertEquals(
                            StreamParameters.DEFAULT_APP_ID,
                            streamParameters.appId()); // todo maybe remove this check? or check if
                    // subscriptions are empty?
                } else {
                    OpenQueue openQueue = clientRequest.openQueue();
                    assertNotNull(openQueue);
                    QueueHandleParameters queueHandleParameters = openQueue.getHandleParameters();
                    assertNotNull(queueHandleParameters);
                    Integer queueId = queueHandleParameters.getQId();
                    Boolean isOpenRequestAlreadyParsed = expectedIds.get(queueId);
                    assertFalse(isOpenRequestAlreadyParsed);
                    expectedIds.replace(queueId, true);
                    QueueHandle expectedQueue = queueIdMap.get(queueId);
                    assertNotNull(expectedQueue);
                    QueueHandleParameters expectedParameters = expectedQueue.getParameters();
                    TestTools.assertQueueHandleParamsAreEqual(
                            expectedParameters, queueHandleParameters);

                    // Check substreamcount hasn't changed after reconnect
                    assertEquals(1, queueManager.getSubStreamCount((QueueImpl) expectedQueue));
                }
            }

            // BlazingMQ EVENTS
            // 1) e_CONNECTION_LOST
            // 2) e_RECONNECTED
            // [3..N+2]) e_QUEUE_REOPEN_RESULT (E_SUCCESS)
            // N+3) e_STATE_RESTORED
            assertEquals(3 + numberOfQueuesOpened, events.size());
            // 1) e_CONNECTION_LOST
            BrokerSessionEvent sessionEvent = (BrokerSessionEvent) events.pollLast();
            assertNotNull(sessionEvent);
            assertEquals(BrokerSessionEvent.Type.e_CONNECTION_LOST, sessionEvent.getEventType());

            // 2) e_RECONNECTED
            sessionEvent = (BrokerSessionEvent) events.pollLast();
            assertNotNull(sessionEvent);
            assertEquals(BrokerSessionEvent.Type.e_RECONNECTED, sessionEvent.getEventType());

            // [3..N+2]) e_QUEUE_REOPEN_RESULT (E_SUCCESS)
            for (int i = 0; i < numberOfQueuesOpened; i++) {
                QueueControlEvent reopenEvent = (QueueControlEvent) events.pollLast();
                assertNotNull(reopenEvent);
                assertEquals(
                        QueueControlEvent.Type.e_QUEUE_REOPEN_RESULT, reopenEvent.getEventType());
                assertEquals(OpenQueueResult.SUCCESS, reopenEvent.getStatus());
                QueueId queueId = reopenEvent.getQueue().getFullQueueId();
                assertNotNull(queueId);
                assertTrue(queueIdSet.contains(queueId));
                assertTrue(queueIdSet.remove(queueId));
            }

            // N+3) e_STATE_RESTORED
            sessionEvent = (BrokerSessionEvent) events.pollLast();
            assertNotNull(sessionEvent);
            assertEquals(BrokerSessionEvent.Type.e_STATE_RESTORED, sessionEvent.getEventType());

            int i = 0;
            for (QueueHandle qh : queueIdMap.values()) {
                assertEquals(CloseQueueResult.SUCCESS, qh.close(TEST_REQUEST_TIMEOUT));
                // Substreamcount should decrement to zero
                assertEquals(0, queueManager.getSubStreamCount((QueueImpl) qh));

                int baseId = numberOfQueuesOpened * 4 + 4 + i;
                verifyConfigureRequest(baseId + 1, server.nextClientRequest());
                verifyCloseRequest(baseId + 2, server.nextClientRequest(), qh.getParameters());

                i += 2;
            }
        } catch (AssertionError e) {
            logger.error("Assertion: ", e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            int id = numberOfQueuesOpened * 6 + 4 + 1;
            verifyDisconnectRequest(id, server.nextClientRequest());
            server.stop();
            session.linger();
            logger.info("Server stopped");
        }
    }

    @Test
    public void stateRestoredWithoutQueuesTest() {
        logger.info("===============================================================");
        logger.info("BEGIN Testing BrokerSessionIT stateRestoredWithoutQueuesTest.");
        logger.info("===============================================================");
        reOpenQueueTest(0);
        logger.info("===============================================================");
        logger.info("END Testing BrokerSessionIT stateRestoredWithoutQueuesTest.");
        logger.info("===============================================================");
    }

    @Test
    public void multipleQueuesReopenTest() {
        logger.info("=========================================================");
        logger.info("BEGIN Testing BrokerSessionIT multipleQueuesReopenTest.");
        logger.info("=========================================================");
        reOpenQueueTest(10);
        logger.info("=======================================================");
        logger.info("END Testing BrokerSessionIT multipleQueuesReopenTest.");
        logger.info("=======================================================");
    }

    /**
     * Critical test to check clean up logic during session stop.
     *
     * <ul>
     *   <li>start test server in active mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>create queue
     *   <li>open the first queue asynchronously
     *   <li>stop broker session
     *   <li>attempt to close opening queue
     *   <li>check that close operation returned rc=ALREADY_CLOSED
     *   <li>check that single event was posted, e_QUEUE_OPEN_RESULT/e_CANCELED
     *   <li>linger broker session and stop server
     * </ul>
     */
    @Test
    public void closeQueueOnStoppedSession() {
        logger.info("===========================================================");
        logger.info("BEGIN Testing BrokerSessionIT closeQueueOnStoppedSession.");
        logger.info("===========================================================");

        final int MAX_UNCONFIRMED_BYTES = 10000;
        final int MAX_UNCONFIRMED_MESSAGES = 1000;
        final int INITIAL_CUSTOMER_PRIORITY = 2;
        final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(1);
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_AUTO_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, events::push);

        QueueStateManager queueManager =
                (QueueStateManager) TestTools.getInternalState(session, "queueStateManager");
        assertNotNull(queueManager);

        try {
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));
            long flags = QueueFlags.setReader(0);
            QueueOptions queueOptions =
                    QueueOptions.builder()
                            .setMaxUnconfirmedBytes(MAX_UNCONFIRMED_BYTES)
                            .setMaxUnconfirmedMessages(MAX_UNCONFIRMED_MESSAGES)
                            .setConsumerPriority(INITIAL_CUSTOMER_PRIORITY)
                            .build();
            QueueHandle q1 = createQueue(session, createUri(), flags);
            q1.openAsync(queueOptions, SEQUENCE_TIMEOUT);
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));

            int reqId = 0;
            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyDisconnectRequest(++reqId, server.nextClientRequest());
            // While we are waiting for disconnect response, open configure
            // request is sent as well
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            assertEquals(CloseQueueResult.UNKNOWN_QUEUE, q1.close(SEQUENCE_TIMEOUT));
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            assertEquals(1, events.size());
            QueueControlEvent queueControlEvent = (QueueControlEvent) events.pollLast();
            assertNotNull(queueControlEvent);
            assertEquals(
                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT, queueControlEvent.getEventType());
            assertEquals(OpenQueueResult.CANCELED, queueControlEvent.getStatus());
            // Check that eventually substreamcount has been set to zero
            assertEquals(0, queueManager.getSubStreamCount((QueueImpl) q1));
        } finally {
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            session.linger();
            server.stop();

            logger.info("=========================================================");
            logger.info("END Testing BrokerSessionIT closeQueueOnStoppedSession.");
            logger.info("=========================================================");
        }
    }

    /**
     * Critical test to check return statuses of precondition failure of close queue sequence.
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>create queue
     *   <li>attempt to close queue
     *   <li>check that close operation returned rc=UNKNOWN_QUEUE
     *   <li>open the queue asynchronously
     *   <li>just after start of open queue sequence attempt to close given queue
     *   <li>check that close operation returned rc=ALREADY_IN_PROGRESS
     *   <li>wait until queue is opened
     *   <li>tune server to answer after timeout and attempt to close queue
     *   <li>check that close operation returned rc=ALREADY_CLOSED
     *   <li>stop and linger broker session and stop server
     *   <li>check that 3 events were posted: - QUEUE_CLOSE_RESULT/UNKNOWN_QUEUE, -
     *       QUEUE_CLOSE_RESULT/ALREADY_IN_PROGRESS - QUEUE_OPEN_RESULT/e_SUCCESS
     * </ul>
     */
    @Test
    public void closeQueuePreconditionFailures() {
        logger.info("===============================================================");
        logger.info("BEGIN Testing BrokerSessionIT closeQueuePreconditionFailures.");
        logger.info("===============================================================");

        final Semaphore closeQueueSema = new Semaphore(0);

        final int MAX_UNCONFIRMED_BYTES = 10000;
        final int MAX_UNCONFIRMED_MESSAGES = 1000;
        final int INITIAL_CUSTOMER_PRIORITY = 2;
        final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(1);
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, events::push);
        QueueStateManager queueManager =
                (QueueStateManager) TestTools.getInternalState(session, "queueStateManager");
        assertNotNull(queueManager);

        server.pushSuccess();
        try {
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));
            long flags = QueueFlags.setReader(0);
            QueueOptions queueOptions =
                    QueueOptions.builder()
                            .setMaxUnconfirmedBytes(MAX_UNCONFIRMED_BYTES)
                            .setMaxUnconfirmedMessages(MAX_UNCONFIRMED_MESSAGES)
                            .setConsumerPriority(INITIAL_CUSTOMER_PRIORITY)
                            .build();
            QueueHandle q1 = createQueue(session, createUri(), flags);

            // check UNKNOWN_QUEUE result code
            assertEquals(
                    CloseQueueResult.UNKNOWN_QUEUE,
                    q1.closeAsync(SEQUENCE_TIMEOUT).get(TEST_FUTURE_TIMEOUT));

            server.pushSuccess(2);

            q1.openAsync(queueOptions, SEQUENCE_TIMEOUT);
            // check ALREADY_IN_PROGRESS result code
            assertEquals(
                    CloseQueueResult.ALREADY_IN_PROGRESS,
                    q1.closeAsync(SEQUENCE_TIMEOUT).get(TEST_FUTURE_TIMEOUT));

            TestTools.sleepForSeconds(1);
            // Check substreamcount has incremented
            assertEquals(1, queueManager.getSubStreamCount((QueueImpl) q1));

            server.pushItem(StatusCategory.E_SUCCESS, Duration.ofSeconds(2));
            // Ok for configure response but out of time
            server.pushItem(StatusCategory.E_SUCCESS, closeQueueSema);
            // Ok for close request

            assertEquals(CloseQueueResult.TIMEOUT, q1.close(SEQUENCE_TIMEOUT));

            // check ALREADY_CLOSED or UNKNOWN_QUEUE result code
            CloseQueueCode res = q1.close(SEQUENCE_TIMEOUT);
            assertTrue(
                    (res == CloseQueueResult.ALREADY_CLOSED
                            || res == CloseQueueResult.UNKNOWN_QUEUE));

            TestTools.acquireSema(closeQueueSema);
            // Check substreamcount and close request
            assertEquals(0, queueManager.getSubStreamCount((QueueImpl) q1));

            // Verify requests
            int reqId = 0;

            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(++reqId, server.nextClientRequest(), q1.getParameters());

            server.pushSuccess();
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());

            // check generated events
            assertEquals(3, events.size());
            QueueControlEvent queueControlEvent = (QueueControlEvent) events.pollLast();
            assertNotNull(queueControlEvent);
            assertEquals(
                    QueueControlEvent.Type.e_QUEUE_CLOSE_RESULT, queueControlEvent.getEventType());

            assertEquals(CloseQueueResult.UNKNOWN_QUEUE, queueControlEvent.getStatus());

            queueControlEvent = (QueueControlEvent) events.pollLast();
            assertNotNull(queueControlEvent);
            assertEquals(
                    QueueControlEvent.Type.e_QUEUE_CLOSE_RESULT, queueControlEvent.getEventType());

            assertEquals(CloseQueueResult.ALREADY_IN_PROGRESS, queueControlEvent.getStatus());

            queueControlEvent = (QueueControlEvent) events.pollLast();
            assertNotNull(queueControlEvent);
            assertEquals(
                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT, queueControlEvent.getEventType());

            assertEquals(OpenQueueResult.SUCCESS, queueControlEvent.getStatus());
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            assertEquals(GenericResult.SUCCESS, session.linger());
            server.stop();

            logger.info("=============================================================");
            logger.info("END Testing BrokerSessionIT closeQueuePreconditionFailures.");
            logger.info("=============================================================");
        }
    }

    /**
     * Critical test to check opened queue can be closed if there is a pending configure request.
     * TODO: is 'just after' done good enough?
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
    // Temporarily disable this test
    // Until achieved more stable repeatability on slow hosts
    // @Test
    public void closeQueueOnPendingConfigureRequest() {
        logger.info("==================================================================");
        logger.info("BEGIN Testing BrokerSessionIT closeQueueOnPendingConfigureRequest.");
        logger.info("==================================================================");

        logger.info("Step 1. Start broker simulator in manual mode");

        int reqId = 0;

        // start test server in manual mode
        final int MAX_UNCONFIRMED_BYTES = 10000;
        final int MAX_UNCONFIRMED_MESSAGES = 1000;
        final int INITIAL_CUSTOMER_PRIORITY = 2;
        final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(5);
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        // create broker session
        HostHealthMonitor monitor = mock(HostHealthMonitor.class);
        // setup monitor to return healthy state by default
        when(monitor.hostHealthState()).thenReturn(HostHealthState.Healthy);

        SessionOptions sessionOptions =
                SessionOptions.builder()
                        .setBrokerUri(server.getURI())
                        .setHostHealthMonitor(monitor)
                        .build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, events::push);

        server.pushSuccess();
        try {
            // start broker session
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));

            logger.info("Step 2. Create queue");

            long flags = QueueFlags.setReader(0);
            QueueOptions queueOptions =
                    QueueOptions.builder()
                            .setMaxUnconfirmedBytes(MAX_UNCONFIRMED_BYTES)
                            .setMaxUnconfirmedMessages(MAX_UNCONFIRMED_MESSAGES)
                            .setConsumerPriority(INITIAL_CUSTOMER_PRIORITY)
                            .setSuspendsOnBadHostHealth(true)
                            .build();
            QueueHandle q1 = createQueue(session, createUri(), flags);

            logger.info("Step 3. Close queue when there is a pending standalone configure request");

            // open the queue async
            server.pushSuccess(2);
            BmqFuture<OpenQueueCode> openFuture = q1.openAsync(queueOptions, SEQUENCE_TIMEOUT);

            // just after start of open queue sequence attempt to close given queue
            // check that close operation returned rc=ALREADY_IN_PROGRESS
            assertEquals(
                    CloseQueueResult.ALREADY_IN_PROGRESS,
                    q1.closeAsync(SEQUENCE_TIMEOUT).get(TEST_FUTURE_TIMEOUT));

            logger.info("Step 4. Wait until queue is opened");

            assertEquals(OpenQueueResult.SUCCESS, openFuture.get(TEST_FUTURE_TIMEOUT));
            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            logger.info("Step 5. Send configure request to the queue");

            QueueOptions newOptions =
                    QueueOptions.builder()
                            .setMaxUnconfirmedBytes(MAX_UNCONFIRMED_BYTES - 100)
                            .setMaxUnconfirmedMessages(MAX_UNCONFIRMED_MESSAGES - 10)
                            .setConsumerPriority(INITIAL_CUSTOMER_PRIORITY)
                            .build();

            BmqFuture<ConfigureQueueCode> configureFuture =
                    q1.configureAsync(newOptions, SEQUENCE_TIMEOUT);
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            logger.info("Step 6. Try to close the queue during configure");

            // just after sending the configure request attempt to close given queue
            // check that configure is canceled and close operation returned rc=SUCCESS
            server.pushSuccess(2);
            assertEquals(CloseQueueResult.SUCCESS, q1.close(SEQUENCE_TIMEOUT));
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(++reqId, server.nextClientRequest(), q1.getParameters());

            // Standalone configure request should be canceled.
            assertEquals(ConfigureQueueResult.CANCELED, configureFuture.get(TEST_FUTURE_TIMEOUT));

            logger.info("Step 7. Check generated events");

            assertEquals(3, events.size());
            verifyQueueControlEvent(
                    events.pollLast(),
                    QueueControlEvent.Type.e_QUEUE_CLOSE_RESULT,
                    CloseQueueResult.ALREADY_IN_PROGRESS,
                    q1);
            verifyQueueControlEvent(
                    events.pollLast(),
                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT,
                    OpenQueueResult.SUCCESS,
                    q1);
            verifyQueueControlEvent(
                    events.pollLast(),
                    QueueControlEvent.Type.e_QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.CANCELED,
                    q1);

            logger.info("Step 8. Close queue when there is a pending suspend configure request");
            // open the queue
            server.pushSuccess(2);
            assertEquals(OpenQueueResult.SUCCESS, q1.open(queueOptions, SEQUENCE_TIMEOUT));
            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            // set host health to unhealthy
            session.onHostHealthStateChanged(HostHealthState.Unhealthy);
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            // just after sending the suspend request attempt to close given queue
            // check that suspension is canceled and close operation returned rc=SUCCESS
            server.pushSuccess(2);
            assertEquals(CloseQueueResult.SUCCESS, q1.close(SEQUENCE_TIMEOUT));
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(++reqId, server.nextClientRequest(), q1.getParameters());

            logger.info("Step 9. Check generated events");

            assertEquals(1, events.size());
            verifyBrokerSessionEvent(events.pollLast(), BrokerSessionEvent.Type.e_HOST_UNHEALTHY);

            logger.info("Step 10. Close queue when there is a pending resume configure request");

            // open the queue
            server.pushSuccess(1);
            assertEquals(OpenQueueResult.SUCCESS, q1.open(queueOptions, SEQUENCE_TIMEOUT));
            verifyOpenRequest(++reqId, server.nextClientRequest());
            assertNull(server.nextClientRequest()); // no configure request

            // set host health back to healthy
            session.onHostHealthStateChanged(HostHealthState.Healthy);
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            // just after sending the resume request attempt to close given queue
            // check that resuming is canceled and close operation returned rc=SUCCESS
            server.pushSuccess(2);
            assertEquals(CloseQueueResult.SUCCESS, q1.close(SEQUENCE_TIMEOUT));
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(++reqId, server.nextClientRequest(), q1.getParameters());

            logger.info("Step 11. Check generated events");

            assertEquals(1, events.size());
            verifyBrokerSessionEvent(
                    events.pollLast(), BrokerSessionEvent.Type.e_HOST_HEALTH_RESTORED);

            logger.info("Step 12. Stop the session");

            server.pushSuccess();
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());

        } catch (TimeoutException e) {
            logger.error("Timeout: ", e);
            fail();
        } catch (Exception e) {
            logger.error("Exception: ", e);
            fail();
        } finally {
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            assertEquals(GenericResult.SUCCESS, session.linger());
            server.stop();

            logger.info("===================================================================");
            logger.info("END Testing BrokerSessionIT closeQueueOnPendingConfigureRequest.");
            logger.info("===================================================================");
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
    public void closeQueueOnDeferredResumeRequest() {
        logger.info("================================================================");
        logger.info("BEGIN Testing BrokerSessionIT closeQueueOnDeferredResumeRequest.");
        logger.info("================================================================");

        int reqId = 0;

        // start test server in manual mode
        final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(1);
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        // create broker session
        HostHealthMonitor monitor = mock(HostHealthMonitor.class);
        // setup monitor to return healthy state by default
        when(monitor.hostHealthState()).thenReturn(HostHealthState.Healthy);

        SessionOptions sessionOptions =
                SessionOptions.builder()
                        .setBrokerUri(server.getURI())
                        .setHostHealthMonitor(monitor)
                        .build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, events::push);

        server.pushSuccess();
        try {
            // start broker session
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));

            // create queue
            long flags = QueueFlags.setReader(0);
            QueueHandle queue = createQueue(session, createUri(), flags);

            // open the queue
            server.pushSuccess(2);
            QueueOptions options = QueueOptions.builder().setSuspendsOnBadHostHealth(true).build();
            assertEquals(OpenQueueResult.SUCCESS, queue.open(options, SEQUENCE_TIMEOUT));

            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            // check generated events
            assertEquals(0, events.size());

            // set host health to unhealthy
            session.onHostHealthStateChanged(HostHealthState.Unhealthy);
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            // check generated events
            assertEquals(1, events.size());
            verifyBrokerSessionEvent(events.pollLast(), BrokerSessionEvent.Type.e_HOST_UNHEALTHY);

            // set host health back to healthy
            session.onHostHealthStateChanged(HostHealthState.Healthy);

            // check that resuming is deferred
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // close the queue
            server.pushSuccess(2);
            assertEquals(CloseQueueResult.SUCCESS, queue.close(SEQUENCE_TIMEOUT));
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(++reqId, server.nextClientRequest(), queue.getParameters());

            // check generated events
            // check HOST_HEALTH_RESTORED event
            // check that there is no QUEUE_SUSPENDED and QUEUE_RESUMED events

            assertEquals(1, events.size());
            verifyBrokerSessionEvent(
                    events.pollLast(), BrokerSessionEvent.Type.e_HOST_HEALTH_RESTORED);

            // Stop the session
            server.pushSuccess();
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());

        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            assertEquals(GenericResult.SUCCESS, session.linger());
            server.stop();

            logger.info("==============================================================");
            logger.info("END Testing BrokerSessionIT closeQueueOnDeferredResumeRequest.");
            logger.info("==============================================================");
        }
    }

    /**
     * Critical test to check return statuses of precondition failure of configure queue sequence.
     *
     * <ul>
     *   <li>start test server in active mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>create queue
     *   <li>attempt to configure queue
     *   <li>check that configure operation returned rc=INVALID_QUEUE
     *   <li>open the queue synchronously
     *   <li>start configure queue sequence asynchronously
     *   <li>just after start of configure queue sequence attempt to configure given queue
     *   <li>check that configure operation returned rc=ALREADY_IN_PROGRESS
     *   <li>close queue
     *   <li>stop and linger broker session and stop server
     *   <li>check that 3 events were posted: - QUEUE_OPEN_RESULT/INVALID_QUEUE -
     *       QUEUE_OPEN_RESULT/ALREADY_IN_PROGRESS - QUEUE_OPEN_RESULT/SUCCESS
     * </ul>
     */
    @Test
    public void configureQueuePreconditionFailures() {
        logger.info("===================================================================");
        logger.info("BEGIN Testing BrokerSessionIT configureQueuePreconditionFailures.");
        logger.info("===================================================================");

        final int MAX_UNCONFIRMED_BYTES = 10000;
        final int MAX_UNCONFIRMED_MESSAGES = 1000;
        final int INITIAL_CUSTOMER_PRIORITY = 2;
        final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(1);
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_AUTO_MODE);
        server.start();

        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, events::push);

        QueueStateManager queueManager =
                (QueueStateManager) TestTools.getInternalState(session, "queueStateManager");
        assertNotNull(queueManager);

        try {
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));
            long flags = QueueFlags.setReader(0);
            QueueOptions queueOptions =
                    QueueOptions.builder()
                            .setMaxUnconfirmedBytes(MAX_UNCONFIRMED_BYTES)
                            .setMaxUnconfirmedMessages(MAX_UNCONFIRMED_MESSAGES)
                            .setConsumerPriority(INITIAL_CUSTOMER_PRIORITY)
                            .build();
            QueueImpl q1 = createQueue(session, createUri(), flags);

            // check INVALID_QUEUE result code
            assertEquals(
                    ConfigureQueueResult.INVALID_QUEUE,
                    q1.configureAsync(queueOptions, SEQUENCE_TIMEOUT).get(TEST_FUTURE_TIMEOUT));

            // Check substreamcount is zero
            assertEquals(0, queueManager.getSubStreamCount(q1));

            assertEquals(OpenQueueResult.SUCCESS, q1.open(queueOptions, SEQUENCE_TIMEOUT));

            // Check substreamcount has incremented
            assertEquals(1, queueManager.getSubStreamCount(q1));

            BmqFuture<ConfigureQueueCode> future1 =
                    q1.configureAsync(queueOptions, SEQUENCE_TIMEOUT);
            BmqFuture<ConfigureQueueCode> future2 =
                    q1.configureAsync(queueOptions, SEQUENCE_TIMEOUT);
            assertEquals(ConfigureQueueResult.SUCCESS, future1.get(TEST_FUTURE_TIMEOUT));
            // check ALREADY_IN_PROGRESS result code
            assertEquals(
                    ConfigureQueueResult.ALREADY_IN_PROGRESS, future2.get(TEST_FUTURE_TIMEOUT));
            TestTools.sleepForSeconds(1);

            // Check substreamcount hasn't changed
            assertEquals(1, queueManager.getSubStreamCount(q1));

            assertEquals(CloseQueueResult.SUCCESS, q1.close(SEQUENCE_TIMEOUT));

            // Verify requests
            int reqId = 0;

            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(++reqId, server.nextClientRequest(), q1.getParameters());

            // Check substreamcount has decremented
            assertEquals(0, queueManager.getSubStreamCount(q1));

            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());

            // check generated events
            assertEquals(3, events.size());
            QueueControlEvent queueControlEvent = (QueueControlEvent) events.pollLast();
            assertNotNull(queueControlEvent);
            assertEquals(
                    QueueControlEvent.Type.e_QUEUE_CONFIGURE_RESULT,
                    queueControlEvent.getEventType());
            assertEquals(ConfigureQueueResult.INVALID_QUEUE, queueControlEvent.getStatus());

            queueControlEvent = (QueueControlEvent) events.pollLast();
            assertNotNull(queueControlEvent);
            assertEquals(
                    QueueControlEvent.Type.e_QUEUE_CONFIGURE_RESULT,
                    queueControlEvent.getEventType());
            assertEquals(ConfigureQueueResult.ALREADY_IN_PROGRESS, queueControlEvent.getStatus());

            queueControlEvent = (QueueControlEvent) events.pollLast();
            assertNotNull(queueControlEvent);
            assertEquals(
                    QueueControlEvent.Type.e_QUEUE_CONFIGURE_RESULT,
                    queueControlEvent.getEventType());
            assertEquals(ConfigureQueueResult.SUCCESS, queueControlEvent.getStatus());
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } finally {
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            assertEquals(GenericResult.SUCCESS, session.linger());
            server.stop();

            logger.info("=================================================================");
            logger.info("END Testing BrokerSessionIT configureQueuePreconditionFailures.");
            logger.info("=================================================================");
        }
    }

    /**
     * Critical test to check that standalone configure request for suspended queue is sent only
     * when the queue resumes.
     *
     * <ul>
     *   <li>start test server in active mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>create insensitive and sensitive to host health queues
     *   <li>open the queues
     *   <li>configure insensitive queue with unspecified host-health awareness
     *   <li>configure sensitive queue with unspecified host-health awareness
     *   <li>configure insensitive write-only queue with unspecified host-health awareness
     *   <li>set host health to unhealthy
     *   <li>check insensitive queues are not suspended, sensitive one is suspended
     *   <li>configure insensitive queue (configuration request is sent)
     *   <li>configure sensitive queue (configuration request is not sent)
     *   <li>configure insensitive write-only queue (configuration request is not sent)
     *   <li>reconfigure insensitive write-only queue to sensitive (configuration request is not
     *       sent)
     *   <li>set host health back to healthy
     *   <li>check that sensitive queue resume request with new options is sent
     *   <li>check all queues are not suspended
     *   <li>stop and linger broker session and stop server
     * </ul>
     */
    @Test
    public void configureSuspendedQueue() throws ExecutionException, InterruptedException {
        logger.info("======================================================");
        logger.info("BEGIN Testing BrokerSessionIT configureSuspendedQueue.");
        logger.info("======================================================");

        final Semaphore testSema = new Semaphore(0);
        final int MAX_UNCONFIRMED_BYTES = 5000000;
        final int MAX_UNCONFIRMED_MESSAGES = 500;
        final int CONSUMER_PRIORITY = 100;
        final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(1);

        int reqId = 0;
        // start test server in active mode
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_AUTO_MODE);
        server.start();

        // create broker session
        HostHealthMonitor monitor = mock(HostHealthMonitor.class);
        // setup monitor to return healthy state by default
        when(monitor.hostHealthState()).thenReturn(HostHealthState.Healthy);

        SessionOptions sessionOptions =
                SessionOptions.builder()
                        .setBrokerUri(server.getURI())
                        .setHostHealthMonitor(monitor)
                        .build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

        LinkedList<Event> events = new LinkedList<>();

        com.bloomberg.bmq.impl.events.EventHandler eventDispatcher =
                new com.bloomberg.bmq.impl.events.EventHandler() {
                    @Override
                    public void handleEvent(Event event) {
                        logger.error("Unexpected event caught: {}", event);
                        addEvent(event);
                    }

                    @Override
                    public void handleBrokerSessionEvent(BrokerSessionEvent event) {
                        logger.info("Broker session event: {}", event);
                        addEvent(event);
                    }

                    @Override
                    public void handleQueueEvent(QueueControlEvent event) {
                        logger.info("Queue control event: {}", event);
                        addEvent(event);

                        QueueHandle queue = event.getQueue();
                        if (queue == null) {
                            throw new RuntimeException(
                                    "Failure: Queue is null. Ev: " + event.toString());
                        }
                        queue.handleQueueEvent(event);
                    }

                    private void addEvent(Event event) {
                        events.push(event);
                        testSema.release();
                    }
                };

        EventHandler eventHandler = event -> event.dispatch(eventDispatcher);
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, eventHandler);

        try {
            // start broker session
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));
            long readerFlags = QueueFlags.setReader(0);
            long writerFlags = QueueFlags.setWriter(0);

            // create queues
            // Use non-null QueueControlEventHandler in order to re(set) 'isSuspended' flag
            // when queue events are dispatching and handled by `QueueImpl::handleQueueEvent`
            // method.
            QueueControlEventHandler queueHandler = mock(QueueControlEventHandler.class);
            QueueImpl q1 = createQueue(session, createUri(), readerFlags, queueHandler);
            QueueImpl q2 = createQueue(session, createUri(), readerFlags, queueHandler);
            QueueImpl q3 = createQueue(session, createUri(), writerFlags, queueHandler);

            // open the queues
            QueueOptions insensitiveOptions =
                    QueueOptions.builder().setMaxUnconfirmedBytes(MAX_UNCONFIRMED_BYTES).build();

            assertEquals(OpenQueueResult.SUCCESS, q1.open(insensitiveOptions, SEQUENCE_TIMEOUT));
            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            QueueOptions sensitiveOptions =
                    QueueOptions.builder()
                            .setSuspendsOnBadHostHealth(true)
                            .setMaxUnconfirmedBytes(MAX_UNCONFIRMED_BYTES)
                            .build();

            assertEquals(OpenQueueResult.SUCCESS, q2.open(sensitiveOptions, SEQUENCE_TIMEOUT));
            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            assertEquals(OpenQueueResult.SUCCESS, q3.open(insensitiveOptions, SEQUENCE_TIMEOUT));
            verifyOpenRequest(++reqId, server.nextClientRequest());
            // For write-only queue there should be no configure request
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // configure insensitive queue with unspecified host-health awareness
            QueueOptions hostHealthUnspecifiedOptions =
                    QueueOptions.builder().setConsumerPriority(CONSUMER_PRIORITY).build();

            assertEquals(
                    ConfigureQueueResult.SUCCESS,
                    q1.configureAsync(hostHealthUnspecifiedOptions, SEQUENCE_TIMEOUT)
                            .get(TEST_FUTURE_TIMEOUT));
            // wait for queue event
            TestTools.acquireSema(testSema);
            verifyQueueControlEvent(
                    events.pollLast(),
                    QueueControlEvent.Type.e_QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    q1);
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            // configure sensitive queue with unspecified host-health awareness
            assertEquals(
                    ConfigureQueueResult.SUCCESS,
                    q2.configure(hostHealthUnspecifiedOptions, SEQUENCE_TIMEOUT));
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            // configure insensitive write-only queue with unspecified host-health awareness
            assertEquals(
                    ConfigureQueueResult.SUCCESS,
                    q3.configure(hostHealthUnspecifiedOptions, SEQUENCE_TIMEOUT));
            // there should be no configure requests
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // set host health to unhealthy
            session.onHostHealthStateChanged(HostHealthState.Unhealthy);

            // wait for two events
            TestTools.acquireSema(testSema);
            verifyBrokerSessionEvent(events.pollLast(), BrokerSessionEvent.Type.e_HOST_UNHEALTHY);

            TestTools.acquireSema(testSema);
            verifyQueueControlEvent(
                    events.pollLast(),
                    QueueControlEvent.Type.e_QUEUE_SUSPEND_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    q2);

            assertEquals(0, events.size());

            verifyConfigureRequest(++reqId, server.nextClientRequest());
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // check insensitive queues are not suspended, sensitive one is suspended
            assertFalse(service.submit(q1::getIsSuspendedWithBroker).get());
            assertFalse(q1.getIsSuspended());
            assertTrue(service.submit(q2::getIsSuspendedWithBroker).get());
            assertTrue(q2.getIsSuspended());
            assertFalse(service.submit(q3::getIsSuspendedWithBroker).get());
            assertFalse(q3.getIsSuspended());

            // configure insensitive queue (configuration request is sent)
            QueueOptions newOptions =
                    QueueOptions.builder()
                            .setMaxUnconfirmedMessages(MAX_UNCONFIRMED_MESSAGES)
                            .build();

            assertEquals(ConfigureQueueResult.SUCCESS, q1.configure(newOptions, SEQUENCE_TIMEOUT));
            {
                ControlMessageChoice msg = server.nextClientRequest();
                verifyConfigureRequest(++reqId, msg);

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
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            assertEquals(
                    QueueOptions.builder()
                            .merge(insensitiveOptions)
                            .merge(hostHealthUnspecifiedOptions)
                            .merge(newOptions)
                            .build(),
                    service.submit(q1::getQueueOptions).get());

            // configure sensitive queue (configuration request is not sent)
            assertEquals(ConfigureQueueResult.SUCCESS, q2.configure(newOptions, SEQUENCE_TIMEOUT));
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            assertEquals(
                    QueueOptions.builder()
                            .merge(sensitiveOptions)
                            .merge(hostHealthUnspecifiedOptions)
                            .merge(newOptions)
                            .build(),
                    service.submit(q2::getQueueOptions).get());

            // configure insensitive write-only queue (configuration request is not sent)
            assertEquals(ConfigureQueueResult.SUCCESS, q3.configure(newOptions, SEQUENCE_TIMEOUT));
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            assertEquals(
                    QueueOptions.builder()
                            .merge(insensitiveOptions)
                            .merge(hostHealthUnspecifiedOptions)
                            .merge(newOptions)
                            .build(),
                    service.submit(q3::getQueueOptions).get());

            // reconfigure insensitive write-only queue to become sensitive (configuration request
            // is not sent)
            assertEquals(
                    ConfigureQueueResult.SUCCESS,
                    q3.configureAsync(sensitiveOptions, SEQUENCE_TIMEOUT).get(TEST_FUTURE_TIMEOUT));
            // wait for queue events
            TestTools.acquireSema(testSema);
            verifyQueueControlEvent(
                    events.pollLast(),
                    QueueControlEvent.Type.e_QUEUE_SUSPEND_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    q3);
            TestTools.acquireSema(testSema);
            verifyQueueControlEvent(
                    events.pollLast(),
                    QueueControlEvent.Type.e_QUEUE_CONFIGURE_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    q3);
            assertEquals(0, events.size());
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            assertEquals(
                    QueueOptions.builder()
                            .merge(insensitiveOptions)
                            .merge(hostHealthUnspecifiedOptions)
                            .merge(newOptions)
                            .merge(sensitiveOptions)
                            .build(),
                    service.submit(q3::getQueueOptions).get());

            // check the queue is suspended
            assertTrue(service.submit(q3::getIsSuspendedWithBroker).get());
            assertTrue(q3.getIsSuspended());

            // set host health back to healthy
            session.onHostHealthStateChanged(HostHealthState.Healthy);

            // wait for three events
            // write-only queue should be resumed earlier due to skipped configure request
            TestTools.acquireSema(testSema);
            verifyQueueControlEvent(
                    events.pollLast(),
                    QueueControlEvent.Type.e_QUEUE_RESUME_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    q3);
            TestTools.acquireSema(testSema);
            verifyQueueControlEvent(
                    events.pollLast(),
                    QueueControlEvent.Type.e_QUEUE_RESUME_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    q2);
            TestTools.acquireSema(testSema);
            verifyBrokerSessionEvent(
                    events.pollLast(), BrokerSessionEvent.Type.e_HOST_HEALTH_RESTORED);

            assertEquals(0, events.size());

            // check that sensitive queue resume request with new options is sent
            {
                ControlMessageChoice msg = server.nextClientRequest();
                verifyConfigureRequest(++reqId, msg);

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

            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // check all queues are not suspended
            assertFalse(service.submit(q1::getIsSuspendedWithBroker).get());
            assertFalse(q1.getIsSuspended());
            assertFalse(service.submit(q2::getIsSuspendedWithBroker).get());
            assertFalse(q2.getIsSuspended());
            assertFalse(service.submit(q3::getIsSuspendedWithBroker).get());
            assertFalse(q3.getIsSuspended());

            // Stop the session
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            assertEquals(GenericResult.SUCCESS, session.linger());
            server.stop();

            logger.info("====================================================");
            logger.info("END Testing BrokerSessionIT configureSuspendedQueue.");
            logger.info("====================================================");
        }
    }

    /**
     * Critical test to check that configure requests properly send parameters to broker and save
     * options in different use cases.
     *
     * <ul>
     *   <li>start test server in active mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>open sensitive queue (send parameters, update options)
     *   <li>bad configure queue (send parameters)
     *   <li>configure queue (send parameters, update options)
     *   <li>set host health to unhealthy
     *   <li>queue suspends (send deconfigure parameters)
     *   <li>queue closes (send deconfigure parameters)
     *   <li>queue opens
     *   <li>bad reconfigure to insensitive (send new parameters)
     *   <li>queue suspends (send deconfigure parameters)
     *   <li>reconfigure to insensitive (send new parameters, update options)
     *   <li>bad reconfigure to sensitive (send deconfigure parameters)
     *   <li>reconnecting and reopen queue
     *   <li>reconfigure to sensitive (send deconfigure parameters, update options)
     *   <li>set host health to healthy
     *   <li>queue resumes (send queue parameters)
     *   <li>queue closes (send deconfigure parameters)
     *   <li>stop and linger broker session and stop server
     * </ul>
     */
    @Test
    public void configureOptionsAndParameters() throws ExecutionException, InterruptedException {
        logger.info("======================================================");
        logger.info("BEGIN Testing BrokerSessionIT configureOptionsAndParameters.");
        logger.info("======================================================");

        final Semaphore testSema = new Semaphore(0);
        final int MAX_UNCONFIRMED_BYTES = 5000000;
        final int MAX_UNCONFIRMED_MESSAGES = 500;
        final int CONSUMER_PRIORITY = 100;
        final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(1);

        int reqId = 0;
        logger.info("1. start test server in manual mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        logger.info("2. create broker session");
        HostHealthMonitor monitor = mock(HostHealthMonitor.class);
        // setup monitor to return healthy state by default
        when(monitor.hostHealthState()).thenReturn(HostHealthState.Healthy);

        SessionOptions sessionOptions =
                SessionOptions.builder()
                        .setBrokerUri(server.getURI())
                        .setHostHealthMonitor(monitor)
                        .build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

        LinkedList<Event> events = new LinkedList<>();

        com.bloomberg.bmq.impl.events.EventHandler eventDispatcher =
                new com.bloomberg.bmq.impl.events.EventHandler() {
                    @Override
                    public void handleEvent(Event event) {
                        logger.error("Unexpected event caught: {}", event);
                        events.push(event);
                    }

                    @Override
                    public void handleBrokerSessionEvent(BrokerSessionEvent event) {
                        logger.info("Handle session event: {}", event);
                        events.push(event);
                    }

                    @Override
                    public void handleQueueEvent(QueueControlEvent event) {
                        logger.info("Handle queue event: {}", event);
                        events.push(event);

                        QueueHandle queue = event.getQueue();
                        if (queue == null) {
                            throw new RuntimeException(
                                    "Failure: Queue is null. Ev: " + event.toString());
                        }
                        queue.handleQueueEvent(event);
                    }
                };

        EventHandler eventHandler =
                (Event event) -> {
                    event.dispatch(eventDispatcher);
                    testSema.release();
                };
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, eventHandler);

        try {
            logger.info("3. start broker session");
            server.pushSuccess(); // OK for nego
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));
            long flags = QueueFlags.setReader(0);

            logger.info("4. open sensitive queue (send parameters, update options)");
            // Use non-null QueueControlEventHandler in order to re(set) 'isSuspended' flag
            // when queue events are dispatching and handled by `QueueImpl::handleQueueEvent`
            // method.
            QueueImpl queue =
                    createQueue(session, createUri(), flags, mock(QueueControlEventHandler.class));

            QueueOptions openOptions =
                    QueueOptions.builder().setSuspendsOnBadHostHealth(true).build();

            server.pushSuccess(2);
            assertEquals(OpenQueueResult.SUCCESS, queue.open(openOptions, SEQUENCE_TIMEOUT));

            assertFalse(service.submit(queue::getIsSuspendedWithBroker).get());
            assertFalse(queue.getIsSuspended());

            verifyOpenRequest(++reqId, server.nextClientRequest());

            // Verify configure parameters
            QueueOptions currentOptions = openOptions;
            verifyConfigureRequest(++reqId, server.nextClientRequest(), currentOptions);
            // Verify options are updated
            assertEquals(currentOptions, service.submit(queue::getQueueOptions).get());

            logger.info("5. bad configure queue (send parameters)");
            QueueOptions configureOptions =
                    QueueOptions.builder().setConsumerPriority(CONSUMER_PRIORITY).build();

            server.pushItem(StatusCategory.E_UNKNOWN);
            assertEquals(
                    ConfigureQueueResult.UNKNOWN,
                    queue.configure(configureOptions, SEQUENCE_TIMEOUT));

            assertFalse(service.submit(queue::getIsSuspendedWithBroker).get());
            assertFalse(queue.getIsSuspended());

            // Verify configure parameters
            verifyConfigureRequest(
                    ++reqId,
                    server.nextClientRequest(),
                    mergeOptions(currentOptions, configureOptions));
            // Verify options are not updated
            assertEquals(currentOptions, service.submit(queue::getQueueOptions).get());

            logger.info("6. configure queue (send parameters, update options)");
            server.pushSuccess();
            assertEquals(
                    ConfigureQueueResult.SUCCESS,
                    queue.configure(configureOptions, SEQUENCE_TIMEOUT));

            assertFalse(service.submit(queue::getIsSuspendedWithBroker).get());
            assertFalse(queue.getIsSuspended());

            // Verify configure parameters
            currentOptions = mergeOptions(currentOptions, configureOptions);
            verifyConfigureRequest(++reqId, server.nextClientRequest(), currentOptions);
            // Verify options are updated
            assertEquals(currentOptions, service.submit(queue::getQueueOptions).get());

            logger.info("7. set host health to unhealthy");
            server.pushSuccess();
            session.onHostHealthStateChanged(HostHealthState.Unhealthy);

            TestTools.acquireSema(testSema);
            verifyBrokerSessionEvent(events.pollLast(), BrokerSessionEvent.Type.e_HOST_UNHEALTHY);

            logger.info("8. queue suspends (send deconfigure parameters)");
            TestTools.acquireSema(testSema);
            verifyQueueControlEvent(
                    events.pollLast(),
                    QueueControlEvent.Type.e_QUEUE_SUSPEND_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    queue);

            assertTrue(service.submit(queue::getIsSuspendedWithBroker).get());
            assertTrue(queue.getIsSuspended());

            // Verify deconfigure parameters
            QueueOptions deconfigureOptions =
                    QueueOptions.builder()
                            .setMaxUnconfirmedMessages(0)
                            .setMaxUnconfirmedBytes(0)
                            .setConsumerPriority(StreamParameters.CONSUMER_PRIORITY_INVALID)
                            .build();

            verifyConfigureRequest(++reqId, server.nextClientRequest(), deconfigureOptions);
            // Verify options are not updated
            assertEquals(currentOptions, service.submit(queue::getQueueOptions).get());

            logger.info("9. queue closes (send deconfigure parameters)");
            server.pushSuccess(2);

            assertEquals(CloseQueueResult.SUCCESS, queue.close(SEQUENCE_TIMEOUT));

            assertTrue(service.submit(queue::getIsSuspendedWithBroker).get());
            assertTrue(queue.getIsSuspended());

            // Verify deconfigure parameters
            verifyConfigureRequest(++reqId, server.nextClientRequest(), deconfigureOptions);
            // Verify options are not updated
            assertEquals(currentOptions, service.submit(queue::getQueueOptions).get());

            verifyCloseRequest(++reqId, server.nextClientRequest(), queue.getParameters());

            // Clear options
            service.submit(() -> queue.setQueueOptions(null));

            logger.info("10. queue opens");
            server.pushSuccess(1);

            assertEquals(OpenQueueResult.SUCCESS, queue.open(currentOptions, SEQUENCE_TIMEOUT));

            assertTrue(service.submit(queue::getIsSuspendedWithBroker).get());
            assertTrue(queue.getIsSuspended());

            verifyOpenRequest(++reqId, server.nextClientRequest());

            // Verify configure request is not sent
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // Verify options are updated
            assertEquals(currentOptions, service.submit(queue::getQueueOptions).get());

            logger.info("11. bad reconfigure to insensitive (send new parameters)");
            QueueOptions insensitiveOptions =
                    QueueOptions.builder()
                            .setMaxUnconfirmedMessages(MAX_UNCONFIRMED_MESSAGES)
                            .setSuspendsOnBadHostHealth(false)
                            .build();

            server.pushItem(StatusCategory.E_UNKNOWN);
            server.pushSuccess(); // for subsequent suspend request
            assertEquals(
                    ConfigureQueueResult.UNKNOWN,
                    queue.configure(insensitiveOptions, SEQUENCE_TIMEOUT));

            assertFalse(queue.getIsSuspended());

            // Verify configure parameters
            verifyConfigureRequest(
                    ++reqId,
                    server.nextClientRequest(),
                    mergeOptions(currentOptions, insensitiveOptions));
            // Verify options are not updated
            assertEquals(currentOptions, service.submit(queue::getQueueOptions).get());

            logger.info("12. queue suspends (send deconfigure parameters)");
            TestTools.acquireSema(testSema);
            verifyQueueControlEvent(
                    events.pollLast(),
                    QueueControlEvent.Type.e_QUEUE_SUSPEND_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    queue);

            assertTrue(service.submit(queue::getIsSuspendedWithBroker).get());
            assertTrue(queue.getIsSuspended());

            // Verify deconfigure parameters
            verifyConfigureRequest(++reqId, server.nextClientRequest(), deconfigureOptions);
            // Verify options are not updated
            assertEquals(currentOptions, service.submit(queue::getQueueOptions).get());

            logger.info("13. reconfigure to insensitive (send new parameters, update options)");
            server.pushSuccess();
            assertEquals(
                    ConfigureQueueResult.SUCCESS,
                    queue.configure(insensitiveOptions, SEQUENCE_TIMEOUT));

            assertFalse(service.submit(queue::getIsSuspendedWithBroker).get());
            assertFalse(queue.getIsSuspended());

            // Verify configure parameters
            currentOptions = mergeOptions(currentOptions, insensitiveOptions);
            verifyConfigureRequest(++reqId, server.nextClientRequest(), currentOptions);
            // Verify options are updated
            assertEquals(currentOptions, service.submit(queue::getQueueOptions).get());

            logger.info("14. bad reconfigure to sensitive (send deconfigure parameters)");
            QueueOptions sensitiveOptions =
                    QueueOptions.builder()
                            .setMaxUnconfirmedBytes(MAX_UNCONFIRMED_BYTES)
                            .setSuspendsOnBadHostHealth(true)
                            .build();

            server.pushItem(StatusCategory.E_UNKNOWN);
            server.pushSuccess(3); // for subsequent nego, reopen and conf requests
            assertEquals(
                    ConfigureQueueResult.UNKNOWN,
                    queue.configure(sensitiveOptions, SEQUENCE_TIMEOUT));

            assertTrue(queue.getIsSuspended());

            // Verify deconfigure parameters
            verifyConfigureRequest(++reqId, server.nextClientRequest(), deconfigureOptions);
            // Verify options are not updated
            assertEquals(currentOptions, service.submit(queue::getQueueOptions).get());

            logger.info(("15. reconnecting and reopen queue"));
            TestTools.acquireSema(testSema);
            verifyBrokerSessionEvent(events.pollLast(), BrokerSessionEvent.Type.e_CONNECTION_LOST);
            TestTools.acquireSema(testSema);
            verifyBrokerSessionEvent(events.pollLast(), BrokerSessionEvent.Type.e_RECONNECTED);
            TestTools.acquireSema(testSema);
            verifyQueueControlEvent(
                    events.pollLast(),
                    QueueControlEvent.Type.e_QUEUE_REOPEN_RESULT,
                    OpenQueueResult.SUCCESS,
                    queue);

            assertFalse(service.submit(queue::getIsSuspendedWithBroker).get());
            assertFalse(queue.getIsSuspended());

            verifyOpenRequest(++reqId, server.nextClientRequest());
            // Verify queue parameters are sent
            verifyConfigureRequest(++reqId, server.nextClientRequest(), currentOptions);
            // Verify options are not updated
            assertEquals(currentOptions, service.submit(queue::getQueueOptions).get());

            TestTools.acquireSema(testSema);
            verifyBrokerSessionEvent(events.pollLast(), BrokerSessionEvent.Type.e_STATE_RESTORED);

            logger.info(
                    "16. reconfigure to sensitive (send deconfigure parameters, update options)");
            server.pushSuccess();
            assertEquals(
                    ConfigureQueueResult.SUCCESS,
                    queue.configure(sensitiveOptions, SEQUENCE_TIMEOUT));

            assertTrue(service.submit(queue::getIsSuspendedWithBroker).get());
            assertTrue(queue.getIsSuspended());

            // Verify deconfigure parameters
            verifyConfigureRequest(++reqId, server.nextClientRequest(), deconfigureOptions);
            // Verify options are updated
            currentOptions = mergeOptions(currentOptions, sensitiveOptions);
            assertEquals(currentOptions, service.submit(queue::getQueueOptions).get());

            logger.info("17. set host health to healthy");
            server.pushSuccess(); // for resume request
            session.onHostHealthStateChanged(HostHealthState.Healthy);

            logger.info("18. queue resumes (send queue parameters)");
            TestTools.acquireSema(testSema);
            verifyQueueControlEvent(
                    events.pollLast(),
                    QueueControlEvent.Type.e_QUEUE_RESUME_RESULT,
                    ConfigureQueueResult.SUCCESS,
                    queue);

            assertFalse(service.submit(queue::getIsSuspendedWithBroker).get());
            assertFalse(queue.getIsSuspended());

            // Verify queue parameters
            verifyConfigureRequest(++reqId, server.nextClientRequest(), currentOptions);
            // Verify options are not updated
            assertEquals(currentOptions, service.submit(queue::getQueueOptions).get());

            verifyBrokerSessionEvent(
                    events.pollLast(), BrokerSessionEvent.Type.e_HOST_HEALTH_RESTORED);

            logger.info("19. queue closes (send deconfigure parameters)");
            server.pushSuccess(2);

            assertEquals(CloseQueueResult.SUCCESS, queue.close(SEQUENCE_TIMEOUT));

            assertFalse(service.submit(queue::getIsSuspendedWithBroker).get());
            assertFalse(queue.getIsSuspended());

            // Verify deconfigure parameters
            verifyConfigureRequest(++reqId, server.nextClientRequest(), deconfigureOptions);
            // Verify options are not updated
            assertEquals(currentOptions, service.submit(queue::getQueueOptions).get());

            verifyCloseRequest(++reqId, server.nextClientRequest(), queue.getParameters());

            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            assertTrue(events.isEmpty());

            logger.info("20. stop the session");
            server.pushSuccess();
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());
        } catch (AssertionError e) {
            logger.error("Assertion: ", e);
            throw e;
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
            assertEquals(GenericResult.SUCCESS, session.linger());
            server.stop();

            logger.info("==========================================================");
            logger.info("END Testing BrokerSessionIT configureOptionsAndParameters.");
            logger.info("==========================================================");
        }
    }

    /**
     * Test for asynchronous open queue operation
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
    public void queueOpenErrorTest() throws BMQException {
        logger.info("================================================");
        logger.info("BEGIN Testing BrokerSessionIT queueOpenErrorTest");
        logger.info("================================================");

        final Semaphore testSema = new Semaphore(0);

        logger.info("Step 1. start broker simulator in manual mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_MANUAL_MODE);
        server.start();

        logger.info("Step 2. create session");
        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        EventHandler eventHandler =
                (Event event) -> {
                    events.push(event);
                    testSema.release();
                };
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, eventHandler);

        QueueStateManager queueManager =
                (QueueStateManager) TestTools.getInternalState(session, "queueStateManager");
        assertNotNull(queueManager);

        server.pushItem(StatusCategory.E_SUCCESS); // Ok for nego request

        final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(1);
        final Duration FUTURE_TIMEOUT = Duration.ofSeconds(2);

        int reqId = 0;

        try {
            logger.info("Step 3. start session");
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));

            logger.info("Step 4. create queue");
            QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();

            long flags = 0;
            flags = QueueFlags.setReader(flags);

            QueueImpl queue = createQueue(session, createUri(), flags);

            assertEquals(QueueState.e_CLOSED, queue.getState());
            // Check substreamcount == 0
            assertEquals(0, queueManager.getSubStreamCount(queue));

            logger.info("Step 5. open queue asynchronously");
            server.pushItem(StatusCategory.E_UNKNOWN); // error for open request
            assertEquals(
                    OpenQueueResult.UNKNOWN,
                    queue.openAsync(queueOptions, SEQUENCE_TIMEOUT).get(FUTURE_TIMEOUT));

            // Check substreamcount == 0 after receiveng  open queue bad response
            assertEquals(0, queueManager.getSubStreamCount(queue));

            TestTools.acquireSema(testSema);

            logger.info("Step 6. check the only open request was sent");
            verifyOpenRequest(++reqId, server.nextClientRequest());
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            logger.info("Step 7. check that open queue result event was emitted");
            assertEquals(QueueState.e_CLOSED, queue.getState());

            assertEquals(1, events.size());
            Event event = events.pollLast();
            assertNotNull(event);
            assertEquals(event.getClass(), QueueControlEvent.class);

            QueueControlEvent queueControlEvent = (QueueControlEvent) event;

            assertEquals(
                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT, queueControlEvent.getEventType());
            assertEquals(OpenQueueResult.UNKNOWN, queueControlEvent.getStatus());

            logger.info("Step 8. open queue asynchronously");
            server.pushItem(StatusCategory.E_SUCCESS); // ok for open request
            server.pushItem(StatusCategory.E_UNKNOWN); // error for config request
            server.pushItem(StatusCategory.E_SUCCESS); // ok for close request
            assertEquals(
                    OpenQueueResult.UNKNOWN,
                    queue.openAsync(queueOptions, SEQUENCE_TIMEOUT).get(FUTURE_TIMEOUT));

            // Check substreamcount == 0 after receiveng  configure queue bad response
            assertEquals(0, queueManager.getSubStreamCount(queue));

            TestTools.acquireSema(testSema);

            logger.info("Step 9. check open and config requests were sent");

            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            logger.info("Step 10. check that open queue result event was emitted");
            assertEquals(QueueState.e_CLOSED, queue.getState());

            assertEquals(1, events.size());
            event = events.pollLast();
            assertNotNull(event);
            assertEquals(event.getClass(), QueueControlEvent.class);

            queueControlEvent = (QueueControlEvent) event;

            assertEquals(
                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT, queueControlEvent.getEventType());
            assertEquals(OpenQueueResult.UNKNOWN, queueControlEvent.getStatus());

            logger.info("Step 11. check close queue request was sent");
            verifyCloseRequest(++reqId, server.nextClientRequest(), queue.getParameters());

            logger.info("Step 12. check there are no more requests");
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            logger.info("12. stop session");
            server.pushItem(StatusCategory.E_SUCCESS); // Ok for disconnect request
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());
            session.linger();
            server.stop();

            logger.info("===============================================");
            logger.info("END Testing BrokerSessionIT queueOpenErrorTest.");
            logger.info("===============================================");
        }
    }

    /**
     * Critical test to check substreamcount field when opening multiple subqueues
     *
     * <ul>
     *   <li>start broker simulator in active mode
     *   <li>create session
     *   <li>start session
     *   <li>create subqueues
     *   <li>open subqueues one by one
     *   <li>close subqueues one by one
     *   <li>stop session
     * </ul>
     */
    @Test
    public void openCloseMultipleSubqueuesTest() {
        logger.info("============================================================");
        logger.info("BEGIN Testing BrokerSessionIT openCloseMultipleSubqueuesTest");
        logger.info("============================================================");

        // Start broker simulator
        logger.info("Start broker simulator in manual mode");
        BmqBrokerSimulator server = new BmqBrokerSimulator(BmqBrokerSimulator.Mode.BMQ_MANUAL_MODE);
        server.start();

        // Create session
        logger.info("Create session");
        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, events::push);

        QueueStateManager queueManager =
                (QueueStateManager) TestTools.getInternalState(session, "queueStateManager");
        assertNotNull(queueManager);
        RequestManager requestManager =
                (RequestManager) TestTools.getInternalState(session, "requestManager");
        assertNotNull(requestManager);

        server.pushItem(StatusCategory.E_SUCCESS); // Ok for nego request

        final Duration SEQUENCE_TIMEOUT = Duration.ofSeconds(2);
        final Duration REQUEST_DELAY_SUCCESS = Duration.ofSeconds(1);
        final Duration REQUEST_DELAY_FAILURE = Duration.ofSeconds(3);
        final int CLIENT_REQUEST_TIMEOUT = 5;
        final Semaphore subQueueTestSema = new Semaphore(0);

        int reqId = 0;

        try {
            // Start session
            logger.info("Start session");
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));

            // Create subqueues
            logger.info("Create subqueues");
            QueueOptions queueOptions = QueueOptions.builder().setConsumerPriority(2).build();

            long flags = 0;
            flags = QueueFlags.setReader(flags);

            Uri uri = createUri();

            // Valid subqueue
            QueueImpl validQueue =
                    createQueue(session, new Uri(uri.canonical() + "?id=valid"), flags);

            // Fail to open subqueue:
            //  - failed to send open queue request (covered in unit tests)

            //  - bad open response
            QueueHandle badOpenResponseQueue =
                    createQueue(session, new Uri(uri.canonical() + "?id=badopenresponse"), flags);

            //  - late open response
            QueueHandle lateOpenResponseQueue =
                    createQueue(session, new Uri(uri.canonical() + "?id=lateopenresponse"), flags);

            //  - failed to send configure queue request (covered in unit tests)

            //  - bad configure
            QueueHandle badOpenConfigResponseQueue =
                    createQueue(
                            session, new Uri(uri.canonical() + "?id=badopenconfigresponse"), flags);

            //  - late configure response
            QueueHandle lateOpenConfigResponseQueue =
                    createQueue(
                            session,
                            new Uri(uri.canonical() + "?id=lateopenconfigresponse"),
                            flags);

            //  - canceled configure request
            QueueHandle canceledOpenConfigRequestQueue =
                    createQueue(
                            session,
                            new Uri(uri.canonical() + "?id=canceledopenconfigrequest"),
                            flags);

            // Fail to close properly:
            //  - failed to send configure request (covered in unit tests)

            //  - bad configure response
            QueueHandle badCloseConfigResponseQueue =
                    createQueue(
                            session,
                            new Uri(uri.canonical() + "?id=badcloseconfigresponse"),
                            flags);

            //  - late configure response
            QueueHandle lateCloseConfigResponseQueue =
                    createQueue(
                            session,
                            new Uri(uri.canonical() + "?id=latecloseconfigresponse"),
                            flags);

            //  - canceled configure request
            QueueHandle canceledCloseConfigRequestQueue =
                    createQueue(
                            session,
                            new Uri(uri.canonical() + "?id=canceledcloseconfigrequest"),
                            flags);

            //  - failed to send close request (covereted in unit tests)

            //  - bad close response
            QueueHandle badCloseResponseQueue =
                    createQueue(session, new Uri(uri.canonical() + "?id=badcloseresponse"), flags);

            //  - late close response
            QueueHandle lateCloseResponseQueue =
                    createQueue(session, new Uri(uri.canonical() + "?id=latecloseresponse"), flags);

            // Check substreamcount == 0
            int numOpened = 0;
            assertEquals(numOpened, queueManager.getSubStreamCount(validQueue));

            // **************************
            // *** Open valid queue ***
            logger.info("Open valid queue");
            server.pushSuccess(2); // ok for open and configure
            assertEquals(OpenQueueResult.SUCCESS, validQueue.open(queueOptions, SEQUENCE_TIMEOUT));
            assertEquals(QueueState.e_OPENED, validQueue.getState());
            // Substreamcount should increment
            assertEquals(++numOpened, queueManager.getSubStreamCount(validQueue));

            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            // *****************************************
            // *** Open queue with bad open response ***
            logger.info("Open queue with bad open response");
            server.pushItem(StatusCategory.E_UNKNOWN); // bad open response
            assertEquals(
                    OpenQueueResult.UNKNOWN,
                    badOpenResponseQueue.open(queueOptions, SEQUENCE_TIMEOUT));
            assertEquals(QueueState.e_CLOSED, badOpenResponseQueue.getState());
            // Substreamcount shouldn't change
            assertEquals(numOpened, queueManager.getSubStreamCount(validQueue));

            verifyOpenRequest(++reqId, server.nextClientRequest());

            // ******************************************
            // *** Open queue with late open response ***
            logger.info("Open queue with late open response");
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY_FAILURE); // late open response
            server.pushItem(StatusCategory.E_SUCCESS, subQueueTestSema); // ok for close

            assertEquals(
                    OpenQueueResult.TIMEOUT,
                    lateOpenResponseQueue.open(queueOptions, SEQUENCE_TIMEOUT));
            assertEquals(QueueState.e_CLOSED, lateOpenResponseQueue.getState());
            // Substreamcount should increment
            assertEquals(++numOpened, queueManager.getSubStreamCount(validQueue));

            TestTools.acquireSema(subQueueTestSema);

            verifyOpenRequest(++reqId, server.nextClientRequest());
            // Wait for and verify close request
            verifyCloseRequest(
                    ++reqId,
                    server.nextClientRequest(),
                    lateOpenResponseQueue.getParameters(),
                    false);
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // Substreamcount should decrement
            assertEquals(--numOpened, queueManager.getSubStreamCount(validQueue));

            // **********************************************
            // *** Open queue with bad configure response ***
            logger.info("Open queue with bad configure response");
            server.pushSuccess(); // ok for open queue
            server.pushItem(StatusCategory.E_UNKNOWN); // bad configure
            assertEquals(
                    OpenQueueResult.UNKNOWN,
                    badOpenConfigResponseQueue.open(queueOptions, SEQUENCE_TIMEOUT));
            assertEquals(QueueState.e_CLOSED, badOpenConfigResponseQueue.getState());
            // Substreamcount shouldn't change
            assertEquals(numOpened, queueManager.getSubStreamCount(validQueue));

            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(
                    ++reqId,
                    server.nextClientRequest(),
                    badOpenConfigResponseQueue.getParameters(),
                    false);
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // ***********************************************
            // *** Open queue with late configure response ***
            logger.info("Open queue with late configure response");
            server.pushSuccess(); // ok for open queue
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY_FAILURE); // late configure
            server.pushItem(StatusCategory.E_SUCCESS); // ok for configure
            server.pushItem(StatusCategory.E_SUCCESS, subQueueTestSema); // ok for close

            assertEquals(
                    OpenQueueResult.TIMEOUT,
                    lateOpenConfigResponseQueue.open(queueOptions, SEQUENCE_TIMEOUT));
            assertEquals(QueueState.e_CLOSED, lateOpenConfigResponseQueue.getState());
            // Substreamcount should increment
            assertEquals(++numOpened, queueManager.getSubStreamCount(validQueue));

            TestTools.acquireSema(subQueueTestSema);

            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            // Verify configure and close requests
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(
                    ++reqId,
                    server.nextClientRequest(),
                    lateOpenConfigResponseQueue.getParameters(),
                    false);
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // Substreamcount should decrement
            assertEquals(--numOpened, queueManager.getSubStreamCount(validQueue));

            // **************************************************
            // *** Open queue with canceled configure request ***
            logger.info("Open queue with canceled configure request");
            server.pushSuccess(); // ok for open queue
            // No response setup for configure request

            // Since the variables captured by lambda must be effectively final,
            // will need to use final variables for reqId and numOpened
            final int openReqId = reqId;
            final int openNumOpened = numOpened;

            // Run the future to cancel the request
            CompletableFuture<Void> cancelOpenConfigureFuture =
                    CompletableFuture.runAsync(
                            () -> {
                                verifyOpenRequest(openReqId + 1, server.nextClientRequest());

                                // Substreamcount should increment
                                assertEquals(
                                        openNumOpened + 1,
                                        queueManager.getSubStreamCount(validQueue));

                                verifyConfigureRequest(openReqId + 2, server.nextClientRequest());

                                // Cancel pending request (in the scheduler context)
                                service.execute(requestManager::cancelAllRequests);
                            });
            assertEquals(
                    OpenQueueResult.CANCELED,
                    canceledOpenConfigRequestQueue.open(queueOptions, SEQUENCE_TIMEOUT));
            assertEquals(QueueState.e_CLOSED, canceledOpenConfigRequestQueue.getState());

            assertTrue(cancelOpenConfigureFuture.isDone());
            assertFalse(cancelOpenConfigureFuture.isCompletedExceptionally());

            // Update variables after the future completes
            reqId += 2;
            numOpened += 1;

            // Substreamcount should decrement back
            assertEquals(--numOpened, queueManager.getSubStreamCount(validQueue));
            // No close request should be sent
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // ****************************************************
            // *** Open queue with bad close configure response ***
            logger.info("Open queue with bad close configure response");
            server.pushSuccess(2); // ok for open and configure
            assertEquals(
                    OpenQueueResult.SUCCESS,
                    badCloseConfigResponseQueue.open(queueOptions, SEQUENCE_TIMEOUT));
            assertEquals(QueueState.e_OPENED, badCloseConfigResponseQueue.getState());
            // Substreamcount should increment
            assertEquals(++numOpened, queueManager.getSubStreamCount(validQueue));

            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            // *****************************************************
            // *** Open queue with late close configure response ***
            logger.info("Open queue with late close configure response");
            server.pushSuccess(2); // ok for open and configure
            assertEquals(
                    OpenQueueResult.SUCCESS,
                    lateCloseConfigResponseQueue.open(queueOptions, SEQUENCE_TIMEOUT));
            assertEquals(QueueState.e_OPENED, lateCloseConfigResponseQueue.getState());
            // Substreamcount should increment
            assertEquals(++numOpened, queueManager.getSubStreamCount(validQueue));

            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            // *********************************************************
            // *** Open queue with canceled close configure request ***
            logger.info("Open queue with cancelled close configure request");
            server.pushSuccess(2); // ok for open and configure
            assertEquals(
                    OpenQueueResult.SUCCESS,
                    canceledCloseConfigRequestQueue.open(queueOptions, SEQUENCE_TIMEOUT));
            assertEquals(QueueState.e_OPENED, canceledCloseConfigRequestQueue.getState());
            // Substreamcount should increment
            assertEquals(++numOpened, queueManager.getSubStreamCount(validQueue));

            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            // ******************************************
            // *** Open queue with bad close response ***
            logger.info("Open queue with bad close response");
            server.pushSuccess(2); // ok for open and configure
            assertEquals(
                    OpenQueueResult.SUCCESS,
                    badCloseResponseQueue.open(queueOptions, SEQUENCE_TIMEOUT));
            assertEquals(QueueState.e_OPENED, badCloseResponseQueue.getState());
            // Substreamcount should increment
            assertEquals(++numOpened, queueManager.getSubStreamCount(validQueue));

            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            // *******************************************
            // *** Open queue with late close response ***
            logger.info("Open queue with late close response");
            server.pushSuccess(2); // ok for open and configure
            assertEquals(
                    OpenQueueResult.SUCCESS,
                    lateCloseResponseQueue.open(queueOptions, SEQUENCE_TIMEOUT));
            assertEquals(QueueState.e_OPENED, lateCloseResponseQueue.getState());
            // Substreamcount should increment
            assertEquals(++numOpened, queueManager.getSubStreamCount(validQueue));

            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());

            // Check there are 6 opened queues
            assertEquals(6, numOpened);

            // ***************************
            // *** Close valid queue ***
            logger.info("Close valid queue");
            server.pushSuccess(2); // ok for configure and close
            assertEquals(CloseQueueResult.SUCCESS, validQueue.close(SEQUENCE_TIMEOUT));
            assertEquals(QueueState.e_CLOSED, validQueue.getState());
            // Substreamcount should decrement
            assertEquals(--numOpened, queueManager.getSubStreamCount(validQueue));

            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(
                    ++reqId, server.nextClientRequest(), validQueue.getParameters(), false);

            // ***********************************************
            // *** Close queue with bad configure response ***
            logger.info("Close queue with bad configure response");
            server.pushItem(StatusCategory.E_UNKNOWN); // bad configure
            server.pushItem(StatusCategory.E_SUCCESS, subQueueTestSema); // ok close
            assertEquals(
                    CloseQueueResult.UNKNOWN, badCloseConfigResponseQueue.close(SEQUENCE_TIMEOUT));
            assertEquals(QueueState.e_CLOSED, badCloseConfigResponseQueue.getState());

            TestTools.acquireSema(subQueueTestSema);

            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(
                    ++reqId, // sent after returning from close method
                    server.nextClientRequest(),
                    badCloseConfigResponseQueue.getParameters(),
                    false);
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // Substreamcount should decrement after sending close request
            assertEquals(--numOpened, queueManager.getSubStreamCount(validQueue));

            // ************************************************
            // *** Close queue with late configure response ***
            logger.info("Close queue with late configure response");
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY_FAILURE); // late configure

            server.pushItem(StatusCategory.E_SUCCESS, subQueueTestSema); // ok close

            assertEquals(
                    CloseQueueResult.TIMEOUT, lateCloseConfigResponseQueue.close(SEQUENCE_TIMEOUT));
            assertEquals(QueueState.e_CLOSED, lateCloseConfigResponseQueue.getState());

            TestTools.acquireSema(subQueueTestSema);

            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(
                    ++reqId, // sent after receiving late response
                    server.nextClientRequest(),
                    lateCloseConfigResponseQueue.getParameters(),
                    false);
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // Substreamcount should decrement after sending close request
            assertEquals(--numOpened, queueManager.getSubStreamCount(validQueue));

            // ***************************************************
            // *** Close queue with canceled configure request ***
            logger.info("Close queue with canceled configure request");
            // No response setup for configure request

            // Since the variables captured by lambda must be effectively final,
            // will need to use final variables for reqId and numOpened
            final int closeReqId = reqId;

            // Run the future to cancel the request
            CompletableFuture<Void> cancelCloseConfigureFuture =
                    CompletableFuture.runAsync(
                            () -> {
                                verifyConfigureRequest(closeReqId + 1, server.nextClientRequest());

                                // Cancel pending request (in the scheduler context)
                                service.execute(requestManager::cancelAllRequests);
                            });
            assertEquals(
                    CloseQueueResult.CANCELED,
                    canceledCloseConfigRequestQueue.close(SEQUENCE_TIMEOUT));
            assertEquals(QueueState.e_CLOSED, canceledCloseConfigRequestQueue.getState());

            assertTrue(cancelCloseConfigureFuture.isDone());
            assertFalse(cancelCloseConfigureFuture.isCompletedExceptionally());

            // Update variables after the future completes
            reqId += 1;

            // Substreamcount should decrement after sending close request
            assertEquals(--numOpened, queueManager.getSubStreamCount(validQueue));
            // No close request should be sent
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // *******************************************
            // *** Close queue with bad close response ***
            logger.info("Close queue with bad close response");
            server.pushSuccess(); // ok for configure
            server.pushItem(StatusCategory.E_UNKNOWN); // bad close response
            assertEquals(CloseQueueResult.UNKNOWN, badCloseResponseQueue.close(SEQUENCE_TIMEOUT));
            assertEquals(QueueState.e_CLOSED, badCloseResponseQueue.getState());
            // Substreamcount should decrement
            assertEquals(--numOpened, queueManager.getSubStreamCount(validQueue));

            verifyConfigureRequest(++reqId, server.nextClientRequest());
            verifyCloseRequest(
                    ++reqId,
                    server.nextClientRequest(),
                    badCloseResponseQueue.getParameters(),
                    false);
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // ********************************************
            // *** Close queue with late close response ***
            logger.info("Close queue with late close response");
            server.pushSuccess(); // ok for configure
            server.pushItem(
                    StatusCategory.E_SUCCESS,
                    subQueueTestSema,
                    REQUEST_DELAY_FAILURE); // late close response
            assertEquals(CloseQueueResult.TIMEOUT, lateCloseResponseQueue.close(SEQUENCE_TIMEOUT));
            assertEquals(QueueState.e_CLOSED, lateCloseResponseQueue.getState());
            // Substreamcount should decrement
            assertEquals(--numOpened, queueManager.getSubStreamCount(validQueue));

            // Wait for late close response
            TestTools.acquireSema(subQueueTestSema);

            verifyConfigureRequest(++reqId, server.nextClientRequest());
            // final close request
            verifyCloseRequest(
                    ++reqId, // sent after receiving late response
                    server.nextClientRequest(),
                    lateCloseResponseQueue.getParameters());

            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // Check number of requests
            assertEquals(35, reqId);

            // Check there are no opened queues
            assertEquals(0, numOpened);

        } catch (Exception | AssertionError e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            // Stop session
            logger.info("Stop session");
            server.pushItem(StatusCategory.E_SUCCESS); // Ok for disconnect request
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            verifyDisconnectRequest(++reqId, server.nextClientRequest());
            session.linger();
            server.stop();

            logger.info("==========================================================");
            logger.info("END Testing BrokerSessionIT openCloseMultipleSubqueuesTest");
            logger.info("==========================================================");
        }
    }

    /**
     * Critical test to check sync session stop
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>create queues
     *   <li>open queue 1 synchronously
     *   <li>open queue 2 asynchronously (open request without response)
     *   <li>close queue 1 asynchronously
     *   <li>stop the session (send disconnect request)
     *   <li>send ACK message to queue 1
     *   <li>send PUSH message to queue 1
     *   <li>send close queue response for queue 1
     *   <li>send disconnect response
     *   <li>ensure queue 1 is closed properly
     *   <li>ensure queue 2 opening is canceled
     *   <li>check generated events (ACK, PUSH, queue close and queue open)
     *   <li>stop and linger broker session and stop server
     * </ul>
     */
    @Test
    public void sessionStopTest() throws ExecutionException, InterruptedException {
        logger.info("==============================================");
        logger.info("BEGIN Testing BrokerSessionIT sessionStopTest.");
        logger.info("==============================================");

        final Duration SHORT_SEQUENCE_TIMEOUT = Duration.ofSeconds(5);
        final Duration FUTURE_TIMEOUT = Duration.ofSeconds(6);
        final Duration REQUEST_DELAY = Duration.ofSeconds(2);

        // Start test server
        logger.info("Start test server");
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        // Create the session
        logger.info("Create the session");
        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, events::push);
        try {
            // Start the session
            logger.info("Start the session");
            server.pushSuccess();
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));

            // Create queues
            logger.info("Create queues");
            long rFlags = QueueFlags.setReader(0);
            long waFlags = QueueFlags.setAck(QueueFlags.setWriter(0));

            QueueOptions queueOptions = QueueOptions.builder().build();
            QueueHandle q1 = createQueue(session, createUri(), waFlags);
            QueueHandle q2 = createQueue(session, createUri(), rFlags);

            // Open queue 1
            logger.info("Open queue 1");
            server.pushSuccess(1);
            assertEquals(OpenQueueResult.SUCCESS, q1.open(queueOptions, TEST_REQUEST_TIMEOUT));
            assertEquals(QueueState.e_OPENED, q1.getState());

            int reqId = 0;
            verifyOpenRequest(++reqId, server.nextClientRequest());

            // Start opening queue 2
            logger.info("Start opening queue 2");
            server.pushSuccess(); // ok for open request
            // No auto response for configure, the request will be canceled
            BmqFuture<OpenQueueCode> openFuture = q2.openAsync(queueOptions, TEST_REQUEST_TIMEOUT);

            // === Test timeline: ~0 sec after start
            TestTools.sleepForSeconds(1);

            // === Test timeline: ~1 sec after start
            // Open queue 2 request should be sent, open response should be
            // received, open configure request should be sent
            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            assertEquals(QueueState.e_OPENING, q2.getState());

            // Start closing queue 1
            logger.info("Start closing queue 1");
            // Close queue response will be sent manually in disconnectFuture
            BmqFuture<CloseQueueCode> closeFuture = q1.closeAsync(TEST_REQUEST_TIMEOUT);

            TestTools.sleepForSeconds(1);

            // === Test timeline: ~2 sec after start
            // Close queue request should be sent
            int closeRequestId = ++reqId;
            verifyCloseRequest(closeRequestId, server.nextClientRequest(), q1.getParameters());

            // After initializing session stop procedure:
            //  - ensure disconnect request has been sent
            //  - send ACK and PUSH message back to queue 1
            //  - send close queue response for queue 1
            //  - send disconnect response
            int disconnectRequestId = ++reqId;
            CompletableFuture<Void> disconnectFuture =
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    // Ensure disconnect request has been sent
                                    verifyDisconnectRequest(
                                            disconnectRequestId, server.nextClientRequest());

                                    // Send ACK message
                                    int qId = q1.getQueueId();
                                    logger.info("Send unknown ACK to queue 1");

                                    try {
                                        server.write(
                                                TestTools.prepareAckEventData(
                                                        ResultCodes.AckResult.UNKNOWN,
                                                        CorrelationIdImpl.NULL_CORRELATION_ID,
                                                        MessageGUID.createEmptyGUID(),
                                                        qId));
                                    } catch (IOException e) {
                                        logger.error("Failed to send ACK message", e);
                                        throw new RuntimeException(e);
                                    }

                                    // Send PUSH message
                                    logger.info("Send PUSH message to queue 1");
                                    server.writePushRequest(qId);

                                    // Send close queue response
                                    logger.info("Send close response to queue 1");
                                    server.writeCloseQueueResponse(closeRequestId);

                                    // Send disconnect response
                                    logger.info("Send disconnect response");
                                    server.writeDisconnectResponse(disconnectRequestId);
                                } catch (Exception e) {
                                    logger.error("Disconnect future error: ", e);
                                    throw new RuntimeException(e);
                                }
                            });

            // Stop the session
            logger.info("Stop the session");
            // Disconnect response will be sent manually in disconnectFuture
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));

            // It is expected that ACK and PUSH messages, queue 1 close response
            // and disconnect response has been sent successfully
            logger.info("Verify disconnect future completed successfully");
            disconnectFuture.get(FUTURE_TIMEOUT.getSeconds(), TimeUnit.SECONDS);

            // It is expected that queue 1 closing was completed
            logger.info("Verify queue 1 closed properly");
            assertEquals(CloseQueueResult.SUCCESS, closeFuture.get(FUTURE_TIMEOUT));
            assertEquals(QueueState.e_CLOSED, q1.getState());

            // It is expected that queue 2 opening was canceled
            logger.info("Verify queue 2 opening was canceled");
            assertEquals(OpenQueueResult.CANCELED, openFuture.get(FUTURE_TIMEOUT));
            assertEquals(QueueState.e_CLOSED, q2.getState());

            // Check generated events
            logger.info("Check generated events");
            assertEquals(4, events.size());

            // ACK message event
            AckMessageEvent ackMessageEvent = (AckMessageEvent) events.pollLast();
            assertNotNull(ackMessageEvent);
            assertNotNull(ackMessageEvent.rawMessage());

            // PUSH message event
            PushMessageEvent pushMessageEvent = (PushMessageEvent) events.pollLast();
            assertNotNull(pushMessageEvent);
            assertNotNull(pushMessageEvent.rawMessage());

            // Queue 1 async close event
            QueueControlEvent queueControlEvent = (QueueControlEvent) events.pollLast();
            assertNotNull(queueControlEvent);
            assertEquals(
                    QueueControlEvent.Type.e_QUEUE_CLOSE_RESULT, queueControlEvent.getEventType());
            assertEquals(CloseQueueResult.SUCCESS, queueControlEvent.getStatus());

            // Queue 2 async open event
            queueControlEvent = (QueueControlEvent) events.pollLast();
            assertNotNull(queueControlEvent);
            assertEquals(
                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT, queueControlEvent.getEventType());
            assertEquals(OpenQueueResult.CANCELED, queueControlEvent.getStatus());
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            // Stop the session and the server
            logger.info("Stop session (already stopped) and server");
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            // There should be no requests to the server
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            assertEquals(GenericResult.SUCCESS, session.linger());
            server.stop();

            logger.info("============================================");
            logger.info("END Testing BrokerSessionIT sessionStopTest.");
            logger.info("============================================");
        }
    }

    /**
     * Check sync session stop with null timeout argument
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>stop the session with null timeout. SessionTimeout should be used
     *   <li>linger broker session and stop server
     * </ul>
     */
    @Test
    public void sessionStopNullTimeoutTest() {
        logger.info("=========================================================");
        logger.info("BEGIN Testing BrokerSessionIT sessionStopNullTimeoutTest.");
        logger.info("=========================================================");

        final Duration TIMEOUT = Duration.ofSeconds(5);

        // Start test server
        logger.info("Start test server");
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        // Create the session
        logger.info("Create the session");
        SessionOptions sessionOptions =
                SessionOptions.builder()
                        .setBrokerUri(server.getURI())
                        .setStopTimeout(TIMEOUT)
                        .build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, events::push);
        try {
            // Start the session
            logger.info("Start the session");
            server.pushSuccess();
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));

            // Stop the session
            logger.info("Stop the session");
            server.pushItem(StatusCategory.E_SUCCESS, TIMEOUT.minusMillis(500));
            assertEquals(GenericResult.SUCCESS, session.stop(null));

            int reqId = 0;
            verifyDisconnectRequest(++reqId, server.nextClientRequest());
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            // Stop the session and the server
            logger.info("Stop session (already stopped) and server");
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            // There should be no requests to the server
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            assertEquals(GenericResult.SUCCESS, session.linger());
            server.stop();

            logger.info("=======================================================");
            logger.info("END Testing BrokerSessionIT sessionStopNullTimeoutTest.");
            logger.info("=======================================================");
        }
    }

    /**
     * Critical test to check async session stop
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>create queues
     *   <li>open queue 1 synchronously
     *   <li>open queue 2 asynchronously (open request without response)
     *   <li>close queue 1 asynchronously
     *   <li>stop the session async (send disconnect request)
     *   <li>send ACK message to queue 1
     *   <li>send PUSH message to queue 1
     *   <li>send close queue response for queue 1
     *   <li>send disconnect response
     *   <li>ensure queue 1 is closed properly
     *   <li>ensure queue 2 opening is canceled
     *   <li>check generated events (ACK, PUSH, queue close, queue open and disconnect)
     *   <li>stop and linger broker session and stop server
     * </ul>
     */
    @Test
    public void sessionStopAsyncTest() throws IOException {
        logger.info("===================================================");
        logger.info("BEGIN Testing BrokerSessionIT sessionStopAsyncTest.");
        logger.info("===================================================");

        final Duration SHORT_SEQUENCE_TIMEOUT = Duration.ofSeconds(5);
        final Duration FUTURE_TIMEOUT = Duration.ofSeconds(6);
        final Duration REQUEST_DELAY = Duration.ofSeconds(2);

        // Start test server
        logger.info("Start test server");
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        // Create the session
        logger.info("Create the session");
        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, events::push);
        try {
            // Start the session
            logger.info("Start the session");
            server.pushSuccess();
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));

            // Create queues
            logger.info("Create queues");
            long rFlags = QueueFlags.setReader(0);
            long waFlags = QueueFlags.setAck(QueueFlags.setWriter(0));
            QueueOptions queueOptions = QueueOptions.builder().build();
            QueueHandle q1 = createQueue(session, createUri(), waFlags);
            QueueHandle q2 = createQueue(session, createUri(), rFlags);

            // Open queue 1
            logger.info("Open queue 1");
            server.pushSuccess(1);
            assertEquals(OpenQueueResult.SUCCESS, q1.open(queueOptions, TEST_REQUEST_TIMEOUT));
            assertEquals(QueueState.e_OPENED, q1.getState());

            int reqId = 0;
            verifyOpenRequest(++reqId, server.nextClientRequest());

            // Start opening queue 2
            logger.info("Start opening queue 2");
            server.pushSuccess(); // ok for open request
            // No auto response for configure, the request will be canceled
            BmqFuture<OpenQueueCode> openFuture = q2.openAsync(queueOptions, TEST_REQUEST_TIMEOUT);

            // === Test timeline: ~0 sec after start
            TestTools.sleepForSeconds(1);

            // === Test timeline: ~1 sec after start
            // Open queue 2 request should be sent, open response should be
            // received, open configure request should be sent
            verifyOpenRequest(++reqId, server.nextClientRequest());
            verifyConfigureRequest(++reqId, server.nextClientRequest());
            assertEquals(QueueState.e_OPENING, q2.getState());

            // Start closing queue 1
            logger.info("Start closing queue 1");
            // Close queue response will be sent manually later
            BmqFuture<CloseQueueCode> closeFuture = q1.closeAsync(TEST_REQUEST_TIMEOUT);

            TestTools.sleepForSeconds(1);

            // === Test timeline: ~2 sec after start
            // Close queue request should be sent
            int closeRequestId = 4;
            verifyCloseRequest(closeRequestId, server.nextClientRequest(), q1.getParameters());

            // Stop the session
            logger.info("Stop the session");
            // Disconnect response will be sent manually later
            session.stopAsync(TEST_REQUEST_TIMEOUT);

            // Ensure disconnect request has been sent
            int disconnectRequestId = 5;
            verifyDisconnectRequest(disconnectRequestId, server.nextClientRequest());

            // Send ACK message
            int qId = q1.getQueueId();
            logger.info("Send unknown ACK to queue 1");

            try {
                server.write(
                        TestTools.prepareAckEventData(
                                ResultCodes.AckResult.UNKNOWN,
                                CorrelationIdImpl.NULL_CORRELATION_ID,
                                MessageGUID.createEmptyGUID(),
                                qId));
            } catch (IOException e) {
                logger.error("Failed to send ACK message", e);
                throw new RuntimeException(e);
            }

            // Send PUSH message
            logger.info("Send PUSH message to queue 1");
            server.writePushRequest(qId);

            // Send close queue response
            logger.info("Send close response to queue 1");
            server.writeCloseQueueResponse(closeRequestId);

            // Send disconnect response
            logger.info("Send disconnect response");
            server.writeDisconnectResponse(disconnectRequestId);

            // It is expected that queue 1 closing was completed
            logger.info("Verify queue 1 closed properly");
            assertEquals(CloseQueueResult.SUCCESS, closeFuture.get(FUTURE_TIMEOUT));
            assertEquals(QueueState.e_CLOSED, q1.getState());

            // It is expected that queue 2 opening was canceled
            logger.info("Verify queue 2 opening was canceled");
            assertEquals(OpenQueueResult.CANCELED, openFuture.get(FUTURE_TIMEOUT));
            assertEquals(QueueState.e_CLOSED, q2.getState());

            // Wait for 1s
            TestTools.sleepForSeconds(1);

            // Check generated events
            logger.info("Check generated events");
            assertEquals(5, events.size());

            // ACK message event
            AckMessageEvent ackMessageEvent = (AckMessageEvent) events.pollLast();
            assertNotNull(ackMessageEvent);
            assertNotNull(ackMessageEvent.rawMessage());

            // PUSH message event
            PushMessageEvent pushMessageEvent = (PushMessageEvent) events.pollLast();
            assertNotNull(pushMessageEvent);
            assertNotNull(pushMessageEvent.rawMessage());

            // Queue 1 async close event
            QueueControlEvent queueControlEvent = (QueueControlEvent) events.pollLast();
            assertNotNull(queueControlEvent);
            assertEquals(
                    QueueControlEvent.Type.e_QUEUE_CLOSE_RESULT, queueControlEvent.getEventType());
            assertEquals(CloseQueueResult.SUCCESS, queueControlEvent.getStatus());

            // Queue 2 async open event
            queueControlEvent = (QueueControlEvent) events.pollLast();
            assertNotNull(queueControlEvent);
            assertEquals(
                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT, queueControlEvent.getEventType());
            assertEquals(OpenQueueResult.CANCELED, queueControlEvent.getStatus());

            // Disconnect event
            BrokerSessionEvent sessionEvent = (BrokerSessionEvent) events.pollLast();
            assertNotNull(sessionEvent);
            assertEquals(BrokerSessionEvent.Type.e_DISCONNECTED, sessionEvent.getEventType());
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            // Stop the session and the server
            logger.info("Stop session (already stopped) and server");
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            // There should be no requests to the server
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            assertEquals(GenericResult.SUCCESS, session.linger());
            server.stop();

            logger.info("=================================================");
            logger.info("END Testing BrokerSessionIT sessionStopAsyncTest.");
            logger.info("=================================================");
        }
    }

    /**
     * Check async session stop with null timeout argument
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>stop the session with null timeout. SessionTimeout should be used
     *   <li>linger broker session and stop server
     * </ul>
     */
    @Test
    public void sessionStopAsyncNullTimeoutTest() {
        logger.info("==============================================================");
        logger.info("BEGIN Testing BrokerSessionIT sessionStopAsyncNullTimeoutTest.");
        logger.info("==============================================================");

        final Duration TIMEOUT = Duration.ofSeconds(5);
        final Semaphore stopSema = new Semaphore(0);

        // Start test server
        logger.info("Start test server");
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        // Create the session
        logger.info("Create the session");
        SessionOptions sessionOptions =
                SessionOptions.builder()
                        .setBrokerUri(server.getURI())
                        .setStopTimeout(TIMEOUT)
                        .build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        EventHandler eventHandler =
                (Event event) -> {
                    events.push(event);
                    stopSema.release();
                };
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, eventHandler);
        try {
            // Start the session
            logger.info("Start the session");
            server.pushSuccess();
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));

            // Stop the session
            logger.info("Stop the session");
            server.pushItem(StatusCategory.E_SUCCESS, TIMEOUT.minusMillis(500));
            session.stopAsync(null);

            TestTools.acquireSema(stopSema);

            int reqId = 0;
            verifyDisconnectRequest(++reqId, server.nextClientRequest());

            assertEquals(1, events.size());

            // Disconnect event
            BrokerSessionEvent sessionEvent = (BrokerSessionEvent) events.pollLast();
            assertNotNull(sessionEvent);
            assertEquals(BrokerSessionEvent.Type.e_DISCONNECTED, sessionEvent.getEventType());
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            // Stop the session and the server
            logger.info("Stop session (already stopped) and server");
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            // There should be no requests to the server
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            assertEquals(GenericResult.SUCCESS, session.linger());
            server.stop();

            logger.info("============================================================");
            logger.info("END Testing BrokerSessionIT sessionStopAsyncNullTimeoutTest.");
            logger.info("============================================================");
        }
    }

    /**
     * Critical test to check sync session stop with timeout
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>stop the session with timeout
     *   <li>stop and linger broker session and stop server
     * </ul>
     */
    @Test
    public void sessionStopTimeoutTest() {
        logger.info("=====================================================");
        logger.info("BEGIN Testing BrokerSessionIT sessionStopTimeoutTest.");
        logger.info("=====================================================");

        final Duration SHORT_SEQUENCE_TIMEOUT = Duration.ofSeconds(1);
        final Duration REQUEST_DELAY_FAILURE = Duration.ofSeconds(2);

        // Start test server
        logger.info("Start test server");
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        // Create the session
        logger.info("Create the session");
        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, events::push);
        try {
            // Start the session
            logger.info("Start the session");
            server.pushSuccess();
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));

            // Stop the session with timeout
            logger.info("Stop the session with timeout");
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY_FAILURE);
            assertEquals(GenericResult.TIMEOUT, session.stop(SHORT_SEQUENCE_TIMEOUT));
            int reqId = 0;
            verifyDisconnectRequest(++reqId, server.nextClientRequest());

            // After timeout expires, the session stops waiting for disconnect
            // response and initiates channel closing which may take some time.
            // Let`s wait for 500ms and check whether the connection is closed.
            // If it's not enough we will need to use another approach e.g. wait
            // in a cycle or use a callback
            TestTools.sleepForMilliSeconds(500);

            // Verify new requests cannot be sent
            long flags = QueueFlags.setReader(0);
            QueueOptions queueOptions = QueueOptions.builder().build();
            QueueHandle queue = createQueue(session, createUri(), flags);
            assertEquals(
                    OpenQueueResult.NOT_CONNECTED, queue.open(queueOptions, TEST_REQUEST_TIMEOUT));

            // No requests should be sent
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            // Stop the session and the server
            logger.info("Stop session (already stopped) and server");
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            // There should be no requests to the server
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            assertEquals(GenericResult.SUCCESS, session.linger());
            server.stop();

            logger.info("===================================================");
            logger.info("END Testing BrokerSessionIT sessionStopTimeoutTest.");
            logger.info("===================================================");
        }
    }

    /**
     * Critical test to check async session stop with timeout
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>stop the session with timeout
     *   <li>stop and linger broker session and stop server
     * </ul>
     */
    @Test
    public void sessionStopAsyncTimeoutTest() {
        logger.info("==========================================================");
        logger.info("BEGIN Testing BrokerSessionIT sessionStopAsyncTimeoutTest.");
        logger.info("==========================================================");

        final Duration SHORT_SEQUENCE_TIMEOUT = Duration.ofSeconds(1);
        final Duration REQUEST_DELAY_FAILURE = Duration.ofSeconds(2);

        Semaphore closeSema = new Semaphore(0);

        // Start test server
        logger.info("Start test server");
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        // Create the session
        logger.info("Create the session");
        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        EventHandler eventHandler =
                (Event event) -> {
                    events.push(event);
                    closeSema.release();
                };
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, eventHandler);
        try {
            // Start the session
            logger.info("Start the session");
            server.pushSuccess();
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));

            // Stop the session with timeout
            logger.info("Stop the session with timeout");
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY_FAILURE);
            session.stopAsync(SHORT_SEQUENCE_TIMEOUT);
            int reqId = 0;
            verifyDisconnectRequest(++reqId, server.nextClientRequest());

            // Wait for event
            TestTools.acquireSema(closeSema);

            assertEquals(1, events.size());
            BrokerSessionEvent sessionEvent = (BrokerSessionEvent) events.pollLast();
            assertNotNull(sessionEvent);
            assertEquals(
                    BrokerSessionEvent.Type.e_DISCONNECTION_TIMEOUT, sessionEvent.getEventType());

            // After timeout expires, the session stops waiting for disconnect
            // response and initiates channel closing which may take some time.
            // Let`s wait for 500ms and check whether the connection is closed.
            // If it's not enough we will need to use another approach e.g. wait
            // in a cycle or use a callback
            TestTools.sleepForMilliSeconds(500);

            // Verify new requests cannot be sent
            long flags = QueueFlags.setReader(0);
            QueueOptions queueOptions = QueueOptions.builder().build();
            QueueHandle queue = createQueue(session, createUri(), flags);
            assertEquals(
                    OpenQueueResult.NOT_CONNECTED, queue.open(queueOptions, TEST_REQUEST_TIMEOUT));
            // No requests should be sent
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            // Stop the session and the server
            logger.info("Stop session (already stopped) and server");
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            // There should be no requests to the server
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            assertEquals(GenericResult.SUCCESS, session.linger());
            server.stop();

            logger.info("========================================================");
            logger.info("END Testing BrokerSessionIT sessionStopAsyncTimeoutTest.");
            logger.info("========================================================");
        }
    }

    /**
     * Critical test to check sync session stop during executing stop process
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>stop the session sync with disconnect response delay
     *   <li>stop the session sync
     *   <li>stop and linger broker session and stop server
     * </ul>
     */
    @Test
    public void sessionStopInProgressTest() {
        logger.info("========================================================");
        logger.info("BEGIN Testing BrokerSessionIT sessionStopInProgressTest.");
        logger.info("========================================================");

        final Duration REQUEST_DELAY = Duration.ofSeconds(1);

        // Start test server
        logger.info("Start test server");
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        // Create the session
        logger.info("Create the session");
        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, events::push);
        try {
            // Start the session
            logger.info("Start the session");
            server.pushSuccess();
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));

            // Stop the session with disconnect response delay
            logger.info("Stop the session with disconnect response delay");
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY);
            CompletableFuture<GenericResult> stopFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                return session.stop(TEST_REQUEST_TIMEOUT);
                            });

            int reqId = 0;
            verifyDisconnectRequest(++reqId, server.nextClientRequest());

            // Stop the session one more time
            logger.info("Stop the session one more time");
            assertEquals(GenericResult.NOT_SUPPORTED, session.stop(TEST_REQUEST_TIMEOUT));

            // There should be no requests
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            // Wait for successful disconnection
            assertEquals(GenericResult.SUCCESS, stopFuture.get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted: ", e);
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            logger.error("Execution: ", e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            // Stop the session and the server
            logger.info("Stop session (already stopped) and server");
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            // There should be no requests to the server
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            assertEquals(GenericResult.SUCCESS, session.linger());
            server.stop();

            logger.info("======================================================");
            logger.info("END Testing BrokerSessionIT sessionStopInProgressTest.");
            logger.info("======================================================");
        }
    }

    /**
     * Critical test to check async session stop during executing stop process
     *
     * <ul>
     *   <li>start test server in manual mode
     *   <li>create broker session
     *   <li>start broker session
     *   <li>stop the session async with disconnect response delay
     *   <li>stop the session async
     *   <li>ensure necessary events are generated
     *   <li>stop and linger broker session and stop server
     * </ul>
     */
    @Test
    public void sessionStopAsyncInProgressTest() {
        logger.info("=============================================================");
        logger.info("BEGIN Testing BrokerSessionIT sessionStopAsyncInProgressTest.");
        logger.info("=============================================================");

        final Duration REQUEST_DELAY = Duration.ofSeconds(1);

        Semaphore closeSema = new Semaphore(0);

        // Start test server
        logger.info("Start test server");
        BmqBrokerSimulator server = new BmqBrokerSimulator(Mode.BMQ_MANUAL_MODE);
        server.start();

        // Create the session
        logger.info("Create the session");
        SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(server.getURI()).build();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedList<Event> events = new LinkedList<>();
        EventHandler eventHandler =
                (Event event) -> {
                    events.push(event);
                    closeSema.release();
                };
        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, eventHandler);
        try {
            // Start the session
            logger.info("Start the session");
            server.pushSuccess();
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));

            // Stop the session with disconnect response delay
            logger.info("Stop the session with disconnect response delay");
            server.pushItem(StatusCategory.E_SUCCESS, REQUEST_DELAY);
            session.stopAsync(TEST_REQUEST_TIMEOUT);

            int reqId = 0;
            verifyDisconnectRequest(++reqId, server.nextClientRequest());

            // Stop the session one more time
            logger.info("Stop the session one more time");
            session.stopAsync(TEST_REQUEST_TIMEOUT);

            // Wait for in progress event
            TestTools.acquireSema(closeSema);

            assertEquals(1, events.size());
            BrokerSessionEvent sessionEvent = (BrokerSessionEvent) events.pollLast();
            assertNotNull(sessionEvent);
            assertEquals(
                    BrokerSessionEvent.Type.e_DISCONNECTION_IN_PROGRESS,
                    sessionEvent.getEventType());

            // Wait for in disconnected event
            TestTools.acquireSema(closeSema);

            assertEquals(1, events.size());
            sessionEvent = (BrokerSessionEvent) events.pollLast();
            assertNotNull(sessionEvent);
            assertEquals(BrokerSessionEvent.Type.e_DISCONNECTED, sessionEvent.getEventType());

            // There should be no requests
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));
        } catch (Exception e) {
            logger.error("Exception: ", e);
            throw e;
        } finally {
            // Stop the session and the server
            logger.info("Stop session (already stopped) and server");
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            // There should be no requests to the server
            assertNull(server.nextClientRequest(NO_CLIENT_REQUEST_TIMEOUT));

            assertEquals(GenericResult.SUCCESS, session.linger());
            server.stop();

            logger.info("===========================================================");
            logger.info("END Testing BrokerSessionIT sessionStopAsyncInProgressTest.");
            logger.info("===========================================================");
        }
    }

    /**
     * Test to check inbound buffer size during HWM and LWM events
     *
     * <ul>
     *   <li>create the session with inbound LWM=1 and HWM=2
     *   <li>open queues in async mode and wait for results
     *   <li>handle the first NOT_CONNECTED queue event
     *   <li>handle HWM event
     *   <li>handle the other Queue NOT_CONNECTED events
     *   <li>handle LWM event
     *   <li>handle the last Queue NOT_CONNECTED event
     *   <li>verify that there are no more inbound events
     *   <li>close the session
     * </ul>
     */
    @Test
    public void inboundWatermarksTest() throws TimeoutException, InterruptedException {
        logger.info("====================================================");
        logger.info("BEGIN Testing BrokerSessionIT inboundWatermarksTest.");
        logger.info("====================================================");

        final int EVENT_TIMEOUT = 15;

        // create the session with inbound LWM=1 and HWM=2
        SessionOptions sessionOptions =
                SessionOptions.builder()
                        .setInboundBufferWaterMark(
                                new SessionOptions.InboundEventBufferWaterMark(1, 2))
                        .build();

        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        SynchronousQueue<Event> events = new SynchronousQueue<>();
        EventHandler eventHandler =
                event -> {
                    try {
                        logger.info("Handle event: {}", event);
                        assertTrue(events.offer(event, EVENT_TIMEOUT, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                };

        BrokerSession session =
                BrokerSession.createInstance(
                        sessionOptions, connectionFactory, service, eventHandler);

        try {
            // open queues in async mode and wait for results
            final int NUM_OF_QUEUES = 10;
            final Duration SHORT_TIMEOUT = Duration.ofSeconds(5);

            QueueImpl[] queues = new QueueImpl[NUM_OF_QUEUES];
            QueueControlEventHandler queueControlEventHandler =
                    new QueueControlEventHandler() {
                        @Override
                        public void handleQueueOpenResult(QueueControlEvent event) {
                            eventHandler.handleEvent(event);
                        }

                        @Override
                        public void handleQueueReopenResult(QueueControlEvent event) {
                            eventHandler.handleEvent(event);
                        }

                        @Override
                        public void handleQueueCloseResult(QueueControlEvent event) {
                            eventHandler.handleEvent(event);
                        }

                        @Override
                        public void handleQueueConfigureResult(QueueControlEvent event) {
                            eventHandler.handleEvent(event);
                        }

                        @Override
                        public void handleQueueSuspendResult(QueueControlEvent event) {
                            eventHandler.handleEvent(event);
                        }

                        @Override
                        public void handleQueueResumeResult(QueueControlEvent event) {
                            eventHandler.handleEvent(event);
                        }
                    };

            final long flags = QueueFlags.setWriter(0);
            for (int i = 0; i < NUM_OF_QUEUES; i++) {
                QueueImpl queue =
                        createQueue(session, createUri(), flags, queueControlEventHandler);
                assertEquals(
                        OpenQueueResult.NOT_CONNECTED,
                        queue.openAsync(QueueOptions.createDefault(), SHORT_TIMEOUT)
                                .get(SHORT_TIMEOUT));
                queues[i] = queue;

                // We need to wait for a bit, so the `queueControlEventHandler` has time to poll
                // the very first event from `InboundEventBuffer`. If we don't do it, there is a
                // chance that HWM `BrokerSessionEvent` will be generated early and placed in
                // the head of the buffer, resulting in this test's failure.
                // We also need to wait for the second time on the third queue open, so the other
                // thread has time to enqueue HWM `BrokerSessionEvent` to `InboundEventBuffer`.
                // So this sleep is a workaround for a race condition existing here by test's
                // design.
                TestTools.sleepForMilliSeconds(100);
            }

            // Here we expect one NOT_CONNECTED event which has been already
            // popped out of the inbound event buffer. In the buffer there
            // should be one HWM event plus (NUM_OF_QUEUES - 1) events of the type
            // NOT_CONNECTED, thus NUM_OF_QUEUES events totally.
            assertEquals(NUM_OF_QUEUES, session.inboundBufferSize());

            // handle the first NOT_CONNECTED queue event
            verifyQueueControlEvent(
                    events.poll(EVENT_TIMEOUT, TimeUnit.SECONDS),
                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT,
                    OpenQueueResult.NOT_CONNECTED,
                    queues[0]);

            // handle HWM event
            // We expect that the incoming Queue NOT_CONNECTED events
            // caused the inbound event buffer exceeded HWM value, and as a result
            // HWM event has been added into the head of the event queue.
            verifyBrokerSessionEvent(
                    events.poll(EVENT_TIMEOUT, TimeUnit.SECONDS),
                    BrokerSessionEvent.Type.e_SLOWCONSUMER_HIGHWATERMARK);

            // handle the other Queue NOT_CONNECTED events
            for (int i = 1; i < NUM_OF_QUEUES - 1; i++) {
                verifyQueueControlEvent(
                        events.poll(EVENT_TIMEOUT, TimeUnit.SECONDS),
                        QueueControlEvent.Type.e_QUEUE_OPEN_RESULT,
                        OpenQueueResult.NOT_CONNECTED,
                        queues[i]);
            }

            // 7) Handle LWM event
            // Since we have the last Queue NOT_CONNECTED event in the queue
            // that means we've reached LWM value, so LWM event should
            // be reported first.
            verifyBrokerSessionEvent(
                    events.poll(EVENT_TIMEOUT, TimeUnit.SECONDS),
                    BrokerSessionEvent.Type.e_SLOWCONSUMER_NORMAL);

            // 8) Handle the last Queue NOT_CONNECTED event
            verifyQueueControlEvent(
                    events.poll(EVENT_TIMEOUT, TimeUnit.SECONDS),
                    QueueControlEvent.Type.e_QUEUE_OPEN_RESULT,
                    OpenQueueResult.NOT_CONNECTED,
                    queues[NUM_OF_QUEUES - 1]);

            // verify that there are no more inbound events
            assertNull(events.poll());
            assertNull(events.poll());
            assertEquals(0, session.inboundBufferSize());

        } finally {
            // close the session
            session.linger();

            logger.info("==================================================");
            logger.info("END Testing BrokerSessionIT inboundWatermarksTest.");
            logger.info("==================================================");
        }
    }
}
