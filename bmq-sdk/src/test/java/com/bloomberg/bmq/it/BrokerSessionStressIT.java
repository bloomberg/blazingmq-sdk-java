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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.bloomberg.bmq.BMQException;
import com.bloomberg.bmq.QueueFlags;
import com.bloomberg.bmq.QueueOptions;
import com.bloomberg.bmq.ResultCodes;
import com.bloomberg.bmq.ResultCodes.CloseQueueResult;
import com.bloomberg.bmq.ResultCodes.GenericResult;
import com.bloomberg.bmq.ResultCodes.OpenQueueResult;
import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.Uri;
import com.bloomberg.bmq.impl.BrokerSession;
import com.bloomberg.bmq.impl.QueueImpl;
import com.bloomberg.bmq.impl.events.AckMessageEvent;
import com.bloomberg.bmq.impl.events.Event;
import com.bloomberg.bmq.impl.events.PushMessageEvent;
import com.bloomberg.bmq.impl.infr.msg.SubQueueIdInfo;
import com.bloomberg.bmq.impl.infr.net.NettyTcpConnectionFactory;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnectionFactory;
import com.bloomberg.bmq.impl.infr.proto.AckMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.PushMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.PutHeader;
import com.bloomberg.bmq.impl.infr.proto.PutMessageImpl;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.infr.util.SystemUtil;
import com.bloomberg.bmq.impl.intf.EventHandler;
import com.bloomberg.bmq.impl.intf.QueueHandle;
import com.bloomberg.bmq.impl.intf.QueueState;
import com.bloomberg.bmq.it.util.BmqBroker;
import com.bloomberg.bmq.it.util.TestTools;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerSessionStressIT {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static final Duration TEST_REQUEST_TIMEOUT = Duration.ofSeconds(15);

    static class TransferValidator {

        enum TransferState {
            SENDING_PUT,
            WAITING_PUSH,
            DONE
        }

        LinkedBlockingQueue<Event> eventFIFO;
        BrokerSession session;
        QueueHandle writer;
        QueueHandle reader;
        LinkedList<PutMessageImpl> putMessages = new LinkedList<>();
        LinkedList<AckMessageImpl> ackMessages = new LinkedList<>();
        LinkedList<PushMessageImpl> pushMessages = new LinkedList<>();
        LinkedList<ByteBuffer> payloads = new LinkedList<>();
        int numMsg = 0;

        public TransferValidator(
                LinkedBlockingQueue<Event> eventQueue,
                BrokerSession session,
                QueueHandle writer,
                QueueHandle reader) {
            eventFIFO = Argument.expectNonNull(eventQueue, "eventQueue");
            this.session = Argument.expectNonNull(session, "session");
            this.writer = Argument.expectNonNull(writer, "writer");
            this.reader = Argument.expectNonNull(reader, "reader");
            Argument.expectCondition(
                    writer.getState() == QueueState.e_OPENED, "'writer' must be OPENED");
            Argument.expectCondition(
                    reader.getState() == QueueState.e_OPENED, "'reader' must be OPENED");
        }

        public void transfer(
                int payloadSize,
                int numMsgs,
                int numPutsPerEvent,
                boolean waitPush,
                boolean isOldStyleProperties) {
            if (!putMessages.isEmpty()) {
                throw new IllegalStateException("'putMessages' expected to be empty");
            }

            final int MID_CHECK_QUEUE_SIZE = 100000;

            numMsg = numMsgs;
            TransferState state = TransferState.SENDING_PUT;
            while (state != TransferState.DONE) {
                switch (state) {
                    case SENDING_PUT:
                        try {
                            if (pushMessages.size() > MID_CHECK_QUEUE_SIZE) {
                                validate();
                            }
                            int minMsg = Math.min(numMsgs - putMessages.size(), numPutsPerEvent);
                            PutMessageImpl[] messages = new PutMessageImpl[minMsg];
                            for (int i = 0; i < messages.length; i++) {
                                String payload = createPayload(payloadSize, Integer.toString(i));
                                PutMessageImpl msg =
                                        TestTools.preparePutMessage(payload, isOldStyleProperties);
                                logger.info("Sending {}", msg);

                                putMessages.add(msg);
                                payloads.add(
                                        TestTools.prepareUnpaddedData(
                                                payload, isOldStyleProperties));
                                messages[i] = msg;
                            }
                            session.post(writer, messages);
                            state = TransferState.WAITING_PUSH;
                        } catch (IOException e) {
                            logger.error("Exception: ", e);
                            state = TransferState.DONE;
                        } catch (BMQException e) {
                            logger.error("BMQException: ", e);
                            state = TransferState.DONE;
                        }
                        break;
                    case WAITING_PUSH:
                        Event event = null;
                        try {
                            event = eventFIFO.poll(30, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            logger.error("Interrupted: ", e);
                        }
                        if (event == null) {
                            logger.error("No incoming events");
                            state = TransferState.DONE;
                        }
                        if (event instanceof PushMessageEvent) {
                            PushMessageImpl pm = ((PushMessageEvent) event).rawMessage();
                            String guid = pm.messageGUID().toString();
                            AckMessageImpl lastAck = ackMessages.peekLast();

                            logger.info("Got PUSH with GUID: {}", guid);
                            if (guid.endsWith("00000000000000000")) {
                                logger.error("Got strange PUSH: {}", pm);
                                logger.error("Previous PUSH: {}", pushMessages.peekLast());
                                state = TransferState.DONE;
                            }

                            // Send Confirm
                            if (GenericResult.SUCCESS == session.confirm(reader, pm)) {
                                pushMessages.add(pm);
                            } else {
                                logger.error("Failed to CONFIRM. GUID: {}", pm.messageGUID());
                                state = TransferState.DONE;
                            }

                            if (pushMessages.size() == numMsg && ackMessages.size() == numMsg) {
                                logger.info("Received all ACKs and PUSHs");
                                state = TransferState.DONE;
                            }
                        } else if (event instanceof AckMessageEvent) {
                            AckMessageImpl msg = ((AckMessageEvent) event).rawMessage();
                            logger.info(
                                    "Got ACK [{}] with GUID: {}", msg.status(), msg.messageGUID());
                            logger.info("Got ACK: {}", msg);
                            ackMessages.add(msg);
                        }
                        break;
                    default:
                        logger.error("Unexpected state: {}", state);
                        state = TransferState.DONE;
                }
                if (putMessages.size() < numMsg) {
                    PutMessageImpl lastPut = putMessages.peekLast();
                    PushMessageImpl lastPush = pushMessages.peekLast();
                    AckMessageImpl lastAck = ackMessages.peekLast();
                    boolean sendPut = false;

                    if (lastPut != null && lastAck != null) {
                        String ackGuid = lastAck.messageGUID().toString();
                        if (lastPut.correlationId() == lastAck.correlationId()) {
                            sendPut = true;
                        }
                    }
                    if (sendPut && waitPush) {
                        sendPut = false;
                        if (lastPush != null) {
                            String ackGuid = lastAck.messageGUID().toString();
                            String pushGuid = lastPush.messageGUID().toString();
                            if (ackGuid.equals(pushGuid)) {
                                sendPut = true;
                            }
                        }
                    }
                    if (sendPut) {
                        state = TransferState.SENDING_PUT;
                    }
                }
            }
        }

        void printStat() {
            logger.error("Sent PUTs: {}", putMessages.size());
            logger.error("Got ACKs : {}", ackMessages.size());
            logger.error("Got PUSHs: {}", pushMessages.size());

            if (pushMessages.size() > 0) {
                logger.error("Last PUT : {}", putMessages.peekLast());
                logger.error("Last ACK : {}", ackMessages.peekLast());
                logger.error("Last PUSH: {}", pushMessages.peekLast().header());
            }
        }

        public void validate() {

            logger.error("Begin validation. Memory used: {}", TestTools.getUsedMemoryMB());

            final int NUM_MESSAGES = pushMessages.size();

            printStat();

            assertTrue(putMessages.size() >= NUM_MESSAGES);
            assertTrue(ackMessages.size() >= NUM_MESSAGES);
            assertTrue(payloads.size() >= NUM_MESSAGES);

            HashSet<String> guids = new HashSet<>();
            for (AckMessageImpl ackMsg : ackMessages) {
                // Check GUIDs
                String guid = ackMsg.messageGUID().toString();
                assertTrue(guids.add(guid)); // checks uniqueness
            }
            for (int i = 0; i < NUM_MESSAGES; i++) {
                PutMessageImpl putMsg = putMessages.poll();
                AckMessageImpl ackMsg = ackMessages.poll();
                PushMessageImpl pshMsg = pushMessages.poll();
                ByteBuffer data = payloads.poll();

                assertNotNull(putMsg);
                assertNotNull(ackMsg);
                assertNotNull(pshMsg);
                assertNotNull(data);

                // Check ACK status
                assertEquals(ackMsg.status(), ResultCodes.AckResult.SUCCESS);

                // Check Correlation IDs
                assertEquals(putMsg.correlationId(), ackMsg.correlationId());

                // Check GUIDs
                assertTrue(guids.contains(pshMsg.messageGUID().toString()));

                // Check SubQueueId
                if (reader.getSubQueueId() != SubQueueIdInfo.DEFAULT_SUB_ID) {
                    Integer[] subQueueIds = pshMsg.subQueueIds();
                    assertNotNull(subQueueIds);
                    assertEquals(subQueueIds.length, 1);
                    assertEquals(subQueueIds[0].intValue(), reader.getSubQueueId());
                }

                // Check payload
                try {
                    assertTrue(TestTools.equalContent(data, pshMsg.appData().applicationData()));
                } catch (IOException e) {
                    logger.error("IOException: ", e);
                    fail();
                }
            }
            numMsg -= NUM_MESSAGES;

            assertTrue(numMsg >= 0);
            assertEquals(0, pushMessages.size());

            logger.error("Validation complete. Memory used: {}", TestTools.getUsedMemoryMB());
        }

        String createPayload(int msgSize, String filler) {
            StringBuilder sb = new StringBuilder(msgSize);
            while (sb.capacity() < msgSize) {
                sb.append("[").append(filler).append("]");
            }
            sb.setLength(msgSize);
            return sb.toString();
        }
    }

    static QueueImpl createQueue(BrokerSession session, Uri uri, long flags) {
        return new QueueImpl(session, uri, flags, null, null, null);
    }

    void testThroughput(int payloadSize, int numMsgs, int numPutsPerEvent, boolean waitPushes)
            throws IOException {

        final Duration TEST_REQUEST_TIMEOUT = Duration.ofSeconds(25);

        final Uri QUEUE_URI = BmqBroker.Domains.Priority.generateQueueUri();

        long QUEUE_FLAGS = 0;
        QUEUE_FLAGS = QueueFlags.setWriter(QUEUE_FLAGS);
        QUEUE_FLAGS = QueueFlags.setAck(QUEUE_FLAGS);
        QUEUE_FLAGS = QueueFlags.setReader(QUEUE_FLAGS);

        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedBlockingQueue<Event> eventFIFO = new LinkedBlockingQueue<>();

        EventHandler eventHandler =
                (Event event) -> {
                    try {
                        eventFIFO.put(event);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                };

        // Create a BrokerSession and open queues for writing and reading
        BmqBroker broker = BmqBroker.createStartedBroker();

        BrokerSession session =
                BrokerSession.createInstance(
                        broker.sessionOptions(), connectionFactory, service, eventHandler);

        try {
            assertEquals(
                    GenericResult.SUCCESS,
                    session.start(BrokerSessionStressIT.TEST_REQUEST_TIMEOUT));

            QueueHandle queueHandle = createQueue(session, QUEUE_URI, QUEUE_FLAGS);

            QueueOptions queueOptions =
                    QueueOptions.builder()
                            .setConsumerPriority(1)
                            .setMaxUnconfirmedMessages(numMsgs)
                            .setMaxUnconfirmedBytes(32768L * numMsgs)
                            .build();

            assertEquals(
                    OpenQueueResult.SUCCESS, queueHandle.open(queueOptions, TEST_REQUEST_TIMEOUT));

            TransferValidator validator =
                    new TransferValidator(eventFIFO, session, queueHandle, queueHandle);

            validator.transfer(
                    payloadSize,
                    numMsgs,
                    numPutsPerEvent,
                    waitPushes,
                    broker.isOldStyleMessageProperties());

            // Close the queue.
            assertEquals(CloseQueueResult.SUCCESS, queueHandle.close(TEST_REQUEST_TIMEOUT));

            validator.validate();

            broker.setDropTmpFolder();

        } finally {
            // Stop the session.
            assertEquals(
                    GenericResult.SUCCESS,
                    session.stop(BrokerSessionStressIT.TEST_REQUEST_TIMEOUT));
            session.linger();

            // Stop the broker.
            broker.close();
        }
    }

    @Test
    public void testMultipleEvents() throws IOException {
        // Send multiple PUT messages each with unique payload via native Broker,
        // receive related PUSH messages from the queue, verify the payload.

        logger.info("=================================");
        logger.info("BEGIN Testing testMultipleEvents.");
        logger.info("=================================");

        final int NUM_MESSAGES = 1000;
        final int MSG_SIZE = 4;
        final int NUM_PUTS_PER_EVENT = 1;

        final boolean WAIT_FOR_PUSH = true;

        testThroughput(MSG_SIZE, NUM_MESSAGES, NUM_PUTS_PER_EVENT, WAIT_FOR_PUSH);

        logger.info("===============================");
        logger.info("END Testing testMultipleEvents.");
        logger.info("===============================");
    }

    @Test
    public void testMultipleEventsNoPushWait() throws IOException {
        // Send multiple PUT messages each with unique payload via native Broker,
        // waiting only for related ACK messages.
        // Receive related PUSH messages from the queue, verify the payload.

        logger.info("===========================================");
        logger.info("BEGIN Testing testMultipleEventsNoPushWait.");
        logger.info("===========================================");

        final int NUM_MESSAGES = 1000;
        final int MSG_SIZE = 4;
        final int NUM_PUTS_PER_EVENT = 5;

        final boolean WAIT_FOR_PUSH = false;

        testThroughput(MSG_SIZE, NUM_MESSAGES, NUM_PUTS_PER_EVENT, WAIT_FOR_PUSH);

        logger.info("=========================================");
        logger.info("END Testing testMultipleEventsNoPushWait.");
        logger.info("=========================================");
    }

    void startStopSession(BrokerSession session) {
        logger.info("BEGIN startStopSession");

        // Start the session.
        assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));

        logger.info("STARTED startStopSession");

        // Stop the session.
        assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
        logger.info("END startStopSession");
    }

    void startStopSession(SessionOptions so) {
        logger.info("BEGIN startStopSession2");
        // Create a BrokerSession
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

        BrokerSession session =
                BrokerSession.createInstance(so, connectionFactory, service, (Event ev) -> {});
        // Start the session.
        assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));

        // Stop the session.
        assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
        session.linger();
        logger.info("END startStopSession2");
    }

    @Test
    public void testMultipleSessionStartStop() throws IOException {

        logger.info("===========================================");
        logger.info("BEGIN Testing testMultipleSessionStartStop.");
        logger.info("===========================================");

        // In this test, we have two scenarios:
        // - Start/Stop a BrokerSession multiple times in a tight loop.
        // - Create and start/stop a BrokerSession multiple times in a tight
        //   loop.

        final int NUM_CYCLES = 20;
        BmqBroker broker = BmqBroker.createStartedBroker();
        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

        BrokerSession session =
                BrokerSession.createInstance(
                        broker.sessionOptions(), connectionFactory, service, (Event ev) -> {});
        try {
            logger.error("Memory usage 1: {}", TestTools.getUsedMemoryMB());

            // Cycle with created session.
            for (int i = 0; i < NUM_CYCLES; i++) {
                startStopSession(session);
            }

            logger.error("Memory usage 2: {}", TestTools.getUsedMemoryMB());

            // Cycle that creates a new session on each iteration.
            for (int i = 0; i < NUM_CYCLES; i++) {
                startStopSession(broker.sessionOptions());
            }
            logger.error("Memory usage 3: {}", TestTools.getUsedMemoryMB());

            broker.setDropTmpFolder();

        } finally {
            session.linger();

            // Stop the broker.
            broker.close();

            logger.info("========================================");
            logger.info("END Testing testMultipleSessionStartStop.");
            logger.info("========================================");
        }
    }

    @Test
    public void testBigEvent() throws IOException {
        // Send a single PUT messages with a big payload via native Broker,
        // receive related PUSH message from the queue, verify the payload.

        logger.info("===========================");
        logger.info("BEGIN Testing testBigEvent.");
        logger.info("===========================");

        final int NUM_MESSAGES = 1;
        final int MSG_SIZE = PutHeader.MAX_PAYLOAD_SIZE_SOFT - TestTools.MESSAGE_PROPERTIES_SIZE;

        final int NUM_PUTS_PER_EVENT = 1;
        final boolean WAIT_FOR_PUSH = true;

        testThroughput(MSG_SIZE, NUM_MESSAGES, NUM_PUTS_PER_EVENT, WAIT_FOR_PUSH);

        logger.info("=========================");
        logger.info("END Testing testBigEvent.");
        logger.info("=========================");
    }

    @Test
    public void testFanoutThroughput() throws IOException {
        // Send multiple PUT messages each with unique payload via native Broker,
        // receive related PUSH messages from the queue, verify the payload.

        logger.info("===================================");
        logger.info("BEGIN Testing FanoutThroughputTest.");
        logger.info("===================================");

        final Duration TEST_REQUEST_TIMEOUT = Duration.ofSeconds(15);

        int defaultNumMsgs = 100;
        if (SystemUtil.getOsName().equalsIgnoreCase("linux")) {
            // Bump up num msgs to a higher value on the faster platform.
            defaultNumMsgs = 1000;
        }
        final int NUM_MESSAGES = defaultNumMsgs;

        final int MSG_SIZE = 4;
        final int NUM_PUTS_PER_EVENT = 1;
        final boolean WAIT_FOR_PUSHES = true;
        final String APP_NAME = "foo";

        final Uri WRITER_QUEUE_URI = BmqBroker.Domains.Fanout.generateQueueUri();
        final Uri READER_QUEUE_URI =
                BmqBroker.Domains.Fanout.generateQueueUri(WRITER_QUEUE_URI, APP_NAME);

        long QUEUE_WRITER_FLAGS = 0, QUEUE_READER_FLAGS = 0;

        QUEUE_WRITER_FLAGS = QueueFlags.setWriter(QUEUE_WRITER_FLAGS);
        QUEUE_WRITER_FLAGS = QueueFlags.setAck(QUEUE_WRITER_FLAGS);
        QUEUE_READER_FLAGS = QueueFlags.setReader(QUEUE_READER_FLAGS);

        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        LinkedBlockingQueue<Event> eventFIFO = new LinkedBlockingQueue<>();

        EventHandler eventHandler =
                (Event event) -> {
                    try {
                        eventFIFO.put(event);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                };

        // Create a BrokerSession and open queues for writing and reading
        BmqBroker broker = BmqBroker.createStartedBroker();

        BrokerSession session =
                BrokerSession.createInstance(
                        broker.sessionOptions(), connectionFactory, service, eventHandler);

        try {
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));

            QueueHandle queueWriterHandle =
                    createQueue(session, WRITER_QUEUE_URI, QUEUE_WRITER_FLAGS);

            QueueHandle queueReaderHandle =
                    createQueue(session, READER_QUEUE_URI, QUEUE_READER_FLAGS);

            QueueOptions queueOptions =
                    QueueOptions.builder()
                            .setConsumerPriority(1)
                            .setMaxUnconfirmedMessages(NUM_MESSAGES)
                            .setMaxUnconfirmedBytes(32768L * NUM_MESSAGES)
                            .build();

            assertEquals(
                    OpenQueueResult.SUCCESS,
                    queueReaderHandle.open(queueOptions, TEST_REQUEST_TIMEOUT));
            assertEquals(
                    OpenQueueResult.SUCCESS,
                    queueWriterHandle.open(queueOptions, TEST_REQUEST_TIMEOUT));

            TransferValidator validator =
                    new TransferValidator(eventFIFO, session, queueWriterHandle, queueReaderHandle);

            validator.transfer(
                    MSG_SIZE,
                    NUM_MESSAGES,
                    NUM_PUTS_PER_EVENT,
                    WAIT_FOR_PUSHES,
                    broker.isOldStyleMessageProperties());

            // Close the queues.
            assertEquals(CloseQueueResult.SUCCESS, queueReaderHandle.close(TEST_REQUEST_TIMEOUT));
            assertEquals(CloseQueueResult.SUCCESS, queueWriterHandle.close(TEST_REQUEST_TIMEOUT));

            validator.validate();

            broker.setDropTmpFolder();

        } finally {
            // Stop the session.
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            session.linger();

            broker.close();

            logger.info("=================================");
            logger.info("END Testing FanoutThroughputTest.");
            logger.info("=================================");
        }
    }
}
