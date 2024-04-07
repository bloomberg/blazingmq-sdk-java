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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.bloomberg.bmq.AbstractSession;
import com.bloomberg.bmq.AckMessage;
import com.bloomberg.bmq.AckMessageHandler;
import com.bloomberg.bmq.BMQException;
import com.bloomberg.bmq.CompressionAlgorithm;
import com.bloomberg.bmq.Event;
import com.bloomberg.bmq.MessageProperties;
import com.bloomberg.bmq.PushMessage;
import com.bloomberg.bmq.PushMessageHandler;
import com.bloomberg.bmq.PutMessage;
import com.bloomberg.bmq.Queue;
import com.bloomberg.bmq.QueueControlEvent;
import com.bloomberg.bmq.QueueEventHandler;
import com.bloomberg.bmq.QueueFlags;
import com.bloomberg.bmq.QueueOptions;
import com.bloomberg.bmq.ResultCodes.GenericResult;
import com.bloomberg.bmq.Session;
import com.bloomberg.bmq.SessionEvent;
import com.bloomberg.bmq.Uri;
import com.bloomberg.bmq.impl.infr.proto.Protocol;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.it.util.BmqBroker;
import com.bloomberg.bmq.it.util.TestTools;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SessionStressIT {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static final Duration TEST_REQUEST_TIMEOUT = Duration.ofSeconds(20);

    static class TransferValidator {

        enum TransferState {
            SENDING_PUT,
            WAITING_PUSH,
            DONE
        }

        LinkedBlockingQueue<Event> eventFIFO;

        Queue writer;
        Queue[] readers;

        LinkedList<PutMessage> putMessages = new LinkedList<>();
        LinkedList<AckMessage> ackMessages = new LinkedList<>();
        LinkedList<PushMessage> pushMessages = new LinkedList<>();
        LinkedList<ByteBuffer> payloads = new LinkedList<>();

        int numberOfPutMessages;
        int numberOfPushMessages;
        int messagePayloadSize;
        int messagesPerEvent;

        Optional<CompressionAlgorithm> algorithm;

        public TransferValidator(
                LinkedBlockingQueue<Event> eventQueue, Queue writer, Queue... readers) {

            eventFIFO = Argument.expectNonNull(eventQueue, "eventQueue");
            this.writer = Argument.expectNonNull(writer, "writer");
            this.readers = Argument.expectNonNull(readers, "readers");

            Argument.expectCondition(readers.length > 0, "'readers' must be non-empty");
            for (Queue reader : readers) {
                Argument.expectNonNull(reader, "reader");
                Argument.expectCondition(reader.isOpen(), "All readers must be opened");
            }
            Argument.expectCondition(writer.isOpen(), "'writer' must be opened");
        }

        private PutMessage preparePutMessage(String payload) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(payload.getBytes());
            PutMessage msg = writer.createPutMessage(byteBuffer);
            msg.setCorrelationId();

            MessageProperties mp = msg.messageProperties();

            mp.setPropertyAsString("routingId", "abcd-efgh-ijkl");
            mp.setPropertyAsInt64("timestamp", 123456789L);

            return msg;
        }

        private TransferState sendPut() throws BMQException {
            if (numberOfPutMessages <= 0) {
                throw new IllegalStateException("No put messages");
            }
            if (messagesPerEvent <= 0) {
                throw new IllegalStateException("'messagesPerEvent' must be positive");
            }

            int minMsg = Math.min(numberOfPutMessages - putMessages.size(), messagesPerEvent);
            for (int i = 0; i < minMsg; i++) {
                String payload = createPayload(messagePayloadSize, Integer.toString(i));
                PutMessage msg = preparePutMessage(payload);

                algorithm.ifPresent(msg::setCompressionAlgorithm);

                logger.debug("Sending {}", msg);

                putMessages.add(msg);
                payloads.add(ByteBuffer.wrap(payload.getBytes()));
                writer.pack(msg);
            }
            writer.flush();
            return TransferState.WAITING_PUSH;
        }

        private TransferState waitPushAndAck() throws BMQException {
            Event event = null;
            TransferState state = TransferState.WAITING_PUSH;
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
            if (event instanceof PushMessage) {
                PushMessage pm = (PushMessage) event;
                String guid = pm.messageGUID().toString();
                AckMessage lastAck = ackMessages.peekLast();

                logger.debug("Got PUSH with GUID: {}", guid);

                // Send Confirm
                if (GenericResult.SUCCESS == pm.confirm()) {
                    pushMessages.add(pm);
                } else {
                    logger.error("Failed to CONFIRM. GUID: {}", pm.messageGUID());
                    state = TransferState.DONE;
                }
                if (pushMessages.size() == numberOfPushMessages
                        && ackMessages.size() == numberOfPutMessages) {
                    logger.info("Received all ACKs and PUSHs");
                    state = TransferState.DONE;
                }
            } else if (event instanceof AckMessage) {
                AckMessage msg = (AckMessage) event;
                logger.debug("Got ACK [{}] with GUID: {}", msg.status(), msg.messageGUID());
                ackMessages.add(msg);
            }
            return state;
        }

        private boolean canSendNextPut(boolean waitForPush) {
            boolean sendPut = false;
            if (putMessages.size() < numberOfPutMessages) {
                PutMessage lastPut = putMessages.peekLast();
                PushMessage lastPush = pushMessages.peekLast();
                AckMessage lastAck = ackMessages.peekLast();

                if (lastPut != null && lastAck != null) {
                    String ackGuid = lastAck.messageGUID().toString();
                    if (lastPut.correlationId() == lastAck.correlationId()) {
                        sendPut = true;
                    }
                }
                if (sendPut && waitForPush) {
                    sendPut = false;
                    if (lastPush != null) {
                        String ackGuid = lastAck.messageGUID().toString();
                        String pushGuid = lastPush.messageGUID().toString();
                        if (ackGuid.equals(pushGuid)) {
                            sendPut = true;
                        }
                    }
                }
            }

            return sendPut;
        }

        public void transfer(int payloadSize, int numMsgs, int numPutsPerEvent, boolean waitPush) {
            transfer(payloadSize, numMsgs, numPutsPerEvent, waitPush, Optional.empty());
        }

        public void transfer(
                int payloadSize,
                int numMsgs,
                int numPutsPerEvent,
                boolean waitPush,
                Optional<CompressionAlgorithm> algorithm)
                throws BMQException {
            Argument.expectNonNull(algorithm, "algorithm");
            if (putMessages.size() > 0) {
                throw new IllegalStateException("'putMessages' expected to be empty");
            }

            final int VALIDATION_THRESHOLD = 100000 / readers.length;

            numberOfPutMessages = numMsgs;
            numberOfPushMessages = numMsgs * readers.length;
            messagesPerEvent = numPutsPerEvent;
            messagePayloadSize = payloadSize;
            this.algorithm = algorithm;

            TransferState state = TransferState.SENDING_PUT;
            while (state != TransferState.DONE) {
                switch (state) {
                    case SENDING_PUT:
                        if (pushMessages.size() > VALIDATION_THRESHOLD) {
                            validate();
                        }
                        state = sendPut();
                        break;
                    case WAITING_PUSH:
                        state = waitPushAndAck();
                        if (state != TransferState.DONE && canSendNextPut(waitPush)) {
                            state = TransferState.SENDING_PUT;
                        }
                        break;
                    default:
                        logger.error("Unexpected state: {}", state);
                        state = TransferState.DONE;
                }
            }
        }

        void printStat() {
            logger.info("Sent PUTs: {}", putMessages.size());
            logger.info("Got ACKs : {}", ackMessages.size());
            logger.info("Got PUSHs: {}", pushMessages.size());

            if (pushMessages.size() > 0) {
                logger.info("Last PUT : {}", putMessages.peekLast());
                logger.info("Last ACK : {}", ackMessages.peekLast());
                logger.info("Last PUSH: {}", pushMessages.peekLast());
            }
        }

        public void validate() throws BMQException {
            if (isFanoutMode()) {
                validateFanoutMode();
            } else {
                validateSingleMode();
            }
        }

        void validateSingleMode() throws BMQException {

            logger.info("Begin validation. Memory used: {} MB", TestTools.getUsedMemoryMB());

            final int NUM_MESSAGES = pushMessages.size();

            printStat();

            if (putMessages.size() < NUM_MESSAGES
                    || ackMessages.size() < NUM_MESSAGES
                    || payloads.size() < NUM_MESSAGES) {
                throw new BMQException(
                        "Missed messages: "
                                + putMessages.size()
                                + " "
                                + ackMessages.size()
                                + " "
                                + payloads.size());
            }

            HashSet<String> guids = new HashSet<>();
            for (AckMessage ackMsg : ackMessages) {
                // Check GUIDs
                String guid = ackMsg.messageGUID().toString();
                if (!guids.add(guid)) { // checks uniqueness
                    throw new BMQException("GUID not unique: " + guid);
                }
            }
            for (int i = 0; i < NUM_MESSAGES; i++) {
                // Warning. Assumption here is that related  PUT, ACK and PUSH messages are in the
                // same order.

                PutMessage putMsg = putMessages.poll();
                AckMessage ackMsg = ackMessages.poll();
                PushMessage pshMsg = pushMessages.poll();
                ByteBuffer data = payloads.poll();

                if (putMsg == null || ackMsg == null || pshMsg == null || data == null) {
                    throw new BMQException(
                            "NULL message or payload: "
                                    + putMsg
                                    + " "
                                    + ackMsg
                                    + " "
                                    + pshMsg
                                    + " "
                                    + data);
                }

                // Check ACK status
                if (!ackMsg.status().isSuccess()) {
                    throw new BMQException("Error ACK status: " + ackMsg);
                }

                // Check Correlation IDs
                if (!putMsg.correlationId().equals(ackMsg.correlationId())) {
                    throw new BMQException(
                            "Correlation IDs are not equal " + ackMsg + " " + putMsg);
                }

                // Check GUIDs
                if (!guids.contains(pshMsg.messageGUID().toString())) {
                    throw new BMQException("Unknown GUID: " + pshMsg);
                }

                // Check payload
                if (!TestTools.equalContent(data, pshMsg.payload())) {
                    throw new BMQException("Payload check failed: " + pshMsg);
                }
            }
            numberOfPutMessages -= NUM_MESSAGES;
            numberOfPushMessages -= NUM_MESSAGES;

            if (numberOfPutMessages < 0) {
                throw new BMQException("Wrong number of PUT messages: " + numberOfPutMessages);
            }

            if (numberOfPushMessages < 0) {
                throw new BMQException("Wrong number of PUSH messages: " + numberOfPushMessages);
            }

            if (pushMessages.size() != 0) {
                throw new BMQException("Unexpected PUSH messages: " + pushMessages.size());
            }

            logger.error("Validation complete. Memory used: {}", TestTools.getUsedMemoryMB());
        }

        void validateFanoutMode() throws BMQException {

            logger.error("Begin validation. Memory used: {}", TestTools.getUsedMemoryMB());

            final int NUM_MESSAGES = ackMessages.size();

            printStat();

            if (putMessages.size() < NUM_MESSAGES || payloads.size() < NUM_MESSAGES) {
                throw new BMQException(
                        "Missed messages: "
                                + putMessages.size()
                                + " "
                                + ackMessages.size()
                                + " "
                                + payloads.size());
            }
            final int NUM_PUTS = putMessages.size();
            final int NUM_PUSH = pushMessages.size();

            HashSet<String> guids = new HashSet<>();
            for (AckMessage ackMsg : ackMessages) {
                // Check GUIDs
                String guid = ackMsg.messageGUID().toString();
                if (!guids.add(guid)) { // checks uniqueness
                    throw new BMQException("GUID not unique: " + guid);
                }
            }
            for (int i = 0; i < NUM_MESSAGES; i++) {
                PutMessage putMsg = putMessages.poll();
                AckMessage ackMsg = ackMessages.poll();
                ByteBuffer data = payloads.poll();

                if (putMsg == null || ackMsg == null || data == null) {
                    throw new BMQException(
                            "NULL message or payload: " + putMsg + " " + ackMsg + " " + data);
                }
                PushMessage[] pshMsgs = new PushMessage[readers.length];
                for (int j = 0; j < readers.length; j++) {
                    PushMessage pshMsg = pushMessages.poll();
                    if (pshMsg == null) {
                        throw new BMQException("NULL PUSH message");
                    }
                    pshMsgs[j] = pshMsg;
                }

                // Check ACK status
                if (!ackMsg.status().isSuccess()) {
                    throw new BMQException("Error ACK status: " + ackMsg);
                }

                // Check Correlation IDs
                if (!putMsg.correlationId().equals(ackMsg.correlationId())) {
                    throw new BMQException(
                            "Correlation IDs are not equal " + ackMsg + " " + putMsg);
                }

                // Check GUIDs
                for (PushMessage pshMsg : pshMsgs) {
                    if (!guids.contains(pshMsg.messageGUID().toString())) {
                        throw new BMQException("Unknown GUID: " + pshMsg);
                    }
                }

                for (PushMessage pshMsg : pshMsgs) {
                    // Check payload
                    if (!TestTools.equalContent(data, pshMsg.payload())) {
                        throw new BMQException("Payload check failed: " + pshMsg);
                    }
                }
            }
            numberOfPutMessages -= NUM_PUTS;
            numberOfPushMessages -= NUM_PUSH;

            if (numberOfPutMessages < 0) {
                throw new BMQException("Wrong number of PUT messages: " + numberOfPutMessages);
            }

            if (numberOfPushMessages < 0) {
                throw new BMQException("Wrong number of PUSH messages: " + numberOfPushMessages);
            }

            if (pushMessages.size() != 0) {
                throw new BMQException("Unexpected PUSH messages: " + pushMessages.size());
            }

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

        boolean isFanoutMode() {
            return readers.length > 1;
        }
    }

    void testThroughput(int payloadSize, int numMsgs, int numPutsPerEvent, boolean waitPushes)
            throws BMQException, IOException {
        testThroughput(payloadSize, numMsgs, numPutsPerEvent, waitPushes, Optional.empty());
    }

    void testThroughput(
            int payloadSize,
            int numMsgs,
            int numPutsPerEvent,
            boolean waitPushes,
            Optional<CompressionAlgorithm> algorithm)
            throws BMQException, IOException {

        final Duration TEST_REQUEST_TIMEOUT = Duration.ofSeconds(25);

        final Uri QUEUE_URI = BmqBroker.Domains.Priority.generateQueueUri();

        long QUEUE_FLAGS = 0;
        QUEUE_FLAGS = QueueFlags.setWriter(QUEUE_FLAGS);
        QUEUE_FLAGS = QueueFlags.setAck(QUEUE_FLAGS);
        QUEUE_FLAGS = QueueFlags.setReader(QUEUE_FLAGS);

        LinkedBlockingQueue<Event> eventFIFO = new LinkedBlockingQueue<>();

        AckMessageHandler ackMessageHandler =
                (AckMessage msg) -> {
                    try {
                        eventFIFO.put(msg);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                };

        PushMessageHandler pushMessageHandler =
                (PushMessage msg) -> {
                    try {
                        eventFIFO.put(msg);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                };

        // Create a BrokerSession and open queues for writing and reading
        BmqBroker broker = BmqBroker.createStartedBroker();

        AbstractSession session =
                new Session(
                        broker.sessionOptions(),
                        (SessionEvent e) -> {
                            /*No-op*/
                        });

        try {
            session.start(SessionStressIT.TEST_REQUEST_TIMEOUT);

            Queue queue =
                    session.getQueue(
                            QUEUE_URI,
                            QUEUE_FLAGS,
                            null, // QueueEventHandler
                            ackMessageHandler, // AckMessageHandler
                            pushMessageHandler // PushMessageHandler
                            );

            QueueOptions queueOptions =
                    QueueOptions.builder()
                            .setConsumerPriority(1)
                            .setMaxUnconfirmedMessages(numMsgs)
                            .setMaxUnconfirmedBytes(32768L * numMsgs)
                            .build();

            queue.open(queueOptions, TEST_REQUEST_TIMEOUT);

            TransferValidator validator = new TransferValidator(eventFIFO, queue, queue);

            validator.transfer(payloadSize, numMsgs, numPutsPerEvent, waitPushes, algorithm);

            // Close the queue.
            queue.close(TEST_REQUEST_TIMEOUT);

            validator.validate();

            broker.setDropTmpFolder();
        } catch (Exception e) {
            throw new BMQException(e);
        } finally {
            // Stop the session.
            session.stop(SessionStressIT.TEST_REQUEST_TIMEOUT);
            session.linger();

            broker.close();
        }
    }

    void testMultithreadedWriteThroughput(int numWriters, int numMsgsPerWriter)
            throws BMQException, IOException {
        Argument.expectPositive(numWriters, "numWriters");
        Argument.expectPositive(numMsgsPerWriter, "numMsgsPerWriter");

        final int MSG_SIZE = 4;
        final int NUM_PUTS_PER_EVENT = 1;
        final Duration TEST_REQUEST_TIMEOUT = Duration.ofSeconds(25);
        final int NUM_MESSAGES = numMsgsPerWriter;
        final boolean WAIT_FOR_PUSH = false;
        final int TRANSFER_TIMEOUT_SEC = 600;

        // Create a BrokerSession and open queues for writing and reading
        BmqBroker broker = BmqBroker.createStartedBroker();

        AbstractSession session =
                new Session(
                        broker.sessionOptions(),
                        (SessionEvent e) -> {
                            /*No-op*/
                        });

        QueueOptions queueOptions =
                QueueOptions.builder()
                        .setConsumerPriority(1)
                        .setMaxUnconfirmedMessages(NUM_MESSAGES)
                        .setMaxUnconfirmedBytes(32768L * NUM_MESSAGES)
                        .build();

        long QUEUE_FLAGS = 0;
        QUEUE_FLAGS = QueueFlags.setWriter(QUEUE_FLAGS);
        QUEUE_FLAGS = QueueFlags.setAck(QUEUE_FLAGS);
        QUEUE_FLAGS = QueueFlags.setReader(QUEUE_FLAGS);

        ExecutorService threadPool = Executors.newFixedThreadPool(numWriters);

        try {
            session.start(SessionStressIT.TEST_REQUEST_TIMEOUT);

            Queue[] openedQueues = new Queue[numWriters];
            for (int i = 0; i < numWriters; i++) {

                final Uri QUEUE_URI = BmqBroker.Domains.Priority.generateQueueUri();

                LinkedBlockingQueue<Event> eventFIFO = new LinkedBlockingQueue<>();

                AckMessageHandler ackMessageHandler =
                        (AckMessage msg) -> {
                            try {
                                eventFIFO.put(msg);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException(e);
                            }
                        };

                PushMessageHandler pushMessageHandler =
                        (PushMessage msg) -> {
                            try {
                                eventFIFO.put(msg);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException(e);
                            }
                        };

                Queue queue =
                        session.getQueue(
                                QUEUE_URI,
                                QUEUE_FLAGS,
                                null, // QueueEventHandler
                                ackMessageHandler, // AckMessageHandler
                                pushMessageHandler // PushMessageHandler
                                );

                queue.open(queueOptions, TEST_REQUEST_TIMEOUT);
                openedQueues[i] = queue;

                threadPool.submit(
                        () -> {
                            TransferValidator validator =
                                    new TransferValidator(eventFIFO, queue, queue);
                            validator.transfer(
                                    MSG_SIZE, NUM_MESSAGES, NUM_PUTS_PER_EVENT, WAIT_FOR_PUSH);
                            validator.validate();
                        });
            }
            threadPool.shutdown();
            if (!threadPool.awaitTermination(TRANSFER_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                logger.error(
                        "Failed to finish message transfer within {} seconds",
                        TRANSFER_TIMEOUT_SEC);
                fail();
            }
            for (Queue queue : openedQueues) {
                // Close the queue.
                queue.close(TEST_REQUEST_TIMEOUT);
            }

            broker.setDropTmpFolder();
        } catch (Exception e) {
            throw new BMQException(e);
        } finally {
            // Stop the session.
            session.stop(SessionStressIT.TEST_REQUEST_TIMEOUT);
            session.linger();

            broker.close();
        }
    }

    void restartBroker(
            BmqBroker broker,
            AbstractSession session,
            Exchanger<SessionEvent> sessionEventEx,
            LinkedList<Queue> queues,
            LinkedBlockingQueue<QueueControlEvent> queueEventFIFO)
            throws InterruptedException, TimeoutException {
        logger.info("Stopping the broker");
        broker.stop();

        logger.info("Waiting for the session event ConnectionLost");
        SessionEvent ev = null;
        ev = sessionEventEx.exchange(ev, 25, TimeUnit.SECONDS);
        assertTrue(ev instanceof SessionEvent.ConnectionLost);

        logger.info("Starting the broker");
        broker.start();

        logger.info("Waiting for the session event Reconnected");
        ev = sessionEventEx.exchange(ev, 25, TimeUnit.SECONDS);
        assertTrue(ev instanceof SessionEvent.Reconnected);

        logger.info("Waiting for the session event StateRestored");
        ev = sessionEventEx.exchange(ev, 25, TimeUnit.SECONDS);
        assertTrue(ev instanceof SessionEvent.StateRestored);

        logger.info("Checking queue events");
        outer:
        for (int i = 0; i < queues.size(); i++) {
            QueueControlEvent queueEv = queueEventFIFO.poll();
            assertNotNull(queueEv);
            assertTrue(queueEv instanceof QueueControlEvent.ReopenQueueResult);
            assertTrue(((QueueControlEvent.ReopenQueueResult) queueEv).result().isSuccess());
            for (Queue q : queues) {
                if (q.equals(queueEv.queue())) {
                    continue outer;
                }
            }
            throw new IllegalStateException("Unknown queue: " + queueEv.queue());
        }
        assertEquals(0, queueEventFIFO.size());
    }

    void testConnectionLost(int numQueues, int numMsgs) throws BMQException, IOException {
        Argument.expectPositive(numQueues, "numQueues");
        Argument.expectPositive(numMsgs, "numMsgs");

        final Duration TEST_REQUEST_TIMEOUT = Duration.ofSeconds(25);
        final int NUM_MESSAGES = numMsgs;
        final int MSG_SIZE = 4;
        final int NUM_PUTS_PER_EVENT = 5;
        final boolean WAIT_FOR_PUSH = false;

        long PRODUCER_QUEUE_FLAGS = QueueFlags.setWriter(0);
        PRODUCER_QUEUE_FLAGS = QueueFlags.setAck(PRODUCER_QUEUE_FLAGS);
        PRODUCER_QUEUE_FLAGS = QueueFlags.setReader(PRODUCER_QUEUE_FLAGS);

        QueueOptions QUEUE_OPTIONS =
                QueueOptions.builder()
                        .setConsumerPriority(1)
                        .setMaxUnconfirmedMessages(NUM_MESSAGES)
                        .setMaxUnconfirmedBytes(32768L * NUM_MESSAGES)
                        .build();

        LinkedBlockingQueue<Event> eventFIFO = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<QueueControlEvent> queueEventFIFO = new LinkedBlockingQueue<>();
        Exchanger<SessionEvent> sessionEventEx = new Exchanger<>();

        QueueEventHandler queueEventHandler =
                (QueueControlEvent event) -> {
                    try {
                        queueEventFIFO.put(event);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                };

        AckMessageHandler ackMessageHandler =
                (AckMessage msg) -> {
                    try {
                        eventFIFO.put(msg);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                };

        PushMessageHandler pushMessageHandler =
                (PushMessage msg) -> {
                    try {
                        eventFIFO.put(msg);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                };

        logger.info("Starting a Broker");
        BmqBroker broker = BmqBroker.createStartedBroker();

        AbstractSession session =
                new Session(
                        broker.sessionOptions(),
                        (SessionEvent event) -> {
                            try {
                                sessionEventEx.exchange(event, 25, TimeUnit.SECONDS);
                            } catch (TimeoutException e) {
                                logger.info("Exchange timeout");
                                throw new IllegalStateException("Missed event");
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException("Interrupted");
                            }
                        });

        try {
            logger.info("Starting a session");
            session.start(SessionStressIT.TEST_REQUEST_TIMEOUT);

            LinkedList<Queue> queues = new LinkedList<>();
            for (int i = 0; i < numQueues; i++) {
                final Uri QUEUE_URI = BmqBroker.Domains.Priority.generateQueueUri();

                Queue queue =
                        session.getQueue(
                                QUEUE_URI,
                                PRODUCER_QUEUE_FLAGS,
                                queueEventHandler, // QueueEventHandler
                                ackMessageHandler, // AckMessageHandler
                                pushMessageHandler // PushMessageHandler
                                );
                logger.info("Opening producer queue {}", i);
                queue.open(QUEUE_OPTIONS, TEST_REQUEST_TIMEOUT);
                queues.add(queue);
            }

            logger.info("Checking queue messaging");
            assertEquals(queues.size(), numQueues);
            for (Queue q : queues) {
                TransferValidator validator = new TransferValidator(eventFIFO, q, q);
                validator.transfer(MSG_SIZE, NUM_MESSAGES, NUM_PUTS_PER_EVENT, WAIT_FOR_PUSH);

                validator.validate();

                restartBroker(broker, session, sessionEventEx, queues, queueEventFIFO);
            }
            for (Queue q : queues) {
                // Close the queue.
                q.close(TEST_REQUEST_TIMEOUT);
            }

            broker.setDropTmpFolder();
        } catch (Exception e) {
            throw new BMQException(e);
        } finally {
            // Stop the session.
            session.stop(SessionStressIT.TEST_REQUEST_TIMEOUT);
            session.linger();

            broker.close();
        }
    }

    void testFanoutMode(int numReaderQueues, int numMsgs) throws BMQException, IOException {
        Argument.expectPositive(numReaderQueues, "numReaderQueues");
        Argument.expectPositive(numMsgs, "numMsgs");

        final Duration TEST_REQUEST_TIMEOUT = Duration.ofSeconds(25);
        final int NUM_MESSAGES = numMsgs;
        final int MSG_SIZE = 4;
        final int NUM_PUTS_PER_EVENT = 5;
        final boolean WAIT_FOR_PUSH = false;
        final String[] APP_IDS = {"foo", "bar", "baz"};

        Argument.expectNotGreater(numReaderQueues, APP_IDS.length, "numReaderQueues");

        long PRODUCER_QUEUE_FLAGS = QueueFlags.setWriter(0);
        PRODUCER_QUEUE_FLAGS = QueueFlags.setAck(PRODUCER_QUEUE_FLAGS);

        long CONSUMER_QUEUE_FLAGS = QueueFlags.setReader(0);

        QueueOptions QUEUE_OPTIONS =
                QueueOptions.builder()
                        .setConsumerPriority(1)
                        .setMaxUnconfirmedMessages(NUM_MESSAGES)
                        .setMaxUnconfirmedBytes(32768L * NUM_MESSAGES)
                        .build();

        LinkedBlockingQueue<Event> eventFIFO = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<QueueControlEvent> queueEventFIFO = new LinkedBlockingQueue<>();
        Exchanger<SessionEvent> sessionEventEx = new Exchanger<>();

        QueueEventHandler queueEventHandler =
                (QueueControlEvent event) -> {
                    try {
                        queueEventFIFO.put(event);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                };

        AckMessageHandler ackMessageHandler =
                (AckMessage msg) -> {
                    try {
                        eventFIFO.put(msg);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                };

        PushMessageHandler pushMessageHandler =
                (PushMessage msg) -> {
                    try {
                        eventFIFO.put(msg);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                };

        logger.info("Starting a Broker");

        BmqBroker broker = BmqBroker.createStartedBroker();

        AbstractSession session =
                new Session(
                        broker.sessionOptions(),
                        (SessionEvent event) -> {
                            try {
                                sessionEventEx.exchange(event, 25, TimeUnit.SECONDS);
                            } catch (TimeoutException e) {
                                logger.info("Exchange timeout");
                                throw new IllegalStateException("Missed event");
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException("Interrupted");
                            }
                        });

        try {
            logger.info("Starting a session");
            session.start(SessionStressIT.TEST_REQUEST_TIMEOUT);

            final Uri PRODUCER_QUEUE_URI = BmqBroker.Domains.Fanout.generateQueueUri();

            Queue producerQueue =
                    session.getQueue(
                            PRODUCER_QUEUE_URI,
                            PRODUCER_QUEUE_FLAGS,
                            queueEventHandler, // QueueEventHandler
                            ackMessageHandler, // AckMessageHandler
                            null // PushMessageHandler
                            );
            producerQueue.open(QUEUE_OPTIONS, TEST_REQUEST_TIMEOUT);

            Queue[] consumerQueues = new Queue[numReaderQueues];
            for (int i = 0; i < numReaderQueues; i++) {
                final Uri CONSUMER_QUEUE_URI =
                        BmqBroker.Domains.Fanout.generateQueueUri(PRODUCER_QUEUE_URI, APP_IDS[i]);
                Queue queue =
                        session.getQueue(
                                CONSUMER_QUEUE_URI,
                                CONSUMER_QUEUE_FLAGS,
                                queueEventHandler, // QueueEventHandler
                                null, // AckMessageHandler
                                pushMessageHandler // PushMessageHandler
                                );
                queue.open(QUEUE_OPTIONS, TEST_REQUEST_TIMEOUT);
                consumerQueues[i] = queue;
            }

            logger.info("Checking queue messaging");
            TransferValidator validator =
                    new TransferValidator(eventFIFO, producerQueue, consumerQueues);
            validator.transfer(MSG_SIZE, NUM_MESSAGES, NUM_PUTS_PER_EVENT, WAIT_FOR_PUSH);

            validator.validate();

            // Close the queues.
            producerQueue.close(TEST_REQUEST_TIMEOUT);
            for (Queue q : consumerQueues) {
                q.close(TEST_REQUEST_TIMEOUT);
            }

            broker.setDropTmpFolder();
        } catch (Exception e) {
            logger.error("Exception", e);
            throw new BMQException(e);
        } finally {
            // Stop the session.
            session.stop(SessionStressIT.TEST_REQUEST_TIMEOUT);
            session.linger();

            broker.close();
        }
    }

    @Test
    void testZlibIncompressableSize() throws BMQException, IOException {
        // Send multiple PUT messages each with unique payload via native Broker,
        // receive related PUSH messages from the queue, verify the payload.
        // For each PUT message Zlib compression is set, but payload( + props) is not compressed due
        // to its size less than 1 KiB

        logger.info("=========================================================");
        logger.info("BEGIN Testing SessionStressIT testZlibIncompressableSize.");
        logger.info("=========================================================");

        final int NUM_MESSAGES = 1000;
        final int MSG_SIZE =
                Protocol.COMPRESSION_MIN_APPDATA_SIZE - 1 - 60; // 1024 - 1 - props size
        final int NUM_PUTS_PER_EVENT = 1;

        final boolean WAIT_FOR_PUSH = true;

        testThroughput(
                MSG_SIZE,
                NUM_MESSAGES,
                NUM_PUTS_PER_EVENT,
                WAIT_FOR_PUSH,
                Optional.of(CompressionAlgorithm.Zlib));

        logger.info("=======================================================");
        logger.info("END Testing SessionStressIT testZlibIncompressableSize.");
        logger.info("=======================================================");
    }

    @Test
    void testZlibCompressableSize() throws BMQException, IOException {
        // Send multiple PUT messages each with unique payload via native Broker,
        // receive related PUSH messages from the queue, verify the payload.
        // For each PUT message Zlib compression is set

        logger.info("=======================================================");
        logger.info("BEGIN Testing SessionStressIT testZlibCompressableSize.");
        logger.info("=======================================================");

        final int NUM_MESSAGES = 1000;
        final int MSG_SIZE = Protocol.COMPRESSION_MIN_APPDATA_SIZE - 60; // 1024 - props size
        final int NUM_PUTS_PER_EVENT = 1;

        final boolean WAIT_FOR_PUSH = true;

        testThroughput(
                MSG_SIZE,
                NUM_MESSAGES,
                NUM_PUTS_PER_EVENT,
                WAIT_FOR_PUSH,
                Optional.of(CompressionAlgorithm.Zlib));

        logger.info("=====================================================");
        logger.info("END Testing SessionStressIT testZlibCompressableSize.");
        logger.info("=====================================================");
    }

    @Test
    void testZlibBigSize() throws BMQException, IOException {
        // Send multiple PUT messages each with unique payload via native Broker,
        // receive related PUSH messages from the queue, verify the payload.
        // For each PUT message Zlib compression is set

        logger.info("=======================================================");
        logger.info("BEGIN Testing SessionStressIT testZlibBigSize.");
        logger.info("=======================================================");

        final int NUM_MESSAGES = 100;
        final int MSG_SIZE = 1024 * 1024; // 1 MiB
        final int NUM_PUTS_PER_EVENT = 5;

        final boolean WAIT_FOR_PUSH = true;

        testThroughput(
                MSG_SIZE,
                NUM_MESSAGES,
                NUM_PUTS_PER_EVENT,
                WAIT_FOR_PUSH,
                Optional.of(CompressionAlgorithm.Zlib));

        logger.info("=====================================================");
        logger.info("END Testing SessionStressIT testZlibBigSize.");
        logger.info("=====================================================");
    }

    @Test
    void testMultipleEvents() throws BMQException, IOException {
        // Send multiple PUT messages each with unique payload via native Broker,
        // receive related PUSH messages from the queue, verify the payload.

        logger.info("===================================================");
        logger.info("BEGIN Testing SessionStressIT testMultipleEvents.");
        logger.info("===================================================");

        final int NUM_MESSAGES = 1000;
        final int MSG_SIZE = 4;
        final int NUM_PUTS_PER_EVENT = 1;

        final boolean WAIT_FOR_PUSH = true;

        testThroughput(MSG_SIZE, NUM_MESSAGES, NUM_PUTS_PER_EVENT, WAIT_FOR_PUSH);

        logger.info("=================================================");
        logger.info("END Testing SessionStressIT testMultipleEvents.");
        logger.info("=================================================");
    }

    @Test
    void testMultipleEventsNoPushWait() throws BMQException, IOException {
        // Send multiple PUT messages each with unique payload via native Broker,
        // waiting only for related ACK messages.
        // Receive related PUSH messages from the queue, verify the payload.

        logger.info("=============================================================");
        logger.info("BEGIN Testing SessionStressIT testMultipleEventsNoPushWait.");
        logger.info("=============================================================");

        final int NUM_MESSAGES = 1000;
        final int MSG_SIZE = 4;
        final int NUM_PUTS_PER_EVENT = 5;

        final boolean WAIT_FOR_PUSH = false;

        testThroughput(MSG_SIZE, NUM_MESSAGES, NUM_PUTS_PER_EVENT, WAIT_FOR_PUSH);

        logger.info("===========================================================");
        logger.info("END Testing SessionStressIT testMultipleEventsNoPushWait.");
        logger.info("===========================================================");
    }

    @Test
    void testMultipleQueuesConnectionLost() throws BMQException, IOException {
        // Create and open multiple read/write queues.
        // For each queue:
        //     - send multiple PUT messages via native Broker;
        //     - receive related PUSH messages from the queue, verify the payload;
        //     - restart the broker and check incoming REOPEN events for all the queues.
        // Close the queues, the session and the broker.

        logger.info("==============================================================");
        logger.info("BEGIN Testing SessionStressIT testMultipleQueuesConnectionLost");
        logger.info("==============================================================");

        final int NUM_QUEUES = 10;
        final int NUM_MESSAGES = 100;

        testConnectionLost(NUM_QUEUES, NUM_MESSAGES);

        logger.info("============================================================");
        logger.info("END Testing SessionStressIT testMultipleQueuesConnectionLost");
        logger.info("============================================================");
    }

    @Test
    void testFanoutThroughput() throws IOException {
        // Send multiple PUT messages each with unique payload via native Broker,
        // receive related PUSH messages from the queues, verify the payload.

        logger.info("===================================================");
        logger.info("BEGIN Testing SessionStressIT FanoutThroughputTest.");
        logger.info("===================================================");

        final int NUM_READER_QUEUES = 3;
        final int NUM_MESSAGES = 20;

        testFanoutMode(NUM_READER_QUEUES, NUM_MESSAGES);

        logger.info("=================================================");
        logger.info("END Testing SessionStressIT FanoutThroughputTest.");
        logger.info("=================================================");
    }

    @Test
    void testMultithreadedWriters() throws IOException {
        // Using one Session and multiple writer queues each posting from its
        // own thread send multiple PUT messages with unique payload via native Broker,
        // receive related PUSH messages from the queues, verify the payload.

        logger.info("=======================================================");
        logger.info("BEGIN Testing SessionStressIT testMultithreadedWriters.");
        logger.info("=======================================================");

        final int NUM_WRITERS = 10;
        final int NUM_MESSAGES_PER_WRITER = 1000;

        testMultithreadedWriteThroughput(NUM_WRITERS, NUM_MESSAGES_PER_WRITER);

        logger.info("=====================================================");
        logger.info("END Testing SessionStressIT testMultithreadedWriters.");
        logger.info("=====================================================");
    }
}
