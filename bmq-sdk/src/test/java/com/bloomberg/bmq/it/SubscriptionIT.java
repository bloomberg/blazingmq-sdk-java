/*
 * Copyright 2023 Bloomberg Finance L.P.
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

import static org.junit.Assert.*;

import com.bloomberg.bmq.*;
import com.bloomberg.bmq.Queue;
import com.bloomberg.bmq.it.util.BmqBroker;
import com.bloomberg.bmq.it.util.TestTools;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionIT {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(20);

    static class QueueTester {
        Queue queue;

        QueueOptions options;

        LinkedBlockingQueue<PushMessage> messages;

        private QueueTester(Queue queue, QueueOptions options) {
            this.queue = queue;
            this.options = options;

            messages = new LinkedBlockingQueue<>();
        }

        public void open() {
            if (!queue.isOpen()) {
                queue.open(options, DEFAULT_TIMEOUT);
            }
        }

        public void close() {
            if (queue.isOpen()) {
                logger.info("#QUEUE closing the queue [{}]...", queue.uri());
                queue.close(DEFAULT_TIMEOUT);
                logger.info("#QUEUE queue [{}] closed", queue.uri());
            }
        }

        public void configure(QueueOptions options) {
            assertTrue("Trying to configure not-opened queue", queue.isOpen());

            this.options = options;

            logger.info("#QUEUE configuring queue [{}]...", queue.uri());
            queue.configure(options, DEFAULT_TIMEOUT);
            logger.info("#QUEUE queue [{}] configured", queue.uri());
        }

        public void enqueueMessage(PushMessage message) {
            try {
                messages.put(message);
            } catch (InterruptedException e) {
                logger.error("Interrupted: ", e);
                Thread.currentThread().interrupt();
            }
        }

        public void expectMessage(String expectedPayload) {
            expectMessage(expectedPayload, null);
        }

        public void expectMessage(String expectedPayload, String userData) {
            try {
                PushMessage message = messages.poll(DEFAULT_TIMEOUT.getSeconds(), TimeUnit.SECONDS);
                assertNotNull(message);
                assertNotNull(message.subscription().getCorrelationId());

                ByteBuffer bb = TestTools.mergeBuffers(message.payload());
                String payload = new String(bb.array(), StandardCharsets.UTF_8);

                assertEquals(expectedPayload, payload);

                if (userData != null) {
                    assertNotNull(message.subscription().getCorrelationId());
                    assertNotNull(message.subscription().getCorrelationId().userData());
                    assertEquals(userData, message.subscription().getCorrelationId().userData());
                }

                message.confirm();
            } catch (InterruptedException e) {
                logger.error("Interrupted: ", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    static class Consumer
            implements AutoCloseable, SessionEventHandler, QueueEventHandler, PushMessageHandler {

        AbstractSession session;

        HashMap<Uri, QueueTester> openedQueues;

        private Consumer() {
            openedQueues = new HashMap<>();
        }

        public static Consumer createStarted(String brokerUri) {
            Consumer consumer = new Consumer();
            consumer.start(brokerUri);
            return consumer;
        }

        @Override
        public void handleSessionEvent(SessionEvent event) {
            logger.info("#CONSUMER handleSessionEvent: {}", event);
        }

        @Override
        public void handleQueueEvent(QueueControlEvent event) {
            logger.info("#CONSUMER handleQueueEvent: {}", event);
        }

        @Override
        public void handlePushMessage(PushMessage msg) {
            logger.debug("#CONSUMER PUSH message received: {}", msg);

            assertTrue(openedQueues.containsKey(msg.queue().uri()));
            openedQueues.get(msg.queue().uri()).enqueueMessage(msg);
        }

        private void start(String brokerUri) {
            logger.info("#CONSUMER starting session...");

            session =
                    new Session(
                            SessionOptions.builder().setBrokerUri(URI.create(brokerUri)).build(),
                            this); // SessionEventHandler
            session.start(DEFAULT_TIMEOUT);

            logger.info("#CONSUMER session started");
        }

        public QueueTester openQueue(Uri uri, QueueOptions options) {
            if (openedQueues.containsKey(uri)) {
                openedQueues.get(uri).close();
                openedQueues.remove(uri);
            }

            Queue queue =
                    session.getQueue(
                            uri, // Queue uri
                            QueueFlags.setReader(0), // Queue mode (=READ)
                            this, // QueueEventHandler
                            null, // AckMessageHandler
                            this); // PushMessageHandler

            QueueTester tester = new QueueTester(queue, options);

            logger.info("#CONSUMER opening queue [{}]...", uri);
            tester.open();
            logger.info("#CONSUMER queue [{}] opened", uri);

            openedQueues.put(uri, tester);
            return tester;
        }

        @Override
        public void close() {
            logger.info("#CONSUMER closing...");

            for (QueueTester tester : openedQueues.values()) {
                tester.close();
            }

            if (session != null) {
                logger.info("#CONSUMER stopping session...");
                session.stop(DEFAULT_TIMEOUT);
                logger.info("#CONSUMER session successfully stopped");

                logger.info("#CONSUMER lingering session...");
                session.linger();
                logger.info("#CONSUMER session linger complete");

                session = null;
            }

            logger.info("#CONSUMER closed");
        }
    }

    static class Producer
            implements AutoCloseable, SessionEventHandler, QueueEventHandler, AckMessageHandler {

        AbstractSession session;

        Queue queue;

        Set<CorrelationId> correlationIds = Collections.synchronizedSet(new HashSet<>());

        static class PayloadForwarder {
            // This helper class exists for better readability, compare:
            // ArrayList<String> expected = new ArrayList<>();
            // expected.add(producer.post("x", 123));      // post() -> String
            // producer.post("x", 123).appendTo(expected); // post() -> PayloadForwarder
            private final String payload;

            public PayloadForwarder(String payload) {
                this.payload = payload;
            }

            public void appendTo(List<String> container) {
                container.add(payload);
            }
        }

        private Producer() {
            // Empty
        }

        public static Producer createStarted(String brokerUri) {
            Producer producer = new Producer();
            producer.start(brokerUri);
            return producer;
        }

        @Override
        public void handleSessionEvent(SessionEvent event) {
            logger.info("#PRODUCER handleSessionEvent: {}", event);
        }

        @Override
        public void handleQueueEvent(QueueControlEvent event) {
            logger.info("#PRODUCER handleQueueEvent: {}", event);
        }

        @Override
        public void handleAckMessage(AckMessage msg) {
            logger.debug("#PRODUCER ACK message received: {}", msg);

            CorrelationId corrId = msg.correlationId();

            assertTrue(correlationIds.contains(corrId));
            correlationIds.remove(corrId);
        }

        public void start(String brokerUri) {
            logger.info("#PRODUCER starting session...");

            session =
                    new Session(
                            SessionOptions.builder().setBrokerUri(URI.create(brokerUri)).build(),
                            this); // SessionEventHandler
            session.start(DEFAULT_TIMEOUT);

            logger.info("#PRODUCER session started");
        }

        public void openQueue(Uri uri) {
            assertNull("Expect 'queue' to be null", queue);

            queue =
                    session.getQueue(
                            uri, // Queue uri
                            QueueFlags.setWriter(0), // Queue mode (=WRITE)
                            this, // QueueEventHandler
                            this, // AckMessageHandler
                            null); // PushMessageHandler

            logger.info("#PRODUCER opening queue [{}]...", uri);
            queue.open(QueueOptions.createDefault(), DEFAULT_TIMEOUT);
            logger.info("#PRODUCER queue [{}] opened", uri);
        }

        public PayloadForwarder post(String propertyName, int propertyValue) {
            String payload = String.format("%s is %d", propertyName, propertyValue);

            byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
            ByteBuffer bb = ByteBuffer.wrap(bytes);

            PutMessage msg = queue.createPutMessage(bb);

            // This correlationId makes sense only for the current producer, it will appear
            // in the received ACK message.  Consumer will receive correlationId associated
            // with its subscription handle instead.
            // Set this correlationId just for testing, and use message payload to verify
            // delivery on consumer side.
            CorrelationId corrId = msg.setCorrelationId(payload);
            assertTrue(correlationIds.add(corrId));

            msg.setCompressionAlgorithm(CompressionAlgorithm.Zlib);

            MessageProperties mp = msg.messageProperties();
            mp.setPropertyAsInt32(propertyName, propertyValue);

            queue.post(msg);

            return new PayloadForwarder(payload);
        }

        public void close() {
            logger.info("#PRODUCER closing...");

            if (queue != null && queue.isOpen()) {
                logger.info("#PRODUCER closing the queue [{}]...", queue.uri());
                queue.close(DEFAULT_TIMEOUT);
                logger.info("#PRODUCER queue successfully closed");

                queue = null;
            }

            if (session != null) {
                logger.info("#PRODUCER stopping session...");
                session.stop(DEFAULT_TIMEOUT);
                logger.info("#PRODUCER session successfully stopped");

                logger.info("#PRODUCER lingering session...");
                session.linger();
                logger.info("#PRODUCER session linger complete");

                session = null;
            }

            logger.info("#PRODUCER closed");
        }
    }

    @Test
    public void testBreathing() throws BMQException, IOException {
        logger.info("=============================================");
        logger.info("BEGIN Testing SubscriptionIT testBreathing.");
        logger.info("=============================================");

        try (BmqBroker broker = BmqBroker.createStoppedBroker()) {
            logger.info("Step 1: Bring up the broker");

            Assert.assertFalse(broker.isOldStyleMessageProperties());

            broker.start();

            logger.info("Step 2: Start producer/consumer");

            Consumer consumer =
                    Consumer.createStarted(broker.sessionOptions().brokerUri().toString());
            Producer producer =
                    Producer.createStarted(broker.sessionOptions().brokerUri().toString());

            logger.info("Step 3: Open consumer, queue with subscriptions");
            Uri uri = BmqBroker.Domains.Priority.generateQueueUri();

            Subscription s1 = Subscription.builder().setExpressionText("x >= 10").build();

            Subscription s2 = Subscription.builder().setExpressionText("y >= 30").build();

            QueueOptions options =
                    QueueOptions.builder().addSubscription(s1).addSubscription(s2).build();

            QueueTester q = consumer.openQueue(uri, options);

            logger.info("Step 4: Open producer, produce messages");

            producer.openQueue(uri);

            ArrayList<String> expected = new ArrayList<>();

            producer.post("x", 15).appendTo(expected);
            producer.post("x", 27).appendTo(expected);

            producer.post("x", 6); // not expected
            producer.post("y", 29); // not expected

            producer.post("y", 30).appendTo(expected);
            producer.post("y", 901).appendTo(expected);

            logger.info("Step 4: Consume messages");

            for (String payload : expected) {
                q.expectMessage(payload);
            }

            logger.info("Step 5: Close producer/consumer");

            producer.close();
            consumer.close();
        }

        logger.info("===========================================");
        logger.info("END Testing SubscriptionIT testBreathing.");
        logger.info("===========================================");
    }

    @Test
    public void testFanout() throws BMQException, IOException {
        logger.info("=============================================");
        logger.info("BEGIN Testing SubscriptionIT testFanout.");
        logger.info("=============================================");

        final String[] APP_IDS = {"foo", "bar", "baz"};

        try (BmqBroker broker = BmqBroker.createStoppedBroker()) {
            logger.info("Step 1: Bring up the broker");

            Assert.assertFalse(broker.isOldStyleMessageProperties());

            broker.start();

            logger.info("Step 2: Start producer");
            Uri producerUri = BmqBroker.Domains.Fanout.generateQueueUri();
            Producer producer =
                    Producer.createStarted(broker.sessionOptions().brokerUri().toString());
            Consumer consumer =
                    Consumer.createStarted(broker.sessionOptions().brokerUri().toString());

            logger.info("Step 3: Start and open fanout consumers");

            QueueTester[] queues = new QueueTester[APP_IDS.length];
            for (int i = 0; i < APP_IDS.length; i++) {
                Subscription s1 = Subscription.builder().setExpressionText("x >= 10").build();

                QueueOptions options = QueueOptions.builder().addSubscription(s1).build();

                final Uri consumerUri =
                        BmqBroker.Domains.Fanout.generateQueueUri(producerUri, APP_IDS[i]);

                queues[i] = consumer.openQueue(consumerUri, options);
            }

            logger.info("Step 4: Open producer, produce messages");
            producer.openQueue(producerUri);

            ArrayList<String> expected = new ArrayList<>();

            producer.post("x", 15).appendTo(expected);
            producer.post("x", 27).appendTo(expected);

            producer.post("x", 6); // not expected
            producer.post("x", -4); // not expected

            producer.post("x", 31).appendTo(expected);
            producer.post("x", 56).appendTo(expected);

            logger.info("Step 4: Consume messages");

            for (QueueTester queue : queues) {
                for (String payload : expected) {
                    queue.expectMessage(payload);
                }
            }

            logger.info("Step 5: Close producer/consumer");

            producer.close();
            consumer.close();
        }

        logger.info("===========================================");
        logger.info("END Testing SubscriptionIT testFanout.");
        logger.info("===========================================");
    }

    @Test
    public void testReuseQueueOptions() throws BMQException, IOException {
        logger.info("=============================================");
        logger.info("BEGIN Testing SubscriptionIT testReuseQueueOptions.");
        logger.info("=============================================");

        final String[] APP_IDS = {"foo", "bar", "baz"};

        try (BmqBroker broker = BmqBroker.createStoppedBroker()) {
            logger.info("Step 1: Bring up the broker");

            Assert.assertFalse(broker.isOldStyleMessageProperties());

            broker.start();

            logger.info("Step 2: Start producer");
            Uri producerUri = BmqBroker.Domains.Fanout.generateQueueUri();
            Producer producer =
                    Producer.createStarted(broker.sessionOptions().brokerUri().toString());
            Consumer consumer =
                    Consumer.createStarted(broker.sessionOptions().brokerUri().toString());

            logger.info("Step 3: Build the common QueueOptions");

            // Is it okay to reuse QueueOptions?
            Subscription s1 = Subscription.builder().setExpressionText("x >= 10").build();
            QueueOptions options = QueueOptions.builder().addSubscription(s1).build();

            logger.info("Step 4: Start and open fanout consumers");

            QueueTester[] queues = new QueueTester[APP_IDS.length];
            for (int i = 0; i < APP_IDS.length; i++) {
                final Uri consumerUri =
                        BmqBroker.Domains.Fanout.generateQueueUri(producerUri, APP_IDS[i]);
                queues[i] = consumer.openQueue(consumerUri, options);
            }

            logger.info("Step 5: Open producer, produce messages");
            producer.openQueue(producerUri);

            ArrayList<String> expected = new ArrayList<>();

            producer.post("x", 15).appendTo(expected);
            producer.post("x", 27).appendTo(expected);

            producer.post("x", 6); // not expected
            producer.post("x", -4); // not expected

            producer.post("x", 31).appendTo(expected);
            producer.post("x", 56).appendTo(expected);

            logger.info("Step 6: Consume messages");

            for (QueueTester queue : queues) {
                for (String payload : expected) {
                    queue.expectMessage(payload);
                }
            }

            logger.info("Step 7: Close producer/consumer");

            producer.close();
            consumer.close();
        }

        logger.info("===========================================");
        logger.info("END Testing SubscriptionIT testFanout.");
        logger.info("===========================================");
    }

    @Test
    public void testUpdateSubscription() throws BMQException, IOException {
        logger.info("=============================================");
        logger.info("BEGIN Testing SubscriptionIT testUpdateSubscription.");
        logger.info("=============================================");

        final String HANDLE_USER_DATA = "handle";

        try (BmqBroker broker = BmqBroker.createStoppedBroker()) {
            logger.info("Step 1: Bring up the broker");

            Assert.assertFalse(broker.isOldStyleMessageProperties());

            broker.start();

            logger.info("Step 2: Start producer/consumer");

            Consumer consumer =
                    Consumer.createStarted(broker.sessionOptions().brokerUri().toString());
            Producer producer =
                    Producer.createStarted(broker.sessionOptions().brokerUri().toString());

            logger.info("Step 3: Open consumer, queue with subscriptions");
            Uri uri = BmqBroker.Domains.Priority.generateQueueUri();

            Subscription s1 =
                    Subscription.builder()
                            .setExpressionText("x >= 10")
                            .setUserData(HANDLE_USER_DATA)
                            .build();
            Subscription s2 =
                    Subscription.builder()
                            .setExpressionText("x >= -1000")
                            .setUserData(HANDLE_USER_DATA)
                            .build();

            // Passing the same handle, expect only one final subscription 's1'
            QueueOptions options =
                    QueueOptions.builder()
                            .addSubscription(s2)
                            .addSubscription(s1)
                            .removeSubscription(s2)
                            .build();

            QueueTester queue = consumer.openQueue(uri, options);

            logger.info("Step 4: Open producer, produce messages");

            producer.openQueue(uri);

            ArrayList<String> expected_step4 = new ArrayList<>();
            ArrayList<String> expected_step5 = new ArrayList<>();

            producer.post("x", 15).appendTo(expected_step4);
            producer.post("x", 27).appendTo(expected_step4);

            producer.post("x", 6).appendTo(expected_step5); // not expected here
            producer.post("x", 4).appendTo(expected_step5); // not expected here

            producer.post("x", 30).appendTo(expected_step4);
            producer.post("x", 901).appendTo(expected_step4);

            logger.info("Step 4: Consume messages");

            for (String payload : expected_step4) {
                queue.expectMessage(payload, HANDLE_USER_DATA);
            }

            logger.info("Step 5: Reconfigure consumer and consume remaining messages");

            // Use less strict subscription expression to receive the remaining messages
            QueueOptions options_step5 = QueueOptions.builder().addSubscription(s2).build();

            queue.configure(options_step5);
            for (String payload : expected_step5) {
                queue.expectMessage(payload, HANDLE_USER_DATA);
            }

            logger.info("Step 6: Close producer/consumer");

            producer.close();
            consumer.close();
        }

        logger.info("===========================================");
        logger.info("END Testing SubscriptionIT testBreathing.");
        logger.info("===========================================");
    }

    @Test
    public void testStress() throws BMQException, IOException {
        logger.info("=============================================");
        logger.info("BEGIN Testing SubscriptionIT testStress.");
        logger.info("=============================================");

        final int EPOCH_NUM = 10;
        final int SUBSCRIPTIONS_NUM = 256;
        final int MESSAGES_PER_SUBSCRIPTION = 10;
        final Uri uri = BmqBroker.Domains.Priority.generateQueueUri();

        try (BmqBroker broker = BmqBroker.createStoppedBroker()) {
            logger.info("Step 1: Bring up the broker");

            Assert.assertFalse(broker.isOldStyleMessageProperties());

            broker.start();

            logger.info("Step 2: Start producer/consumer");

            Consumer consumer =
                    Consumer.createStarted(broker.sessionOptions().brokerUri().toString());
            Producer producer =
                    Producer.createStarted(broker.sessionOptions().brokerUri().toString());

            logger.info("Step 3: Open producer");
            producer.openQueue(uri);

            logger.info("Step 4: Run producer/consumer");

            for (int epoch = 0; epoch < EPOCH_NUM; epoch++) {
                logger.info("Epoch {}/{}", epoch + 1, EPOCH_NUM);

                QueueOptions.Builder builder = QueueOptions.builder();
                ArrayList<String> userDataList = new ArrayList<>();
                for (int sIndex = 0; sIndex < SUBSCRIPTIONS_NUM; sIndex++) {
                    String userData = "epoch" + epoch + "_s" + sIndex;
                    Subscription subscription =
                            Subscription.builder()
                                    .setExpressionText("x == " + sIndex)
                                    .setUserData(userData)
                                    .build();
                    userDataList.add(userData);
                    builder.addSubscription(subscription);
                }

                QueueOptions options = builder.build();
                QueueTester queue = consumer.openQueue(uri, options);

                for (int sIndex = 0; sIndex < SUBSCRIPTIONS_NUM; sIndex++) {
                    ArrayList<String> expected = new ArrayList<>();
                    for (int messageIndex = 0;
                            messageIndex < MESSAGES_PER_SUBSCRIPTION;
                            messageIndex++) {
                        producer.post("x", sIndex).appendTo(expected);
                    }

                    for (int messageIndex = 0;
                            messageIndex < MESSAGES_PER_SUBSCRIPTION;
                            messageIndex++) {
                        queue.expectMessage(expected.get(messageIndex), userDataList.get(sIndex));
                    }
                }

                queue.close();
            }

            logger.info("Step 5: Close producer/consumer");

            producer.close();
            consumer.close();
        }

        logger.info("===========================================");
        logger.info("END Testing SubscriptionIT testStress.");
        logger.info("===========================================");
    }
}
