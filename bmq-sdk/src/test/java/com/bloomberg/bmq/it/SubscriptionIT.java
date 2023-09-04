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
import com.bloomberg.bmq.it.util.BmqBrokerTestServer;
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

    static class Consumer
            implements AutoCloseable, SessionEventHandler, QueueEventHandler, PushMessageHandler {

        AbstractSession session;

        Queue queue;

        QueueOptions options;

        LinkedBlockingQueue<PushMessage> messages;

        private Consumer() {
            messages = new LinkedBlockingQueue<>();
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
            logger.info("#CONSUMER PUSH message received: {}", msg);

            try {
                messages.put(msg);
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
                assertNotNull(message.correlationId());

                ByteBuffer bb = TestTools.mergeBuffers(message.payload());
                String payload = new String(bb.array(), StandardCharsets.UTF_8);

                assertEquals(expectedPayload, payload);

                if (userData != null) {
                    assertNotNull(message.correlationId());
                    assertNotNull(message.correlationId().userData());
                    assertEquals(userData, message.correlationId().userData());
                }

                message.confirm();
            } catch (InterruptedException e) {
                logger.error("Interrupted: ", e);
                Thread.currentThread().interrupt();
            }
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

        public void openQueue(Uri uri, QueueOptions options) {
            assertNull("Expect 'queue' to be null", queue);

            this.options = options;

            queue =
                    session.getQueue(
                            uri, // Queue uri
                            QueueFlags.setReader(0), // Queue mode (=READ)
                            this, // QueueEventHandler
                            null, // AckMessageHandler
                            this); // PushMessageHandler

            logger.info("#CONSUMER opening queue [{}]...", uri);
            queue.open(options, DEFAULT_TIMEOUT);
            logger.info("#CONSUMER queue [{}] opened", uri);
        }

        public void closeQueue() {
            assertNotNull("Expect 'queue' to be non-null", queue);

            Uri uri = queue.uri();

            logger.info("#CONSUMER closing the queue [{}]...", uri);
            queue.close(DEFAULT_TIMEOUT);
            logger.info("#CONSUMER queue [{}] closed", uri);

            options = null;
            queue = null;
        }

        public void configureQueue(QueueOptions options) {
            assertNotNull("Expect 'queue' to be non-null", queue);

            this.options = options;

            logger.info("#CONSUMER configuring queue [{}]...", queue.uri());
            queue.configure(options, DEFAULT_TIMEOUT);
            logger.info("#CONSUMER queue [{}] configured", queue.uri());
        }

        @Override
        public void close() {
            logger.info("#CONSUMER closing...");

            if (queue != null && queue.isOpen()) {
                closeQueue();
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
            logger.info("#PRODUCER ACK message received: {}", msg);

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

        final int TESTED_PORT = 30114; // SystemUtil.getEphemeralPort();

        try (BmqBroker broker = BmqBrokerTestServer.createStoppedBroker(TESTED_PORT)) {
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

            consumer.openQueue(uri, options);

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
                consumer.expectMessage(payload);
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

        final int TESTED_PORT = 30114; // SystemUtil.getEphemeralPort();
        final String[] APP_IDS = {"foo", "bar", "baz"};

        try (BmqBroker broker = BmqBrokerTestServer.createStoppedBroker(TESTED_PORT)) {
            logger.info("Step 1: Bring up the broker");

            Assert.assertFalse(broker.isOldStyleMessageProperties());

            broker.start();

            logger.info("Step 2: Start producer");
            Uri producerUri = BmqBroker.Domains.Fanout.generateQueueUri();
            Producer producer =
                    Producer.createStarted(broker.sessionOptions().brokerUri().toString());

            logger.info("Step 3: Start and open fanout consumers");

            Consumer[] consumers = new Consumer[APP_IDS.length];
            for (int i = 0; i < APP_IDS.length; i++) {
                // Is it okay to reuse QueueOptions?
                Subscription s1 = Subscription.builder().setExpressionText("x >= 10").build();

                QueueOptions options = QueueOptions.builder().addSubscription(s1).build();

                final Uri consumerUri =
                        BmqBroker.Domains.Fanout.generateQueueUri(producerUri, APP_IDS[i]);

                Consumer consumer =
                        Consumer.createStarted(broker.sessionOptions().brokerUri().toString());
                consumer.openQueue(consumerUri, options);

                consumers[i] = consumer;
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

            for (Consumer consumer : consumers) {
                for (String payload : expected) {
                    consumer.expectMessage(payload);
                }
            }

            logger.info("Step 5: Close producer/consumer");

            producer.close();
            for (Consumer consumer : consumers) {
                consumer.close();
            }
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

        final int TESTED_PORT = 30114; // SystemUtil.getEphemeralPort();
        final String HANDLE_USER_DATA = "handle";

        try (BmqBroker broker = BmqBrokerTestServer.createStoppedBroker(TESTED_PORT)) {
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
            Subscription s2 = Subscription.builder().setExpressionText("x >= -1000").build();
            SubscriptionHandle h1 = new SubscriptionHandle(HANDLE_USER_DATA);

            // Passing the same handle, expect only one final subscription 's1'
            QueueOptions options =
                    QueueOptions.builder()
                            .addOrUpdateSubscription(h1, s2)
                            .addOrUpdateSubscription(h1, s1)
                            .build();

            consumer.openQueue(uri, options);

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
                consumer.expectMessage(payload, HANDLE_USER_DATA);
            }

            logger.info("Step 5: Reconfigure consumer and consume remaining messages");

            // Use less strict subscription expression to receive the remaining messages
            QueueOptions options_step5 =
                    QueueOptions.builder().merge(options).addOrUpdateSubscription(h1, s2).build();

            consumer.configureQueue(options_step5);
            for (String payload : expected_step5) {
                consumer.expectMessage(payload, HANDLE_USER_DATA);
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

        final int TESTED_PORT = 30114; // SystemUtil.getEphemeralPort();
        final int EPOCH_NUM = 10;
        final int SUBSCRIPTIONS_NUM = 256;
        final int MESSAGES_PER_SUBSCRIPTION = 10;
        final Uri uri = BmqBroker.Domains.Priority.generateQueueUri();

        try (BmqBroker broker = BmqBrokerTestServer.createStoppedBroker(TESTED_PORT)) {
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
                    Subscription subscription =
                            Subscription.builder().setExpressionText("x == " + sIndex).build();

                    String userData = "epoch" + epoch + "_s" + sIndex;
                    userDataList.add(userData);
                    SubscriptionHandle handle = new SubscriptionHandle(userData);

                    builder.addOrUpdateSubscription(handle, subscription);
                }

                QueueOptions options = builder.build();
                consumer.openQueue(uri, options);

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
                        consumer.expectMessage(
                                expected.get(messageIndex), userDataList.get(sIndex));
                    }
                }

                consumer.closeQueue();
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
