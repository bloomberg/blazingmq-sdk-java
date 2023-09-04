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

import com.bloomberg.bmq.AbstractSession;
import com.bloomberg.bmq.AckMessage;
import com.bloomberg.bmq.AckMessageHandler;
import com.bloomberg.bmq.BMQException;
import com.bloomberg.bmq.CompressionAlgorithm;
import com.bloomberg.bmq.CorrelationId;
import com.bloomberg.bmq.MessageProperties;
import com.bloomberg.bmq.PushMessage;
import com.bloomberg.bmq.PushMessageHandler;
import com.bloomberg.bmq.PutMessage;
import com.bloomberg.bmq.Queue;
import com.bloomberg.bmq.QueueControlEvent;
import com.bloomberg.bmq.QueueEventHandler;
import com.bloomberg.bmq.QueueFlags;
import com.bloomberg.bmq.QueueOptions;
import com.bloomberg.bmq.Session;
import com.bloomberg.bmq.SessionEvent;
import com.bloomberg.bmq.SessionEventHandler;
import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.Subscription;
import com.bloomberg.bmq.Uri;
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
            try {
                PushMessage message = messages.poll(DEFAULT_TIMEOUT.getSeconds(), TimeUnit.SECONDS);
                assertNotNull(message);
                assertNotNull(message.correlationId());

                ByteBuffer bb = TestTools.mergeBuffers(message.payload());
                String payload = new String(bb.array(), StandardCharsets.UTF_8);

                assertEquals(expectedPayload, payload);

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

        @Override
        public void close() {
            logger.info("#CONSUMER closing...");

            if (queue != null && queue.isOpen()) {
                logger.info("#CONSUMER closing the queue [{}]...", queue.uri());
                queue.close(DEFAULT_TIMEOUT);
                logger.info("#CONSUMER queue successfully closed");

                queue = null;
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

        public String post(String propertyName, int propertyValue) {
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

            return payload;
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

            expected.add(producer.post("x", 15));
            expected.add(producer.post("x", 27));

            producer.post("x", 6); // not expected
            producer.post("y", 29); // not expected

            expected.add(producer.post("y", 30));
            expected.add(producer.post("y", 901));

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

            expected.add(producer.post("x", 15));
            expected.add(producer.post("x", 27));

            producer.post("x", 6); // not expected
            producer.post("x", -4); // not expected

            expected.add(producer.post("x", 31));
            expected.add(producer.post("x", 56));

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
}
