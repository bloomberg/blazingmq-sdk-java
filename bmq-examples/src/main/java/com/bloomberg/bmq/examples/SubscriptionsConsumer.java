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
package com.bloomberg.bmq.examples;

import com.bloomberg.bmq.AbstractSession;
import com.bloomberg.bmq.BMQException;
import com.bloomberg.bmq.MessageProperty;
import com.bloomberg.bmq.PushMessage;
import com.bloomberg.bmq.PushMessageHandler;
import com.bloomberg.bmq.Queue;
import com.bloomberg.bmq.QueueControlEvent;
import com.bloomberg.bmq.QueueEventHandler;
import com.bloomberg.bmq.QueueFlags;
import com.bloomberg.bmq.QueueOptions;
import com.bloomberg.bmq.ResultCodes;
import com.bloomberg.bmq.Session;
import com.bloomberg.bmq.SessionEvent;
import com.bloomberg.bmq.SessionEventHandler;
import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.Subscription;
import com.bloomberg.bmq.SubscriptionExpression;
import com.bloomberg.bmq.SubscriptionHandle;
import com.bloomberg.bmq.Uri;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Iterator;
import java.util.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample application which connects to BlazingMQ broker and consumes messages. Part of the
 * BlazingMQ tutorial.
 *
 * <h2>Interactive Producer/Consumer use case</h2>
 *
 * The simplest use case involves a Producer, a Consumer and a Message Broker. The Producer and/or
 * the Consumer create a named queue in the Broker. The Producer pushes messages into the queue, and
 * the consumer pulls them. Once a message has been consumed it can be removed from the queue.
 *
 * <p>In essence the queue is a mailbox for messages to be sent to a Consumer. It allows decoupling
 * the processing between the Producer and the Consumer: one of the tasks may be down without
 * impacting the other, and the Consumer can process messages at a different pace than the Producer.
 *
 * <h2>Consumer</h2>
 *
 * In a nutshell, this example does the following:
 *
 * <ul>
 *   <li>1. Connects to BlazingMQ framework
 *   <li>2. Opens queue in read mode
 *   <li>3. Consumes messages from the queue
 * </ul>
 *
 * <p>In order to consume messages, {@code Consumer} class implements {@link PushMessageHandler}
 * interface. When new messages are being delivered from the queue, {@link #handlePushMessage}
 * method is called.
 *
 * <p>Also, {@code Consumer} implements {@link SessionEventHandler} and {@link QueueEventHandler}
 * interfaces in order to be able to handle various events related to {@link Session} and {@link
 * Queue}.
 *
 * <p>Finally, {@code Consumer} implements {@link AutoCloseable} interface. When used in
 * 'try-with-resources' statement, {@link #close} method will be called automatically after
 * execution of the block. Thus the connection to BlazingMQ framework will be properly closed.
 */
public class SubscriptionsConsumer
        implements AutoCloseable, SessionEventHandler, QueueEventHandler, PushMessageHandler {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static final Uri uri = new Uri("bmq://bmq.test.mem.priority/test-queue");

    AbstractSession session;
    Queue queue;

    public SubscriptionsConsumer() {
        // Empty: call `run` to start the built Consumer
    }

    @Override
    public void handleSessionEvent(SessionEvent event) {
        // User specified handler for session events

        logger.info("handleSessionEvent: {}", event);
    }

    @Override
    public void handleQueueEvent(QueueControlEvent event) {
        // User specified handler for queue events

        logger.info("handleQueueEvent: {}", event);
    }

    @Override
    public void handlePushMessage(PushMessage msg) {
        // User specified PUSH event handler

        logger.info("*** Got a message from the queue.***");
        logger.info("GUID: {}", msg.messageGUID());
        logger.info("CorrelationId: {}", msg.correlationId().userData());
        logger.info("Queue URI: {}", msg.queue().uri());

        // Check if any properties are associated with the message.
        // Typically, applications would use properties for internal routing, filtering,
        // pipelining, etc.
        if (msg.hasMessageProperties()) {
            // There are two ways to access message properties.
            // In this example, we will use both.

            // First, we may simply iterate over the properties and print them.
            Iterator<MessageProperty> propIterator = msg.propertyIterator();
            while (propIterator.hasNext()) {
                MessageProperty prop = propIterator.next();

                if (logger.isInfoEnabled()) {
                    logger.info(
                            "Property name: {} type: {} value: {}",
                            prop.name(),
                            prop.type(),
                            prop.value());
                }
            }

            // Second, we may use `getProperty` method to access properties by their name.
            // If a property with such name exists, a reference to the property
            // is returned. Otherwise, the method returns null.
            MessageProperty timestampProperty = msg.getProperty("timestamp");
            if (timestampProperty != null) {
                // We may also use MessageProperty `toString()` method
                // to print property information into the log
                logger.info("Found property: {}", timestampProperty);
            }
        }

        // Get the data in the message.
        StringBuilder contentBuilder = new StringBuilder();
        for (ByteBuffer bb : msg.payload()) {
            // It is safe to convert ByteBuffer payload to String only if it's known that
            // payload represents a valid string
            // (e.g. it doesn't make sense to do this if producer sent a protobuf encoded
            // payload)
            String content = StandardCharsets.UTF_8.decode(bb).toString();
            contentBuilder.append(content);
        }

        logger.info("Content: {}", contentBuilder);

        // Confirm reception of the messages so that it can be deleted from the queue
        ResultCodes.GenericResult confirmResult = msg.confirm();
        logger.info("Confirm status: {}", confirmResult);

        logger.info("*** Press enter to exit ***");
    }

    public void run() {
        logger.info("*** Run consumer ***");

        // Create a new session object and get interface to it.
        // Provide session options and session event handler object
        String brokerUri = System.getenv("BMQ_BROKER_URI");
        if (brokerUri == null) {
            brokerUri = "tcp://localhost:30114";
        }

        session =
                new Session(
                        SessionOptions.builder().setBrokerUri(URI.create(brokerUri)).build(),
                        this); // SessionEventHandler

        logger.info("Starting the session with a timeout of 15 seconds");

        // Start the session with the message broker. Since the session was provided default
        // options,
        // this makes the SDK connect to the local broker.
        session.start(Duration.ofSeconds(15));

        logger.info("Session successfully started");

        // We are now connected to the broker. The next step is to open the queue
        // and read messages.

        // Create a queue representation.
        // Each queue is identified by a short URL containing a namespace and a queue name.
        // We don't need to pass a valid AckMessageHandler here because we will only consume from
        // the queue, and thus will not receive ACK messages
        queue =
                session.getQueue(
                        uri, // Queue uri
                        QueueFlags.setReader(0), // Queue mode (=READ)
                        this, // QueueEventHandler
                        null, // AckMessageHandler
                        this); // PushMessageHandler

        logger.info("Opening queue [{}] with a timeout of 15 seconds", uri);

        Subscription s1 =
                Subscription.builder()
                        .setExpressionText("firmId == \"FOO\"")
                        .setExpressionVersion(SubscriptionExpression.Version.e_VERSION_1)
                        .build();
        SubscriptionHandle h1 =
                SubscriptionHandle.builder().setCorrelationIdUserData("foo").build();

        Subscription s2 =
                Subscription.builder()
                        .setExpressionText("firmId == \"BAR\" && price < 25")
                        .setExpressionVersion(SubscriptionExpression.Version.e_VERSION_1)
                        .build();
        SubscriptionHandle h2 =
                SubscriptionHandle.builder().setCorrelationIdUserData("bar_low_price").build();

        Subscription s3 =
                Subscription.builder()
                        .setExpressionText("firmId == \"BAR\" && price >= 25")
                        .setExpressionVersion(SubscriptionExpression.Version.e_VERSION_1)
                        .build();
        SubscriptionHandle h3 =
                SubscriptionHandle.builder().setCorrelationIdUserData("bar_high_price").build();

        QueueOptions options =
                QueueOptions.builder()
                        .addOrUpdateSubscription(h1, s1)
                        .addOrUpdateSubscription(h2, s2)
                        .addOrUpdateSubscription(h3, s3)
                        .build();

        // Open a queue using default options
        queue.open(
                options, // Default queue options
                Duration.ofSeconds(15)); // Timeout

        logger.info("Successfully opened the queue [{}]", uri);
    }

    public void close() {
        logger.info("*** Close consumer ***");

        if (queue != null && queue.isOpen()) {
            // Close the queue
            logger.info("Closing the queue [{}]", uri);

            // Sometimes it's not possible to close the queue, so an exception may be thrown
            try {
                queue.close(Duration.ofMinutes(1));
                logger.info("Queue successfully closed");
            } catch (BMQException e) {
                logger.warn("BMQException when closing the queue [{}]: {}", uri, e.getMessage());
            }
        }

        if (session != null) {
            // Gracefully disconnect from the BlazingMQ broker and stop the operation of this
            // session
            logger.info("Stopping session");

            // Sometimes it's not possible to stop the session, so an exception may be thrown
            try {
                session.stop(Duration.ofSeconds(5));
                logger.info("Session successfully stopped");
            } catch (BMQException e) {
                logger.warn("BMQException when stopping the session: ", e);
            }

            // Shutdown all connection and event handling threads
            logger.info("Lingering session");
            // Session linger doesn't throw exceptions
            session.linger();
            logger.info("Session linger complete");
        }
    }

    static class Util {
        private Util() {
            throw new IllegalStateException("Utility class");
        }

        public static String getStackTrace(Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            return sw.toString();
        }
    }

    public static void main(String[] args) {
        // Consumer class implements AutoCloseable interface and can be used in `try-with-resource`
        // statement.
        // But here we close it manually in order to display error messages before
        // log messages printed in Consumer.close() method
        SubscriptionsConsumer consumer = new SubscriptionsConsumer();
        try (Scanner sc = new Scanner(System.in, StandardCharsets.UTF_8.name())) {
            consumer.run();

            logger.info("*** Waiting for messages (press enter to exit) ***");

            // Expect user input in UTF8 encoding
            sc.nextLine();
        } catch (BMQException e) {
            // Catch BlazingMQ specific exception first
            logger.error("BMQException: {}, StackTrace: {}", e.getMessage(), Util.getStackTrace(e));

            // BlazingMQ related handling code
        } catch (Exception e) {
            // Catch other exceptions
            logger.error("Exception: {}, StackTrace: {}", e.getMessage(), Util.getStackTrace(e));

            // Common exception handling code
        } finally {
            consumer.close();
        }
    }
}
