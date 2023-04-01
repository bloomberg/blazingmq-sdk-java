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
import com.bloomberg.bmq.AckMessage;
import com.bloomberg.bmq.AckMessageHandler;
import com.bloomberg.bmq.BMQException;
import com.bloomberg.bmq.CompressionAlgorithm;
import com.bloomberg.bmq.CorrelationId;
import com.bloomberg.bmq.MessageProperties;
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
import com.bloomberg.bmq.Uri;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample application which connects to BlazingMQ broker, takes user input and send messages. Part of
 * the BlazingMQ tutorial.
 *
 * <h2>Producer/Consumer use case</h2>
 *
 * The simplest use case involves a Producer, a Consumer and a Message Broker. The Producer and/or
 * the Consumer create a named queue in the Broker. The Producer pushes messages into the queue, and
 * the consumer pulls them. Once a message has been consumed it can be removed from the queue.
 *
 * <p>In essence the queue is a mailbox for messages to be sent to a Consumer. It allows decoupling
 * the processing between the Producer and the Consumer: one of the tasks may be down without
 * impacting the other, and the Consumer can process messages at a different pace than the Producer.
 *
 * <h2>Producer</h2>
 *
 * In a nutshell, this example does the following:
 *
 * <ul>
 *   <li>1. Connects to BlazingMQ framework
 *   <li>2. Opens queue in write mode
 *   <li>3. Gets user input and sends messages to the queue
 * </ul>
 *
 * <p>When a new message is being sent, acknowledgement ("ack") from the broker is requested and
 * correlation ID is set.
 *
 * <p>In order to receive ack messages, {@code Producer} class implements {@link
 * com.bloomberg.bmq.AckMessageHandler} interface. When new ack messages are being delivered from
 * the broker, {@link #handleAckMessage} method is called.
 *
 * <p>Also, {@code Producer} implements {@link com.bloomberg.bmq.SessionEventHandler} and {@link
 * com.bloomberg.bmq.QueueEventHandler} interfaces in order to be able to handle various events
 * related to {@link com.bloomberg.bmq.Session} and {@link com.bloomberg.bmq.Queue}.
 *
 * <p>Finally, {@code Producer} implements {@link AutoCloseable} interface. When used in
 * 'try-with-resources' statement, {@link #close} method will be called automatically after
 * execution of the block. Thus the connection to BlazingMQ framework will be properly closed.
 */
public class Producer
        implements AutoCloseable, SessionEventHandler, QueueEventHandler, AckMessageHandler {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    static final Uri uri = new Uri("bmq://bmq.tutorial.hello/test-queue");

    AbstractSession session;
    Queue queue;
    Set<CorrelationId> correlationIds = Collections.synchronizedSet(new HashSet<>());

    Producer() {
        // Empty: call `run` to start the built Producer
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
    public void handleAckMessage(AckMessage msg) {
        // User specified handler for ACK messages

        // This message is received because we specified correlation IDs in the
        // messages and requested acknowledgement, which requires the broker to send us
        // acknowledgement
        // messages.  We can query the acknoledgement status (success/failure)
        // as well as the correlation ID of the published message.

        logger.info("*** Got ack message from the Broker, that it got our message with: ***");
        logger.info("*** GUID: {}", msg.messageGUID());
        logger.info("*** Queue URI: {}", msg.queue().uri());
        logger.info("*** Ack status: {}", msg.status());

        // Check that the CorrelationId from the ACK message equals
        // to the CorrelationId from the initial PUT message
        CorrelationId corrId = msg.correlationId();
        logger.info("*** Correlation ID: {}", corrId);
        if (!correlationIds.contains(corrId)) {
            logger.error("Unexpected CorrelationId: {}", corrId);
            return;
        }
        Object userData = corrId.userData();
        if (userData != null) {
            logger.info("*** Correlation ID User Data: {}", userData);
        }

        // Prompt user to enter next message on the command line.
        logger.info(
                "Enter a message to publish, or an empty message to exit (e.g. just press return)");
    }

    public void run() {
        logger.info("*** Run producer ***");

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
        // and post messages.

        // Create a queue representation.
        // Each queue is identified by a short URL containing a namespace and a queue name.
        // We don't need to pass a valid PushMessageHandler here because we will only put messages
        // into the queue, and thus will not receive PUSH messages
        queue =
                session.getQueue(
                        uri, // Queue uri
                        QueueFlags.setWriter(0), // Queue mode (=WRITE)
                        this, // QueueEventHandler
                        this, // AckMessageHandler
                        null); // PushMessageHandler

        logger.info("Opening queue [{}] with a timeout of 15 seconds", uri);

        // Open a queue using default options
        queue.open(
                QueueOptions.createDefault(), // Default queue options
                Duration.ofSeconds(15)); // Timeout

        logger.info("Successfully opened the queue [{}]", uri);
    }

    public void post(String payload) {
        // Convert String to ByteBuffer
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.wrap(bytes);

        // Create a new PutMessage.
        // We provide the message payload as the first argument to the factory method.
        // We also request the broker to send back an acknowledgement of receiving the message.
        // For that we must call 'setCorrelationId()' to automatically assign unique
        // 'CorrelationId'.
        // Returned 'CorrelationId' can be used to identify the message when broker sends an ACK
        // message.
        PutMessage msg = queue.createPutMessage(bb); // message payload
        CorrelationId corrId =
                msg.setCorrelationId(payload); // setup correlation Id that holds user data

        // Store the CorrelationId in the queue to compare it later
        // with a CorrelationId from incoming ACK message
        if (!correlationIds.add(corrId)) {
            throw new IllegalStateException("CorrelationId is not unique: " + corrId);
        }

        // Enable compression for this message.
        msg.setCompressionAlgorithm(CompressionAlgorithm.Zlib);

        // We will associated some properties with the published message
        MessageProperties mp = msg.messageProperties();
        mp.setPropertyAsString("routingId", "42");
        mp.setPropertyAsInt64("timestamp", new Date().getTime());

        // Send the message to the broker.
        // The broker will distribute the message to the corresponding queues (in
        // our case, just one queue).
        queue.post(msg);
    }

    public void close() {
        logger.info("*** Close producer ***");

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
            // Gracefully disconnect from the BlazingMQ broker and stop the operation of this session
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
        // Producer class implements AutoCloseable interface and can be used in `try-with-resource`
        // statement.
        // But here we close it manually in order to display error messages before
        // log messages printed in Producer.close() method
        Producer producer = new Producer();
        try (Scanner sc = new Scanner(System.in, StandardCharsets.UTF_8.name())) {
            producer.run();

            logger.info(
                    "Enter a message to publish, or an empty message to exit (e.g. just press return)");

            while (true) {
                // Expect user input in UTF8 encoding
                String line = sc.nextLine();

                if (line.isEmpty()) break;

                logger.info("Sending the message: {} ", line);
                producer.post(line);
            }
        } catch (BMQException e) {
            // Catch BlazingMQ specific exception first
            logger.error("BMQException: {}, StackTrace: {}", e.getMessage(), Util.getStackTrace(e));

            // BlazingMQ related handling code
        } catch (Exception e) {
            // Catch other exceptions
            logger.error("Exception: {}, StackTrace: {}", e.getMessage(), Util.getStackTrace(e));

            // Common exception handling code
        } finally {
            producer.close();
        }
    }
}
