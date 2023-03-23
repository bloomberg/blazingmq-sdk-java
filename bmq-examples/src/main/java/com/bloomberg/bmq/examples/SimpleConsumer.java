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
import com.bloomberg.bmq.PushMessage;
import com.bloomberg.bmq.Queue;
import com.bloomberg.bmq.QueueFlags;
import com.bloomberg.bmq.QueueOptions;
import com.bloomberg.bmq.ResultCodes;
import com.bloomberg.bmq.Session;
import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.Uri;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple application which connects to BlazingMQ broker and consumes messages. Part of the
 * BlazingMQ tutorial.
 *
 * <h2>Simple Producer/Consumer use case</h2>
 *
 * The simplest use case involves a Producer, a Consumer and a Message Broker. The Producer and/or
 * the Consumer create a named queue in the Broker. The Producer pushes messages into the queue, and
 * the consumer pulls them. Once a message has been consumed it can be removed from the queue.
 *
 * <h2>Consumer</h2>
 *
 * In a nutshell, this example does the following:
 *
 * <ul>
 *   <li>1. Connects to BlazingMQ framework
 *   <li>2. Opens queue in read mode
 *   <li>3. Waits for two messages
 *   <li>4. Closes the queue and disconnects
 * </ul>
 */
public class SimpleConsumer {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static void handlePushMessage(PushMessage msg, BlockingQueue<String> receivedMessages) {
        // Get the data in the message.
        StringBuilder contentBuilder = new StringBuilder();
        for (ByteBuffer bb : msg.payload()) {
            // It is safe to convert ByteBuffer payload to String only if it's known that
            // payload represents a valid string
            // (e.g. it doesn't make sense to do this if producer sent a protobuf encoded
            // payload)
            String bufferContent = StandardCharsets.UTF_8.decode(bb).toString();
            contentBuilder.append(bufferContent);
        }

        final String content = contentBuilder.toString();

        // Confirm reception of the messages so that it can be deleted from the queue
        ResultCodes.GenericResult confirmResult = msg.confirm();
        logger.debug("Confirm status: {}", confirmResult);

        try {
            receivedMessages.put(content);
        } catch (InterruptedException e) {
            logger.info("InterruptedException: ", e);
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        final Uri uri = new Uri("bmq://bmq.tutorial.hello/queue-simple");
        final Duration TIMEOUT = Duration.ofSeconds(15); // 15 seconds
        final int MSG_TIMEOUT = 60; // 60 seconds

        String brokerUri = System.getenv("BMQ_BROKER_URI");
        if (brokerUri == null) {
            brokerUri = "tcp://localhost:30114";
        }

        final SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(URI.create(brokerUri)).build();
        final AbstractSession session = new Session(sessionOptions, event -> {});

        final BlockingQueue<String> receivedMessages = new LinkedBlockingQueue<String>();

        try {
            logger.info("Starting the session");
            session.start(TIMEOUT);

            logger.info("Create a reader queue [{}]", uri);
            final Queue queue =
                    session.getQueue(
                            uri, // Queue uri
                            QueueFlags.setReader(0), // Queue mode (=READ)
                            event -> {}, // QueueEventHandler
                            null, // AckMessageHandler
                            msg -> { // PushMessageHandler
                                handlePushMessage(msg, receivedMessages);
                            });

            logger.info("Open the queue");
            queue.open(
                    QueueOptions.createDefault(), // Default queue options
                    TIMEOUT); // Timeout

            logger.info("*** Waiting for messages ({} seconds timeout) ***", MSG_TIMEOUT);
            String part1 = receivedMessages.poll(MSG_TIMEOUT, TimeUnit.SECONDS);
            logger.info("First message: {}", part1 != null ? part1 : "*** timeout ***");

            String part2 = receivedMessages.poll(MSG_TIMEOUT, TimeUnit.SECONDS);
            logger.info("Second message: {}", part2 != null ? part2 : "*** timeout ***");

            logger.info("Close the queue");
            queue.close(TIMEOUT);
        } finally {
            logger.info("Stop the session");
            session.stop(TIMEOUT);

            logger.info("Linger the session");
            session.linger();
        }
    }
}
