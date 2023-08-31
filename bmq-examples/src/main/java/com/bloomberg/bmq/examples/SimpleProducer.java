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
import com.bloomberg.bmq.PutMessage;
import com.bloomberg.bmq.Queue;
import com.bloomberg.bmq.QueueFlags;
import com.bloomberg.bmq.QueueOptions;
import com.bloomberg.bmq.Session;
import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.Uri;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple application which connects to BlazingMQ broker, takes user input and send messages. Part
 * of the BlazingMQ tutorial.
 *
 * <h2>Simple Producer/Consumer use case</h2>
 *
 * The simplest use case involves a Producer, a Consumer and a Message Broker. The Producer and/or
 * the Consumer create a named queue in the Broker. The Producer pushes two messages into the queue,
 * and the consumer pulls them.
 *
 * <h2>Producer</h2>
 *
 * In a nutshell, this example does the following:
 *
 * <ul>
 *   <li>1. Connects to BlazingMQ framework
 *   <li>2. Opens queue in write mode
 *   <li>3. Sends two messages to the queue
 *   <li>4. Closes the queue and disconnects
 * </ul>
 */
public class SimpleProducer {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static ByteBuffer getBinaryPayload(String payload) {
        // Convert String to ByteBuffer
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.wrap(bytes);
    }

    public static void main(String[] args) {
        final Uri uri = new Uri("bmq://bmq.test.mem.priority/queue-simple");
        final Duration TIMEOUT = Duration.ofSeconds(15); // 15 seconds

        String brokerUri = System.getenv("BMQ_BROKER_URI");
        if (brokerUri == null) {
            brokerUri = "tcp://localhost:30114";
        }

        final SessionOptions sessionOptions =
                SessionOptions.builder().setBrokerUri(URI.create(brokerUri)).build();
        final AbstractSession session = new Session(sessionOptions, event -> {});

        try {
            logger.info("Starting the session");
            session.start(TIMEOUT);

            logger.info("Create a writer queue [{}]", uri);
            final Queue queue =
                    session.getQueue(
                            uri, // Queue uri
                            QueueFlags.setWriter(0), // Queue mode (=WRITE)
                            event -> {}, // QueueEventHandler
                            null, // AckMessageHandler
                            null); // PushMessageHandler

            logger.info("Open the queue");
            queue.open(
                    QueueOptions.createDefault(), // Default queue options
                    TIMEOUT); // Timeout

            logger.info("Post two messages");
            PutMessage put1 =
                    queue.createPutMessage(getBinaryPayload("Hello from BlazingMQ Part 1"));
            queue.post(put1);

            PutMessage put2 =
                    queue.createPutMessage(getBinaryPayload("Hello from BlazingMQ Part 2"));
            queue.post(put2);

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
