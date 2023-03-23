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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.Uri;
import com.bloomberg.bmq.impl.infr.net.ConnectionOptions;
import com.bloomberg.bmq.impl.infr.util.PrintUtil;
import com.bloomberg.bmq.it.util.BmqBroker;
import com.bloomberg.bmq.it.util.TestTools;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PayloadIT {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // Disable this test because it uses plain sockets and read BMQ
    // events unreliably
    // @Test
    public void testPayloadPlain() throws IOException {
        // This is a complex test where a test payload is sent via
        // Producer and is received via Consumer.
        // It is expected that the incoming payload should contain
        // the initial message together with two message properties
        // added by the Broker.
        // If BMQ_BROKER_PATH environment variable is set then the
        // Broker will be started form BmqBroker.java
        // Otherwise the Broker should be started manually.
        // Test steps:
        // 1. Start the Broker, prepare a unique payload string.
        // 2. Send the payload to the Broker using PlainProducerIT.sendMessage().
        // 3. Receive a message from the Broker using PlainConsumerIT.getLastMessage().
        // 4. Verify that incoming payload contains initial string and two expected
        //    message properties.

        logger.info("=========================================================================");
        logger.info("BEGIN Testing PayloadIT transfer payload b/w raw producer and consumer.");
        logger.info("=========================================================================");

        final String TEST_MESSAGE = "Hello, World!" + System.currentTimeMillis();
        final Uri QUEUE_URI = BmqBroker.Domains.Priority.generateQueueUri();

        try (BmqBroker broker = BmqBroker.createStartedBroker()) {
            final ConnectionOptions OPTS = new ConnectionOptions(broker.sessionOptions());
            final int PORT = OPTS.brokerUri().getPort();

            ByteBuffer unpaddedPayload =
                    TestTools.prepareUnpaddedData(
                            TEST_MESSAGE, broker.isOldStyleMessageProperties());

            // ==================================
            // Check plain producer and consumer
            // ==================================

            PlainProducerIT.sendMessage(
                    TEST_MESSAGE, PORT, QUEUE_URI, broker.isOldStyleMessageProperties());

            ByteBuffer[] res = PlainConsumerIT.getLastMessage(PORT, QUEUE_URI);
            logger.info("Last message: \n{}", PrintUtil.hexDump(res));

            assertNotNull(res);
            assertTrue(TestTools.equalContent(unpaddedPayload, res));

            broker.setDropTmpFolder();
        }

        logger.info("=======================================================================");
        logger.info("END Testing PayloadIT transfer payload b/w raw producer and consumer.");
        logger.info("=======================================================================");
    }

    @Test
    public void testPayloadNetty() throws IOException {
        // This is a complex test where a test payload is sent via Producer and
        // is received via Consumer.  It is expected that the incoming payload
        // should contain the initial message together with two message
        // properties added by the producer.

        // Test steps:
        // 1. Start the Broker, prepare a unique payload string.
        // 2. Send the payload to the Broker using NettyProducerIT.sendMessage().
        // 3. Receive a message from the broker using NettyProducerIT.getLastMessage().
        // 4. Verify that incoming payload contains initial string and two
        // expected message properties.

        logger.info("=========================================================================");
        logger.info("BEGIN Testing PayloadIT transfer payload b/w netty producer and consumer.");
        logger.info("=========================================================================");

        String TEST_MESSAGE = "Hello, World!" + System.currentTimeMillis();
        Uri QUEUE_URI = BmqBroker.Domains.Priority.generateQueueUri();

        try (BmqBroker broker = BmqBroker.createStartedBroker()) {
            final SessionOptions OPTS = broker.sessionOptions();

            final boolean isOldStyleProperties = broker.isOldStyleMessageProperties();
            ByteBuffer unpaddedPayload =
                    TestTools.prepareUnpaddedData(TEST_MESSAGE, isOldStyleProperties);

            // ==================================
            // Check netty producer and consumer
            // ==================================
            final int NUM_MESSAGES = 1;
            String[] payloads = new String[NUM_MESSAGES];
            Arrays.fill(payloads, TEST_MESSAGE);

            NettyProducerIT.sendMessage(payloads, OPTS, QUEUE_URI, isOldStyleProperties);

            // Read PUSH message but don't confirm it
            boolean DO_CONFIRM = false;

            ByteBuffer[] res = NettyConsumerIT.getLastMessage(OPTS, QUEUE_URI, DO_CONFIRM);
            assertNotNull(res);
            logger.info("Last message: \n{}", PrintUtil.hexDump(res));
            assertTrue(TestTools.equalContent(unpaddedPayload, res));

            // Read unconfirmed PUSH message and confirm it this time
            DO_CONFIRM = true;

            res = NettyConsumerIT.getLastMessage(OPTS, QUEUE_URI, DO_CONFIRM);
            assertNotNull(res);
            assertTrue(TestTools.equalContent(unpaddedPayload, res));

            // No more PUSH messages should be in the queue
            res = NettyConsumerIT.getLastMessage(OPTS, QUEUE_URI, DO_CONFIRM);
            assertNull(res);

            broker.setDropTmpFolder();
        }

        logger.info("=========================================================================");
        logger.info("END Testing PayloadIT transfer payload b/w netty producer and consumer.");
        logger.info("=========================================================================");
    }
}
