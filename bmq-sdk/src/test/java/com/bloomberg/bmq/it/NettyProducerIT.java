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
import static org.junit.jupiter.api.Assertions.fail;

import com.bloomberg.bmq.QueueFlags;
import com.bloomberg.bmq.QueueOptions;
import com.bloomberg.bmq.ResultCodes.CloseQueueResult;
import com.bloomberg.bmq.ResultCodes.ConfigureQueueResult;
import com.bloomberg.bmq.ResultCodes.GenericResult;
import com.bloomberg.bmq.ResultCodes.OpenQueueResult;
import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.Uri;
import com.bloomberg.bmq.impl.BrokerSession;
import com.bloomberg.bmq.impl.QueueImpl;
import com.bloomberg.bmq.impl.events.Event;
import com.bloomberg.bmq.impl.infr.net.NettyTcpConnectionFactory;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnectionFactory;
import com.bloomberg.bmq.impl.infr.proto.PutMessageImpl;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.intf.QueueHandle;
import com.bloomberg.bmq.it.util.BmqBroker;
import com.bloomberg.bmq.it.util.TestTools;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyProducerIT {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static QueueImpl createQueue(BrokerSession session, Uri uri, long flags) {
        return new QueueImpl(session, uri, flags, null, null, null);
    }

    public static void sendMessage(
            String[] msgPayloads,
            SessionOptions sesOpts,
            Uri queueUri,
            boolean isOldStyleProperties) {
        Argument.expectNonNull(sesOpts, "sesOpts");

        final Duration TEST_REQUEST_TIMEOUT = Duration.ofSeconds(45);

        long flags = 0;

        flags = QueueFlags.setWriter(flags);
        flags = QueueFlags.setAck(flags);

        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();

        LinkedList<Event> events = new LinkedList<>();

        BrokerSession session =
                BrokerSession.createInstance(sesOpts, connectionFactory, service, events::push);

        try {
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));

            logger.info("Session started");

            QueueHandle qh = createQueue(session, queueUri, flags);

            QueueOptions queueOptions =
                    QueueOptions.builder()
                            .setConsumerPriority(1)
                            .setMaxUnconfirmedBytes(32768L * 1024L)
                            .setMaxUnconfirmedMessages(1024L)
                            .build();

            assertEquals(OpenQueueResult.SUCCESS, qh.open(queueOptions, TEST_REQUEST_TIMEOUT));

            logger.info("Queue opened");

            for (String msgPayload : msgPayloads) {
                PutMessageImpl message =
                        TestTools.preparePutMessage(msgPayload, isOldStyleProperties);
                session.post(qh, message);
            }

            logger.info("Message sent");

            assertEquals(
                    ConfigureQueueResult.SUCCESS, qh.configure(queueOptions, TEST_REQUEST_TIMEOUT));
            assertEquals(CloseQueueResult.SUCCESS, qh.close(TEST_REQUEST_TIMEOUT));
            logger.info("Queue closed");
        } catch (Exception e) {
            logger.error("Exception: ", e);
            fail();
        } finally {
            assertEquals(GenericResult.SUCCESS, session.stop(TEST_REQUEST_TIMEOUT));
            session.linger();
            logger.info("Session stopped");
        }
    }

    @Test
    void testProducer() throws IOException {
        logger.info("==================================================");
        logger.info("BEGIN Testing NettyProducerIT sending a message.");
        logger.info("==================================================");

        try (BmqBroker broker = BmqBroker.createStartedBroker()) {
            final String MSG = "I'm Netty producer!";
            final Uri QUEUE_URI = BmqBroker.Domains.Priority.generateQueueUri();

            sendMessage(
                    new String[] {MSG},
                    broker.sessionOptions(),
                    QUEUE_URI,
                    broker.isOldStyleMessageProperties());

            broker.setDropTmpFolder();
        }

        logger.info("================================================");
        logger.info("END Testing NettyProducerIT sending a message.");
        logger.info("================================================");
    }
}
