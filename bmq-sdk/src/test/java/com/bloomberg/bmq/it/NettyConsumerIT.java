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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import com.bloomberg.bmq.impl.events.PushMessageEvent;
import com.bloomberg.bmq.impl.infr.net.NettyTcpConnectionFactory;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnectionFactory;
import com.bloomberg.bmq.impl.infr.proto.PushMessageImpl;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.intf.EventHandler;
import com.bloomberg.bmq.impl.intf.QueueHandle;
import com.bloomberg.bmq.it.util.BmqBroker;
import com.bloomberg.bmq.it.util.TestTools;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyConsumerIT {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static QueueImpl createQueue(BrokerSession session, Uri uri, long flags) {
        return new QueueImpl(session, uri, flags, null, null, null);
    }

    public static ByteBuffer[] getLastMessage(SessionOptions sesOpts, Uri uri, boolean doConfirm) {
        Argument.expectNonNull(sesOpts, "sesOpts");

        final Duration TEST_REQUEST_TIMEOUT = Duration.ofSeconds(45);

        ByteBuffer[] msg = null;
        long flags = 0;

        flags = QueueFlags.setReader(flags);

        Semaphore dataSem = new Semaphore(0);

        TcpConnectionFactory connectionFactory = new NettyTcpConnectionFactory();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        ConcurrentLinkedDeque<Event> events = new ConcurrentLinkedDeque<>();
        EventHandler eventHandler =
                (Event event) -> {
                    events.push(event);
                    dataSem.release();
                };

        BrokerSession session =
                BrokerSession.createInstance(sesOpts, connectionFactory, service, eventHandler);
        try {
            assertEquals(GenericResult.SUCCESS, session.start(TEST_REQUEST_TIMEOUT));

            logger.info("Session started");

            QueueHandle qh = createQueue(session, uri, flags);
            qh.getParameters().setFlags(flags);

            QueueOptions queueOptions =
                    QueueOptions.builder()
                            .setConsumerPriority(1)
                            .setMaxUnconfirmedMessages(1024L)
                            .setMaxUnconfirmedBytes(33554432L)
                            .build();

            assertEquals(OpenQueueResult.SUCCESS, qh.open(queueOptions, TEST_REQUEST_TIMEOUT));
            logger.info("Queue opened");

            TestTools.acquireSema(dataSem, 15);

            if (events.isEmpty()) {
                logger.info("No incoming messages");
                return msg;
            }

            Event ev = events.getLast();
            assertNotNull(ev);
            assertTrue(ev instanceof PushMessageEvent);

            PushMessageImpl pm = ((PushMessageEvent) ev).rawMessage();
            assertNotNull(pm);
            logger.info("Received PUSH msg: {}", pm);

            if (doConfirm) {
                assertEquals(GenericResult.SUCCESS, session.confirm(qh, pm));
            }

            msg = pm.appData().applicationData();

            queueOptions = QueueOptions.builder().build();

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
        return msg;
    }

    @Test
    public void testConsumer() throws IOException {

        logger.info("====================================================================");
        logger.info("BEGIN Testing NettyConsumerIT getting message from the BMQ Broker.");
        logger.info("====================================================================");

        Uri QUEUE_URI = BmqBroker.Domains.Priority.generateQueueUri();

        try (BmqBroker broker = BmqBroker.createStartedBroker()) {
            final boolean DO_CONFIRM = true;
            getLastMessage(broker.sessionOptions(), QUEUE_URI, DO_CONFIRM);

            broker.setDropTmpFolder();
        }

        logger.info("==================================================================");
        logger.info("END Testing NettyConsumerIT getting message from the BMQ Broker.");
        logger.info("==================================================================");
    }
}
