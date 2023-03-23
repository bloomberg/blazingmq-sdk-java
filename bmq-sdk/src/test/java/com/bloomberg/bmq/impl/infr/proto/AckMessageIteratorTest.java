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
package com.bloomberg.bmq.impl.infr.proto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.bloomberg.bmq.MessageGUID;
import com.bloomberg.bmq.impl.CorrelationIdImpl;
import com.bloomberg.bmq.impl.ResultCodeUtils;
import com.bloomberg.bmq.impl.infr.msg.MessagesTestSamples;
import com.bloomberg.bmq.util.TestHelpers;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AckMessageIteratorTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testWithPattern() throws IOException {
        ByteBuffer buf = TestHelpers.readFile(MessagesTestSamples.ACK_MSG.filePath());

        AckEventImpl ackEvent = new AckEventImpl(new ByteBuffer[] {buf});

        assertTrue(ackEvent.isValid());

        Iterator<AckMessageImpl> ackIt = ackEvent.iterator();

        AckHeader ackHeader = ackEvent.ackHeader();

        assertEquals(0, ackHeader.flags());
        assertEquals(1, ackHeader.headerWords());
        assertEquals(6, ackHeader.perMessageWords());

        int i = 0;
        String GUID = "0000000000003039CD8101000000270F";
        // Above hex string represents a valid guid with these values:
        //   TS = 12345
        //   IP = 98765
        //   ID = 9999

        while (ackIt.hasNext()) {
            AckMessageImpl ackMsg = ackIt.next();
            assertEquals(GUID, ackMsg.messageGUID().toHex());
            assertEquals(ResultCodeUtils.ackResultFromInt(i % 3), ackMsg.status());
            assertEquals(i, ackMsg.queueId());
            assertEquals(CorrelationIdImpl.restoreId(i * i), ackMsg.correlationId());
            i++;
        }
        assertEquals(10, i);
    }

    @Test
    public void testWithBuilder() throws IOException {
        String GUID = "0000000000003039CD8101000000270F";
        final int NUM_MSGS = 1000;

        MessageGUID guid = MessageGUID.fromHex(GUID);

        AckEventBuilder builder = new AckEventBuilder();
        for (int i = 0; i < NUM_MSGS; i++) {
            AckMessageImpl m = new AckMessageImpl();
            m.setStatus(ResultCodeUtils.ackResultFromInt(i % 5));
            m.setCorrelationId(CorrelationIdImpl.restoreId(i * i));
            m.setQueueId(i);
            m.setMessageGUID(guid);
            EventBuilderResult rc = builder.packMessage(m);
            assertEquals(EventBuilderResult.SUCCESS, rc);
        }

        AckEventImpl ackEvent = new AckEventImpl(builder.build());

        assertTrue(ackEvent.isValid());

        // Check twice
        for (int j = 0; j < 2; j++) {
            logger.info("Iteration {}", j);

            Iterator<AckMessageImpl> ackIt = ackEvent.iterator();

            AckHeader ackHeader = ackEvent.ackHeader();

            assertEquals(0, ackHeader.flags());
            assertEquals(1, ackHeader.headerWords());
            assertEquals(6, ackHeader.perMessageWords());

            int i = 0;
            while (ackIt.hasNext()) {
                AckMessageImpl ackMsg = ackIt.next();
                assertEquals(GUID, ackMsg.messageGUID().toHex());
                assertEquals(ResultCodeUtils.ackResultFromInt(i % 5), ackMsg.status());
                assertEquals(i, ackMsg.queueId());
                assertEquals(CorrelationIdImpl.restoreId(i * i), ackMsg.correlationId());
                i++;
            }
            assertEquals(NUM_MSGS, i);
        }
    }
}
