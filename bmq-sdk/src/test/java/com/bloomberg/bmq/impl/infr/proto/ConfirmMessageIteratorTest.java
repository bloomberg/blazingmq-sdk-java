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
import static org.junit.Assert.fail;

import com.bloomberg.bmq.MessageGUID;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfirmMessageIteratorTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testWithBuilder() {
        String GUID = "0000000000003039CD8101000000270F";
        final int NUM_MSGS = 1000;

        try {
            MessageGUID guid = MessageGUID.fromHex(GUID);

            ConfirmEventBuilder builder = new ConfirmEventBuilder();
            for (int i = 0; i < NUM_MSGS; i++) {
                ConfirmMessage m = new ConfirmMessage();
                m.setQueueId(i);
                m.setSubQueueId(i % 5);
                m.setMessageGUID(guid);
                EventBuilderResult rc = builder.packMessage(m);
                assertEquals(EventBuilderResult.SUCCESS, rc);
            }

            ConfirmEventImpl confEvent = new ConfirmEventImpl(builder.build());

            assertTrue(confEvent.isValid());

            // Check twice
            for (int j = 0; j < 2; j++) {

                ConfirmMessageIterator confIt = confEvent.iterator();

                assertTrue(confIt.isValid());

                ConfirmHeader confHeader = confIt.header();

                assertEquals(1, confHeader.headerWords());
                assertEquals(6, confHeader.perMessageWords());

                int i = 0;
                while (confIt.hasNext()) {
                    ConfirmMessage confMsg = confIt.next();
                    assertEquals(GUID, confMsg.messageGUID().toHex());
                    assertEquals(i, confMsg.queueId());
                    assertEquals(i % 5, confMsg.subQueueId());
                    i++;
                }
                assertEquals(NUM_MSGS, i);
            }
        } catch (IOException e) {
            logger.info("IOException: ", e);
            fail();
        }
    }
}
