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
import static org.junit.Assert.fail;

import com.bloomberg.bmq.MessageGUID;
import com.bloomberg.bmq.impl.infr.msg.MessagesTestSamples;
import com.bloomberg.bmq.util.TestHelpers;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfirmEventImplBuilderTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testErrorMessages() {
        try {
            EventBuilderResult res;
            ConfirmEventBuilder builder = new ConfirmEventBuilder();
            ConfirmMessage message = new ConfirmMessage();

            for (int i = 0; i <= ConfirmEventBuilder.MAX_MSG_COUNT - 1; i++) {
                res = builder.packMessage(message);
                assertEquals(EventBuilderResult.SUCCESS, res);
            }

            assertEquals(ConfirmEventBuilder.MAX_MSG_COUNT, builder.messageCount());

            // Adding one more fails
            res = builder.packMessage(message);

            assertEquals(EventBuilderResult.EVENT_TOO_BIG, res);
        } catch (IOException e) {
            logger.info("IOException: ", e);
            fail();
        }
    }

    @Test
    public void testBuildConfirmMessage() {
        String GUID = "0000000000003039CD8101000000270F";
        // Above hex string represents a valid guid with these values:
        //   TS = 12345
        //   IP = 98765
        //   ID = 9999

        final int NUM_MSGS = 10;

        try {
            MessageGUID guid = MessageGUID.fromHex(GUID);

            ConfirmEventBuilder builder = new ConfirmEventBuilder();
            for (int i = 0; i < NUM_MSGS; i++) {
                ConfirmMessage m = new ConfirmMessage();
                m.setQueueId(i * 3);
                m.setSubQueueId(i * 3 + 1);
                m.setMessageGUID(guid);
                builder.packMessage(m);
            }

            ByteBuffer[] message;
            message = builder.build();

            TestHelpers.compareWithFileContent(message, MessagesTestSamples.CONFIRM_MSG);
        } catch (Exception e) {
            logger.info("Exception: ", e);
            fail();
        }
    }
}
