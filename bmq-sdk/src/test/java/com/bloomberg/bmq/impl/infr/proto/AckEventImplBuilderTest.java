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

import com.bloomberg.bmq.MessageGUID;
import com.bloomberg.bmq.impl.CorrelationIdImpl;
import com.bloomberg.bmq.impl.ResultCodeUtils;
import com.bloomberg.bmq.impl.infr.msg.MessagesTestSamples;
import com.bloomberg.bmq.util.TestHelpers;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AckEventImplBuilderTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testErrorMessages() throws IOException {
        EventBuilderResult res;
        AckEventBuilder builder = new AckEventBuilder();
        AckMessageImpl message = new AckMessageImpl();

        for (int i = 0; i <= AckEventBuilder.MAX_MSG_COUNT - 1; i++) {
            res = builder.packMessage(message);
            assertEquals(EventBuilderResult.SUCCESS, res);
        }

        assertEquals(AckEventBuilder.MAX_MSG_COUNT, builder.messageCount());

        // Adding one more fails
        res = builder.packMessage(message);

        assertEquals(EventBuilderResult.EVENT_TOO_BIG, res);
    }

    @Test
    public void testBuildAckMessage() throws IOException {
        String GUID = "0000000000003039CD8101000000270F";
        // Above hex string represents a valid guid with these values:
        //   TS = 12345
        //   IP = 98765
        //   ID = 9999

        final int NUM_MSGS = 10;

        MessageGUID guid = MessageGUID.fromHex(GUID);

        AckEventBuilder builder = new AckEventBuilder();
        for (int i = 0; i < NUM_MSGS; i++) {
            AckMessageImpl m = new AckMessageImpl();
            m.setStatus(ResultCodeUtils.ackResultFromInt(i % 3));
            m.setCorrelationId(CorrelationIdImpl.restoreId(i * i));
            m.setQueueId(i);
            m.setMessageGUID(guid);
            builder.packMessage(m);
        }

        ByteBuffer[] message;
        message = builder.build();

        TestHelpers.compareWithFileContent(message, MessagesTestSamples.ACK_MSG);
    }
}
