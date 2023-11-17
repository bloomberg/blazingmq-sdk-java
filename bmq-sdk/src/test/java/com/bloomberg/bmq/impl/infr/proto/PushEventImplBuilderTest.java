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
import static org.junit.Assert.assertFalse;

import com.bloomberg.bmq.MessageGUID;
import com.bloomberg.bmq.impl.infr.msg.MessagesTestSamples;
import com.bloomberg.bmq.util.TestHelpers;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushEventImplBuilderTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testPackPushMessage() throws IOException {
        for (boolean isOldStyleProperties : new boolean[] {true, false}) {
            PushMessageImpl pushMsg = new PushMessageImpl();
            PushEventBuilder builder = new PushEventBuilder();

            EventBuilderResult res = builder.packMessage(pushMsg, isOldStyleProperties);
            PushHeaderFlags flags = PushHeaderFlags.fromInt(pushMsg.flags());

            assertEquals(EventBuilderResult.SUCCESS, res);
            assertEquals(PushHeaderFlags.IMPLICIT_PAYLOAD, flags);

            pushMsg.reset();

            pushMsg.appData()
                    .setPayload(TestHelpers.filledBuffer(PushHeader.MAX_PAYLOAD_SIZE_SOFT + 1));

            res = builder.packMessage(pushMsg, isOldStyleProperties);
            assertEquals(EventBuilderResult.PAYLOAD_TOO_BIG, res);

            pushMsg.reset();
            builder.reset();

            final int numMsgs = EventHeader.MAX_SIZE_SOFT / PushHeader.MAX_PAYLOAD_SIZE_SOFT;

            for (int i = 0; i < numMsgs; i++) {
                // Cannot pack more than 'numMsgs' having a unpackedSize of
                // 'PushHeader.MAX_PAYLOAD_SIZE_SOFT' in 1 bmqp event.
                pushMsg.appData()
                        .setPayload(TestHelpers.filledBuffer(PushHeader.MAX_PAYLOAD_SIZE_SOFT));
                res = builder.packMessage(pushMsg, isOldStyleProperties);
                assertEquals(EventBuilderResult.SUCCESS, res);
            }

            pushMsg.appData()
                    .setPayload(TestHelpers.filledBuffer(PushHeader.MAX_PAYLOAD_SIZE_SOFT));
            // Try to add one more message, which must fail with event_too_big.
            res = builder.packMessage(pushMsg, isOldStyleProperties);
            assertEquals(EventBuilderResult.EVENT_TOO_BIG, res);

            pushMsg.reset();
            builder.reset();

            pushMsg.appData()
                    .setPayload(TestHelpers.filledBuffer(PushHeader.MAX_PAYLOAD_SIZE_SOFT));

            res = builder.packMessage(pushMsg, isOldStyleProperties);
            assertEquals(EventBuilderResult.SUCCESS, res);

            boolean isSet =
                    PushHeaderFlags.isSet(pushMsg.flags(), PushHeaderFlags.IMPLICIT_PAYLOAD);
            assertFalse(isSet);
        }
    }

    @Test
    public void testBuildPushMessage() throws IOException {
        final String PAYLOAD = "abcdefghijklmnopqrstuvwxyz";
        final String GUID = "ABCDEF0123456789ABCDEF0123456789";

        final MessageGUID guid = MessageGUID.fromHex(GUID);

        final MessagePropertiesImpl props = new MessagePropertiesImpl();
        props.setPropertyAsInt32("encoding", 3);
        props.setPropertyAsInt64("timestamp", 1234567890L);
        props.setPropertyAsString("id", "myCoolId");

        final PushEventBuilder builder = new PushEventBuilder();

        for (int i = 0; i < 2; ++i) {
            PushMessageImpl pushMsg = new PushMessageImpl();
            pushMsg.setQueueId(9876);
            pushMsg.setMessageGUID(guid);
            ByteBuffer payload = ByteBuffer.allocate(PAYLOAD.getBytes().length);
            payload.put(PAYLOAD.getBytes());
            pushMsg.appData().setPayload(payload);
            pushMsg.appData().setProperties(props);

            // set compression to none in order to match file content
            pushMsg.setCompressionType(CompressionAlgorithmType.E_NONE);

            final boolean isOldStyleProperties = i % 2 == 0;
            assertEquals(
                    EventBuilderResult.SUCCESS, builder.packMessage(pushMsg, isOldStyleProperties));

            // Compare with value stored in the binary pattern
            logger.info("PUSH header {}: {}", i + 1, pushMsg.header());
            assertEquals(i, pushMsg.header().schemaWireId());
            assertEquals(isOldStyleProperties, pushMsg.appData().isOldStyleProperties());
        }

        ByteBuffer[] message = builder.build();

        TestHelpers.compareWithFileContent(message, MessagesTestSamples.PUSH_MULTI_MSG);
    }
}
