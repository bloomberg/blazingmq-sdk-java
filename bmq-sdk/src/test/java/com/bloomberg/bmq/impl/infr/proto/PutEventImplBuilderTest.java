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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.bloomberg.bmq.CorrelationId;
import com.bloomberg.bmq.impl.CorrelationIdImpl;
import com.bloomberg.bmq.impl.infr.msg.MessagesTestSamples;
import com.bloomberg.bmq.util.TestHelpers;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PutEventImplBuilderTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testErrorPutMessage() throws IOException {
        for (boolean isOldStyleProperties : new boolean[] {false, true}) {
            PutMessageImpl putMsg = new PutMessageImpl();
            PutEventBuilder builder = new PutEventBuilder();

            EventBuilderResult res = builder.packMessage(putMsg, isOldStyleProperties);
            assertEquals(EventBuilderResult.PAYLOAD_EMPTY, res);

            putMsg = new PutMessageImpl();
            ByteBuffer buffer = ByteBuffer.allocate(0);
            putMsg.appData().setPayload(buffer);

            res = builder.packMessage(putMsg, isOldStyleProperties);
            assertEquals(EventBuilderResult.PAYLOAD_EMPTY, res);

            putMsg = new PutMessageImpl();
            buffer = ByteBuffer.allocate(PutHeader.MAX_PAYLOAD_SIZE_SOFT + 1);
            putMsg.appData().setPayload(buffer);

            // set compression to none in order to get PAYLOAD_TOO_BIG result
            putMsg.setCompressionType(CompressionAlgorithmType.E_NONE);

            res = builder.packMessage(putMsg, isOldStyleProperties);
            assertEquals(EventBuilderResult.PAYLOAD_TOO_BIG, res);

            putMsg = new PutMessageImpl();

            final int numMsgs = EventHeader.MAX_SIZE_SOFT / PutHeader.MAX_PAYLOAD_SIZE_SOFT;
            // Cannot pack more than 'numMsgs' having a unpackedSize of
            // 'PutHeader.MAX_PAYLOAD_SIZE_SOFT' in 1 bmqp event.

            buffer = ByteBuffer.allocate(PutHeader.MAX_PAYLOAD_SIZE_SOFT);
            putMsg.appData().setPayload(buffer);

            // set compression to none in order to get EVENT_TOO_BIG result
            putMsg.setCompressionType(CompressionAlgorithmType.E_NONE);

            for (int i = 0; i < numMsgs; i++) {
                res = builder.packMessage(putMsg, isOldStyleProperties);
                assertEquals(EventBuilderResult.SUCCESS, res);
            }

            // Try to add one more message, which must fail with event_too_big.
            res = builder.packMessage(putMsg, isOldStyleProperties);
            assertEquals(EventBuilderResult.EVENT_TOO_BIG, res);

            putMsg = new PutMessageImpl();

            putMsg.appData().setPayload(buffer);
            putMsg.setFlags(PutHeaderFlags.ACK_REQUESTED.toInt());

            CorrelationId corId = putMsg.correlationId();
            assertNull(corId);

            // Try to pack with default CorrelationID which is zero
            res = builder.packMessage(putMsg, isOldStyleProperties);
            assertEquals(EventBuilderResult.MISSING_CORRELATION_ID, res);
        }
    }

    @Test
    public void testBigPutEvent() throws IOException {
        // Check that ByteBuffer limit is honored when Put event is being built

        for (boolean isOldStyleProperties : new boolean[] {false, true}) {
            PutMessageImpl putMsg = new PutMessageImpl();
            PutEventBuilder builder = new PutEventBuilder();

            ByteBuffer buffer = ByteBuffer.allocate(EventHeader.MAX_SIZE_SOFT + 1024);
            buffer.limit(PutHeader.MAX_PAYLOAD_SIZE_SOFT);

            putMsg.appData().setPayload(buffer);

            // set compression to none in order to get the unpacked size equal to
            // PutHeader.MAX_PAYLOAD_SIZE_SOFT.
            putMsg.setCompressionType(CompressionAlgorithmType.E_NONE);

            EventBuilderResult res = builder.packMessage(putMsg, isOldStyleProperties);
            assertEquals(EventBuilderResult.SUCCESS, res);

            ByteBuffer[] message;
            message = builder.build();

            int size = 0;
            for (ByteBuffer b : message) {
                size += b.limit();
            }

            logger.info("EventImpl size                  : {}", size);
            logger.info("PutHeader.MAX_PAYLOAD_SIZE_SOFT : {}", PutHeader.MAX_PAYLOAD_SIZE_SOFT);
            logger.info("EventHeader.MAX_SIZE_SOFT       : {}", EventHeader.MAX_SIZE_SOFT);

            assertTrue(size <= EventHeader.MAX_SIZE_SOFT);
        }
    }

    @Test
    public void testBuildPutMessageWithProperties() throws IOException {
        final String k = "abcdefghijklmnopqrstuvwxyz";
        final ByteBuffer b = ByteBuffer.wrap(k.getBytes());

        final MessagePropertiesImpl props = new MessagePropertiesImpl();
        props.setPropertyAsInt32("encoding", 3);
        props.setPropertyAsInt64("timestamp", 1234567890L);
        props.setPropertyAsString("id", "myCoolId");

        final PutEventBuilder builder = new PutEventBuilder();

        int flags = PutHeaderFlags.setFlag(0, PutHeaderFlags.ACK_REQUESTED);
        flags = PutHeaderFlags.setFlag(flags, PutHeaderFlags.MESSAGE_PROPERTIES);

        final long[] crc32s = new long[] {3469549003L, 340340870L};

        for (int i = 0; i < 2; i++) {
            PutMessageImpl putMsg = new PutMessageImpl();
            putMsg.setQueueId(9876);
            putMsg.setupCorrelationId(CorrelationIdImpl.restoreId(1234));
            putMsg.appData().setPayload(b);
            putMsg.setFlags(flags);
            putMsg.appData().setProperties(props);

            // set compression to none in order to match file content
            putMsg.setCompressionType(CompressionAlgorithmType.E_NONE);

            final boolean isOldStyleProperties = i % 2 == 0;
            assertEquals(
                    EventBuilderResult.SUCCESS, builder.packMessage(putMsg, isOldStyleProperties));

            // Compare with value stored in the binary pattern
            logger.info("PUT header {}: {}", i + 1, putMsg.header());
            assertEquals(crc32s[i], putMsg.crc32c());
            assertEquals(i, putMsg.header().schemaWireId());
            assertEquals(isOldStyleProperties, putMsg.appData().isOldStyleProperties());
        }

        ByteBuffer[] message = builder.build();

        TestHelpers.compareWithFileContent(message, MessagesTestSamples.PUT_MULTI_MSG);
    }
}
