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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.bloomberg.bmq.CorrelationId;
import com.bloomberg.bmq.MessageProperties;
import com.bloomberg.bmq.impl.CorrelationIdImpl;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.msg.MessagesTestSamples;
import com.bloomberg.bmq.util.TestHelpers;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PutMessageIteratorTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testWithPattern() throws IOException {
        ByteBuffer buf = TestHelpers.readFile(MessagesTestSamples.PUT_MULTI_MSG.filePath());

        PutEventImpl putEvent = new PutEventImpl(new ByteBuffer[] {buf});
        assertTrue(putEvent.isValid());
        // The event contains two PUT messages each with the following properties
        // Type       Name          Value
        // INT32     "encoding"      3
        // INT64     "timestamp"     1234567890L
        // STRING    "id"           "myCoolId"

        PutMessageIterator putIt = putEvent.iterator();
        assertTrue(putIt.isValid());

        final int QUEUE_ID = 9876;
        final CorrelationId CORR_ID = CorrelationIdImpl.restoreId(1234);
        final int FLAGS = 3;
        final String PAYLOAD = "abcdefghijklmnopqrstuvwxyz";

        final MessagePropertiesImpl props = new MessagePropertiesImpl();
        props.setPropertyAsInt32("encoding", 3);
        props.setPropertyAsInt64("timestamp", 1234567890L);
        props.setPropertyAsString("id", "myCoolId");

        int i = 0;
        while (putIt.hasNext()) {
            // Build expected content
            final boolean isOldStyleProperties = i % 2 == 0;

            final ByteBufferOutputStream bbos = new ByteBufferOutputStream();
            if (isOldStyleProperties) {
                props.streamOutOld(bbos);
            } else {
                props.streamOut(bbos);
            }

            bbos.writeAscii(PAYLOAD);

            final int numPaddingBytes = ProtocolUtil.calculatePadding(bbos.size());
            assertEquals(2, numPaddingBytes);

            final ByteBuffer content = ByteBuffer.allocate(bbos.size() + numPaddingBytes);

            // Calculate CRC32c before adding padding
            ByteBuffer[] bb = bbos.reset();
            final long expectedCRC32C = isOldStyleProperties ? 3469549003L : 340340870L;
            final long CRC32C = Crc32c.calculate(bb);
            assertEquals(expectedCRC32C, CRC32C);

            // Fill content buffer with the payload and the padding
            for (ByteBuffer b : bb) {
                content.put(b);
            }
            content.put(ProtocolUtil.getPaddingBytes(numPaddingBytes), 0, numPaddingBytes);
            content.flip();

            // Get and verify PutMessage
            PutMessageImpl putMsg = putIt.next();
            logger.info("PUT header: {}", putMsg.header());

            assertEquals(CORR_ID, putMsg.correlationId());
            assertEquals(QUEUE_ID, putMsg.queueId());
            assertEquals(CRC32C, putMsg.crc32c());
            assertEquals(FLAGS, putMsg.flags());
            assertEquals(isOldStyleProperties ? 0 : 1, putMsg.header().schemaWireId());
            assertEquals(isOldStyleProperties, putMsg.appData().isOldStyleProperties());

            ByteBuffer[] pl = putMsg.appData().applicationData();
            ByteBuffer b =
                    ByteBuffer.allocate(
                            putMsg.appData().unpackedSize() + putMsg.appData().numPaddingBytes());
            for (ByteBuffer p : pl) {
                b.put(p);
            }
            b.put(
                    ProtocolUtil.getPaddingBytes(putMsg.appData().numPaddingBytes()),
                    0,
                    putMsg.appData().numPaddingBytes());
            b.flip();

            assertEquals(content, b);
            i++;
        }

        assertEquals(2, i);
    }

    @Test
    public void testWithBuilder() throws IOException {
        final String PAYLOAD = "abcdefghijklmnopqrstuvwxyz";
        final int NUM_MSGS = 1000;
        final int FLAGS = PutHeaderFlags.setFlag(0, PutHeaderFlags.ACK_REQUESTED);

        final ByteBuffer payload = ByteBuffer.allocate(PAYLOAD.length());
        payload.put(PAYLOAD.getBytes());

        final PutEventBuilder builder = new PutEventBuilder();
        final PutMessageImpl[] puts = new PutMessageImpl[NUM_MSGS];

        for (int i = 0; i < NUM_MSGS; i++) {
            final boolean isOldStyleProperties = i % 2 == 0;

            PutMessageImpl msg = new PutMessageImpl();
            msg.setFlags(FLAGS);
            msg.setQueueId(i);
            msg.setupCorrelationId(CorrelationIdImpl.restoreId(i * i + 1));

            final MessageProperties props = msg.messageProperties();
            props.setPropertyAsInt32("encoding", 3);
            props.setPropertyAsInt64("timestamp", 1234567890L);
            props.setPropertyAsString("id", "myCoolId");

            msg.appData().setPayload(payload);

            EventBuilderResult rc = builder.packMessage(msg, isOldStyleProperties);
            assertEquals(EventBuilderResult.SUCCESS, rc);
            assertEquals(isOldStyleProperties, msg.appData().isOldStyleProperties());
            assertEquals(isOldStyleProperties ? 0 : 1, msg.header().schemaWireId());

            final long expectedCRC32C = isOldStyleProperties ? 3469549003L : 340340870L;
            assertEquals(expectedCRC32C, msg.header().crc32c());

            puts[i] = msg;
        }

        PutEventImpl putEvent = new PutEventImpl(builder.build());
        assertTrue(putEvent.isValid());

        // Check twice
        for (int j = 0; j < 2; j++) {
            PutMessageIterator putIt = putEvent.iterator();
            assertTrue(putIt.isValid());

            int i = 0;
            while (putIt.hasNext()) {
                PutMessageImpl msg = putIt.next();
                PutMessageImpl exp = puts[i];

                assertEquals(exp.correlationId(), msg.correlationId());
                assertEquals(exp.queueId(), msg.queueId());
                assertEquals(exp.flags(), msg.flags());
                assertEquals(exp.crc32c(), msg.crc32c());
                assertEquals(exp.header().schemaWireId(), msg.header().schemaWireId());
                assertEquals(
                        exp.appData().isOldStyleProperties(), msg.appData().isOldStyleProperties());
                assertEquals(exp.appData().numPaddingBytes(), msg.appData().numPaddingBytes());
                assertArrayEquals(exp.appData().applicationData(), msg.appData().applicationData());

                i++;
            }
            assertEquals(NUM_MSGS, i);
        }
    }

    @Test
    public void testMultipleIterators() throws IOException {
        // Verify that two message iterators in single thread mode don't affect each other.
        // 1. Using a pattern create PUT event that contains 2 PUT messages
        // 2. Create one message iterator and advance it with next()
        // 3. Create another iterator
        // 4. Advance the first iterator one more time
        // 5. Now advance it again and verify a exception is thrown

        ByteBuffer buf = TestHelpers.readFile(MessagesTestSamples.PUT_MULTI_MSG.filePath());

        PutEventImpl putEvent = new PutEventImpl(new ByteBuffer[] {buf});
        assertTrue(putEvent.isValid());

        PutMessageIterator putIt = putEvent.iterator();
        assertTrue(putIt.isValid());

        putIt.next();

        PutMessageIterator putIt2 = putEvent.iterator();

        putIt.next();

        try {
            putIt.next();
            fail();
        } catch (NoSuchElementException e) {
            assertEquals("No PUT messages", e.getMessage());
        }
    }
}
