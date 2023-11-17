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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.bloomberg.bmq.MessageGUID;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.msg.MessagesTestSamples;
import com.bloomberg.bmq.util.TestHelpers;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushMessageIteratorTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testPatternWithOptions() throws IOException {
        ByteBuffer buf =
                TestHelpers.readFile(MessagesTestSamples.PUSH_WITH_SUBQUEUE_IDS_MSG.filePath());

        PushEventImpl pushEvent = new PushEventImpl(new ByteBuffer[] {buf});
        assertTrue(pushEvent.isValid());

        PushMessageIterator pushIt = pushEvent.iterator();
        assertTrue(pushIt.isValid());

        int i = 0;
        final String GUID = "ABCDEF0123456789ABCDEF0123456789";
        final String PAYLOAD = "abcdefghijklmnopqrstuvwxyz";
        final int numPad = ProtocolUtil.calculatePadding(PAYLOAD.length());

        ByteBuffer paddedPayload = ByteBuffer.allocate(PAYLOAD.length() + numPad);
        paddedPayload.put(PAYLOAD.getBytes());
        paddedPayload.put(ProtocolUtil.getPaddingBytes(numPad), 0, numPad);
        paddedPayload.flip();

        while (pushIt.hasNext()) {
            PushMessageImpl pushMsg = pushIt.next();
            assertEquals(GUID, pushMsg.messageGUID().toHex());
            assertEquals(9876, pushMsg.queueId());

            SubQueueIdsOption subId = pushMsg.options().subQueueIdsOption();
            assertNotNull(subId);

            Integer[] ids = subId.subQueueIds();
            assertNotNull(ids);
            assertEquals(3, ids.length);

            Integer[] patt = {777, 11, 987};
            assertArrayEquals(patt, ids);

            ByteBuffer[] payload = pushMsg.appData().applicationData();
            assertNotNull(payload);

            ByteBuffer b =
                    ByteBuffer.allocate(
                            pushMsg.appData().unpackedSize() + pushMsg.appData().numPaddingBytes());
            for (ByteBuffer p : payload) {
                b.put(p);
            }
            b.put(
                    ProtocolUtil.getPaddingBytes(pushMsg.appData().numPaddingBytes()),
                    0,
                    pushMsg.appData().numPaddingBytes());
            b.flip();

            assertEquals(paddedPayload, b);
            i++;
        }
        assertEquals(1, i);
    }

    @Test
    public void testWithPattern() throws IOException {
        ByteBuffer buf = TestHelpers.readFile(MessagesTestSamples.PUSH_MULTI_MSG.filePath());

        PushEventImpl pushEvent = new PushEventImpl(new ByteBuffer[] {buf});
        assertTrue(pushEvent.isValid());
        // The event contains two PUSH messages each with the following properties
        // Type       Name          Value
        // INT32     "encoding"      3
        // INT64     "timestamp"     1234567890L
        // STRING    "id"           "myCoolId"

        PushMessageIterator pushIt = pushEvent.iterator();
        assertTrue(pushIt.isValid());

        final String GUID = "ABCDEF0123456789ABCDEF0123456789";
        final String PAYLOAD = "abcdefghijklmnopqrstuvwxyz";
        final int QUEUE_ID = 9876;
        final int FLAGS = PushHeaderFlags.setFlag(0, PushHeaderFlags.MESSAGE_PROPERTIES);

        final MessagePropertiesImpl props = new MessagePropertiesImpl();
        props.setPropertyAsInt32("encoding", 3);
        props.setPropertyAsInt64("timestamp", 1234567890L);
        props.setPropertyAsString("id", "myCoolId");

        int i = 0;
        while (pushIt.hasNext()) {
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

            bbos.write(ProtocolUtil.getPaddingBytes(numPaddingBytes), 0, numPaddingBytes);

            final ByteBuffer content = ByteBuffer.allocate(bbos.size());
            for (ByteBuffer b : bbos.reset()) {
                content.put(b);
            }
            content.flip();

            // Get and verify PushMessage
            PushMessageImpl pushMsg = pushIt.next();
            logger.info("PUSH header: {}", pushMsg.header());

            assertEquals(GUID, pushMsg.messageGUID().toHex());
            assertEquals(QUEUE_ID, pushMsg.queueId());
            assertEquals(FLAGS, pushMsg.flags());
            assertEquals(isOldStyleProperties ? 0 : 1, pushMsg.header().schemaWireId());
            assertEquals(isOldStyleProperties, pushMsg.appData().isOldStyleProperties());
            assertArrayEquals(new Integer[] {0}, pushMsg.subQueueIds());

            ByteBuffer[] data = pushMsg.appData().applicationData();
            assertNotNull(data);

            ByteBuffer b =
                    ByteBuffer.allocate(
                            pushMsg.appData().unpackedSize() + pushMsg.appData().numPaddingBytes());
            for (ByteBuffer p : data) {
                b.put(p);
            }
            b.put(
                    ProtocolUtil.getPaddingBytes(pushMsg.appData().numPaddingBytes()),
                    0,
                    pushMsg.appData().numPaddingBytes());
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
        final int FLAGS = PushHeaderFlags.setFlag(0, PushHeaderFlags.MESSAGE_PROPERTIES);

        final MessageGUID GUID = MessageGUID.fromHex("0000000000003039CD8101000000270F");

        final ByteBuffer payload = ByteBuffer.allocate(PAYLOAD.length());
        payload.put(PAYLOAD.getBytes());

        final MessagePropertiesImpl props = new MessagePropertiesImpl();
        props.setPropertyAsInt32("encoding", 3);
        props.setPropertyAsInt64("timestamp", 1234567890L);
        props.setPropertyAsString("id", "myCoolId");

        final PushEventBuilder builder = new PushEventBuilder();
        final PushMessageImpl[] pushs = new PushMessageImpl[NUM_MSGS];

        for (int i = 0; i < NUM_MSGS; i++) {
            final boolean isOldStyleProperties = i % 2 == 0;

            PushMessageImpl msg = new PushMessageImpl();
            msg.setFlags(FLAGS);
            msg.setQueueId(i);
            msg.setMessageGUID(GUID);
            msg.appData().setProperties(props);
            msg.appData().setPayload(payload.duplicate());

            EventBuilderResult rc = builder.packMessage(msg, isOldStyleProperties);
            assertEquals(EventBuilderResult.SUCCESS, rc);
            assertEquals(isOldStyleProperties, msg.appData().isOldStyleProperties());
            assertEquals(isOldStyleProperties ? 0 : 1, msg.header().schemaWireId());

            pushs[i] = msg;
        }

        PushEventImpl pushEvent = new PushEventImpl(builder.build());
        assertTrue(pushEvent.isValid());

        // Check twice
        for (int j = 0; j < 2; j++) {
            PushMessageIterator pushIt = pushEvent.iterator();
            assertTrue(pushIt.isValid());

            int i = 0;
            while (pushIt.hasNext()) {
                PushMessageImpl msg = pushIt.next();
                PushMessageImpl exp = pushs[i];

                assertEquals(GUID, msg.messageGUID());
                assertEquals(i, msg.queueId());
                assertEquals(FLAGS, msg.flags());
                assertEquals(i % 2, msg.header().schemaWireId());
                assertEquals(i % 2 == 0, msg.appData().isOldStyleProperties());
                assertEquals(exp.appData().numPaddingBytes(), msg.appData().numPaddingBytes());
                assertArrayEquals(exp.appData().applicationData(), msg.appData().applicationData());

                MessagePropertiesImpl msgProps = msg.appData().properties();
                assertEquals(3, msgProps.numProperties());

                boolean encodingFound = false, timestampFound = false, idFound = false;

                Iterator<Map.Entry<String, MessageProperty>> it = msgProps.iterator();
                while (it.hasNext()) {
                    MessageProperty p = it.next().getValue();
                    switch (p.name()) {
                        case "encoding":
                            assertEquals(PropertyType.INT32, p.type());
                            assertEquals(3, p.getValueAsInt32());
                            encodingFound = true;
                            break;
                        case "timestamp":
                            assertEquals(PropertyType.INT64, p.type());
                            assertEquals(1234567890L, p.getValueAsInt64());
                            timestampFound = true;
                            break;
                        case "id":
                            assertEquals(PropertyType.STRING, p.type());
                            assertEquals("myCoolId", p.getValueAsString());
                            idFound = true;
                            break;
                        default:
                            fail();
                            break;
                    }
                }
                assertTrue(encodingFound);
                assertTrue(timestampFound);
                assertTrue(idFound);
                i++;
            }
            assertEquals(NUM_MSGS, i);
        }
    }

    @Test
    public void testMultipleIterators() throws IOException {
        // Verify that two message iterators in single thread mode don't affect each other.
        // 1. Using a pattern create PUSH event that contains 2 PUSH messages
        // 2. Create one message iterator and advance it with next()
        // 3. Create another iterator
        // 4. Advance the first iterator one more time
        // 5. Now advance it again and verify a exception is thrown

        ByteBuffer buf = TestHelpers.readFile(MessagesTestSamples.PUSH_MULTI_MSG.filePath());

        PushEventImpl pushEvent = new PushEventImpl(new ByteBuffer[] {buf});

        PushMessageIterator pushIt = pushEvent.iterator();
        pushIt.next();
        PushMessageIterator pushIt2 = pushEvent.iterator();
        pushIt.next();
        try {
            pushIt.next();
            fail();
        } catch (NoSuchElementException e) {
            assertEquals("No PUSH messages", e.getMessage());
        }
    }

    @Test
    public void testMultipleCompressedMessages() throws IOException {
        final PushEventBuilder builder = new PushEventBuilder();

        final byte[] bytes = new byte[Protocol.COMPRESSION_MIN_APPDATA_SIZE + 1];
        bytes[0] = 1;
        bytes[Protocol.COMPRESSION_MIN_APPDATA_SIZE - 1] = 1;

        final int NUM = 500;

        for (int i = 0; i < NUM; i++) {
            final boolean isOldStyleProperties = i % 2 == 0;

            PushMessageImpl pushMsg = new PushMessageImpl();

            ByteBuffer payload = ByteBuffer.allocate(bytes.length);
            payload.put(bytes);
            pushMsg.appData().setPayload(payload);

            MessagePropertiesImpl props = new MessagePropertiesImpl();
            props.setPropertyAsInt32("routingId", 42);
            props.setPropertyAsInt64("timestamp", 1234567890L);

            pushMsg.appData().setProperties(props);

            pushMsg.setCompressionType(CompressionAlgorithmType.E_ZLIB);

            builder.packMessage(pushMsg, isOldStyleProperties);
        }

        PushEventImpl pushEvent = new PushEventImpl(builder.build());
        PushMessageIterator pushIt = pushEvent.iterator();

        int counter = 0;
        while (pushIt.hasNext()) {
            PushMessageImpl pushMsg = pushIt.next();

            assertArrayEquals(bytes, TestHelpers.buffersContents(pushMsg.appData().payload()));

            final MessagePropertiesImpl props = pushMsg.appData().properties();
            assertEquals(2, props.numProperties());

            final Iterator<Map.Entry<String, MessageProperty>> it = props.iterator();

            assertTrue(it.hasNext());
            Map.Entry<String, MessageProperty> item1 = it.next();
            assertEquals("routingId", item1.getKey());
            assertEquals(42, item1.getValue().getValueAsInt32());

            assertTrue(it.hasNext());
            Map.Entry<String, MessageProperty> item2 = it.next();
            assertEquals("timestamp", item2.getKey());
            assertEquals(1234567890L, item2.getValue().getValueAsInt64());

            assertFalse(it.hasNext());

            counter++;
        }

        assertEquals(NUM, counter);
    }

    @Test
    public void testUnknownCompression() throws IOException {
        for (boolean isOldStyleProperties : new boolean[] {true, false}) {
            final byte[] bytes = new byte[Protocol.COMPRESSION_MIN_APPDATA_SIZE + 1];

            bytes[0] = 1;
            bytes[Protocol.COMPRESSION_MIN_APPDATA_SIZE - 1] = 1;

            final int NUM = 4;

            final int unknownType =
                    EnumSet.allOf(CompressionAlgorithmType.class).stream()
                                    .mapToInt(CompressionAlgorithmType::toInt)
                                    .max()
                                    .getAsInt()
                            + 1;

            final MessagePropertiesImpl props = new MessagePropertiesImpl();
            props.setPropertyAsInt32("routingId", 42);
            props.setPropertyAsInt64("timestamp", 1234567890L);

            ByteBufferOutputStream bbos = new ByteBufferOutputStream();

            EventHeader header = new EventHeader();
            header.setType(EventType.PUSH);

            final int unpackedSize = props.totalSize() + bytes.length;
            final int numPaddingBytes = ProtocolUtil.calculatePadding(unpackedSize);

            header.setLength(
                    EventHeader.HEADER_SIZE
                            + (PushHeader.HEADER_SIZE_FOR_SCHEMA_ID
                                            + unpackedSize
                                            + numPaddingBytes)
                                    * NUM);

            header.streamOut(bbos);

            for (int i = 0; i < NUM; i++) {
                PushMessageImpl pushMsg = new PushMessageImpl();

                ByteBuffer payload = ByteBuffer.allocate(bytes.length);
                payload.put(bytes);
                pushMsg.appData().setPayload(payload);

                pushMsg.appData().setProperties(props);
                pushMsg.appData().setIsOldStyleProperties(isOldStyleProperties);

                pushMsg.compressData();

                // override compression type for the third message
                if (i == 2) {
                    pushMsg.header().setCompressionType(unknownType);
                }

                assertEquals(unpackedSize, pushMsg.appData().unpackedSize());
                assertEquals(numPaddingBytes, pushMsg.appData().numPaddingBytes());

                pushMsg.streamOut(bbos);
            }

            PushEventImpl pushEvent = new PushEventImpl(bbos.reset());
            PushMessageIterator pushIt = pushEvent.iterator();

            int counter = 0;
            try {
                while (pushIt.hasNext()) {
                    PushMessageImpl pushMsg = pushIt.next();

                    assertArrayEquals(
                            new ByteBuffer[] {ByteBuffer.wrap(bytes)}, pushMsg.appData().payload());
                    counter++;
                }
            } catch (IllegalArgumentException e) {
                // According to PushMessageIterator, when 'next()' method is called,
                // the next item after the current one is also prefetched and parsed.
                // If the next item is invalid then the exception will be thrown for
                // the current one.
                //
                // In our situation, the first item should be processed successfully.
                // When we call 'next()' to get the second one, which is valid,
                // an exception should be thrown related to the third item.
                assertEquals(
                        String.format("'%d' - unknown compression algorithm type", unknownType),
                        e.getMessage());

                // only the first message should be processed successfully
                assertEquals(1, counter);
            }

            assertEquals(1, counter);
        }
    }
}
