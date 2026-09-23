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
package com.bloomberg.bmq.impl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.bloomberg.bmq.MessageGUID;
import com.bloomberg.bmq.ResultCodes.AckResult;
import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.ReadCallback.ReadCompletionStatus;
import com.bloomberg.bmq.impl.infr.proto.AckEventBuilder;
import com.bloomberg.bmq.impl.infr.proto.AckEventImpl;
import com.bloomberg.bmq.impl.infr.proto.AckMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.EventBuilderResult;
import com.bloomberg.bmq.impl.infr.proto.EventType;
import com.bloomberg.bmq.impl.infr.proto.MessagePropertiesImpl;
import com.bloomberg.bmq.impl.infr.proto.PushEventBuilder;
import com.bloomberg.bmq.impl.infr.proto.PushEventImpl;
import com.bloomberg.bmq.impl.infr.proto.PushMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.PushMessageIterator;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ProtocolEventImplTcpReaderTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int QUEUE_ID = 9876;
    private static final String PAYLOAD = "abcdefghijklmnopqrstuvwxyz";
    private static final String ROUTING_ID = "abcd-efgh-ijkl";
    private static final long TIMESTAMP = 123456789L;

    private ByteBuffer[] buildPushMessage() throws IOException {
        MessageGUID guid = MessageGUID.fromHex("ABCDEF0123456789ABCDEF0123456789");

        PushMessageImpl pushMsg = new PushMessageImpl();
        pushMsg.setQueueId(QUEUE_ID);
        pushMsg.setMessageGUID(guid);
        pushMsg.appData().setPayload(ByteBuffer.wrap(PAYLOAD.getBytes()));

        PushEventBuilder builder = new PushEventBuilder();
        builder.packMessage(pushMsg);

        return builder.build();
    }

    private PushMessageImpl createPushMessage(MessageGUID guid) throws IOException {
        MessagePropertiesImpl props = new MessagePropertiesImpl();
        props.setPropertyAsString("routingId", ROUTING_ID);
        props.setPropertyAsInt64("timestamp", TIMESTAMP);

        PushMessageImpl pushMsg = new PushMessageImpl();
        pushMsg.setQueueId(QUEUE_ID);
        pushMsg.setMessageGUID(guid);
        pushMsg.appData().setProperties(props);
        pushMsg.appData().setPayload(ByteBuffer.wrap(PAYLOAD.getBytes()));

        return pushMsg;
    }

    @Test
    void testPushAndAckStream() throws IOException {
        logger.info("===============================================================");
        logger.info("BEGIN Testing ProtocolEventImplTcpReaderTest PUSH and ACK stream.");
        logger.info("===============================================================");

        // Check that ProtocolEventTcpReader correctly reads a stream of PUSH
        // and ACK events split into chunks which do not match event boundaries.
        // Steps:
        // 1. Build PUSH events with message properties and ACK events with the
        //    same GUIDs;
        // 2. Feed ProtocolEventTcpReader with the stream by chunks of different size;
        // 3. Decode BlazingMQ events from the ProtocolEventTcpReader callback;
        // 4. Verify each PUSH message keeps its properties and payload, and has
        //    an ACK message with the same GUID.

        final int NUM_EVENTS = 5;
        final int NUM_MESSAGES = 20;

        ByteBufferOutputStream bbos = new ByteBufferOutputStream();

        for (int i = 0; i < NUM_EVENTS; i++) {
            PushEventBuilder pushBuilder = new PushEventBuilder();
            AckEventBuilder ackBuilder = new AckEventBuilder();

            for (int j = 0; j < NUM_MESSAGES; j++) {
                final MessageGUID guid =
                        MessageGUID.fromHex(String.format("%032X", i * NUM_MESSAGES + j + 1));

                assertEquals(
                        EventBuilderResult.SUCCESS,
                        pushBuilder.packMessage(createPushMessage(guid)));
                assertEquals(
                        EventBuilderResult.SUCCESS,
                        ackBuilder.packMessage(
                                new AckMessageImpl(
                                        AckResult.SUCCESS,
                                        CorrelationIdImpl.restoreId(j),
                                        guid,
                                        QUEUE_ID)));
            }

            for (ByteBuffer b : pushBuilder.build()) {
                bbos.writeBytes(b);
            }
            for (ByteBuffer b : ackBuilder.build()) {
                bbos.writeBytes(b);
            }
        }

        final byte[] stream;
        try (ByteBufferInputStream bbis = new ByteBufferInputStream(bbos.reset())) {
            stream = new byte[bbis.available()];
            assertEquals(stream.length, bbis.read(stream));
        }

        for (int chunkSize : new int[] {1, 13, 512, stream.length}) {
            logger.info("Read {} bytes by chunks of {} bytes", stream.length, chunkSize);

            final ArrayList<PushMessageImpl> pushMsgs = new ArrayList<>();
            final HashSet<String> ackGuids = new HashSet<>();

            ProtocolEventTcpReader reader =
                    new ProtocolEventTcpReader(
                            (eventType, bbuf) -> {
                                switch (eventType) {
                                    case PUSH:
                                        PushMessageIterator pushIt =
                                                new PushEventImpl(bbuf).iterator();
                                        while (pushIt.hasNext()) {
                                            pushMsgs.add(pushIt.next());
                                        }
                                        break;
                                    case ACK:
                                        Iterator<AckMessageImpl> ackIt =
                                                new AckEventImpl(bbuf).iterator();
                                        while (ackIt.hasNext()) {
                                            ackGuids.add(ackIt.next().messageGUID().toString());
                                        }
                                        break;
                                    default:
                                        logger.error("Unexpected event type: {}", eventType);
                                        fail();
                                        break;
                                }
                            });

            ReadCompletionStatus status = new ReadCompletionStatus();
            for (int pos = 0; pos < stream.length; pos += chunkSize) {
                final int size = Math.min(chunkSize, stream.length - pos);
                byte[] chunk = Arrays.copyOfRange(stream, pos, pos + size);
                reader.read(status, new ByteBuffer[] {ByteBuffer.wrap(chunk)});
            }

            assertEquals(NUM_EVENTS * NUM_MESSAGES, pushMsgs.size());

            for (PushMessageImpl msg : pushMsgs) {
                assertTrue(ackGuids.contains(msg.messageGUID().toString()));

                MessagePropertiesImpl props = msg.appData().properties();
                assertEquals(2, props.numProperties());
                assertEquals(ROUTING_ID, props.get("routingId").getValueAsString());
                assertEquals(TIMESTAMP, props.get("timestamp").getValueAsInt64());

                assertArrayEquals(
                        new ByteBuffer[] {ByteBuffer.wrap(PAYLOAD.getBytes())},
                        msg.appData().payload());
            }
        }

        logger.info("=============================================================");
        logger.info("END Testing ProtocolEventImplTcpReaderTest PUSH and ACK stream.");
        logger.info("=============================================================");
    }

    @Test
    void testPartialReading() throws IOException {

        // Check that ProtocolEventTcpReader correctly reads BlazingMQ events
        // even if incoming buffer contains partial EventImpl header or body.
        // Steps:
        // 1. Generate BlazingMQ EventImpl with several PUSH messages;
        // 2. Fill a plain buffer with the event content;
        // 3. Read from this buffer by portions with different size (from 1
        //    up to the whole buffer) and feed ProtocolEventTcpReader with those portions;
        // 4. Check that ProtocolEventTcpReader correctly composes BlazingMQ Events.

        final int NUM_MESSAGES = 3;

        // 1. Generate BlazingMQ EventImpl with several PUSH messages;
        ByteBuffer[] event = buildPushMessage();
        ByteBufferInputStream inpStream = new ByteBufferInputStream(event);
        ReadCompletionStatus status = new ReadCompletionStatus();

        ArrayList<ByteBuffer[]> dataList = new ArrayList<>();

        ProtocolEventTcpReader reader =
                new ProtocolEventTcpReader(
                        (eventType, bbuf) -> {
                            dataList.add(bbuf);
                            assertEquals(EventType.PUSH, eventType);
                        });
        // 2. Fill a plain buffer with the event content;
        final int PLAIN_BUF_SIZE = inpStream.available() * NUM_MESSAGES;
        ByteBuffer plainBuffer = ByteBuffer.allocate(PLAIN_BUF_SIZE);
        for (int i = 0; i < NUM_MESSAGES; i++) {
            for (ByteBuffer b : event) {
                b.rewind();
                plainBuffer.put(b);
            }
        }
        plainBuffer.rewind();

        // 3. Read from this buffer by portions with different size (from 1
        //    up to the whole buffer) and feed ProtocolEventTcpReader with those portions;
        for (int i = 1; i <= PLAIN_BUF_SIZE; i++) {
            ArrayList<ByteBuffer> payloads = new ArrayList<>();
            while (plainBuffer.hasRemaining()) {
                int sz = Math.min(i, plainBuffer.remaining());
                byte[] ar = new byte[sz];
                plainBuffer.get(ar);
                payloads.add(ByteBuffer.wrap(ar));
            }
            ByteBuffer[] bb = new ByteBuffer[payloads.size()];
            bb = payloads.toArray(bb);
            reader.read(status, bb);
            plainBuffer.rewind();
        }
        // 4. Check that ProtocolEventTcpReader correctly composes BlazingMQ Events.
        assertEquals(dataList.size(), PLAIN_BUF_SIZE * NUM_MESSAGES);
        for (ByteBuffer[] data : dataList) {
            inpStream.reset();
            ByteBufferInputStream istr = new ByteBufferInputStream(data);
            assertEquals(istr.available(), inpStream.available());
            while (istr.available() > 0) {
                assertEquals(istr.readByte(), inpStream.readByte());
            }
        }
    }
}
