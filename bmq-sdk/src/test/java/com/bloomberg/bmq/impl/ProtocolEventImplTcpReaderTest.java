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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bloomberg.bmq.MessageGUID;
import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.ReadCallback.ReadCompletionStatus;
import com.bloomberg.bmq.impl.infr.proto.EventType;
import com.bloomberg.bmq.impl.infr.proto.PushEventBuilder;
import com.bloomberg.bmq.impl.infr.proto.PushMessageImpl;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ProtocolEventImplTcpReaderTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private ByteBuffer[] buildPushMessage(boolean isOldStyleProperties) throws IOException {
        String PAYLOAD = "abcdefghijklmnopqrstuvwxyz";
        String GUID = "ABCDEF0123456789ABCDEF0123456789";

        MessageGUID guid = MessageGUID.fromHex(GUID);

        PushMessageImpl pushMsg = new PushMessageImpl();
        pushMsg.setQueueId(9876);
        pushMsg.setMessageGUID(guid);
        pushMsg.appData().setPayload(ByteBuffer.wrap(PAYLOAD.getBytes()));

        PushEventBuilder builder = new PushEventBuilder();
        builder.packMessage(pushMsg, isOldStyleProperties);

        return builder.build();
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

        for (boolean isOldStyleProperties : new boolean[] {true, false}) {
            // 1. Generate BlazingMQ EventImpl with several PUSH messages;
            ByteBuffer[] event = buildPushMessage(isOldStyleProperties);
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
}
