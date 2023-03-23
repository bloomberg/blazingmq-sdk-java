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
import static org.junit.Assert.fail;

import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.intf.SessionEventHandler;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushEventImplTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testDispatchUnknownCompression() throws IOException {
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

                pushMsg.appData().setPayload(ByteBuffer.wrap(bytes));

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

            SessionEventHandler handler =
                    new SessionEventHandler() {
                        public void handleControlEvent(ControlEventImpl controlEvent) {
                            throw new UnsupportedOperationException();
                        }

                        public void handleAckMessage(AckMessageImpl ackMsg) {
                            throw new UnsupportedOperationException();
                        }

                        public void handlePushMessage(PushMessageImpl pushMsg) {
                            try {
                                assertArrayEquals(
                                        new ByteBuffer[] {ByteBuffer.wrap(bytes)},
                                        pushMsg.appData().payload());
                                return;
                            } catch (IOException e) {
                                logger.error("IOException has been thrown", e);
                            }

                            fail(); // should not get here
                        }

                        public void handlePutEvent(PutEventImpl putEvent) {
                            throw new UnsupportedOperationException();
                        }

                        public void handleConfirmEvent(ConfirmEventImpl confirmEvent) {
                            throw new UnsupportedOperationException();
                        }
                    };

            try {
                pushEvent.dispatch(handler);
                fail(); // should not get here
            } catch (IllegalArgumentException e) {
                // According to PushMessageIterator used in 'dispatch' method,
                // when 'next()' method is called, the next item after
                // the current one is also prefetched and parsed.
                // If the next item is invalid then the exception will be thrown for
                // the current one.
                //
                // In our situation, the first item should be processed successfully.
                // When the second one is being dispatched, which is valid,
                // an exception should be thrown related to the third item.
                // On another hand, in the 'dispatch()', method number of messages is incremented
                // before calling the handler. So eventually when the exception is thrown,
                // the number of messages should be equal to 2
                assertEquals(
                        String.format("'%d' - unknown compression algorithm type", unknownType),
                        e.getMessage());

                // only the first message should be processed successfully
                assertEquals(2, pushEvent.messageCount());
            }

            assertEquals(2, pushEvent.messageCount());
        }
    }
}
