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

import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection;
import com.bloomberg.bmq.impl.infr.proto.EventHeader;
import com.bloomberg.bmq.impl.infr.proto.EventType;
import com.bloomberg.bmq.impl.infr.util.Argument;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtocolEventTcpReader {

    public interface EventHandler {
        void handleEvent(EventType eventType, ByteBuffer[] bbuf);
    }

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    enum ReadState {
        READ_EVENT_HEADER,
        DECODE_EVENT_HEADER,
        CHECK_EVENT_LENGTH,
        EVENT_NOT_COMPLETE,
        EVENT_COMPLETE;
    }

    private ArrayList<ByteBuffer> receivedPayloads;
    private int expectedDataSize = 0;
    private int eventSize = 0;
    private int partialHeaderSize = 0;
    private EventType expectedEventType = EventType.UNDEFINED;
    private final EventHandler eventHandler;
    private ReadState readState = ReadState.READ_EVENT_HEADER;
    private ByteBuffer currentHeader = ByteBuffer.allocate(EventHeader.HEADER_SIZE);

    public ProtocolEventTcpReader(EventHandler eventHandler) {
        this.eventHandler = Argument.expectNonNull(eventHandler, "eventHandler");
        receivedPayloads = new ArrayList<>();
    }

    public void reset() {
        expectedDataSize = 0;
        eventSize = 0;
        partialHeaderSize = 0;
        expectedEventType = EventType.UNDEFINED;
        readState = ReadState.READ_EVENT_HEADER;
        currentHeader.clear();
        receivedPayloads.clear();
    }

    private void addPayload(ByteBuffer b) {
        receivedPayloads.add(b);
    }

    public void read(
            TcpConnection.ReadCallback.ReadCompletionStatus completionStatus, ByteBuffer[] data)
            throws IOException {
        if (logger.isDebugEnabled()) {
            int totalRead = 0;
            for (ByteBuffer b : data) {
                totalRead += b.remaining();
            }
            logger.debug(
                    "Read {} bytes from {} buffers, completion status needed bytes {}",
                    totalRead,
                    data.length,
                    completionStatus.numNeeded());
        }
        try {
            EventHeader eventHeader = null;
            for (ByteBuffer restData : data) {
                boolean done = false;
                while (!done) {
                    if (restData.limit() == 0) {
                        done = true;
                        continue;
                    }
                    switch (readState) {
                        case READ_EVENT_HEADER:
                            while (currentHeader.hasRemaining() && restData.hasRemaining()) {
                                currentHeader.put(restData.get());
                            }
                            restData.rewind();
                            if (currentHeader.hasRemaining()) {
                                addPayload(restData);
                                partialHeaderSize = currentHeader.position();
                                done = true;
                            } else {
                                currentHeader.flip();
                                readState = ReadState.DECODE_EVENT_HEADER;
                            }
                            break;
                        case DECODE_EVENT_HEADER:
                            logger.debug("Start new event");
                            try (ByteBufferInputStream bbis =
                                    new ByteBufferInputStream(currentHeader)) {
                                eventHeader = new EventHeader();
                                eventHeader.streamIn(bbis);
                                eventSize = eventHeader.length();
                                expectedDataSize = eventSize - partialHeaderSize;
                                expectedEventType = eventHeader.type();
                                logger.debug("New event header: {}", eventHeader);
                                readState = ReadState.CHECK_EVENT_LENGTH;
                            }
                            break;
                        case CHECK_EVENT_LENGTH:
                            if (expectedDataSize > (restData.limit())) {
                                readState = ReadState.EVENT_NOT_COMPLETE;
                            } else {
                                readState = ReadState.EVENT_COMPLETE;
                            }
                            break;
                        case EVENT_NOT_COMPLETE:
                            addPayload(restData);
                            expectedDataSize -= restData.limit();
                            readState = ReadState.CHECK_EVENT_LENGTH;
                            logger.debug(
                                    "Partial event: {} {}", expectedEventType, expectedDataSize);
                            done = true;
                            break;
                        case EVENT_COMPLETE:
                            if (expectedDataSize <= 0) {
                                throw new IOException("Wrong event size: " + expectedDataSize);
                            }
                            ByteBuffer payload = restData.slice();
                            payload.limit(expectedDataSize);
                            addPayload(payload);
                            restData.position(restData.position() + expectedDataSize);
                            restData = restData.slice();

                            ByteBuffer[] bb = new ByteBuffer[0];
                            bb = receivedPayloads.toArray(bb);
                            logger.debug(
                                    "dispatching event handler for {} with {} buffers",
                                    expectedEventType,
                                    receivedPayloads.size());
                            eventHandler.handleEvent(expectedEventType, bb);
                            reset();
                            break;
                    }
                }
                completionStatus.setNumNeeded(expectedDataSize);
            }
        } catch (IOException e) {
            logger.error("Failed to decode event: ", e);
            // Reset message accumulator state.
            reset();
            throw e;
        }
    }
}
