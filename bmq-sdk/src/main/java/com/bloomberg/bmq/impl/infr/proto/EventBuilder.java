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

import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class EventBuilder {

    private EventHeader eventHeader;
    protected int msgCount;
    protected ByteBufferOutputStream bbos;

    protected EventBuilder(EventType type) {
        reset(type);
    }

    public void reset(EventType type) {
        eventHeader = new EventHeader();
        eventHeader.setType(type);
        msgCount = 0;
        bbos = ByteBufferOutputStream.smallBlocks();
    }

    public EventHeader header() {
        return eventHeader;
    }

    public ByteBuffer[] build() {
        if (0 == bbos.size()) {
            throw new IllegalStateException("Nothing to build.");
        }

        int payloadLen = bbos.size();
        ByteBuffer[] payload = bbos.peekUnflipped();

        eventHeader.setLength(EventHeader.HEADER_SIZE + payloadLen);

        ByteBufferOutputStream stream = ByteBufferOutputStream.smallBlocks();
        try {
            eventHeader.streamOut(stream);
        } catch (IOException ex) {
            // Should never happen
            throw new IllegalStateException(ex);
        }
        try {
            stream.writeBuffers(payload);
        } catch (IOException e) {
            // Should never happen
            throw new RuntimeException(e);
        }

        return stream.peek();
    }

    public int messageCount() {
        return msgCount;
    }

    public int eventLength() {
        return EventHeader.HEADER_SIZE + bbos.size();
    }
}
