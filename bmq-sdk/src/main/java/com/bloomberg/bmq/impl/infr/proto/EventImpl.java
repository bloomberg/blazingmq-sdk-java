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

import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.proto.intf.Streamable;
import com.bloomberg.bmq.impl.intf.SessionEventHandler;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class EventImpl {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final int eventLength;
    protected int messageCount;

    public abstract void dispatch(SessionEventHandler handler);

    private EventHeader eventHeader;
    protected ByteBufferInputStream blob;

    protected EventImpl(EventType type, ByteBuffer[] bbuf) {
        eventHeader = new EventHeader();
        blob = new ByteBufferInputStream(bbuf);
        eventLength = blob.available();
        try {
            eventHeader.streamIn(blob);
            if (eventHeader.type() != type) {
                logger.error("Unexpected header: {}", eventHeader);
            }
            assert eventHeader.type() == type;
        } catch (IOException e) {
            blob = null;
            logger.error("Failed to decode EventImpl Header: ", e);
        }
    }

    public final int eventLength() {
        return eventLength;
    }

    public final int messageCount() {
        return messageCount;
    }

    public final int available() {
        return blob.available();
    }

    public final void reset() throws IOException {
        blob.reset();
        blob.skip(EventHeader.HEADER_SIZE);
    }

    public final void setPosition(int pos) throws IOException {
        blob.reset();
        blob.skip(pos);
    }

    public final int streamIn(Streamable obj) throws IOException {
        obj.streamIn(blob);
        return blob.position();
    }

    public final boolean isValid() {
        return blob != null;
    }

    public final EventType type() {
        return eventHeader.type();
    }

    public final EventHeader header() {
        return eventHeader;
    }

    @Override
    public String toString() {
        return eventHeader.toString();
    }
}
