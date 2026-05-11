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

import com.bloomberg.bmq.impl.intf.SessionEventHandler;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class AckEventImpl extends EventImpl {

    final AckHeader header;
    final Collection<AckMessageImpl> messages = new ArrayList<>();

    @SuppressWarnings(
            "this-escape") // passing `this` to `AckMessageIterator` is necessary to fully construct
    // `this`
    public AckEventImpl(ByteBuffer[] bbuf) {
        super(EventType.ACK, bbuf);
        AckMessageIterator it = new AckMessageIterator(this);
        header = it.header();
        while (it.hasNext()) {
            messages.add(it.next());
        }

        messageCount = messages.size();
    }

    public AckHeader ackHeader() {
        return header;
    }

    @Override
    public void dispatch(SessionEventHandler handler) {
        for (AckMessageImpl m : messages) {
            handler.handleAckMessage(m);
        }
    }

    public Iterator<AckMessageImpl> iterator() {
        return messages.iterator();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[AckEvent ").append(super.toString()).append(" ");
        Iterator<AckMessageImpl> it = iterator();
        while (it.hasNext()) {
            sb.append(it.next().toString());
        }
        sb.append(" ]");
        return sb.toString();
    }
}
