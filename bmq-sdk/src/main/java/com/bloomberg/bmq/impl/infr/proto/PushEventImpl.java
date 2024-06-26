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

public class PushEventImpl extends EventImpl {

    public PushEventImpl(ByteBuffer[] bbuf) {
        super(EventType.PUSH, bbuf);
    }

    @Override
    public void dispatch(SessionEventHandler handler) {
        messageCount = 0;

        for (PushMessageIterator it = iterator(); it.hasNext(); ) {
            messageCount++;
            handler.handlePushMessage(it.next());
        }
    }

    public PushMessageIterator iterator() {
        return new PushMessageIterator(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ PushEvent ").append(super.toString()).append(" ");
        PushMessageIterator it = iterator();
        while (it.hasNext()) {
            sb.append(it.next().toString());
        }
        sb.append(" ]");
        return sb.toString();
    }
}
