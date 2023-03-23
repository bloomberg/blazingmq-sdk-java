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

import java.io.IOException;

public class AckEventBuilder extends EventBuilder {

    public static final int MAX_MSG_COUNT =
            (EventHeader.MAX_SIZE_SOFT - EventHeader.HEADER_SIZE - AckHeader.HEADER_SIZE)
                    / AckMessageImpl.MESSAGE_SIZE;

    public AckEventBuilder() {
        super(EventType.ACK);
    }

    public void reset() {
        super.reset(EventType.ACK);
    }

    public EventBuilderResult packMessage(AckMessageImpl msg) throws IOException {
        if (msgCount == MAX_MSG_COUNT) {
            return EventBuilderResult.EVENT_TOO_BIG; // RETURN
        }

        if (msgCount == 0) {
            AckHeader ah = new AckHeader();
            ah.streamOut(bbos);
        }

        msg.streamOut(bbos);
        ++msgCount;

        return EventBuilderResult.SUCCESS;
    }
}
