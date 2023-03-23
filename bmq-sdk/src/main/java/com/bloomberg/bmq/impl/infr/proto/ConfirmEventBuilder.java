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

public class ConfirmEventBuilder extends EventBuilder {

    public static final int MAX_MSG_COUNT =
            (EventHeader.MAX_SIZE_SOFT - EventHeader.HEADER_SIZE - ConfirmHeader.HEADER_SIZE)
                    / ConfirmMessage.MESSAGE_SIZE;

    public ConfirmEventBuilder() {
        super(EventType.CONFIRM);
    }

    public void reset() {
        super.reset(EventType.CONFIRM);
    }

    public EventBuilderResult packMessage(ConfirmMessage msg) throws IOException {
        if (msgCount == MAX_MSG_COUNT) {
            return EventBuilderResult.EVENT_TOO_BIG; // RETURN
        }

        if (msgCount == 0) {
            ConfirmHeader h = new ConfirmHeader();
            h.streamOut(bbos);
        }

        msg.streamOut(bbos);
        ++msgCount;

        return EventBuilderResult.SUCCESS;
    }
}
