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
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushEventBuilder extends EventBuilder {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public PushEventBuilder() {
        super(EventType.PUSH);
    }

    public void reset() {
        super.reset(EventType.PUSH);
    }

    // TODO: remove boolean after 2nd release of "new style" brokers
    // TODO: move to test code
    public EventBuilderResult packMessage(PushMessageImpl msg, boolean isOldStyleProperties)
            throws IOException {
        // Warn if payload is empty
        if (msg.appData().payloadSize() == 0) {
            logger.warn("PUSH message payload is empty");
        }

        msg.appData().setIsOldStyleProperties(isOldStyleProperties);

        // Compress data
        msg.compressData();

        int appDataLength = msg.appData().unpackedSize();

        // Validate payload is not too big
        if (appDataLength > PushHeader.MAX_PAYLOAD_SIZE_SOFT) {
            return EventBuilderResult.PAYLOAD_TOO_BIG; // RETURN
        }

        int numPaddingBytes = msg.appData().numPaddingBytes();

        final int sizeNoOptions =
                bbos.size()
                        + PushHeader.HEADER_SIZE_FOR_SCHEMA_ID
                        + appDataLength
                        + numPaddingBytes;

        if (sizeNoOptions > EventHeader.MAX_SIZE_SOFT) {
            return EventBuilderResult.EVENT_TOO_BIG; // RETURN
        }

        msg.streamOut(bbos);

        ++msgCount;

        return EventBuilderResult.SUCCESS;
    }
}
