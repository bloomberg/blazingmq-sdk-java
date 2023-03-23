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

import com.bloomberg.bmq.impl.infr.util.Argument;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PutEventBuilder extends EventBuilder {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private int maxEventSize = EventHeader.MAX_SIZE_SOFT;
    private int maxPayloadSize = PutHeader.MAX_PAYLOAD_SIZE_SOFT;

    public PutEventBuilder() {
        super(EventType.PUT);
    }

    public void reset() {
        super.reset(EventType.PUT);
    }

    public void setMaxEventSize(int value) {
        maxEventSize =
                Argument.expectNotGreater(value, EventHeader.MAX_SIZE_SOFT, "max event size");

        if (maxPayloadSize >= maxEventSize) {
            // set max payload size less than max event size
            // in order to be able to pack at least one message
            maxPayloadSize = value - PutHeader.HEADER_SIZE - 4;
        }

        if (maxPayloadSize <= 0) {
            logger.error(
                    "Bad max payload size. Max payload size: {}. Max event size: {}.",
                    maxPayloadSize,
                    maxEventSize);
            throw new IllegalStateException("Invalid max payload size");
        }
    }

    // TODO: remove boolean after 2nd release of "new style" brokers
    public EventBuilderResult packMessage(PutMessageImpl msg, boolean isOldStyleProperties)
            throws IOException {
        // Validate payload is empty
        if (msg.appData().payloadSize() == 0) {
            return EventBuilderResult.PAYLOAD_EMPTY; // RETURN
        }

        msg.appData().setIsOldStyleProperties(isOldStyleProperties);

        // Compress data
        msg.compressData();
        msg.calculateAndSetCrc32c();

        final int appDataLength = msg.appData().unpackedSize();

        // Validate payload is not too big
        if (appDataLength > maxPayloadSize) {
            logger.error(
                    "Payload too big. Max payload size: {}. Max event size: {}.",
                    maxPayloadSize,
                    maxEventSize);
            return EventBuilderResult.PAYLOAD_TOO_BIG; // RETURN
        }

        if (PutHeaderFlags.isSet(msg.flags(), PutHeaderFlags.ACK_REQUESTED)
                && (msg.correlationId() == null)) {
            return EventBuilderResult.MISSING_CORRELATION_ID; // RETURN
        }

        final int numPaddingBytes = msg.appData().numPaddingBytes();
        final int sizeNoOptions =
                bbos.size() + PutHeader.HEADER_SIZE + appDataLength + numPaddingBytes;

        if (sizeNoOptions > maxEventSize) {
            logger.error(
                    "Event too big. Max payload size: {}. Max event size: {}.",
                    maxPayloadSize,
                    maxEventSize);
            return EventBuilderResult.EVENT_TOO_BIG; // RETURN
        }

        msg.streamOut(bbos);

        ++msgCount;

        return EventBuilderResult.SUCCESS;
    }
}
