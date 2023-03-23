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

import javax.annotation.concurrent.Immutable;

/** Results of the BMQ event creation operation. */
@Immutable
public enum EventBuilderResult {
    /** The event is created successfully. */
    SUCCESS(0),

    /** The event creation failed due to unknown error. */
    UNKNOWN(-1),

    /** The event creation failed due to invalid queue state. Not used. */
    QUEUE_INVALID(-100),

    /** The event creation failed due to the queue is not writable. Not used. */
    QUEUE_READONLY(-101),

    /**
     * The event creation failed due to the absence of the {@link com.bloomberg.bmq.CorrelationId}
     * in the {@link com.bloomberg.bmq.PutMessage} being added into the {@code PUT} event if this
     * message requests to be confirmed by the broker.
     */
    MISSING_CORRELATION_ID(-102),

    /** The event creation failed due to the size of this event exceeded the maximum event size. */
    EVENT_TOO_BIG(-103),

    /**
     * The event creation failed due to the size of the message being added to this event exceeded
     * maximum message size.
     */
    PAYLOAD_TOO_BIG(-104),

    /**
     * The event creation failed due to the the message being added to this event had zero payload
     * (relevant to {@code PUT} event).
     */
    PAYLOAD_EMPTY(-105),

    /**
     * The event creation failed due to the the message being added to this event exceeded maximum
     * message option size. Not used.
     */
    OPTION_TOO_BIG(-106),

    /**
     * The event creation failed due to the the message being added to this event had wrong message
     * group ID. Not used.
     */
    INVALID_MSG_GROUP_ID(-107),

    /** The event creation failed due to the queue is in suspended state. Not used. */
    QUEUE_SUSPENDED(-108);

    private final int id;

    EventBuilderResult(int id) {
        this.id = id;
    }

    /**
     * Returns integer representation of this result code.
     *
     * @return int result code as integer
     */
    public int toInt() {
        return id;
    }

    /**
     * Returns result code objec that matches specified integer value.
     *
     * @param i result code as integer value
     * @return EventBuilderResult result code, or null for unknown integer value
     */
    public static EventBuilderResult fromInt(int i) {
        for (EventBuilderResult r : EventBuilderResult.values()) {
            if (r.toInt() == i) return r;
        }
        return null;
    }
}
