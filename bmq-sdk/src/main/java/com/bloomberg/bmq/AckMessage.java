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
package com.bloomberg.bmq;

/**
 * ACK message representation.
 *
 * <p>ACK message is sent by the broker to the producer to confirm that it gets PUT message sent by
 * the producer. ACK message holds unique {@link com.bloomberg.bmq.MessageGUID} assigned by the
 * broker and {@link com.bloomberg.bmq.CorrelationId} that equals to the corresponding value from
 * the {@link com.bloomberg.bmq.PutMessage} and allows to identify related PUT message.
 */
public interface AckMessage extends QueueEvent {

    /**
     * Returns unique {@link com.bloomberg.bmq.MessageGUID} assigned to this message by the broker.
     *
     * @return MessageGUID unique message GUID
     */
    MessageGUID messageGUID();

    /**
     * Returns {@link com.bloomberg.bmq.CorrelationId} equals to the user specified correlation ID
     * from the {@link com.bloomberg.bmq.PutMessage}.
     *
     * @return CorrelationId message correlation ID
     */
    CorrelationId correlationId();

    /**
     * Returns the status of this message set by the broker.
     *
     * @return ResultCodes.AckResult message status
     * @see ResultCodes.AckResult
     */
    ResultCodes.AckResult status();
}
