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

/** PUT message representation. */
public interface PutMessage extends QueueEvent {
    /**
     * Sets and returns a new {@code CorrelationId} that contains an auto-assigned integer value
     * starting from 1 and up to {@code 0xFFFFFF}.
     *
     * @return CorrelationId auto-generated correlation ID unique within process
     */
    CorrelationId setCorrelationId();

    /**
     * Sets and returns a new {@code CorrelationId} that binds an auto-assigned integer value
     * starting from 1 and up to {@code 0xFFFFFF} and the user specified {@code Object}. The user
     * can expect that a valid {@code CorrelationId} provided to the {@code PutMessage} and the
     * {@code CorrelationId} obtained from the corresponding {@code AckMessage} will hold the same
     * {@code Object} available via {@link CorrelationId#userData} accessor.
     *
     * @param obj user specified object
     * @return CorrelationId auto-generated correlation ID that stores the user specified object
     */
    CorrelationId setCorrelationId(Object obj);

    /**
     * Get correlation Id of the message
     *
     * @return correlation Id
     */
    CorrelationId correlationId();

    /**
     * Get reference to the message properties object.
     *
     * @return MessageProperties
     */
    MessageProperties messageProperties();

    /**
     * Returns an interface to the queue that this message is related to.
     *
     * @return Queue queue interface
     */
    Queue queue();

    /**
     * Sets message payload compression algorithm. See {@code CompressionAlgorithm} for available
     * options
     *
     * @param algorithm payload compression algorithm
     */
    void setCompressionAlgorithm(CompressionAlgorithm algorithm);
}
