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

import java.nio.ByteBuffer;
import java.time.Duration;

/** SDK Queue representation. */
public interface Queue {

    /**
     * Opens the queue synchronously.
     *
     * <p>This operation will block until either success, failure, or timing out happens.
     *
     * @param options the options to configure some advanced settings.
     * @param timeout the operation timeout.
     * @throws BMQException in case of failure.
     * @see com.bloomberg.bmq.QueueOptions
     */
    void open(QueueOptions options, Duration timeout);

    /**
     * Opens the queue asynchronously.
     *
     * <p>This operation will post open queue request and return immediately. Success, failure, or
     * timing out will be reported through {@link com.bloomberg.bmq.QueueEventHandler}
     *
     * @param options the options to configure some advanced settings
     * @param timeout the operation timeout
     * @throws BMQException in case of failure
     * @see com.bloomberg.bmq.QueueOptions
     */
    void openAsync(QueueOptions options, Duration timeout);

    /**
     * Configures the queue synchronously.
     *
     * <p>Fields from 'options' that have not been explicitly set will not be modified.
     *
     * <p>This operation will block until either success, failure, or timing out happens.
     *
     * @param options options to configure some advanced settings
     * @param timeout operation timeout
     * @throws BMQException in case of failure
     * @see com.bloomberg.bmq.QueueOptions
     */
    void configure(QueueOptions options, Duration timeout);

    /**
     * Opens the queue asynchronously.
     *
     * <p>Fields from 'options' that have not been explicitly set will not be modified.
     *
     * <p>This operation will post configure queue request and return immediately. Success, failure,
     * or timing out will be reported through {@link com.bloomberg.bmq.QueueEventHandler}
     *
     * @param options options to configure some advanced settings
     * @param timeout operation timeout
     * @throws BMQException in case of failure
     * @see com.bloomberg.bmq.QueueOptions
     */
    void configureAsync(QueueOptions options, Duration timeout);

    /**
     * Closes the queue synchronously.
     *
     * <p>This operation will block until either success, failure, or timing out happens.
     *
     * @param timeout operation timeout
     * @throws BMQException in case of failure
     */
    void close(Duration timeout);

    /**
     * Closes the queue asynchronously.
     *
     * <p>This operation will post close queue request and return immediately. Success, failure, or
     * timing out will be reported through {@link com.bloomberg.bmq.QueueEventHandler}
     *
     * @param timeout operation timeout
     * @throws BMQException in case of failure
     */
    void closeAsync(Duration timeout);

    /**
     * Creates a PUT message object bounded to this queue with the specified payload
     *
     * @param payload this message payload
     * @return {@link com.bloomberg.bmq.PutMessage} that can be sent via this queue
     * @throws BMQException in case of failure while setting the payload
     * @throws RuntimeException in case of other failures
     */
    PutMessage createPutMessage(ByteBuffer... payload);

    /**
     * Sends a single PUT message to this queue.
     *
     * @param message PUT message to be sent
     * @throws BMQException in case of failure
     */
    void post(PutMessage message);

    /**
     * Adds a single PUT message to be sent withing multi message PUT event.
     *
     * @param message PUT message to add
     * @throws BMQException in case of failure
     */
    void pack(PutMessage message);

    /**
     * Sends previously packed PUT messages in one multi message PUT event.
     *
     * @throws BMQException in case of failure
     */
    void flush();

    /**
     * Gets queue URI.
     *
     * @return {@link com.bloomberg.bmq.Uri} of this queue
     */
    Uri uri();

    /**
     * Checks if the queue is opened.
     *
     * @return true if the queue is opened, otherwise false
     */
    boolean isOpen();
}
