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

import java.time.Duration;

/** Interface to the Session that provides access to the BlazingMQ broker */
public interface AbstractSession {

    /**
     * Connect to the BlazingMQ broker and start the message processing
     *
     * <p>This method blocks until either the 'Session' is connected to the broker, fails to
     * connect, or the operation times out.
     *
     * @param timeout start timeout value
     * @throws BMQException if start attempt failed
     */
    void start(Duration timeout);

    /**
     * Connect to the BlazingMQ broker and start the message processing
     *
     * <p>This method returns without blocking. The result of the operation is communicated with a
     * session event. If the optionally specified 'timeout' is not populated, use the one defined in
     * the session options.
     *
     * @param timeout start timeout value
     * @throws BMQException in case of failure
     */
    void startAsync(Duration timeout);

    /**
     * Check if the 'Session' is started
     *
     * @return true if the 'Session' is started, otherwise false
     */
    boolean isStarted();

    /**
     * Gracefully disconnect from the BlazingMQ broker and stop the operation of this 'Session'.
     *
     * <p>This method blocks waiting for all already invoked event handlers to exit and all
     * session-related operations to be finished.
     *
     * @param timeout stop timeout value
     * @throws BMQException if stop attempt failed
     */
    void stop(Duration timeout);

    /**
     * Gracefully disconnect from the BlazingMQ broker and stop the operation of this 'Session'.
     *
     * <p>This method returns without blocking. The result of the operation is communicated with a
     * session event. If the optionally specified 'timeout' is not populated, use the one defined in
     * the session options.
     *
     * @param timeout stop timeout value
     * @throws BMQException in case of failure
     */
    void stopAsync(Duration timeout);

    /**
     * Shutdown all connection and event handling threads.
     *
     * <p>This method must be called when the {@code Session} is stopped. No other method may be
     * used after this method returns.
     */
    void linger();

    /**
     * Create a queue representation.
     *
     * <p>Returned {@link com.bloomberg.bmq.Queue} may be in any state.
     *
     * @param uri URI of the created {@code Queue}, immutable
     * @param flags a combination of the values defined in {@link com.bloomberg.bmq.QueueFlags}
     * @param handler queue event handler callback interface
     * @param ackHandler callback handler for incoming ACK events, can be null if only consumer
     * @param pushHandler callback handler for incoming PUSH events, can be null if only producer
     * @return Queue interface to the queue instance
     * @throws IllegalArgumentException in case of a wrong combination of the queue flags and the
     *     handlers
     * @throws BMQException for any other unrecoverable errors
     * @see com.bloomberg.bmq.QueueFlags
     */
    Queue getQueue(
            Uri uri,
            long flags,
            QueueEventHandler handler,
            AckMessageHandler ackHandler,
            PushMessageHandler pushHandler);
}
