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
package com.bloomberg.bmq.it.util;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Implemented by {@code BmqBrokerSimulator} and extened by {@code BmqBroker} interface.
 *
 * <p>Extends {@code AutoCloseable} and can be used in try-with-resources statement e.g.
 *
 * <pre>
 * try {TestTcpServer server = getServer()) {
 *     // Create session and connect to the server
 *     // Send and receive messages
 *     // Close the session
 * }
 * </pre>
 *
 * The server will be stopped by calling its `close()' method
 */
public interface TestTcpServer extends AutoCloseable {

    void start();

    void stop();

    void enableRead();

    void disableRead();

    // TODO: remove after 2nd rollout of "new style" brokers
    default boolean isOldStyleMessageProperties() {
        return false;
    }

    default CompletableFuture<Void> startAsync() {
        return CompletableFuture.runAsync(this::start);
    }

    default CompletableFuture<Void> stopAsync() {
        return CompletableFuture.runAsync(this::stop);
    }

    default void close() throws IOException {
        stop();
    }
}
