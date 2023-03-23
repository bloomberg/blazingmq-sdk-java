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
package com.bloomberg.bmq.impl.intf;

import com.bloomberg.bmq.ResultCodes.GenericResult;
import java.nio.ByteBuffer;
import java.time.Duration;

public interface BrokerConnection {
    // Internal interface representing a connection with the BlazingMQ broker.

    enum StartStatus {
        NOT_SUPPORTED,
        SUCCESS,
        CONNECT_FAILURE,
        NEGOTIATION_FAILURE,
        TIMEOUT,
        CANCELLED;
    }

    enum StopStatus {
        NOT_CONNECTED,
        SUCCESS,
        FAILURE;
    }

    interface StartCallback {
        void handleStartCb(StartStatus status);
    }

    interface StopCallback {
        void handleStopCb(StopStatus status);
    }

    void start(StartCallback startCb);

    boolean isStarted();

    void stop(StopCallback stopCb, Duration duration);

    void drop();

    GenericResult write(ByteBuffer[] buffers, boolean waitUntilWritable);

    GenericResult linger();

    // TODO: remove after 2nd release of "new style" brokers
    boolean isOldStyleMessageProperties();
}
