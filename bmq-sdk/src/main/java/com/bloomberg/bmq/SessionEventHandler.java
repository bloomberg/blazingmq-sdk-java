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

/** User callback interface to handle session related events. */
public interface SessionEventHandler {
    /**
     * User specified handler for session events.
     *
     * <p>Session user needs to implement this interface to receive different notification about
     * session state.
     *
     * @param event Session related event
     */
    void handleSessionEvent(SessionEvent event);

    default void handleStartStatusSessionEvent(SessionEvent.StartStatus event) {
        handleSessionEvent(event);
    }

    default void handleStopStatusSessionEvent(SessionEvent.StopStatus event) {
        handleSessionEvent(event);
    }

    default void handleConnectionLostSessionEvent(SessionEvent.ConnectionLost event) {
        handleSessionEvent(event);
    }

    default void handleReconnectedSessionEvent(SessionEvent.Reconnected event) {
        handleSessionEvent(event);
    }

    default void handleStateRestoredSessionEvent(SessionEvent.StateRestored event) {
        handleSessionEvent(event);
    }

    default void handleSlowConsumerHighWatermarkEvent(
            SessionEvent.SlowConsumerHighWatermark event) {
        handleSessionEvent(event);
    }

    default void handleSlowConsumerNormalEvent(SessionEvent.SlowConsumerNormal event) {
        handleSessionEvent(event);
    }

    default void handleHostUnhealthySessionEvent(SessionEvent.HostUnhealthy event) {
        handleSessionEvent(event);
    }

    default void handleHostHealthRestoredSessionEvent(SessionEvent.HostHealthRestored event) {
        handleSessionEvent(event);
    }
}
