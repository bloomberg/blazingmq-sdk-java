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

public interface SessionStatusHandler {
    // Internal interface to handle session status events.

    enum SessionStatus {
        SESSION_UP,
        SESSION_DOWN;
    }

    void handleSessionStatus(SessionStatus status);
    // Process the session 'status'.  Status can be either 'UP' or 'DOWN'.
    // 'UP' is emitted when channel has been established *and* negotiation
    // is complete.  'DOWN' is emitted when the channel goes down
    // ungracefully or upon receiving 'DisconnectResponse' from the broker.
    //
    // Session status events are fired by `BrokerConnection`.
}
