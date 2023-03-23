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
package com.bloomberg.bmq.impl.events;

public interface BrokerSessionEventHandler {
    void handleConnected(BrokerSessionEvent event);

    void handleDisconnected(BrokerSessionEvent event);

    void handleConnectionLost(BrokerSessionEvent event);

    void handleReconnected(BrokerSessionEvent event);

    void handleStateRestored(BrokerSessionEvent event);

    void handleConnectionTimeout(BrokerSessionEvent event);

    void handleSlowConsumerNormal(BrokerSessionEvent event);

    void handleSlowConsumerHighWatermark(BrokerSessionEvent event);

    void handleConnectionInProgress(BrokerSessionEvent event);

    void handleDisconnectionTimeout(BrokerSessionEvent event);

    void handleDisconnectionInProgress(BrokerSessionEvent event);

    void handleHostUnhealthy(BrokerSessionEvent event);

    void handleHostHealthRestored(BrokerSessionEvent event);

    void handleCancelled(BrokerSessionEvent event);

    void handleError(BrokerSessionEvent event);
}
