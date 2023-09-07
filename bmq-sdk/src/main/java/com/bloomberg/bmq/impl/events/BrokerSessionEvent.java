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

import com.bloomberg.bmq.impl.infr.util.UniqId;

public final class BrokerSessionEvent extends SessionEvent<BrokerSessionEvent.Type> {
    private interface Dispatcher {
        void handle(BrokerSessionEventHandler handler, BrokerSessionEvent event);
    }

    public enum Type {
        e_CONNECTED(BrokerSessionEventHandler::handleConnected),
        e_DISCONNECTED(BrokerSessionEventHandler::handleDisconnected),
        e_CONNECTION_LOST(BrokerSessionEventHandler::handleConnectionLost),
        e_RECONNECTED(BrokerSessionEventHandler::handleReconnected),
        e_STATE_RESTORED(BrokerSessionEventHandler::handleStateRestored),
        e_CONNECTION_TIMEOUT(BrokerSessionEventHandler::handleConnectionTimeout),
        e_CONNECTION_IN_PROGRESS(BrokerSessionEventHandler::handleConnectionInProgress),
        e_DISCONNECTION_TIMEOUT(BrokerSessionEventHandler::handleDisconnectionTimeout),
        e_DISCONNECTION_IN_PROGRESS(BrokerSessionEventHandler::handleDisconnectionInProgress),
        e_SLOWCONSUMER_NORMAL(BrokerSessionEventHandler::handleSlowConsumerNormal),
        e_SLOWCONSUMER_HIGHWATERMARK(BrokerSessionEventHandler::handleSlowConsumerHighWatermark),
        e_HOST_UNHEALTHY(BrokerSessionEventHandler::handleHostUnhealthy),
        e_HOST_HEALTH_RESTORED(BrokerSessionEventHandler::handleHostHealthRestored),
        e_ERROR(BrokerSessionEventHandler::handleError),
        e_CANCELED(BrokerSessionEventHandler::handleCancelled);

        private final Dispatcher dispatcher;

        public void dispatch(BrokerSessionEventHandler handler, BrokerSessionEvent event) {
            dispatcher.handle(handler, event);
        }

        Type(Dispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }
    }

    private static final int TYPE_ID = UniqId.getNumber();

    public final void dispatch(BrokerSessionEventHandler handler) {
        getEventType().dispatch(handler, this);
    }

    @Override
    public int getDispatchId() {
        return TYPE_ID;
    }

    private BrokerSessionEvent(Type sessionEventType, String errorDescription) {
        super(sessionEventType, errorDescription);
    }

    public static BrokerSessionEvent createInstance(Type sessionEventType) {
        return createInstance(sessionEventType, "");
    }

    public static BrokerSessionEvent createInstance(
            Type sessionEventType, String errorDescription) {
        return new BrokerSessionEvent(sessionEventType, errorDescription);
    }

    @Override
    public void dispatch(EventHandler handler) {
        handler.handleBrokerSessionEvent(this);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("BrokerSessionEvent [type: ")
                .append(getEventType())
                .append(", error_desc: '")
                .append(getErrorDescription())
                .append("']");
        return builder.toString();
    }
}
