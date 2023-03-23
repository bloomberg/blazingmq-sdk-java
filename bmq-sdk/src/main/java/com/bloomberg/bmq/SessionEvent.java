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

import com.bloomberg.bmq.ResultCodes.GenericResult;

/** Represents a BlazingMQ event related to the whole session state. */
public interface SessionEvent extends Event {

    /** Session event types. */
    enum Type {
        /**
         * Session start status event. Reports result of the {@link
         * com.bloomberg.bmq.AbstractSession#startAsync} operation.
         */
        START_STATUS_SESSION_EVENT,

        /**
         * Session stop status event. Reports result of the {@link
         * com.bloomberg.bmq.AbstractSession#stopAsync} operation.
         */
        STOP_STATUS_SESSION_EVENT,

        /** Session event generated in case of the broker connection is lost. */
        CONNECTION_LOST_SESSION_EVENT,

        /** Session event generated in case of the broker connection is restored. */
        RECONNECTED_SESSION_EVENT,

        /**
         * Session event generated after the broker connection is restored and all of the opened
         * queues are in a valid state.
         */
        STATE_RESTORED_SESSION_EVENT,

        /**
         * Session event generated in case of the inbound event buffer size has exceeded user
         * defined high watermark value.
         */
        SLOW_CONSUMER_HIGH_WATERMARK_SESSION_EVENT,

        /**
         * Session event generated in case of the inbound event buffer size has decreased and
         * returned to user defined low watermark value.
         */
        SLOW_CONSUMER_NORMAL_SESSION_EVENT,

        /**
         * Session event generated in case the host has become unhealthy. Only issued if a {@link
         * com.bloomberg.bmq.HostHealthMonitor} has been installed to the session, via the {@link
         * com.bloomberg.bmq.SessionOptions} object.
         */
        HOST_UNHEALTHY_SESSION_EVENT,

        /**
         * Session event generated in case the health of the host has restored, and all queues have
         * resumed operation. Following a Host Health incident, this indicates that the application
         * has resumed normal operation.
         */
        HOST_HEALTH_RESTORED_SESSION_EVENT,

        /** Unknown event. */
        UNKNOWN_SESSION_EVENT
    }

    /** Session start status event. */
    interface StartStatus extends SessionEvent {
        /**
         * Returns event type, {@link Type#START_STATUS_SESSION_EVENT} by default
         *
         * @return Type always {@link Type#START_STATUS_SESSION_EVENT}
         */
        @Override
        default Type type() {
            return Type.START_STATUS_SESSION_EVENT;
        }

        /**
         * Returns a result of the {@link com.bloomberg.bmq.AbstractSession#startAsync} operation.
         *
         * @return GenericResult session asynchronous start operation result
         */
        GenericResult result();
    }

    /** Session stop status event. */
    interface StopStatus extends SessionEvent {
        /**
         * Returns event type, {@link Type#STOP_STATUS_SESSION_EVENT} by default
         *
         * @return Type always {@link Type#STOP_STATUS_SESSION_EVENT}
         */
        @Override
        default Type type() {
            return Type.STOP_STATUS_SESSION_EVENT;
        }

        /**
         * Returns a result of the {@link com.bloomberg.bmq.AbstractSession#stopAsync} operation.
         *
         * @return GenericResult session asynchronous stop operation result
         */
        GenericResult result();
    }

    /** Event that notifies the session has lost the broker connection. */
    interface ConnectionLost extends SessionEvent {
        /**
         * Returns event type, {@link Type#CONNECTION_LOST_SESSION_EVENT} by default
         *
         * @return Type always {@link Type#CONNECTION_LOST_SESSION_EVENT}
         */
        @Override
        default Type type() {
            return Type.CONNECTION_LOST_SESSION_EVENT;
        }
    }

    /** Event that notifies the session has restored the broker connection. */
    interface Reconnected extends SessionEvent {
        /**
         * Returns event type, {@link Type#RECONNECTED_SESSION_EVENT} by default
         *
         * @return Type always {@link Type#RECONNECTED_SESSION_EVENT}
         */
        @Override
        default Type type() {
            return Type.RECONNECTED_SESSION_EVENT;
        }
    }

    /** Event that notifies the session has restored the state of all opened queues. */
    interface StateRestored extends SessionEvent {
        /**
         * Returns event type, {@link Type#STATE_RESTORED_SESSION_EVENT} by default
         *
         * @return Type always {@link Type#STATE_RESTORED_SESSION_EVENT}
         */
        @Override
        default Type type() {
            return Type.STATE_RESTORED_SESSION_EVENT;
        }
    }

    /** Event that notifies the inbound event buffer has reached the high watermark value. */
    interface SlowConsumerHighWatermark extends SessionEvent {
        /**
         * Returns event type, {@link Type#SLOW_CONSUMER_HIGH_WATERMARK_SESSION_EVENT} by default
         *
         * @return Type always {@link Type#SLOW_CONSUMER_HIGH_WATERMARK_SESSION_EVENT}
         */
        @Override
        default Type type() {
            return Type.SLOW_CONSUMER_HIGH_WATERMARK_SESSION_EVENT;
        }
    }

    /** Event that notifies the inbound event buffer has reached the low watermark value. */
    interface SlowConsumerNormal extends SessionEvent {
        /**
         * Returns event type, {@link Type#SLOW_CONSUMER_NORMAL_SESSION_EVENT} by default
         *
         * @return Type always {@link Type#SLOW_CONSUMER_NORMAL_SESSION_EVENT}
         */
        @Override
        default Type type() {
            return Type.SLOW_CONSUMER_NORMAL_SESSION_EVENT;
        }
    }

    /** Event that notifies the host has become unhealthy. */
    interface HostUnhealthy extends SessionEvent {
        /**
         * Returns event type, {@link Type#HOST_UNHEALTHY_SESSION_EVENT} by default
         *
         * @return Type always {@link Type#HOST_UNHEALTHY_SESSION_EVENT}
         */
        @Override
        default Type type() {
            return Type.HOST_UNHEALTHY_SESSION_EVENT;
        }
    }

    /** Event that notifies the health of the host has restored. */
    interface HostHealthRestored extends SessionEvent {
        /**
         * Returns event type, {@link Type#HOST_HEALTH_RESTORED_SESSION_EVENT} by default
         *
         * @return Type always {@link Type#HOST_HEALTH_RESTORED_SESSION_EVENT}
         */
        @Override
        default Type type() {
            return Type.HOST_HEALTH_RESTORED_SESSION_EVENT;
        }
    }

    /**
     * Returns event type.
     *
     * @return Type the type of this event
     */
    Type type();
}
