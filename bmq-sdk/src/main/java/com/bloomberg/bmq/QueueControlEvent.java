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

/**
 * Represents the queue related event that reports a status of asynchronous operation usually
 * initiated by the user.
 */
public interface QueueControlEvent extends QueueEvent {
    enum Type {
        QUEUE_OPEN_RESULT,
        QUEUE_REOPEN_RESULT,
        QUEUE_CLOSE_RESULT,
        QUEUE_CONFIGURE_RESULT,
        QUEUE_SUSPENDED,
        QUEUE_RESUMED
    }

    /**
     * Returns a result of the asynchronous operation.
     *
     * @return GenericResult queue asynchronous operation result
     */
    ResultCodes.GenericCode result();

    /**
     * Returns event type.
     *
     * @return Type the type of this event
     */
    Type type();

    /** Open queue status event. */
    interface OpenQueueResult extends QueueControlEvent {
        /**
         * Returns event type, {@link Type#QUEUE_OPEN_RESULT} by default
         *
         * @return Type always {@link Type#QUEUE_OPEN_RESULT}
         */
        @Override
        default Type type() {
            return Type.QUEUE_OPEN_RESULT;
        }

        /**
         * Returns a result of the {@link com.bloomberg.bmq.Queue#openAsync} operation.
         *
         * @return ResultCodes.OpenQueueResult queue asynchronous open operation result
         */
        @Override
        ResultCodes.OpenQueueResult result();
    }

    /** Reopen queue status event. */
    interface ReopenQueueResult extends QueueControlEvent {
        /**
         * Returns event type, {@link Type#QUEUE_REOPEN_RESULT} by default
         *
         * @return Type always {@link Type#QUEUE_REOPEN_RESULT}
         */
        @Override
        default Type type() {
            return Type.QUEUE_REOPEN_RESULT;
        }

        /**
         * Returns a result of the queue reopen operation, usually initiated by the SDK.
         *
         * @return ResultCodes.OpenQueueResult queue asynchronous reopen operation result
         */
        @Override
        ResultCodes.OpenQueueResult result();
    }

    /** Close queue status event. */
    interface CloseQueueResult extends QueueControlEvent {
        /**
         * Returns event type, {@link Type#QUEUE_CLOSE_RESULT} by default
         *
         * @return Type always {@link Type#QUEUE_CLOSE_RESULT}
         */
        @Override
        default Type type() {
            return Type.QUEUE_CLOSE_RESULT;
        }

        /**
         * Returns a result of the {@link com.bloomberg.bmq.Queue#closeAsync} operation.
         *
         * @return ResultCodes.CloseQueueResult queue asynchronous close operation result
         */
        @Override
        ResultCodes.CloseQueueResult result();
    }

    /** Configure queue status event. */
    interface ConfigureQueueResult extends QueueControlEvent {
        /**
         * Returns event type, {@link Type#QUEUE_CONFIGURE_RESULT} by default
         *
         * @return Type always {@link Type#QUEUE_CONFIGURE_RESULT}
         */
        @Override
        default Type type() {
            return Type.QUEUE_CONFIGURE_RESULT;
        }

        /**
         * Returns a result of the {@link com.bloomberg.bmq.Queue#configureAsync} operation.
         *
         * @return ResultCodes.ConfigureQueueResult queue asynchronous configure operation result
         */
        @Override
        ResultCodes.ConfigureQueueResult result();
    }

    /** Suspend queue status event. */
    interface SuspendQueueResult extends QueueControlEvent {
        /**
         * Returns event type, {@link Type#QUEUE_SUSPENDED} by default
         *
         * @return Type always {@link Type#QUEUE_SUSPENDED}
         */
        @Override
        default Type type() {
            return Type.QUEUE_SUSPENDED;
        }

        /**
         * Returns a result of the queue suspension.
         *
         * @return ResultCodes.ConfigureQueueResult queue asynchronous configure operation result
         */
        @Override
        ResultCodes.ConfigureQueueResult result();
    }

    /** Resume queue status event. */
    interface ResumeQueueResult extends QueueControlEvent {
        /**
         * Returns event type, {@link Type#QUEUE_RESUMED} by default
         *
         * @return Type always {@link Type#QUEUE_RESUMED}
         */
        @Override
        default Type type() {
            return Type.QUEUE_RESUMED;
        }

        /**
         * Returns a result of the queue suspension.
         *
         * @return ResultCodes.ConfigureQueueResult queue asynchronous configure operation result
         */
        @Override
        ResultCodes.ConfigureQueueResult result();
    }
}
