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

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ResultCodes {

    @Immutable
    public enum GenericResult implements GenericCode {
        SUCCESS,
        UNKNOWN,
        TIMEOUT,
        NOT_CONNECTED,
        CANCELED,
        NOT_SUPPORTED,
        REFUSED,
        INVALID_ARGUMENT,
        NOT_READY,
        OPEN_QUEUE_RESULT,
        CONFIGURE_QUEUE_RESULT,
        CLOSE_QUEUE_RESULT,
        ACK_RESULT;

        @Override
        public GenericResult getGeneralResult() {
            return this;
        }
    }

    @Immutable
    public enum OpenQueueResult implements OpenQueueCode {
        SUCCESS(GenericResult.SUCCESS),
        UNKNOWN(GenericResult.UNKNOWN),
        TIMEOUT(GenericResult.TIMEOUT),
        NOT_CONNECTED(GenericResult.NOT_CONNECTED),
        CANCELED(GenericResult.CANCELED),
        NOT_SUPPORTED(GenericResult.NOT_SUPPORTED),
        REFUSED(GenericResult.REFUSED),
        INVALID_ARGUMENT(GenericResult.INVALID_ARGUMENT),
        NOT_READY(GenericResult.NOT_READY),
        // WARNINGS
        ALREADY_OPENED(GenericResult.OPEN_QUEUE_RESULT), // The queue is already opened
        ALREADY_IN_PROGRESS(GenericResult.OPEN_QUEUE_RESULT), // The queue is already being opened
        // ERRORS
        INVALID_URI(GenericResult.OPEN_QUEUE_RESULT), // The queue uri is invalid
        INVALID_FLAGS(GenericResult.OPEN_QUEUE_RESULT), // The flags provided are invalid
        QUEUE_ID_NOT_UNIQUE(GenericResult.OPEN_QUEUE_RESULT); // The correlationdId is not unique

        private final GenericResult genericResult;

        OpenQueueResult(GenericResult genericResult) {
            this.genericResult = genericResult;
        }

        @Override
        public GenericResult getGeneralResult() {
            return genericResult;
        }

        @Override
        public OpenQueueResult getOperationResult() {
            return this;
        }

        @Nonnull
        public static OpenQueueResult upcast(GenericCode code) {
            if (code.getClass().equals(OpenQueueResult.class)) {
                return (OpenQueueResult) code;
            } else if (code.getClass().equals(GenericResult.class)) {
                switch (code.getGeneralResult()) {
                    case OPEN_QUEUE_RESULT:
                        return (OpenQueueResult) code;
                    case SUCCESS:
                        return OpenQueueResult.SUCCESS;
                    case UNKNOWN:
                        return OpenQueueResult.UNKNOWN;
                    case TIMEOUT:
                        return OpenQueueResult.TIMEOUT;
                    case NOT_CONNECTED:
                        return OpenQueueResult.NOT_CONNECTED;
                    case CANCELED:
                        return OpenQueueResult.CANCELED;
                    case NOT_SUPPORTED:
                        return OpenQueueResult.NOT_SUPPORTED;
                    case REFUSED:
                        return OpenQueueResult.REFUSED;
                    case INVALID_ARGUMENT:
                        return OpenQueueResult.INVALID_ARGUMENT;
                    case NOT_READY:
                        return OpenQueueResult.NOT_READY;
                    default:
                        break;
                }
            }
            throw new RuntimeException("Failure of return code upcast");
        }
    }

    @Immutable
    public enum ConfigureQueueResult implements ConfigureQueueCode {
        SUCCESS(GenericResult.SUCCESS),
        UNKNOWN(GenericResult.UNKNOWN),
        TIMEOUT(GenericResult.TIMEOUT),
        NOT_CONNECTED(GenericResult.NOT_CONNECTED),
        CANCELED(GenericResult.CANCELED),
        NOT_SUPPORTED(GenericResult.NOT_SUPPORTED),
        REFUSED(GenericResult.REFUSED),
        INVALID_ARGUMENT(GenericResult.INVALID_ARGUMENT),
        NOT_READY(GenericResult.NOT_READY),
        // WARNINGS
        ALREADY_IN_PROGRESS(
                GenericResult.CONFIGURE_QUEUE_RESULT), // The queue is already being configured
        // ERRORS
        INVALID_QUEUE(GenericResult.CONFIGURE_QUEUE_RESULT);

        private final GenericResult genericResult;

        ConfigureQueueResult(GenericResult genericResult) {
            this.genericResult = genericResult;
        }

        @Override
        public GenericResult getGeneralResult() {
            return genericResult;
        }

        @Override
        public ConfigureQueueResult getOperationResult() {
            return this;
        }

        @Nonnull
        public static ConfigureQueueResult upcast(GenericCode code) {
            if (code.getClass().equals(ConfigureQueueResult.class)) {
                return (ConfigureQueueResult) code;
            } else if (code.getClass().equals(GenericResult.class)) {
                switch (code.getGeneralResult()) {
                    case OPEN_QUEUE_RESULT:
                        return (ConfigureQueueResult) code;
                    case SUCCESS:
                        return ConfigureQueueResult.SUCCESS;
                    case UNKNOWN:
                        return ConfigureQueueResult.UNKNOWN;
                    case TIMEOUT:
                        return ConfigureQueueResult.TIMEOUT;
                    case NOT_CONNECTED:
                        return ConfigureQueueResult.NOT_CONNECTED;
                    case CANCELED:
                        return ConfigureQueueResult.CANCELED;
                    case NOT_SUPPORTED:
                        return ConfigureQueueResult.NOT_SUPPORTED;
                    case REFUSED:
                        return ConfigureQueueResult.REFUSED;
                    case INVALID_ARGUMENT:
                        return ConfigureQueueResult.INVALID_ARGUMENT;
                    case NOT_READY:
                        return ConfigureQueueResult.NOT_READY;
                    default:
                        break;
                }
            }
            throw new RuntimeException("Failure of return code up cast");
        }
    }

    @Immutable
    public enum CloseQueueResult implements CloseQueueCode {
        SUCCESS(GenericResult.SUCCESS),
        UNKNOWN(GenericResult.UNKNOWN),
        TIMEOUT(GenericResult.TIMEOUT),
        NOT_CONNECTED(GenericResult.NOT_CONNECTED),
        CANCELED(GenericResult.CANCELED),
        NOT_SUPPORTED(GenericResult.NOT_SUPPORTED),
        REFUSED(GenericResult.REFUSED),
        INVALID_ARGUMENT(GenericResult.INVALID_ARGUMENT),
        NOT_READY(GenericResult.NOT_READY),
        // WARNINGS
        ALREADY_CLOSED(GenericResult.CLOSE_QUEUE_RESULT), // The queue is already closed
        ALREADY_IN_PROGRESS(GenericResult.CLOSE_QUEUE_RESULT), // The queue is already being closed
        // ERRORS
        UNKNOWN_QUEUE(GenericResult.CLOSE_QUEUE_RESULT), // The queue doesn't exist
        INVALID_QUEUE(GenericResult.CLOSE_QUEUE_RESULT); // The queue provided is invalid

        private final GenericResult genericResult;

        CloseQueueResult(GenericResult genericResult) {
            this.genericResult = genericResult;
        }

        @Override
        public GenericResult getGeneralResult() {
            return genericResult;
        }

        @Override
        public CloseQueueResult getOperationResult() {
            return this;
        }

        @Nonnull
        public static CloseQueueResult upcast(GenericCode code) {
            if (code.getClass().equals(CloseQueueResult.class)) {
                return (CloseQueueResult) code;
            } else if (code.getClass().equals(GenericResult.class)) {
                switch (code.getGeneralResult()) {
                    case SUCCESS:
                        return CloseQueueResult.SUCCESS;
                    case UNKNOWN:
                        return CloseQueueResult.UNKNOWN;
                    case TIMEOUT:
                        return CloseQueueResult.TIMEOUT;
                    case NOT_CONNECTED:
                        return CloseQueueResult.NOT_CONNECTED;
                    case CANCELED:
                        return CloseQueueResult.CANCELED;
                    case NOT_SUPPORTED:
                        return CloseQueueResult.NOT_SUPPORTED;
                    case REFUSED:
                        return CloseQueueResult.REFUSED;
                    case INVALID_ARGUMENT:
                        return CloseQueueResult.INVALID_ARGUMENT;
                    case NOT_READY:
                        return CloseQueueResult.NOT_READY;
                    default:
                        break;
                }
            }
            throw new RuntimeException("Failure of return code upcast");
        }
    }

    @Immutable
    public enum AckResult implements AckCode {
        SUCCESS(GenericResult.SUCCESS),
        UNKNOWN(GenericResult.UNKNOWN),
        TIMEOUT(GenericResult.TIMEOUT),
        NOT_CONNECTED(GenericResult.NOT_CONNECTED),
        CANCELED(GenericResult.CANCELED),
        NOT_SUPPORTED(GenericResult.NOT_SUPPORTED),
        REFUSED(GenericResult.REFUSED),
        INVALID_ARGUMENT(GenericResult.INVALID_ARGUMENT),
        NOT_READY(GenericResult.NOT_READY),
        // SPECIALIZED
        // ERRORS
        LIMIT_MESSAGES(GenericResult.ACK_RESULT), // = -100 // Messages limit reached
        LIMIT_BYTES(GenericResult.ACK_RESULT), // = -101 // Bytes limit reached
        STORAGE_FAILURE(GenericResult.ACK_RESULT); // = -104 // The storage (on disk) is full
        private final GenericResult genericResult;

        AckResult(GenericResult genericResult) {
            this.genericResult = genericResult;
        }

        @Override
        public AckResult getOperationResult() {
            return this;
        }

        @Override
        public GenericResult getGeneralResult() {
            return genericResult;
        }
    }

    public interface GenericCode {
        GenericResult getGeneralResult();

        default boolean isSuccess() {
            return getGeneralResult() == GenericResult.SUCCESS;
        }

        default boolean isUnknown() {
            return getGeneralResult() == GenericResult.UNKNOWN;
        }

        default boolean isTimeout() {
            return getGeneralResult() == GenericResult.TIMEOUT;
        }

        default boolean isNotConnected() {
            return getGeneralResult() == GenericResult.NOT_CONNECTED;
        }

        default boolean isCanceled() {
            return getGeneralResult() == GenericResult.CANCELED;
        }

        default boolean isNotSupproted() {
            return getGeneralResult() == GenericResult.NOT_SUPPORTED;
        }

        default boolean isRefused() {
            return getGeneralResult() == GenericResult.REFUSED;
        }

        default boolean isInvalidArgument() {
            return getGeneralResult() == GenericResult.INVALID_ARGUMENT;
        }

        default boolean isNotReady() {
            return getGeneralResult() == GenericResult.NOT_READY;
        }

        default boolean isFailure() {
            return getGeneralResult() != GenericResult.SUCCESS;
        }
    }

    public interface OperationResultCode<RESULT extends GenericCode> extends GenericCode {
        RESULT getOperationResult();
    }

    public interface OpenQueueCode extends OperationResultCode<OpenQueueResult> {
        default boolean isAlreadyOpened() {
            return getOperationResult() == OpenQueueResult.ALREADY_OPENED;
        }

        default boolean isAlreadyInProgress() {
            return getOperationResult() == OpenQueueResult.ALREADY_IN_PROGRESS;
        }

        default boolean isInvalidUri() {
            return getOperationResult() == OpenQueueResult.INVALID_URI;
        }

        default boolean isInvalidFlags() {
            return getOperationResult() == OpenQueueResult.INVALID_FLAGS;
        }

        default boolean isQueueIdNotUnique() {
            return getOperationResult() == OpenQueueResult.QUEUE_ID_NOT_UNIQUE;
        }
    }

    public interface ConfigureQueueCode extends OperationResultCode<ConfigureQueueResult> {
        default boolean isAlreadyInProgress() {
            return getOperationResult() == ConfigureQueueResult.ALREADY_IN_PROGRESS;
        }

        default boolean isInvalidQueue() {
            return getOperationResult() == ConfigureQueueResult.INVALID_QUEUE;
        }
    }

    public interface CloseQueueCode extends OperationResultCode<CloseQueueResult> {
        default boolean isAlreadyClosed() {
            return getOperationResult() == CloseQueueResult.ALREADY_CLOSED;
        }

        default boolean isAlreadyInProgress() {
            return getOperationResult() == CloseQueueResult.ALREADY_IN_PROGRESS;
        }

        default boolean isUnknownQueue() {
            return getOperationResult() == CloseQueueResult.UNKNOWN_QUEUE;
        }

        default boolean isInvalidQueue() {
            return getOperationResult() == CloseQueueResult.INVALID_QUEUE;
        }
    }

    public interface AckCode extends OperationResultCode<AckResult> {
        default boolean isLimitMessages() {
            return getOperationResult() == AckResult.LIMIT_MESSAGES;
        }

        default boolean isLimitBytes() {
            return getOperationResult() == AckResult.LIMIT_BYTES;
        }

        default boolean isStorageFailure() {
            return getOperationResult() == AckResult.STORAGE_FAILURE;
        }
    }
}
