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

import java.util.Optional;
import javax.annotation.concurrent.Immutable;

/**
 * A value-semantic type for options related to a queue.
 *
 * <p>Provides a value-semantic type, {@code QueueOptions}, which is used to specify parameters for
 * a queue. The following parameters are supported:
 *
 * <ul>
 *   <li>{@code maxUnconfirmedMessages}: Maximum number of outstanding messages that can be sent by
 *       the broker without being confirmed.
 *   <li>{@code maxUnconfirmedBytes}: Maximum accumulated bytes of all outstanding messages that can
 *       be sent by the broker without being confirmed.
 *   <li>{@code consumerPriority}: Priority of a consumer with respect to delivery of messages.
 *   <li>{@code suspendsOnBadHostHealth}: Sets whether queue should suspend when the host machine is
 *       unhealthy.
 * </ul>
 */
@Immutable
public class QueueOptions {
    /**
     * Default maximum number of outstanding messages that can be sent by the broker without being
     * confirmed.
     */
    public static final long k_MAX_UNCONFIRMED_MESSAGES_DEFAULT = 1000L;

    /**
     * Default maximum accumulated bytes of all outstanding messages that can be sent by the broker
     * without being confirmed.
     */
    public static final long k_MAX_UNCONFIRMED_BYTES_DEFAULT = 33554432;

    /** Default priority of a consumer with respect to delivery of messages. */
    public static final int k_CONSUMER_PRIORITY_DEFAULT = 0;

    /** By default, queue is not sensitive to host-health changes. */
    public static final boolean k_SUSPENDS_ON_BAD_HOST_HEALTH_DEFAULT = false;

    private final Optional<Long> maxUnconfirmedMessages;
    private final Optional<Long> maxUnconfirmedBytes;
    private final Optional<Integer> consumerPriority;
    private final Optional<Boolean> suspendsOnBadHostHealth;

    private QueueOptions(Builder builder) {
        maxUnconfirmedMessages = builder.maxUnconfirmedMessages;
        maxUnconfirmedBytes = builder.maxUnconfirmedBytes;
        consumerPriority = builder.consumerPriority;
        suspendsOnBadHostHealth = builder.suspendsOnBadHostHealth;
    }

    private QueueOptions() {
        maxUnconfirmedMessages = Optional.empty();
        maxUnconfirmedBytes = Optional.empty();
        consumerPriority = Optional.empty();
        suspendsOnBadHostHealth = Optional.empty();
    }

    /**
     * Returns true if this {@code QueueOptions} is equal to the specified {@code obj}, false
     * otherwise. Two {@code QueueOptions} are equal if they have equal properties.
     *
     * @param obj {@code QueueOptions} object to compare with
     * @return boolean true if {@code QueueOptions} are equal, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        // TODO: consider using AutoValue plugin.

        if (obj == null) return false;
        if (obj == this) return true;
        if (!(obj instanceof QueueOptions)) return false;
        QueueOptions queueOptions = (QueueOptions) obj;

        return getConsumerPriority() == queueOptions.getConsumerPriority()
                && getMaxUnconfirmedBytes() == queueOptions.getMaxUnconfirmedBytes()
                && getMaxUnconfirmedMessages() == queueOptions.getMaxUnconfirmedMessages()
                && getSuspendsOnBadHostHealth() == queueOptions.getSuspendsOnBadHostHealth();
    }

    /**
     * Returns a hash code value for this {@code QueueOptions} object.
     *
     * @return int a hash code value for this object.
     */
    @Override
    public int hashCode() {

        Long hash = (long) Long.hashCode(getMaxUnconfirmedMessages());
        hash <<= Integer.SIZE;
        hash |= (long) Long.hashCode(getMaxUnconfirmedBytes());
        hash = (long) hash.hashCode();
        hash <<= Integer.SIZE;
        hash |= (long) Integer.hashCode(getConsumerPriority());
        hash = (long) hash.hashCode();
        hash <<= Integer.SIZE;
        hash |= (long) Boolean.hashCode(getSuspendsOnBadHostHealth());

        return hash.hashCode();
    }

    /**
     * Returns maximum number of outstanding messages that can be sent by the broker without being
     * confirmed. If not set, returns default value.
     *
     * @return long maximum number of outstanding messages
     */
    public long getMaxUnconfirmedMessages() {
        return maxUnconfirmedMessages.orElse(k_MAX_UNCONFIRMED_MESSAGES_DEFAULT);
    }

    /**
     * Returns whether {@code maxUnconfirmedMessages} has been set for this object, or whether it
     * implicitly holds {@code k_MAX_UNCONFIRMED_MESSAGES_DEFAULT}.
     *
     * @return boolean true if {@code maxUnconfirmedMessages} has been set.
     */
    public boolean hasMaxUnconfirmedMessages() {
        return maxUnconfirmedMessages.isPresent();
    }

    /**
     * Returns maximum accumulated bytes of all outstanding messages that can be sent by the broker
     * without being confirmed. If not set returns default value.
     *
     * @return long maximum accumulated bytes of all outstanding messages without being confirmed
     */
    public long getMaxUnconfirmedBytes() {
        return maxUnconfirmedBytes.orElse(k_MAX_UNCONFIRMED_BYTES_DEFAULT);
    }

    /**
     * Returns whether {@code maxUnconfirmedBytes} has been set for this object, or whether it
     * implicitly holds {@code k_MAX_UNCONFIRMED_BYTES_DEFAULT}.
     *
     * @return boolean true if {@code maxUnconfirmedBytes} has been set.
     */
    public boolean hasMaxUnconfirmedBytes() {
        return maxUnconfirmedBytes.isPresent();
    }

    /**
     * Returns priority of a consumer with respect to delivery of messages. If not set, returns
     * default value.
     *
     * @return int priority of a consumer
     */
    public int getConsumerPriority() {
        return consumerPriority.orElse(k_CONSUMER_PRIORITY_DEFAULT);
    }

    /**
     * Returns whether {@code consumerPriority} has been set for this object, or whether it
     * implicitly holds {@code k_CONSUMER_PRIORITY_DEFAULT}.
     *
     * @return boolean true if {@code consumerPriority} has been set.
     */
    public boolean hasConsumerPriority() {
        return consumerPriority.isPresent();
    }

    /**
     * Gets whether queue suspends operation when the host machine is unhealthy. If not set, returns
     * default value.
     *
     * @return boolean true if queue suspends operation when the host machine is unhealthy
     */
    public boolean getSuspendsOnBadHostHealth() {
        return suspendsOnBadHostHealth.orElse(k_SUSPENDS_ON_BAD_HOST_HEALTH_DEFAULT);
    }

    /**
     * Returns whether {@code suspendsOnBadHostHealth} has been set for this object, or whether it
     * implicitly holds {@code k_SUSPENDS_ON_BAD_HOST_HEALTH_DEFAULT}.
     *
     * @return boolean true if {@code suspendsOnBadHostHealth} has been set.
     */
    public boolean hasSuspendsOnBadHostHealth() {
        return suspendsOnBadHostHealth.isPresent();
    }

    /**
     * Returns a helper class object to create immutable {@code QueueOptions} with custom settings.
     *
     * @return Builder a helper class object to create immutable {@code QueueOptions}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns queue options with default values.
     *
     * @return QueueOptions queue option object with default values
     */
    public static QueueOptions createDefault() {
        return new QueueOptions();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ maxUnconfirmedMessages: ")
                .append(getMaxUnconfirmedMessages())
                .append(", maxUnconfirmedBytes: ")
                .append(getMaxUnconfirmedBytes())
                .append(", consumerPriority: ")
                .append(getConsumerPriority())
                .append(", suspendsOnBadHostHealth: ")
                .append(getSuspendsOnBadHostHealth())
                .append(" ]");

        return sb.toString();
    }

    /** A helper class to create immutable {@code QueueOptions} with custom settings. */
    public static class Builder {
        private Optional<Long> maxUnconfirmedMessages;
        private Optional<Long> maxUnconfirmedBytes;
        private Optional<Integer> consumerPriority;
        private Optional<Boolean> suspendsOnBadHostHealth;

        /**
         * Returns queue options based on this object properties.
         *
         * @return QueueOptions immutable queue option object
         */
        public QueueOptions build() {
            return new QueueOptions(this);
        }

        /**
         * Sets maximum number of outstanding messages that can be sent by the broker without being
         * confirmed.
         *
         * @param maxUnconfirmedMessages maximum number of outstanding messages
         * @return Builder this object
         */
        public Builder setMaxUnconfirmedMessages(long maxUnconfirmedMessages) {
            this.maxUnconfirmedMessages = Optional.of(maxUnconfirmedMessages);
            return this;
        }

        /**
         * Sets maximum accumulated bytes of all outstanding messages that can be sent by the broker
         * without being confirmed.
         *
         * @param maxUnconfirmedBytes maximum accumulated bytes of all outstanding messages without
         *     being confirmed
         * @return Builder this object
         */
        public Builder setMaxUnconfirmedBytes(long maxUnconfirmedBytes) {
            this.maxUnconfirmedBytes = Optional.of(maxUnconfirmedBytes);
            return this;
        }

        /**
         * Sets priority of a consumer with respect to delivery of messages.
         *
         * @param consumerPriority priority of a consumer
         * @return Builder this object
         */
        public Builder setConsumerPriority(int consumerPriority) {
            this.consumerPriority = Optional.of(consumerPriority);
            return this;
        }

        /**
         * Sets whether queue should suspend when the host machine is unhealthy.
         *
         * @param suspendsOnBadHostHealth specifies whether queue suspends operation when host is
         *     unhealthy
         * @return Builder this object
         */
        public Builder setSuspendsOnBadHostHealth(boolean suspendsOnBadHostHealth) {
            this.suspendsOnBadHostHealth = Optional.of(suspendsOnBadHostHealth);
            return this;
        }

        /**
         * "Merges" another 'QueueOptions' into this builder, by invoking setF(options.getF()) for
         * all fields 'F' for which 'options.hasF()' is true.
         *
         * @param options specifies options which is merged into the builder
         * @return Builder this object
         */
        public Builder merge(QueueOptions options) {
            if (options.hasMaxUnconfirmedMessages()) {
                setMaxUnconfirmedMessages(options.getMaxUnconfirmedMessages());
            }
            if (options.hasMaxUnconfirmedBytes()) {
                setMaxUnconfirmedBytes(options.getMaxUnconfirmedBytes());
            }
            if (options.hasConsumerPriority()) {
                setConsumerPriority(options.getConsumerPriority());
            }
            if (options.hasSuspendsOnBadHostHealth()) {
                setSuspendsOnBadHostHealth(options.getSuspendsOnBadHostHealth());
            }
            return this;
        }

        private Builder() {
            maxUnconfirmedMessages = Optional.empty();
            maxUnconfirmedBytes = Optional.empty();
            consumerPriority = Optional.empty();
            suspendsOnBadHostHealth = Optional.empty();
        }
    }
}
