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

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
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
    public static final long k_MAX_UNCONFIRMED_MESSAGES_DEFAULT =
            Subscription.k_MAX_UNCONFIRMED_MESSAGES_DEFAULT;

    /**
     * Default maximum accumulated bytes of all outstanding messages that can be sent by the broker
     * without being confirmed.
     */
    public static final long k_MAX_UNCONFIRMED_BYTES_DEFAULT =
            Subscription.k_MAX_UNCONFIRMED_BYTES_DEFAULT;

    /** Default priority of a consumer with respect to delivery of messages. */
    public static final int k_CONSUMER_PRIORITY_DEFAULT = Subscription.k_CONSUMER_PRIORITY_DEFAULT;

    /** By default, queue is not sensitive to host-health changes. */
    public static final boolean k_SUSPENDS_ON_BAD_HOST_HEALTH_DEFAULT = false;

    private final Subscription commonParameters;
    private final HashMap<Integer, Map.Entry<SubscriptionHandle, Subscription>> subscriptions;
    private final Optional<Boolean> suspendsOnBadHostHealth;

    private QueueOptions(Builder builder) {
        commonParameters = builder.commonParametersBuilder.build();
        subscriptions = builder.subscriptions;
        suspendsOnBadHostHealth = builder.suspendsOnBadHostHealth;
    }

    private QueueOptions() {
        commonParameters = Subscription.createDefault();
        subscriptions = new HashMap<>();
        suspendsOnBadHostHealth = Optional.empty();
    }

    /**
     * Returns true if this {@code QueueOptions} is equal to the specified {@code obj}, false
     * otherwise. Two {@code QueueOptions} are equal if they have equal properties, the
     * subscriptions number is the same and the contained subscriptions are equal with the same
     * order.
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

        return commonParameters.equals(queueOptions.commonParameters)
                && subscriptions.equals(queueOptions.subscriptions)
                && getSuspendsOnBadHostHealth() == queueOptions.getSuspendsOnBadHostHealth();
    }

    /**
     * Returns a hash code value for this {@code QueueOptions} object.
     *
     * @return int a hash code value for this object.
     */
    @Override
    public int hashCode() {

        Long hash = (long) commonParameters.hashCode();
        hash <<= Integer.SIZE;
        hash |= (long) Boolean.hashCode(getSuspendsOnBadHostHealth());

        hash = (long) hash.hashCode();
        hash <<= Integer.SIZE;
        hash |= subscriptions.hashCode();

        return hash.hashCode();
    }

    /**
     * Returns maximum number of outstanding messages that can be sent by the broker without being
     * confirmed. If not set, returns default value.
     *
     * @return long maximum number of outstanding messages
     */
    public long getMaxUnconfirmedMessages() {
        return commonParameters.getMaxUnconfirmedMessages();
    }

    /**
     * Returns whether {@code maxUnconfirmedMessages} has been set for this object, or whether it
     * implicitly holds {@code k_MAX_UNCONFIRMED_MESSAGES_DEFAULT}.
     *
     * @return boolean true if {@code maxUnconfirmedMessages} has been set.
     */
    public boolean hasMaxUnconfirmedMessages() {
        return commonParameters.hasMaxUnconfirmedMessages();
    }

    /**
     * Returns maximum accumulated bytes of all outstanding messages that can be sent by the broker
     * without being confirmed. If not set returns default value.
     *
     * @return long maximum accumulated bytes of all outstanding messages without being confirmed
     */
    public long getMaxUnconfirmedBytes() {
        return commonParameters.getMaxUnconfirmedBytes();
    }

    /**
     * Returns whether {@code maxUnconfirmedBytes} has been set for this object, or whether it
     * implicitly holds {@code k_MAX_UNCONFIRMED_BYTES_DEFAULT}.
     *
     * @return boolean true if {@code maxUnconfirmedBytes} has been set.
     */
    public boolean hasMaxUnconfirmedBytes() {
        return commonParameters.hasMaxUnconfirmedBytes();
    }

    /**
     * Returns priority of a consumer with respect to delivery of messages. If not set, returns
     * default value.
     *
     * @return int priority of a consumer
     */
    public int getConsumerPriority() {
        return commonParameters.getConsumerPriority();
    }

    /**
     * Returns whether {@code consumerPriority} has been set for this object, or whether it
     * implicitly holds {@code k_CONSUMER_PRIORITY_DEFAULT}.
     *
     * @return boolean true if {@code consumerPriority} has been set.
     */
    public boolean hasConsumerPriority() {
        return commonParameters.hasConsumerPriority();
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

    // todo docs
    // todo immutable map and pair?
    public HashMap<Integer, Map.Entry<SubscriptionHandle, Subscription>> getSubscriptions() {
        return subscriptions;
    }

    public boolean hasSubscriptions() {
        return !subscriptions.isEmpty();
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
        private final Subscription.Builder commonParametersBuilder;
        private final HashMap<Integer, Map.Entry<SubscriptionHandle, Subscription>> subscriptions;
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
            this.commonParametersBuilder.setMaxUnconfirmedMessages(maxUnconfirmedMessages);
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
            this.commonParametersBuilder.setMaxUnconfirmedBytes(maxUnconfirmedBytes);
            return this;
        }

        /**
         * Sets priority of a consumer with respect to delivery of messages.
         *
         * @param consumerPriority priority of a consumer
         * @return Builder this object
         */
        public Builder setConsumerPriority(int consumerPriority) {
            this.commonParametersBuilder.setConsumerPriority(consumerPriority);
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
         * Adds subscription parameters for a new subscription with a default constructed
         * SubscriptionHandle.
         *
         * @param subscription subscription parameters
         * @return Builder this object
         */
        public Builder addSubscription(Subscription subscription) {
            // todo do we need this or addOrUpdate is enough?
            SubscriptionHandle handle = SubscriptionHandle.builder().build();
            this.subscriptions.put(
                    handle.getId(), new AbstractMap.SimpleImmutableEntry<>(handle, subscription));
            return this;
        }

        /**
         * Adds subscription parameters for a new subscription if provided SubscriptionHandle is not
         * present, or updates subscription parameters if SubscriptionHandle is found.
         *
         * @param handle subscription handle
         * @param subscription subscription parameters
         * @return Builder this object
         */
        public Builder addOrUpdateSubscription(
                SubscriptionHandle handle, Subscription subscription) {
            // todo check args order
            this.subscriptions.put(
                    handle.getId(), new AbstractMap.SimpleImmutableEntry<>(handle, subscription));
            return this;
        }

        /**
         * Remove subscription parameters if SubscriptionHandle is found.
         *
         * @param handle subscription handle
         * @return Builder this object
         */
        public Builder removeSubscription(SubscriptionHandle handle) {
            // todo check exception if not found
            this.subscriptions.remove(handle.getId());
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

            for (Map.Entry<Integer, Map.Entry<SubscriptionHandle, Subscription>> entry :
                    options.subscriptions.entrySet()) {
                subscriptions.merge(
                        entry.getKey(),
                        entry.getValue(),
                        (oldSubscription, newSubscription) ->
                                new AbstractMap.SimpleImmutableEntry<>(
                                        newSubscription.getKey(),
                                        Subscription.builder()
                                                .merge(oldSubscription.getValue())
                                                .merge(newSubscription.getValue())
                                                .build()));
                // TODO simplify?
            }
            return this;
        }

        private Builder() {
            commonParametersBuilder = Subscription.builder();
            subscriptions = new HashMap<>();
            suspendsOnBadHostHealth = Optional.empty();
        }
    }
}
