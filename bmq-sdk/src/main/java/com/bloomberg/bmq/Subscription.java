/*
 * Copyright 2023 Bloomberg Finance L.P.
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

@Immutable
public class Subscription {
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

    private final Optional<Long> maxUnconfirmedMessages;
    private final Optional<Long> maxUnconfirmedBytes;
    private final Optional<Integer> consumerPriority;
    private final SubscriptionExpression expression;

    private Subscription(Builder builder) {
        maxUnconfirmedMessages = builder.maxUnconfirmedMessages;
        maxUnconfirmedBytes = builder.maxUnconfirmedBytes;
        consumerPriority = builder.consumerPriority;
        expression = builder.expressionBuilder.build();
    }

    private Subscription() {
        maxUnconfirmedMessages = Optional.empty();
        maxUnconfirmedBytes = Optional.empty();
        consumerPriority = Optional.empty();
        expression = SubscriptionExpression.createDefault();
    }

    /**
     * Returns true if this {@code Subscription} is equal to the specified {@code obj}, false
     * otherwise. Two {@code Subscription} are equal if they have equal properties.
     *
     * @param obj {@code Subscription} object to compare with
     * @return boolean true if {@code Subscription} are equal, false otherwise
     */
    @Override
    public boolean equals(Object obj) {

        if (obj == null) return false;
        if (obj == this) return true;
        if (!(obj instanceof Subscription)) return false;
        Subscription subscription = (Subscription) obj;

        return getConsumerPriority() == subscription.getConsumerPriority()
                && getMaxUnconfirmedBytes() == subscription.getMaxUnconfirmedBytes()
                && getMaxUnconfirmedMessages() == subscription.getMaxUnconfirmedMessages()
                && getExpression().equals(subscription.getExpression());
    }

    /**
     * Returns a hash code value for this {@code Subscription} object.
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
     * Returns priority of a consumer with respect to delivery of messages. If not set, returns
     * default value.
     *
     * @return int priority of a consumer
     */
    public SubscriptionExpression getExpression() {
        return expression;
    }

    /**
     * Returns whether {@code consumerPriority} has been set for this object, or whether it
     * implicitly holds {@code k_CONSUMER_PRIORITY_DEFAULT}.
     *
     * @return boolean true if {@code consumerPriority} has been set.
     */
    //    public boolean hasExpression() {
    //        return consumerPriority.isPresent();
    //    }
    /**
     * Returns a helper class object to create immutable {@code Subscription} with custom settings.
     *
     * @return Builder a helper class object to create immutable {@code Subscription}
     */
    public static Subscription.Builder builder() {
        return new Subscription.Builder();
    }

    /**
     * Returns subscription parameters with default values.
     *
     * @return Subscription subscription parameters object with default values
     */
    public static Subscription createDefault() {
        return new Subscription();
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
                .append(" ]");
        return sb.toString();
    }

    /** A helper class to create immutable {@code Subscription} with custom settings. */
    public static class Builder {
        private Optional<Long> maxUnconfirmedMessages;
        private Optional<Long> maxUnconfirmedBytes;
        private Optional<Integer> consumerPriority;
        private SubscriptionExpression.Builder expressionBuilder;

        /**
         * Returns subscription parameters based on this object properties.
         *
         * @return Subscription immutable subscription parameters object
         */
        public Subscription build() {
            return new Subscription(this);
        }

        /**
         * Sets maximum number of outstanding messages that can be sent by the broker without being
         * confirmed for this subscription.
         *
         * @param maxUnconfirmedMessages maximum number of outstanding messages
         * @return Builder this object
         */
        public Subscription.Builder setMaxUnconfirmedMessages(long maxUnconfirmedMessages) {
            this.maxUnconfirmedMessages = Optional.of(maxUnconfirmedMessages);
            return this;
        }

        /**
         * Sets maximum accumulated bytes of all outstanding messages that can be sent by the broker
         * without being confirmed for this subscription.
         *
         * @param maxUnconfirmedBytes maximum accumulated bytes of all outstanding messages without
         *     being confirmed
         * @return Builder this object
         */
        public Subscription.Builder setMaxUnconfirmedBytes(long maxUnconfirmedBytes) {
            this.maxUnconfirmedBytes = Optional.of(maxUnconfirmedBytes);
            return this;
        }

        /**
         * Sets priority of a consumer for this subscription with respect to delivery of messages.
         *
         * @param consumerPriority priority of a consumer
         * @return Builder this object
         */
        public Subscription.Builder setConsumerPriority(int consumerPriority) {
            this.consumerPriority = Optional.of(consumerPriority);
            return this;
        }

        /**
         * Sets expression text for this subscription.
         *
         * @param expression expression text
         * @return Builder this object
         */
        public Subscription.Builder setExpressionText(String expression) {
            this.expressionBuilder.setExpression(expression);
            if (expression.length() > 0) {
                setExpressionVersion(SubscriptionExpression.Version.e_VERSION_1);
            }
            return this;
        }

        /**
         * Sets expression version.
         *
         * @param version expression version
         * @return Builder this object
         */
        public Subscription.Builder setExpressionVersion(SubscriptionExpression.Version version) {
            this.expressionBuilder.setVersion(version);
            return this;
        }

        /**
         * "Merges" another 'Subscription' into this builder, by invoking setF(options.getF()) for
         * all fields 'F' for which 'subscription.hasF()' is true.
         *
         * @param subscription specifies subscription parameters which is merged into the builder
         * @return Builder this object
         */
        public Subscription.Builder merge(Subscription subscription) {
            if (subscription.hasMaxUnconfirmedMessages()) {
                setMaxUnconfirmedMessages(subscription.getMaxUnconfirmedMessages());
            }
            if (subscription.hasMaxUnconfirmedBytes()) {
                setMaxUnconfirmedBytes(subscription.getMaxUnconfirmedBytes());
            }
            if (subscription.hasConsumerPriority()) {
                setConsumerPriority(subscription.getConsumerPriority());
            }
            expressionBuilder.merge(subscription.getExpression());
            return this;
        }

        private Builder() {
            maxUnconfirmedMessages = Optional.empty();
            maxUnconfirmedBytes = Optional.empty();
            consumerPriority = Optional.empty();
            expressionBuilder = SubscriptionExpression.builder();
        }
    }
}
