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

import com.bloomberg.bmq.impl.CorrelationIdImpl;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.Immutable;

/**
 * An immutable type used to uniquely identify subscription for a purpose of storage, modify and
 * subscription removal.
 *
 * <p>The main identifier is {@code id} which is accessible with the corresponding getter. It is
 * auto incremented and assigned with {@code SubscripionHandle} build. It is used as a subscription
 * id when the configure request is sent, so it is required to be unique.
 *
 * <p>Also this type holds {@code CorrelationId} for convenience, but these correlations are not
 * required to be unique between different {@code SubscriptionHandle}.
 */
@Immutable
public class SubscriptionHandle {
    private static final AtomicInteger nextInt = new AtomicInteger(0);

    private final int id;

    private final CorrelationId correlationId;

    /**
     * Returns subscription id of subscription handle.
     *
     * @return int subscription id
     */
    public int getId() {
        return id;
    }

    /**
     * Returns correlation id of subscription handle.
     *
     * @return CorrelationId correlation id
     */
    public CorrelationId getCorrelationId() {
        return correlationId;
    }

    private SubscriptionHandle(Builder builder) {
        id = nextInt.incrementAndGet(); // todo check overflow, see also CorrelationIdImpl

        if (builder.correlationId != null) {
            correlationId = builder.correlationId;
        } else {
            // It is fine if `correlationIdUserData` is null
            correlationId = CorrelationIdImpl.nextId(builder.correlationIdUserData);
        }
    }

    /**
     * Returns a hash code value for this {@code SubscriptionHandle} object.
     *
     * @return int a hash code value for this object.
     */
    @Override
    public int hashCode() {
        // All SubscriptionHandle instances have different ids by design. This makes possible to
        // calculate hash just with this field.
        return Integer.hashCode(getId());
    }

    /**
     * Returns a helper class object to create immutable {@code SubscriptionHandle} with custom
     * settings.
     *
     * @return Builder a helper class object to create immutable {@code SubscriptionHandle}
     */
    public static SubscriptionHandle.Builder builder() {
        return new SubscriptionHandle.Builder();
    }

    /** A helper class to create immutable {@code SubscriptionHandle} with custom settings. */
    public static class Builder {
        private Object correlationIdUserData;
        private CorrelationId correlationId;

        /**
         * Returns subscription handle based on this object properties.
         *
         * @return SubscriptionHandle immutable subscription handle object
         */
        public SubscriptionHandle build() {
            return new SubscriptionHandle(this);
        }

        /**
         * Sets user data for correlationId associated with this subscription handle. Note: this
         * setter invalidates conflicting correlationIdUserData if previously specified.
         *
         * @param userData user data object
         * @return Builder this object
         */
        public SubscriptionHandle.Builder setCorrelationIdUserData(Object userData) {
            this.correlationId = null;
            this.correlationIdUserData = userData;
            return this;
        }

        /**
         * Sets correlationId associated with this subscription handle. Note: this setter
         * invalidates conflicting correlationId if previously specified.
         *
         * @param correlationId correlation id object
         * @return Builder this object
         */
        public SubscriptionHandle.Builder setCorrelationId(CorrelationId correlationId) {
            this.correlationId = correlationId;
            this.correlationIdUserData = null;
            return this;
        }

        /**
         * "Merges" another 'SubscriptionHandle' into this builder, by invoking setF(options.getF())
         * for all fields 'F' for which 'handle.hasF()' is true.
         *
         * @param handle specifies subscription parameters which is merged into the builder
         * @return Builder this object
         */
        public SubscriptionHandle.Builder merge(SubscriptionHandle handle) {
            // todo check if we need this method
            correlationId = handle.getCorrelationId();
            correlationIdUserData = null;
            return this;
        }

        private Builder() {
            correlationIdUserData = null;
            correlationId = null;
        }
    }
}
