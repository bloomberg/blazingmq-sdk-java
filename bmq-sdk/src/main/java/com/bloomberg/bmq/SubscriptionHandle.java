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

    public SubscriptionHandle() {
        this(null);
    }

    public SubscriptionHandle(Object userData) {
        id = nextInt.incrementAndGet(); // todo check overflow, see also CorrelationIdImpl

        if (userData instanceof CorrelationId) {
            correlationId = (CorrelationId) userData;
        } else {
            // It is fine if value for correlation id is null
            correlationId = CorrelationIdImpl.nextId(userData);
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
}
