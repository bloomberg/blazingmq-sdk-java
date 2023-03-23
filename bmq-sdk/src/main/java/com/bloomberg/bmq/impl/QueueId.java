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
package com.bloomberg.bmq.impl;

import javax.annotation.concurrent.Immutable;

/**
 * Identifier of the {@link com.bloomberg.bmq.Queue}.
 *
 * <p>This class represents an immutable identifier which is assigned by the SDK to every created
 * {@code Queue}. It consits of two integer values, one is {@code QueueId} and another is {@code
 * SubQueueId}. The combination of these two values uniquely identifies the {@code Queue} object.
 */
@Immutable
public final class QueueId {
    public static final int k_DEFAULT_SUBQUEUE_ID = 0;
    private final int id;
    private final int subId;

    public int getQId() {
        return id;
    }

    public int getSubQId() {
        return subId;
    }

    private QueueId(int id, int subId) {
        this.id = id;
        this.subId = subId;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) return true;
        if (other == null) return false;
        if (!getClass().isInstance(other)) return false;
        QueueId otherQueueId = (QueueId) other;
        return id == otherQueueId.id && subId == otherQueueId.subId;
    }

    @Override
    public int hashCode() {
        long l = id;
        l = l << 32 + subId;
        Long hashed = l;
        return hashed.hashCode();
    }

    @Override
    public String toString() {
        return "[" + id + "," + subId + "]";
    }

    public static QueueId createInstance(int id, int subId) {
        return new QueueId(id, subId);
    }
}
