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

import com.bloomberg.bmq.CorrelationId;
import com.bloomberg.bmq.impl.infr.util.Argument;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class CorrelationIdImpl implements CorrelationId {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final AtomicInteger nextInt = new AtomicInteger(0);
    private static final int CORRELATION_ID_UPPER_BOUND = 0xFFFFFF;

    private final int correlationId;
    private final Object userValue;

    public static final CorrelationIdImpl NULL_CORRELATION_ID = new CorrelationIdImpl(0, null);

    private static int nextUniqueId() {
        int nextInt;
        do {
            nextInt = CorrelationIdImpl.nextInt.incrementAndGet() & CORRELATION_ID_UPPER_BOUND;
        } while (nextInt == 0);
        return nextInt;
    }

    public static CorrelationIdImpl nextId() {
        return nextId(null);
    }

    public static CorrelationIdImpl nextId(Object val) {
        int id = nextUniqueId();
        CorrelationIdImpl obj = new CorrelationIdImpl(id, val);
        logger.debug("Binding {} {} {}", id, val, obj);
        return obj;
    }

    public static CorrelationIdImpl restoreId(int corrId) {
        return new CorrelationIdImpl(corrId, null);
    }

    private CorrelationIdImpl(int corrId, Object val) {
        Argument.expectNonNegative(corrId, "corrId");
        Argument.expectNotGreater(corrId, CORRELATION_ID_UPPER_BOUND, "corrId");
        correlationId = corrId;
        userValue = val;
    }

    @Override
    public Object userData() {
        return userValue;
    }

    public int toInt() {
        return correlationId;
    }

    @Override
    public boolean equals(Object object) {
        // Return true if it's the same object
        if (object == this) {
            return true;
        }

        // Return false if object's type is different or object is null
        if (!(object instanceof CorrelationIdImpl)) {
            logger.info("Wrong type {}", object);
            return false;
        }

        CorrelationIdImpl corrId = (CorrelationIdImpl) object;
        return corrId.correlationId == correlationId;
    }

    @Override
    public int hashCode() {
        return correlationId;
    }

    public boolean isValid() {
        return !NULL_CORRELATION_ID.equals(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ CorrelationId [ ");
        sb.append("UniqueId : ");
        sb.append(correlationId);
        if (userValue != null) {
            sb.append(", UserData Hash: ");
            sb.append(userValue.hashCode());
        }
        sb.append(" ] ]");
        return sb.toString();
    }
}
