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
 * Provides an interface usable as an efficient identifier.
 *
 * <p>This is an interface, which can be used to identify any async operations. The {@code
 * CorrelationId} internally provides an auto-assigned value (32-bit integer with 24 meaningful
 * bits). It may also contain an optional user provided {@code Object}. {@link
 * com.bloomberg.bmq.PutMessage#setCorrelationId} can be used to consecutively generate a unique
 * {@code CorrelationId} starting from 1 and up to {@code 0xFFFFFF} from multiple execution threads.
 * Another flavour of the {@code PutMessage#setCorrelationId} can be used to generate similar unique
 * {@code CorrelationId} that will held a user specified {@code Object}.
 */
public interface CorrelationId {
    /**
     * Returns the user provided {@code Object} associated with this correlation ID.
     *
     * @return Object user provided object
     */
    Object userData();
}
