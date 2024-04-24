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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class QueueFlagsTest {

    @Test
    void testFlags() {
        long l = 0L;

        assertFalse(QueueFlags.isReader(l));
        assertFalse(QueueFlags.isWriter(l));
        assertFalse(QueueFlags.isAdmin(l));
        assertFalse(QueueFlags.isAck(l));

        l = QueueFlags.setWriter(l);
        l = QueueFlags.setAck(l);

        assertEquals(12L, l);

        l = QueueFlags.setReader(l);
        l = QueueFlags.setAdmin(l);

        assertTrue(QueueFlags.isReader(l));
        assertTrue(QueueFlags.isWriter(l));
        assertTrue(QueueFlags.isAdmin(l));
        assertTrue(QueueFlags.isAck(l));
    }
}
