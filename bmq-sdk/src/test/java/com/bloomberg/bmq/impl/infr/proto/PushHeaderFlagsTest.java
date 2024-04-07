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
package com.bloomberg.bmq.impl.infr.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PushHeaderFlagsTest {

    @Test
    void testFlags() {
        int i = 0;
        assertFalse(PushHeaderFlags.isSet(i, PushHeaderFlags.IMPLICIT_PAYLOAD));
        assertFalse(PushHeaderFlags.isSet(i, PushHeaderFlags.MESSAGE_PROPERTIES));
        assertFalse(PushHeaderFlags.isSet(i, PushHeaderFlags.UNUSED3));
        assertFalse(PushHeaderFlags.isSet(i, PushHeaderFlags.UNUSED4));

        i = PushHeaderFlags.setFlag(i, PushHeaderFlags.IMPLICIT_PAYLOAD);
        i = PushHeaderFlags.setFlag(i, PushHeaderFlags.MESSAGE_PROPERTIES);

        assertEquals(3, i);

        assertTrue(PushHeaderFlags.isSet(i, PushHeaderFlags.IMPLICIT_PAYLOAD));
        assertTrue(PushHeaderFlags.isSet(i, PushHeaderFlags.MESSAGE_PROPERTIES));
        assertFalse(PushHeaderFlags.isSet(i, PushHeaderFlags.UNUSED3));
        assertFalse(PushHeaderFlags.isSet(i, PushHeaderFlags.UNUSED4));
    }
}
