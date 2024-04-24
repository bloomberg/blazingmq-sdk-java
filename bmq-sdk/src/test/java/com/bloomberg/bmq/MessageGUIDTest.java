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

class MessageGUIDTest {

    @Test
    void testGUID() {
        // Test some valid & invalid hex reps
        final String INVALID_HEX_REP = "abcdefghijklmnopqrstuvwxyz012345";
        final String VALID_HEX_REP = "ABCDEF0123456789ABCDEF0123456789";

        assertFalse(MessageGUID.isValidHexRepresentation(INVALID_HEX_REP.getBytes()));

        assertTrue(MessageGUID.isValidHexRepresentation(VALID_HEX_REP.getBytes()));

        // Create guid from valid hex rep
        MessageGUID obj1 = MessageGUID.fromHex(VALID_HEX_REP);

        // obj -> binary -> obj
        byte[] binObj1 = new byte[MessageGUID.SIZE_BINARY];
        obj1.toBinary(binObj1);
        MessageGUID obj2 = MessageGUID.fromBinary(binObj1);

        assertEquals(obj2, obj1);
        assertEquals(obj2.hashCode(), obj1.hashCode());

        // obj -> hex -> obj
        String hex = obj2.toHex();
        MessageGUID obj3 = MessageGUID.fromHex(hex);
        assertEquals(obj3, obj2);
        assertEquals(obj3.hashCode(), obj2.hashCode());

        // Create guid from valid hex rep
        String hexObj4 = "00000000010EA8F9515DCACE04742D2E";
        // For above guid:
        //   Version....: [0]
        //   Counter....: [0]
        //   Timer tick.: [297593876864458]
        //   BrokerId...: [CE04742D2E]

        assertTrue(MessageGUID.isValidHexRepresentation(hexObj4.getBytes()));

        MessageGUID obj4 = MessageGUID.fromHex(hexObj4);

        byte[] binObj4 = new byte[MessageGUID.SIZE_BINARY];
        obj4.toBinary(binObj4);

        // Create guids from valid hex and bin reps
        MessageGUID obj5 = MessageGUID.fromBinary(binObj4);
        assertEquals(obj5, obj4);
        assertEquals(obj5.hashCode(), obj4.hashCode());

        MessageGUID obj6 = MessageGUID.fromHex(hexObj4);
        assertEquals(obj4, obj6);
        assertEquals(obj4.hashCode(), obj6.hashCode());
    }
}
