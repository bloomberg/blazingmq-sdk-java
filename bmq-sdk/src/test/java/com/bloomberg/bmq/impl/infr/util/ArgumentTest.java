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
package com.bloomberg.bmq.impl.infr.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

class ArgumentTest {
    @Test
    void testExpectPrimitiveInteger() {
        int val1 = 10;
        assertEquals(10, Argument.expectPositive(val1, "val1"));
        assertEquals(10, Argument.expectNonNegative(val1, "val1"));
        try {
            Argument.expectNotGreater(val1, 5, "val1");
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("'val1' must not exceed 5", e.getMessage());
        }

        int val2 = 0;
        try {
            Argument.expectPositive(val2, "val2");
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("'val2' must be positive", e.getMessage());
        }
        assertEquals(0, Argument.expectNonNegative(val2, "val2"));
        assertEquals(0, Argument.expectNotGreater(val2, 5, "val2"));

        int val3 = -10;
        try {
            Argument.expectPositive(val3, "val3");
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("'val3' must be positive", e.getMessage());
        }
        try {
            Argument.expectNonNegative(val3, "val3");
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("'val3' must be non-negative", e.getMessage());
        }
        assertEquals(-10, Argument.expectNotGreater(val3, 5, "val3"));
    }

    @Test
    void testExpectPrimitiveLong() {
        long val1 = 10;
        assertEquals(10, Argument.expectNonNegative(val1, "val1"));

        long val2 = 0;
        assertEquals(0, Argument.expectNonNegative(val2, "val2"));

        long val3 = -10;
        try {
            Argument.expectNonNegative(val3, "val3");
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("'val3' must be non-negative", e.getMessage());
        }
    }

    @Test
    void testExpectNonNull() {
        // empty string is not a null string so the check must be fine
        assertEquals("", Argument.expectNonNull("", "empty string"));
        try {
            Argument.expectNonNull(null, "sample");
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("'sample' must be non-null", e.getMessage());
        }

        // the array itself is not null, but the only element in the array is null initially
        String[] array = new String[1];
        Argument.expectNonNull(array, "string array");
        try {
            Argument.expectNonNull(array[0], "string array element [0]");
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("'string array element [0]' must be non-null", e.getMessage());
        }

        // the same check for the only element in the array must be fine after initialization
        array[0] = "sample";
        assertEquals(6, Argument.expectNonNull(array[0], "string array element [0]").length());
    }

    @Test
    void testExpectCondition() {
        // basic condition checks without extra message components
        Argument.expectCondition(true);
        try {
            Argument.expectCondition(false);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("", e.getMessage());
        }

        // basic concatenation checks for exception message components
        try {
            Argument.expectCondition(false, "Condition ", "failed");
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Condition failed", e.getMessage());
        }
        try {
            int val1 = 10;
            int val2 = val1 + 5;
            Argument.expectCondition(
                    val1 > val2, "Condition 'val1 > val2': ", val1, " > ", val2, " FAILED");
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Condition 'val1 > val2': 10 > 15 FAILED", e.getMessage());
        }

        // ensure that 'expectCondition' doesn't perform unnecessary string formatting
        // 'toString()' must never be called if expected condition is fine, otherwise the test will
        // fail
        Object lazyToStringChecker =
                new Object() {
                    @Override
                    public String toString() {
                        fail();
                        return "";
                    }
                };
        Argument.expectCondition(true, "Checker: ", lazyToStringChecker);
    }
}
