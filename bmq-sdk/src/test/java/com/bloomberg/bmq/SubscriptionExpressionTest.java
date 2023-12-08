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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.lang.invoke.MethodHandles;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionExpressionTest {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void breathingTest() {
        // Create subscription expression with an empty constructor
        SubscriptionExpression obj = new SubscriptionExpression();

        assertEquals(SubscriptionExpression.k_EXPRESSION_DEFAULT, obj.getExpression());
        assertFalse(obj.hasExpression());
        assertEquals(SubscriptionExpression.k_VERSION_DEFAULT, obj.getVersion());
        assertFalse(obj.hasVersion());

        // Create trivial subscription expression with non-trivial constructor
        SubscriptionExpression objSame =
                new SubscriptionExpression(SubscriptionExpression.k_EXPRESSION_DEFAULT);

        assertEquals(obj, objSame);
        assertEquals(obj.hashCode(), objSame.hashCode());
        assertTrue(objSame.hasExpression());
        assertTrue(objSame.hasVersion());

        // Hash is the same!
        assertEquals(obj, objSame);
        assertEquals(obj.hashCode(), objSame.hashCode());

        // Create non-trivial subscription expression with non-trivial constructor
        SubscriptionExpression objDiff = new SubscriptionExpression("x > 0");

        assertTrue(objDiff.hasExpression());
        assertTrue(objDiff.hasVersion());
        assertEquals("x > 0", objDiff.getExpression());
        assertEquals(SubscriptionExpression.Version.e_VERSION_1, objDiff.getVersion());

        // Different hash
        assertNotEquals(obj, objDiff);
        assertNotEquals(obj.hashCode(), objDiff.hashCode());
    }
}
