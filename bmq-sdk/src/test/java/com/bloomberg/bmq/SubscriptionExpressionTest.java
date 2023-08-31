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
        // Create default subscription expression
        SubscriptionExpression obj = SubscriptionExpression.createDefault();

        assertEquals(SubscriptionExpression.k_EXPRESSION_DEFAULT, obj.getExpression());
        assertFalse(obj.hasExpression());
        assertEquals(SubscriptionExpression.k_VERSION_DEFAULT, obj.getVersion());
        assertFalse(obj.hasVersion());

        // Create default subscription expression with empty builder
        SubscriptionExpression objSame = SubscriptionExpression.builder().build();
        assertFalse(objSame.hasExpression());
        assertFalse(objSame.hasVersion());

        assertEquals(obj, objSame);
        assertEquals(obj.hashCode(), objSame.hashCode());

        // Create non-default subscription expression with default values
        SubscriptionExpression objNonDefaultSameValues =
                SubscriptionExpression.builder()
                        .setExpression(SubscriptionExpression.k_EXPRESSION_DEFAULT)
                        .setVersion(SubscriptionExpression.k_VERSION_DEFAULT)
                        .build();

        assertTrue(objNonDefaultSameValues.hasExpression());
        assertTrue(objNonDefaultSameValues.hasVersion());

        // Hash is the same!
        assertEquals(obj, objNonDefaultSameValues);
        assertEquals(obj.hashCode(), objNonDefaultSameValues.hashCode());

        // Create non-default subscription expression
        SubscriptionExpression objNonDefault =
                SubscriptionExpression.builder().setExpression("x >= 0").build();

        assertTrue(objNonDefault.hasExpression());
        assertFalse(objNonDefault.hasVersion());

        // Different hash
        assertNotEquals(obj, objNonDefault);
        assertNotEquals(obj.hashCode(), objNonDefault.hashCode());
    }

    @Test
    public void mergeTest() {
        SubscriptionExpression baseExpression =
                SubscriptionExpression.builder().setExpression("x >= 0").build();

        assertTrue(baseExpression.hasExpression());
        assertFalse(baseExpression.hasVersion());

        SubscriptionExpression newExpression =
                SubscriptionExpression.builder()
                        .merge(baseExpression)
                        .setExpression("x < 0")
                        .build();

        assertTrue(newExpression.hasExpression());
        assertFalse(newExpression.hasVersion());

        assertEquals("x < 0", newExpression.getExpression());
        assertEquals(SubscriptionExpression.k_VERSION_DEFAULT, newExpression.getVersion());

        // Different hash
        assertNotEquals(baseExpression, newExpression);
        assertNotEquals(baseExpression.hashCode(), newExpression.hashCode());
    }
}
