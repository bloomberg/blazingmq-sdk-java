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

public class SubscriptionTest {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void breathingTest() {
        // Create default subscription parameters
        Subscription obj = Subscription.createDefault();

        assertEquals(
                Subscription.k_MAX_UNCONFIRMED_MESSAGES_DEFAULT, obj.getMaxUnconfirmedMessages());
        assertFalse(obj.hasMaxUnconfirmedMessages());
        assertEquals(Subscription.k_MAX_UNCONFIRMED_BYTES_DEFAULT, obj.getMaxUnconfirmedBytes());
        assertFalse(obj.hasMaxUnconfirmedBytes());
        assertEquals(Subscription.k_CONSUMER_PRIORITY_DEFAULT, obj.getConsumerPriority());
        assertFalse(obj.hasConsumerPriority());

        // Create default subscription parameters with empty builder
        Subscription objSame = Subscription.builder().build();
        assertFalse(objSame.hasMaxUnconfirmedMessages());
        assertFalse(objSame.hasMaxUnconfirmedBytes());
        assertFalse(objSame.hasConsumerPriority());

        assertEquals(obj, objSame);
        assertEquals(obj.hashCode(), objSame.hashCode());

        // Create non-default subscription parameters with default values
        Subscription objNonDefaultSameValues =
                Subscription.builder()
                        .setMaxUnconfirmedMessages(Subscription.k_MAX_UNCONFIRMED_MESSAGES_DEFAULT)
                        .setMaxUnconfirmedBytes(Subscription.k_MAX_UNCONFIRMED_BYTES_DEFAULT)
                        .setConsumerPriority(Subscription.k_CONSUMER_PRIORITY_DEFAULT)
                        .build();

        assertTrue(objNonDefaultSameValues.hasMaxUnconfirmedMessages());
        assertTrue(objNonDefaultSameValues.hasMaxUnconfirmedBytes());
        assertTrue(objNonDefaultSameValues.hasConsumerPriority());

        // Hash is the same!
        assertEquals(obj, objNonDefaultSameValues);
        assertEquals(obj.hashCode(), objNonDefaultSameValues.hashCode());

        // Create non-default subscription parameters
        Subscription objNonDefault = Subscription.builder().setMaxUnconfirmedMessages(128L).build();

        assertTrue(objNonDefault.hasMaxUnconfirmedMessages());
        assertFalse(objNonDefault.hasMaxUnconfirmedBytes());
        assertFalse(objNonDefault.hasConsumerPriority());

        // Different hash
        assertNotEquals(obj, objNonDefault);
        assertNotEquals(obj.hashCode(), objNonDefault.hashCode());
    }

    @Test
    public void mergeTest() {
        Subscription baseSubscription =
                Subscription.builder()
                        .setConsumerPriority(2)
                        .setMaxUnconfirmedMessages(500)
                        .build();

        assertTrue(baseSubscription.hasMaxUnconfirmedMessages());
        assertFalse(baseSubscription.hasMaxUnconfirmedBytes());
        assertTrue(baseSubscription.hasConsumerPriority());

        Subscription newSubscription =
                Subscription.builder()
                        .merge(baseSubscription)
                        .setMaxUnconfirmedMessages(1500)
                        .build();

        assertTrue(newSubscription.hasMaxUnconfirmedMessages());
        assertFalse(newSubscription.hasMaxUnconfirmedBytes());
        assertTrue(newSubscription.hasConsumerPriority());

        assertEquals(1500, newSubscription.getMaxUnconfirmedMessages());
        assertEquals(
                Subscription.k_MAX_UNCONFIRMED_BYTES_DEFAULT,
                newSubscription.getMaxUnconfirmedBytes());
        assertEquals(2, newSubscription.getConsumerPriority());

        // Different hash
        assertNotEquals(baseSubscription, newSubscription);
        assertNotEquals(baseSubscription.hashCode(), newSubscription.hashCode());
    }
}
