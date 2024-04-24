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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.invoke.MethodHandles;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class QueueOptionsTest {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    void breathingTest() {
        // Create default options
        QueueOptions obj = QueueOptions.createDefault();

        assertEquals(
                QueueOptions.k_MAX_UNCONFIRMED_MESSAGES_DEFAULT, obj.getMaxUnconfirmedMessages());
        assertFalse(obj.hasMaxUnconfirmedMessages());
        assertEquals(QueueOptions.k_MAX_UNCONFIRMED_BYTES_DEFAULT, obj.getMaxUnconfirmedBytes());
        assertFalse(obj.hasMaxUnconfirmedBytes());
        assertEquals(QueueOptions.k_CONSUMER_PRIORITY_DEFAULT, obj.getConsumerPriority());
        assertFalse(obj.hasConsumerPriority());
        assertEquals(
                QueueOptions.k_SUSPENDS_ON_BAD_HOST_HEALTH_DEFAULT,
                obj.getSuspendsOnBadHostHealth());
        assertFalse(obj.hasSuspendsOnBadHostHealth());

        // Create default options with empty builder
        QueueOptions objSame = QueueOptions.builder().build();
        assertFalse(objSame.hasMaxUnconfirmedMessages());
        assertFalse(objSame.hasMaxUnconfirmedBytes());
        assertFalse(objSame.hasConsumerPriority());
        assertFalse(objSame.hasSuspendsOnBadHostHealth());

        assertEquals(obj, objSame);
        assertEquals(obj.hashCode(), objSame.hashCode());

        // Create non-default options with default values
        QueueOptions objNonDefaultSameValues =
                QueueOptions.builder()
                        .setMaxUnconfirmedMessages(QueueOptions.k_MAX_UNCONFIRMED_MESSAGES_DEFAULT)
                        .setMaxUnconfirmedBytes(QueueOptions.k_MAX_UNCONFIRMED_BYTES_DEFAULT)
                        .setConsumerPriority(QueueOptions.k_CONSUMER_PRIORITY_DEFAULT)
                        .setSuspendsOnBadHostHealth(
                                QueueOptions.k_SUSPENDS_ON_BAD_HOST_HEALTH_DEFAULT)
                        .build();

        assertTrue(objNonDefaultSameValues.hasMaxUnconfirmedMessages());
        assertTrue(objNonDefaultSameValues.hasMaxUnconfirmedBytes());
        assertTrue(objNonDefaultSameValues.hasConsumerPriority());
        assertTrue(objNonDefaultSameValues.hasSuspendsOnBadHostHealth());

        // Hash is the same!
        assertEquals(obj, objNonDefaultSameValues);
        assertEquals(obj.hashCode(), objNonDefaultSameValues.hashCode());

        // Create non-default options
        QueueOptions objNonDefault =
                QueueOptions.builder()
                        .setSuspendsOnBadHostHealth(
                                !QueueOptions.k_SUSPENDS_ON_BAD_HOST_HEALTH_DEFAULT)
                        .build();

        assertFalse(objNonDefault.hasMaxUnconfirmedMessages());
        assertFalse(objNonDefault.hasMaxUnconfirmedBytes());
        assertFalse(objNonDefault.hasConsumerPriority());
        assertTrue(objNonDefault.hasSuspendsOnBadHostHealth());

        // Different hash
        assertNotEquals(obj, objNonDefault);
        assertNotEquals(obj.hashCode(), objNonDefault.hashCode());
    }

    @Test
    void mergeTest() {
        QueueOptions baseOptions =
                QueueOptions.builder()
                        .setConsumerPriority(2)
                        .setMaxUnconfirmedMessages(500)
                        .build();

        assertTrue(baseOptions.hasMaxUnconfirmedMessages());
        assertFalse(baseOptions.hasMaxUnconfirmedBytes());
        assertTrue(baseOptions.hasConsumerPriority());
        assertFalse(baseOptions.hasSuspendsOnBadHostHealth());

        QueueOptions newOptions =
                QueueOptions.builder()
                        .merge(baseOptions)
                        .setMaxUnconfirmedMessages(1500)
                        .setSuspendsOnBadHostHealth(true)
                        .build();

        assertTrue(newOptions.hasMaxUnconfirmedMessages());
        assertFalse(newOptions.hasMaxUnconfirmedBytes());
        assertTrue(newOptions.hasConsumerPriority());
        assertTrue(newOptions.hasSuspendsOnBadHostHealth());

        assertEquals(1500, newOptions.getMaxUnconfirmedMessages());
        assertEquals(
                QueueOptions.k_MAX_UNCONFIRMED_BYTES_DEFAULT, newOptions.getMaxUnconfirmedBytes());
        assertEquals(2, newOptions.getConsumerPriority());
        assertTrue(newOptions.getSuspendsOnBadHostHealth());

        // Different hash
        assertNotEquals(baseOptions, newOptions);
        assertNotEquals(baseOptions.hashCode(), newOptions.hashCode());
    }
}
