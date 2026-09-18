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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import com.bloomberg.bmq.Uri;
import java.lang.invoke.MethodHandles;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueManagerTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    final BrokerSession session;
    final String uriStr1, uriStr2, uriStr3;
    final Uri uri1, uri2, uri3;
    final QueueId wrongQueueKey = QueueId.createInstance(9, 9);

    public QueueManagerTest() {
        session = mock(BrokerSession.class);
        uriStr1 = "bmq://ts.trades.myapp1/my.queue";
        uriStr2 = "bmq://ts.trades.myapp1/my.queue?id=my.app2";
        uriStr3 = "bmq://ts.trades.myapp2/my.queue?id=my.app1";
        uri1 = new Uri(uriStr1);
        uri2 = new Uri(uriStr2);
        uri3 = new Uri(uriStr3);
    }

    static QueueImpl createQueue(BrokerSession session, Uri uri, long flags) {
        return new QueueImpl(session, uri, flags, null, null, null);
    }

    /**
     * Test for queue manager to test addition of queue to active queues collection.
     *
     * <p>Test steps:
     *
     * <ol>
     *   <li>create queue manager instance
     *   <li>create queue instance
     *   <li>start broker session
     *   <li>insert queue via insert method
     *   <li>check that method returned 'true'
     *   <li>repeat insertion of the same queue
     *   <li>check that method returned 'false'
     *   <li>check that queue can be find via findByQueueId, findByCorrelationId, findByUri methods
     *   <li>check that queue can not be find via findExpiredByQueueId method
     *   <li>repeat previous steps for different uri with the same domain and different appId
     *   <li>check that queue can not be find by incorrect queueId, correlationId, Uri
     * </ol>
     */
    @Test
    void insertActiveQueueTest() {
        logger.info("==================================================================");
        logger.info("BEGIN Testing QueueManager insertActiveQueueTest.");
        logger.info("==================================================================");

        QueueManager obj = QueueManager.createInstance();
        QueueImpl queue1 = createQueue(session, uri1, 0L);
        QueueImpl queue2 = createQueue(session, uri2, 0L);

        QueueId queueKey1 = obj.generateNextQueueId(uri1);
        queue1.setQueueId(queueKey1.getQId()).setSubQueueId(queueKey1.getSubQId());
        assertTrue(obj.insert(queue1));
        assertFalse(obj.insert(queue1));
        assertEquals(queue1, obj.findByQueueId(queueKey1));
        assertEquals(queue1, obj.findByUri(uri1));

        QueueId queueKey2 = obj.generateNextQueueId(uri2);
        queue2.setQueueId(queueKey2.getQId()).setSubQueueId(queueKey2.getSubQId());
        assertTrue(obj.insert(queue2));
        assertFalse(obj.insert(queue2));
        assertEquals(queue2, obj.findByQueueId(queueKey2));
        assertEquals(queue2, obj.findByUri(uri2));

        assertNull(obj.findExpiredByQueueId(queueKey1));
        assertNull(obj.findExpiredByQueueId(queueKey2));

        assertNull(obj.findByQueueId(wrongQueueKey));
        assertNull(obj.findByUri(uri3));

        logger.info("==================================================================");
        logger.info("END Testing QueueManager insertActiveQueueTest.");
        logger.info("==================================================================");
    }

    /**
     * Test for queue manager to test removal of queue from active queues collection.
     *
     * <p>Test steps:
     *
     * <ol>
     *   <li>Repeat test steps of insertActiveQueueTest as pre-condition for testing of removal.
     *   <li>remove queue from active queues collection via remove method
     *   <li>check that method returned 'true'
     *   <li>repeat removal of the same queue
     *   <li>check that method returned 'false'
     *   <li>check that queue can not be found via findByQueueId, findByCorrelationId, findByUri
     *       methods
     *   <li>check that queue can not be find via findExpiredByQueueId method
     *   <li>repeat previous steps for different uri with the same domain and different appId
     *   <li>check that queue can not be find by incorrect queueId, correlationId, Uri
     * </ol>
     */
    @Test
    void removeActiveQueueTest() {
        logger.info("==================================================================");
        logger.info("BEGIN Testing QueueManager removeActiveQueueTest.");
        logger.info("==================================================================");
        QueueManager obj = QueueManager.createInstance();
        QueueImpl queue1 = createQueue(session, uri1, 0L);
        QueueImpl queue2 = createQueue(session, uri2, 0L);

        QueueId queueKey1 = obj.generateNextQueueId(uri1);
        queue1.setQueueId(queueKey1.getQId()).setSubQueueId(queueKey1.getSubQId());
        assertTrue(obj.insert(queue1));
        assertFalse(obj.insert(queue1));
        assertEquals(queue1, obj.findByQueueId(queueKey1));
        assertEquals(queue1, obj.findByUri(uri1));

        QueueId queueKey2 = obj.generateNextQueueId(uri2);
        queue2.setQueueId(queueKey2.getQId()).setSubQueueId(queueKey2.getSubQId());
        assertTrue(obj.insert(queue2));
        assertFalse(obj.insert(queue2));
        assertEquals(queue2, obj.findByQueueId(queueKey2));
        assertEquals(queue2, obj.findByUri(uri2));

        assertNull(obj.findByQueueId(wrongQueueKey));
        assertNull(obj.findByUri(uri3));

        // Tested operation
        assertTrue(obj.remove(queue1));
        assertFalse(obj.remove(queue1));
        assertNull(obj.findByQueueId(queueKey1));
        assertNull(obj.findByUri(queue1.getUri()));
        assertNull(obj.findExpiredByQueueId(queueKey1));

        assertTrue(obj.remove(queue2));
        assertFalse(obj.remove(queue2));
        assertNull(obj.findByQueueId(queueKey2));
        assertNull(obj.findByUri(queue2.getUri()));
        assertNull(obj.findExpiredByQueueId(queueKey2));

        logger.info("==================================================================");
        logger.info("END Testing QueueManager removeActiveQueueTest.");
        logger.info("==================================================================");
    }

    /**
     * Test for queue manager to test addition of queue to expired queues collection.
     *
     * <p>Test steps:
     *
     * <ol>
     *   <li>create queue manager instance
     *   <li>create queue instance
     *   <li>start broker session
     *   <li>insert queue via insertExpired method
     *   <li>check that method returned 'true'
     *   <li>repeat insertion of the same queue
     *   <li>check that method returned 'false'
     *   <li>check that queue can be find via findExpiredByQueueId method
     *   <li>check that queue can not be found via findByQueueId, findByCorrelationId, findByUri
     *       methods
     *   <li>repeat previous steps for different uri with the same domain and different appId
     * </ol>
     */
    @Test
    void insertExpiredQueueTest() {
        logger.info("==================================================================");
        logger.info("BEGIN Testing QueueManager insertExpiredQueueTest.");
        logger.info("==================================================================");

        QueueManager obj = QueueManager.createInstance();

        QueueImpl queue1 = createQueue(session, uri1, 0L);
        QueueId queueKey1 = obj.generateNextQueueId(uri1);
        queue1.setQueueId(queueKey1.getQId()).setSubQueueId(queueKey1.getSubQId());
        assertTrue(obj.insertExpired(queue1));
        assertFalse(obj.insertExpired(queue1));
        assertEquals(queue1, obj.findExpiredByQueueId(queueKey1));
        assertNull(obj.findByQueueId(queueKey1));
        assertNull(obj.findByUri(queue1.getUri()));

        QueueImpl queue2 = createQueue(session, uri2, 0L);
        QueueId queueKey2 = obj.generateNextQueueId(uri2);
        queue2.setQueueId(queueKey2.getQId()).setSubQueueId(queueKey2.getSubQId());
        assertTrue(obj.insertExpired(queue2));
        assertFalse(obj.insertExpired(queue2));
        assertEquals(queue2, obj.findExpiredByQueueId(queueKey2));
        assertNull(obj.findByQueueId(queueKey2));
        assertNull(obj.findByUri(queue2.getUri()));

        logger.info("==================================================================");
        logger.info("END Testing QueueManager insertExpiredQueueTest.");
        logger.info("==================================================================");
    }

    /**
     * Test for queue manager to test removal of queue from expired queue collection.
     *
     * <p>Test steps:
     *
     * <ol>
     *   <li>Repeat test steps of insertExpiredQueueTest as pre-condition for testing of removal.
     *   <li>remove queue from expired queues collection via removeExpired method
     *   <li>check that method returned 'true'
     *   <li>repeat removal of the same queue
     *   <li>check that method returned 'false'
     *   <li>check that queue can not be found via findByQueueId, findByCorrelationId, findByUri
     *       methods
     *   <li>repeat previous steps for different uri with the same domain and different appId
     *   <li>check that queue can not be find via findExpiredByQueueId method
     *   <li>repeat previous step for different uri with the same domain and different appId
     * </ol>
     */
    @Test
    void removeExpiredQueueTest() {
        logger.info("==================================================================");
        logger.info("BEGIN Testing QueueManager removeExpiredQueueTest.");
        logger.info("==================================================================");
        QueueManager obj = QueueManager.createInstance();

        QueueImpl queue1 = createQueue(session, uri1, 0L);
        QueueId queueKey1 = obj.generateNextQueueId(uri1);
        queue1.setQueueId(queueKey1.getQId()).setSubQueueId(queueKey1.getSubQId());
        assertTrue(obj.insertExpired(queue1));
        assertFalse(obj.insertExpired(queue1));
        assertEquals(queue1, obj.findExpiredByQueueId(queueKey1));
        assertNull(obj.findByQueueId(queueKey1));
        assertNull(obj.findByUri(queue1.getUri()));

        QueueImpl queue2 = createQueue(session, uri2, 0L);
        QueueId queueKey2 = obj.generateNextQueueId(uri2);
        queue2.setQueueId(queueKey2.getQId()).setSubQueueId(queueKey2.getSubQId());
        assertTrue(obj.insertExpired(queue2));
        assertFalse(obj.insertExpired(queue2));
        assertEquals(queue2, obj.findExpiredByQueueId(queueKey2));
        assertNull(obj.findByQueueId(queueKey2));
        assertNull(obj.findByUri(queue2.getUri()));

        assertTrue(obj.removeExpired(queue1));
        assertFalse(obj.removeExpired(queue1));

        assertNull(obj.findExpiredByQueueId(queueKey1));
        assertNull(obj.findByQueueId(queueKey1));
        assertNull(obj.findByUri(queue1.getUri()));

        assertTrue(obj.removeExpired(queue2));
        assertFalse(obj.removeExpired(queue2));

        assertNull(obj.findExpiredByQueueId(queueKey2));
        assertNull(obj.findByQueueId(queueKey2));
        assertNull(obj.findByUri(queue2.getUri()));

        logger.info("==================================================================");
        logger.info("END Testing QueueManager removeExpiredQueueTest.");
        logger.info("==================================================================");
    }

    /**
     * Test that an expired queue is looked up by both its canonical URI and its appId.
     *
     * <p>The same appId may be used by queues with different canonical URIs, and each of them has
     * its own subQueueId. {@link LateResponseHandler} rebuilds a {@link QueueId} from the qId of a
     * late configure response and the subQueueId found by appId, so the lookup must give the
     * subQueueId of the queue the response belongs to.
     *
     * <p>Test steps:
     *
     * <ol>
     *   <li>create queue manager instance
     *   <li>insert two active queues of one canonical URI with appIds "foo" and "bar", so that
     *       "bar" gets subQueueId 2
     *   <li>insert an active queue of another canonical URI with appId "bar", which gets subQueueId
     *       1
     *   <li>move all three queues to expired queues, as a local timeout does
     *   <li>for each queue, rebuild a QueueId from its qId and the subQueueId found by its appId,
     *       the way LateResponseHandler does
     *   <li>check that every rebuilt QueueId resolves to the queue it was built from
     * </ol>
     */
    @Test
    void lookupByUriAndAppIdTest() {
        logger.info("====================================================");
        logger.info("BEGIN Testing QueueManager lookupByUriAndAppIdTest.");
        logger.info("====================================================");

        QueueManager obj = QueueManager.createInstance();

        Uri fooOfQueueA = new Uri("bmq://ts.trades.myapp/queueA?id=foo");
        Uri barOfQueueA = new Uri("bmq://ts.trades.myapp/queueA?id=bar");
        Uri barOfQueueB = new Uri("bmq://ts.trades.myapp/queueB?id=bar");

        QueueImpl queueAFoo = insertActive(obj, fooOfQueueA);
        QueueImpl queueABar = insertActive(obj, barOfQueueA);
        QueueImpl queueBBar = insertActive(obj, barOfQueueB);

        // Two appIds of one canonical URI, so the second one gets subQueueId 2.
        assertEquals(1, queueAFoo.getSubQueueId());
        assertEquals(2, queueABar.getSubQueueId());

        // Another canonical URI starts its subQueueIds anew.
        assertEquals(1, queueBBar.getSubQueueId());

        assertTrue(obj.insertExpired(queueAFoo));
        assertTrue(obj.insertExpired(queueABar));
        assertTrue(obj.insertExpired(queueBBar));

        assertEquals(queueAFoo, findExpiredByAppId(obj, queueAFoo));
        assertEquals(queueABar, findExpiredByAppId(obj, queueABar));
        assertEquals(queueBBar, findExpiredByAppId(obj, queueBBar));

        logger.info("==================================================");
        logger.info("END Testing QueueManager lookupByUriAndAppIdTest.");
        logger.info("==================================================");
    }

    private QueueImpl insertActive(QueueManager manager, Uri uri) {
        QueueImpl queue = createQueue(session, uri, 0L);
        QueueId queueId = manager.generateNextQueueId(uri);
        queue.setQueueId(queueId.getQId()).setSubQueueId(queueId.getSubQId());
        assertTrue(manager.insert(queue));
        return queue;
    }

    /** Resolve an expired queue the way LateResponseHandler does. */
    private QueueImpl findExpiredByAppId(QueueManager manager, QueueImpl queue) {
        Integer subQId = manager.findSubQId(queue.getQueueId(), queue.getUri().id());
        assertNotNull(subQId);
        return manager.findExpiredByQueueId(QueueId.createInstance(queue.getQueueId(), subQId));
    }

    /**
     * Test for queue manager to test increment queue substream count
     *
     * <p>Test steps:
     *
     * <ol>
     *   <li>create queue manager instance
     *   <li>check substream count operations with null argument
     *   <li>check substream count operations with not inserted uri
     *   <li>insert queue via insert method
     *   <li>check increment and get operations
     *   <li>check decrement and get operations
     *   <li>check reset operation
     * </ol>
     */
    @Test
    void subStreamCountTest() {
        logger.info("==============================================");
        logger.info("BEGIN Testing QueueManager subStreamCountTest.");
        logger.info("==============================================");

        QueueManager obj = QueueManager.createInstance();

        // Increment
        logger.info("Increment for null queue uri");
        try {
            obj.incrementSubStreamCount(null);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'uri' must be non-null", e.getMessage());
            logger.debug("Caught expected error: ", e);
        }

        // Get
        logger.info("Get for null queue uri");
        try {
            obj.getSubStreamCount(null);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'uri' must be non-null", e.getMessage());
            logger.debug("Caught expected error: ", e);
        }

        // Decrement
        logger.info("Decrement for null queue uri");
        try {
            obj.decrementSubStreamCount(null);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'uri' must be non-null", e.getMessage());
            logger.debug("Caught expected error: ", e);
        }

        // Reset
        logger.info("Reset for null queue uri");
        try {
            obj.resetSubStreamCount(null);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'uri' must be non-null", e.getMessage());
            logger.debug("Caught expected error: ", e);
        }
        String uriStr = "bmq://domain/queue";

        // Get
        logger.info("Get for uri without substreams (not incremented yet)");
        assertEquals(0, obj.getSubStreamCount(uriStr));

        // Decrement
        logger.info("Decrement for uri without substream (not inserted yet)");
        try {
            obj.decrementSubStreamCount(uriStr);
            fail(); // should not get here
        } catch (IllegalStateException e) {
            assertEquals("There are no substreams for such uri", e.getMessage());
            logger.debug("Caught expected error: ", e);
        }

        // Reset
        logger.info("Reset for uri without substreams (not incremented yet)");
        obj.resetSubStreamCount(uriStr);
        assertEquals(0, obj.getSubStreamCount(uriStr));

        // In theory, increment should be part of insert operation and the same
        // for decrement and remove, but since queue removing is done earlier
        // than sending close request, substreamcount updating is done
        // independently. This may be refactored later

        // Increment
        logger.info("Increment substream count");
        obj.incrementSubStreamCount(uriStr);

        assertEquals(1, obj.getSubStreamCount(uriStr));

        // Increment
        logger.info("Increment substream count");
        obj.incrementSubStreamCount(uriStr);

        assertEquals(2, obj.getSubStreamCount(uriStr));

        // Decrement
        logger.info("Decrement substream count");
        obj.decrementSubStreamCount(uriStr);

        assertEquals(1, obj.getSubStreamCount(uriStr));

        // Decrement
        logger.info("Decrement substream count");
        obj.decrementSubStreamCount(uriStr);

        assertEquals(0, obj.getSubStreamCount(uriStr));

        // Decrement
        logger.info("Decrement substream count when it's zero");
        try {
            obj.decrementSubStreamCount(uriStr);
            fail(); // should not get here
        } catch (IllegalStateException e) {
            assertEquals("There are no substreams for such uri", e.getMessage());
            logger.debug("Caught expected error: ", e);
        }

        assertEquals(0, obj.getSubStreamCount(uriStr));

        // Increment
        logger.info("Increment substream count");
        obj.incrementSubStreamCount(uriStr);

        assertEquals(1, obj.getSubStreamCount(uriStr));

        // Increment
        logger.info("Increment substream count");
        obj.incrementSubStreamCount(uriStr);

        assertEquals(2, obj.getSubStreamCount(uriStr));

        // Reset
        logger.info("Reset substream count");
        obj.resetSubStreamCount(uriStr);

        assertEquals(0, obj.getSubStreamCount(uriStr));

        logger.info("============================================");
        logger.info("END Testing QueueManager subStreamCountTest.");
        logger.info("============================================");
    }
}
