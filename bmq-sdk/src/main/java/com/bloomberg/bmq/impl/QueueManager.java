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

import com.bloomberg.bmq.Uri;
import com.bloomberg.bmq.impl.infr.util.Argument;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link com.bloomberg.bmq.impl.QueueManager} Provides a mechanism to manage queues.
 *
 * <p>Thread safety. Thread-safe class.
 *
 * <p>This component defines a mechanism, {@link com.bloomberg.bmq.impl.QueueManager}, to simplify
 * queue management and interactions with the BlazingMQ broker. It also defines
 * 'bmqimp::QueueManager_QueueInfo'.
 */
@ThreadSafe
public class QueueManager {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Object lock;
    private Map<String, Integer> uriSubStreamCount;
    private Map<String, QueueInfo> uriMap;
    private Map<QueueId, QueueImpl> keyQueueIdMap;
    private Map<Integer, QueueImpl> subscriptionIdMap;
    private Map<QueueId, QueueImpl> expiredQueueMap;
    private AtomicInteger nextQueueId;

    private QueueManager() {
        uriSubStreamCount = new HashMap<>();
        uriMap = new TreeMap<>();
        keyQueueIdMap = new HashMap<>();
        subscriptionIdMap = new HashMap<>();
        expiredQueueMap = new HashMap<>();
        lock = new Object();
        nextQueueId = new AtomicInteger(0);
    }

    /** Reset queue manager state. */
    public void reset() {
        synchronized (lock) {
            uriSubStreamCount = new HashMap<>();
            uriMap = new TreeMap<>();
            keyQueueIdMap = new HashMap<>();
            subscriptionIdMap = new HashMap<>();
            expiredQueueMap = new HashMap<>();
            nextQueueId = new AtomicInteger(0);
        }
    }

    /**
     * Create instance of queue manager.
     *
     * @return QueueManager instance
     */
    public static QueueManager createInstance() {
        return new QueueManager();
    }

    /**
     * Insert QueueImpl object in map of active queue.
     *
     * <p>Thread safe.
     *
     * @param queue non-null queue object to be inserted in active queues map.
     * @return 'true' on success, 'false' on failure.
     */
    public boolean insert(QueueImpl queue) {
        synchronized (lock) {
            Uri uri = queue.getUri();
            String uriString = uri.canonical();
            QueueInfo queueInfo = uriMap.get(uriString);
            if (queueInfo == null) {
                queueInfo = new QueueInfo(queue.getQueueId());
            }
            uriMap.put(uriString, queueInfo);
            QueueId queueId = queue.getFullQueueId();
            keyQueueIdMap.put(queueId, queue);

            for (Integer sId : queue.getParameters().getSubscriptions().keySet()) {
                subscriptionIdMap.put(sId, queue);
            }

            Map<String, Integer> subQueueIdsMap = queueInfo.getSubQueueIdsMap();
            Integer subQueueId = subQueueIdsMap.get(uri.id());
            if (subQueueId != null) {
                return false;
            }
            subQueueIdsMap.put(uri.id(), queue.getSubQueueId());
        }
        return true;
    }

    public boolean update(QueueImpl queue) {
        synchronized (lock) {
            for (Integer sId : queue.getParameters().getSubscriptions().keySet()) {
                subscriptionIdMap.put(sId, queue);
            }
        }
        return true;
    }

    /**
     * Insert QueueImpl object in map of expired queues.
     *
     * <p>Thread safe.
     *
     * @param queue non-null queue object to be inserted in expired queues map.
     * @return 'true' on success, 'false' on failure.
     */
    public boolean insertExpired(QueueImpl queue) {
        QueueId queueId = queue.getFullQueueId();
        synchronized (lock) {
            if (findExpiredByQueueId(queueId) != null) {
                return false;
            }
            expiredQueueMap.put(queueId, queue);
        }
        return true;
    }

    /**
     * Remove QueueImpl object from map of active queues.
     *
     * <p>Thread safe.
     *
     * @param queue non-null queue object to be removed from active queues map.
     * @return 'true' on success, 'false' on failure.
     */
    public boolean remove(QueueImpl queue) {
        Argument.expectNonNull(queue, "queue");
        Argument.expectNonNull(queue.getUri(), "queue uri");
        Argument.expectNonNull(queue.getUri().canonical(), "queue uri canonical");

        synchronized (lock) {
            // TODO: logic for multiple subQueues
            QueueInfo qInfo = uriMap.get(queue.getUri().canonical());
            if (qInfo == null) {
                return false;
            }
            Integer subQueueId = qInfo.getSubQueueIdsMap().remove(queue.getUri().id());
            if (subQueueId == null) {
                return false;
            }
            if (subQueueId != queue.getSubQueueId()) {
                throw new IllegalStateException(
                        "Unknown SubQueueId: " + subQueueId + " " + queue.getSubQueueId());
            }
            if (qInfo.getSubQueueIdsMap().isEmpty()) {
                qInfo = uriMap.remove(queue.getUri().canonical());
                if (qInfo == null) {
                    throw new IllegalStateException(
                            "QueueInfo not found by URI: " + queue.getUri().canonical());
                }
            }
            QueueId queueKey = queue.getFullQueueId();
            QueueImpl qHandle = keyQueueIdMap.remove(queueKey);
            if (qHandle == null) {
                throw new IllegalStateException("Queue not found by QueueId: " + queueKey);
            }

            for (Integer sId : queue.getParameters().getSubscriptions().keySet()) {
                subscriptionIdMap.remove(sId);
            }

            if (qInfo.getQueueId() != qHandle.getQueueId()) {
                throw new IllegalStateException(
                        String.format(
                                "Wrong QueueIds: %d != %d",
                                qInfo.getQueueId(), qHandle.getQueueId()));
            }
        }
        return true;
    }

    /**
     * Remove QueueImpl object from map of expired queues.
     *
     * <p>Thread safe.
     *
     * @param queue non-null queue object to be removed from expired queues map.
     * @return 'true' on success, 'false' on failure.
     */
    public boolean removeExpired(QueueImpl queue) {
        Argument.expectNonNull(queue, "queue");

        synchronized (lock) {
            QueueId queueKey = queue.getFullQueueId();
            QueueImpl qHandle = expiredQueueMap.remove(queueKey);
            if (qHandle == null) {
                return false;
            }
            if (queue.getQueueId() != qHandle.getQueueId()) {
                throw new IllegalStateException(
                        String.format(
                                "Wrong QueueIds: %d != %d",
                                queue.getQueueId(), qHandle.getQueueId()));
            }
        }
        return true;
    }

    /**
     * Find queue object among active queues by queue id.
     *
     * <p>Thread safe.
     *
     * @param queueId queue id used for search
     * @return QueueImpl object if it is found, otherwise null.
     */
    public QueueImpl findByQueueId(QueueId queueId) {
        synchronized (lock) {
            return keyQueueIdMap.get(queueId);
        }
    }

    public QueueImpl findBySubscriptionId(int subscriptionId) {
        synchronized (lock) {
            return subscriptionIdMap.get(subscriptionId);
        }
    }

    /**
     * Find queue object among active queues by uri.
     *
     * <p>Thread safe.
     *
     * @param uri URI used for search
     * @return QueueImpl object if it is found, otherwise null.
     */
    public QueueImpl findByUri(Uri uri) {
        Argument.expectNonNull(uri, "uri");

        synchronized (lock) {
            QueueInfo queueInfo = uriMap.get(uri.canonical());
            if (queueInfo == null) {
                return null;
            }
            Integer queueSubId = queueInfo.getSubQueueIdsMap().get(uri.id());
            if (queueSubId == null) {
                return null;
            }
            int qId = queueInfo.getQueueId();
            QueueId queueId = QueueId.createInstance(qId, queueSubId);
            return findByQueueId(queueId);
        }
    }

    /**
     * Find queue object among expired queues by queue id.
     *
     * <p>Thread safe.
     *
     * @param queueId queue id used for search
     * @return QueueImpl object if it is found, otherwise null.
     */
    public QueueImpl findExpiredByQueueId(QueueId queueId) {
        synchronized (lock) {
            return expiredQueueMap.get(queueId);
        }
    }

    /**
     * Generate unique queue id in context of given broker session.
     *
     * <p>Thread safe.
     *
     * @param uri URI of corresponding quue
     * @return QueueId object
     */
    public QueueId generateNextQueueId(Uri uri) {
        // TODO: implement complete logic
        QueueInfo qInfo = uriMap.get(uri.canonical());
        int subQueueId = 0;
        if (qInfo != null) {
            if (!uri.id().isEmpty()) {
                subQueueId = qInfo.getNextSubQueueId();
            }
            return QueueId.createInstance(qInfo.getQueueId(), subQueueId);
        } else {
            if (!uri.id().isEmpty()) {
                subQueueId = QueueInfo.INITIAL_CONSUMER_SUBQUEUE_ID;
            }
            int queueId = nextQueueId.getAndIncrement();
            return QueueId.createInstance(queueId, subQueueId);
        }
    }

    /**
     * Generate list of currently opened queues among active queues.
     *
     * <p>Thread safe.
     *
     * @return list of opened queues
     */
    public Collection<QueueImpl> getOpenedQueues() {
        synchronized (lock) {
            return keyQueueIdMap.values().stream()
                    .filter(QueueImpl::isOpened)
                    .collect(Collectors.toList());
        }
    }

    /**
     * Generate list of all tracked queues.
     *
     * <p>Thread safe.
     *
     * @return list of all queues
     */
    public Collection<QueueImpl> getAllQueues() {
        synchronized (lock) {
            return keyQueueIdMap.values();
        }
    }

    // In theory, increment should be part of insert operation and
    // decrement should be part of remove, but since queue removing is done earlier
    // than sending close request, substreamcount updating is done
    // independently. This may be refactored later

    /**
     * Increment substream count for canonical queue uri
     *
     * <p>Thread safe.
     *
     * @param uri non-null canonical queue uri
     */
    public void incrementSubStreamCount(String uri) {
        Argument.expectNonNull(uri, "uri");
        synchronized (lock) {
            uriSubStreamCount.merge(uri, 1, Integer::sum);
        }
    }

    /**
     * Decrement substream count for canonical queue uri
     *
     * <p>Thread safe.
     *
     * @param uri non-null canonical queue uri
     */
    public void decrementSubStreamCount(String uri) {
        // todo check increment for subscriptions
        Argument.expectNonNull(uri, "uri");
        synchronized (lock) {
            Integer currentValue = uriSubStreamCount.get(uri);
            if (currentValue == null) {
                throw new IllegalStateException("There are no substreams for such uri");
            }

            if (currentValue == 1) {
                uriSubStreamCount.remove(uri);
            } else {
                uriSubStreamCount.put(uri, currentValue - 1);
            }
        }
    }

    /**
     * Returns substream count for canonical queue uri
     *
     * <p>Thread safe.
     *
     * @param uri non-null canonical queue uri
     * @return substream count for canonical queue uri
     */
    public int getSubStreamCount(String uri) {
        Argument.expectNonNull(uri, "uri");
        synchronized (lock) {
            Integer currentValue = uriSubStreamCount.get(uri);
            if (currentValue == null) {
                return 0;
            } else {
                return currentValue;
            }
        }
    }

    /**
     * Resets substream count for canonical queue uri
     *
     * <p>Thread safe.
     *
     * @param uri non-null canonical queue uri
     */
    public void resetSubStreamCount(String uri) {
        Argument.expectNonNull(uri, "uri");
        synchronized (lock) {
            uriSubStreamCount.remove(uri);
        }
    }
}
