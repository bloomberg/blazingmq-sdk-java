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

import java.util.Map;
import java.util.TreeMap;

public class QueueInfo {

    private Map<String, Integer> subQueueIdsMap;
    // Map of appId to associated
    // subQueueId
    private final int queueId;
    // The unique queueId associated with
    // the canonical URI corresponding to
    // this object

    private int nextSubQueueId;
    // The next unique subQueueId
    // associated with the canonical URI
    // corresponding to this object and an
    // appId

    private int subStreamCount;
    // The number of subStreams associated
    // with the canonical URI corresponding
    // to this object

    public static final int INITIAL_CONSUMER_SUBQUEUE_ID = 1;

    public QueueInfo(int queueId) {
        this.queueId = queueId;
        subQueueIdsMap = new TreeMap<>();
        nextSubQueueId = INITIAL_CONSUMER_SUBQUEUE_ID;
        subStreamCount = 0;
    }

    public Map<String, Integer> getSubQueueIdsMap() {
        return subQueueIdsMap;
    }

    public int getQueueId() {
        return queueId;
    }

    public int getNextSubQueueId() {
        return ++nextSubQueueId;
    }

    public void setNextSubQueueId(int nextSubQueueId) {
        this.nextSubQueueId = nextSubQueueId;
    }

    public int getSubStreamCount() {
        return subStreamCount;
    }

    public void setSubStreamCount(int subStreamCount) {
        this.subStreamCount = subStreamCount;
    }
}
