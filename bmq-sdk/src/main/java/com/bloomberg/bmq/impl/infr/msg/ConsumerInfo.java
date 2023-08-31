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
package com.bloomberg.bmq.impl.infr.msg;

public class ConsumerInfo {
    private long maxUnconfirmedMessages;
    private long maxUnconfirmedBytes;
    private int consumerPriority;
    private int consumerPriorityCount;

    public ConsumerInfo() {
        init();
    }

    public void reset() {
        init();
    }

    private void init() {
        maxUnconfirmedMessages = 0L;
        maxUnconfirmedBytes = 0L;
        consumerPriority = 0;
        consumerPriorityCount = 0;
    }

    public long maxUnconfirmedMessages() {
        return maxUnconfirmedMessages;
    }

    public void setMaxUnconfirmedMessages(long val) {
        maxUnconfirmedMessages = val;
    }

    public long maxUnconfirmedBytes() {
        return maxUnconfirmedBytes;
    }

    public void setMaxUnconfirmedBytes(long val) {
        maxUnconfirmedBytes = val;
    }

    public int consumerPriority() {
        return consumerPriority;
    }

    public void setConsumerPriority(int val) {
        consumerPriority = val;
    }

    public int consumerPriorityCount() {
        return consumerPriorityCount;
    }

    public void setConsumerPriorityCount(int val) {
        consumerPriorityCount = val;
    }

    public Object createNewInstance() {
        return new ConsumerInfo();
    }
}
