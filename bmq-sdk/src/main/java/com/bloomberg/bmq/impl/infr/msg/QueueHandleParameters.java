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

import com.bloomberg.bmq.QueueFlags;
import com.bloomberg.bmq.Uri;
import java.util.HashMap;
import java.util.Map;

public class QueueHandleParameters {
    private String uri;
    private Integer qId;
    private SubQueueIdInfo subIdInfo;
    private Long flags;
    private Integer readCount;
    private Integer writeCount;
    private Integer adminCount;
    private transient Map<Integer, Subscription> subscriptions;

    public QueueHandleParameters(QueueHandleParameters copied) {
        uri = copied.uri;
        qId = copied.qId;
        subIdInfo = copied.subIdInfo;
        flags = copied.flags;
        readCount = copied.readCount;
        writeCount = copied.writeCount;
        adminCount = copied.adminCount;
        subscriptions = copied.subscriptions;
    }

    public QueueHandleParameters() {
        init();
    }

    public void reset() {
        init();
    }

    private void init() {
        uri = null;
        qId = 0;
        subIdInfo = null;
        flags = 0L;
        readCount = 0;
        writeCount = 0;
        adminCount = 0;
        subscriptions = new HashMap<>();
    }

    public Uri getUri() {
        return new Uri(uri);
    }

    public QueueHandleParameters setUri(Uri value) {
        if (value != null) {
            uri = value.canonical();
        } else {
            uri = null;
        }
        return this;
    }

    public int getQId() {
        return qId;
    }

    public QueueHandleParameters setQId(int id) {
        qId = id;
        return this;
    }

    public SubQueueIdInfo getSubIdInfo() {
        return subIdInfo;
    }

    public QueueHandleParameters setSubIdInfo(SubQueueIdInfo value) {
        subIdInfo = value;
        return this;
    }

    public Map<Integer, Subscription> getSubscriptions() {
        return subscriptions;
    }

    public QueueHandleParameters setSubscriptions(Subscription[] values) {
        subscriptions = new HashMap<>();
        for (Subscription sb : values) {
            subscriptions.put(sb.id(), sb);
        }
        return this;
    }

    public Long getFlags() {
        return flags;
    }

    public QueueHandleParameters setFlags(Long value) {
        flags = value;
        if (QueueFlags.isReader(flags)) {
            setReadCount(1);
        }
        if (QueueFlags.isWriter(flags)) {
            setWriteCount(1);
        }
        if (QueueFlags.isAdmin(flags)) {
            setAdminCount(1);
        }
        return this;
    }

    public int getReadCount() {
        return readCount;
    }

    public QueueHandleParameters setReadCount(int value) {
        readCount = value;
        return this;
    }

    public int getWriteCount() {
        return writeCount;
    }

    public QueueHandleParameters setWriteCount(int value) {
        writeCount = value;
        return this;
    }

    public int getAdminCount() {
        return adminCount;
    }

    public QueueHandleParameters setAdminCount(int value) {
        adminCount = value;
        return this;
    }

    public QueueHandleParameters createNewInstance() {
        return new QueueHandleParameters();
    }
}
