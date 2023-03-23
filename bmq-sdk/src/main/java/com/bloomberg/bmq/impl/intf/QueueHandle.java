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
package com.bloomberg.bmq.impl.intf;

import com.bloomberg.bmq.QueueOptions;
import com.bloomberg.bmq.ResultCodes.CloseQueueCode;
import com.bloomberg.bmq.ResultCodes.ConfigureQueueCode;
import com.bloomberg.bmq.ResultCodes.OpenQueueCode;
import com.bloomberg.bmq.Uri;
import com.bloomberg.bmq.impl.BmqFuture;
import com.bloomberg.bmq.impl.QueueId;
import com.bloomberg.bmq.impl.events.QueueControlEvent;
import com.bloomberg.bmq.impl.infr.msg.QueueHandleParameters;
import com.bloomberg.bmq.impl.infr.proto.AckMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.PushMessageImpl;
import java.time.Duration;

public interface QueueHandle {
    int getQueueId();

    int getSubQueueId();

    QueueId getFullQueueId();

    QueueState getState();

    Uri getUri();

    QueueHandleParameters getParameters();

    OpenQueueCode open(QueueOptions queueOptions, Duration duration);

    BmqFuture<OpenQueueCode> openAsync(QueueOptions queueOptions, Duration duration);

    ConfigureQueueCode configure(QueueOptions queueOptions, Duration duration);

    BmqFuture<ConfigureQueueCode> configureAsync(QueueOptions queueOptions, Duration duration);

    CloseQueueCode close(Duration duration);

    BmqFuture<CloseQueueCode> closeAsync(Duration duration);

    void handleAckMessage(AckMessageImpl msg);

    void handlePushMessage(PushMessageImpl msg);

    void handleQueueEvent(QueueControlEvent ev);

    boolean getIsSuspended();

    void setIsSuspended(boolean value);
}
