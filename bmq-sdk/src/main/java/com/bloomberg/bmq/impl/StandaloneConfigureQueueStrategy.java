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

import com.bloomberg.bmq.QueueOptions;
import com.bloomberg.bmq.ResultCodes.ConfigureQueueCode;
import com.bloomberg.bmq.ResultCodes.ConfigureQueueResult;
import com.bloomberg.bmq.ResultCodes.GenericResult;
import com.bloomberg.bmq.impl.events.QueueControlEvent;
import com.bloomberg.bmq.impl.infr.proto.RequestManager;
import com.bloomberg.bmq.impl.intf.BrokerConnection;
import com.bloomberg.bmq.impl.intf.QueueState;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandaloneConfigureQueueStrategy extends ConfigureQueueStrategy {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    protected ConfigureQueueCode preconditionCheck() {
        QueueImpl queue = getQueue();
        if (queue.getState() != QueueState.e_OPENED) {
            logger.error("Queue is not opened.");
            return ConfigureQueueResult.INVALID_QUEUE;
        }
        if (queue.getStrategy() != null) {
            logger.error("Queue is in process and busy with other protocol sequences.");
            return ConfigureQueueResult.ALREADY_IN_PROGRESS;
        }
        return ConfigureQueueResult.SUCCESS;
    }

    @Override
    protected void startSequence() {
        // Check if queue is suspendable and host is not healthy
        if (getQueueOptions().getSuspendsOnBadHostHealth() && !getHealthManager().isHostHealthy()) {
            // Store options and return SUCCESS result code.
            // When host becomes back to healthy, a resume request with the
            // the options will be sent to the broker.
            completeConfiguring(GenericResult.SUCCESS);
            return;
        }

        // Send configure request
        sendConfigureRequest();
    }

    @Override
    protected void completeConfiguring(GenericResult result) {
        if (result.isSuccess()) {
            // Consider stream params as applied on the broker side and store in queue instance
            storeOptionsToQueue();
        }
        resultHook(result);
    }

    @Override
    protected QueueControlEvent.Type getQueueControlEventType() {
        return QueueControlEvent.Type.e_QUEUE_CONFIGURE_RESULT;
    }

    public StandaloneConfigureQueueStrategy(
            QueueImpl queue,
            Duration timeout,
            QueueOptions queueOptions,
            boolean isAsync,
            BrokerConnection brokerConnection,
            RequestManager requestManager,
            QueueStateManager queueStateManager,
            BrokerSession.Enqueuer enqueuer,
            ScheduledExecutorService scheduler,
            BrokerSession.HealthManager healthManager,
            CompletableFuture<ConfigureQueueCode> future) {

        super(
                queue,
                timeout,
                queueOptions,
                isAsync,
                brokerConnection,
                requestManager,
                queueStateManager,
                enqueuer,
                scheduler,
                healthManager,
                future);
    }
}
