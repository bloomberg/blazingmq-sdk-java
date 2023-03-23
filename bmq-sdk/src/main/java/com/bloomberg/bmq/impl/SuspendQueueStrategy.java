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
import com.bloomberg.bmq.Uri;
import com.bloomberg.bmq.impl.events.QueueControlEvent;
import com.bloomberg.bmq.impl.infr.msg.QueueStreamParameters;
import com.bloomberg.bmq.impl.infr.msg.SubQueueIdInfo;
import com.bloomberg.bmq.impl.infr.proto.RequestManager;
import com.bloomberg.bmq.impl.intf.BrokerConnection;
import com.bloomberg.bmq.impl.intf.QueueState;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SuspendQueueStrategy extends ConfigureQueueStrategy {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final boolean isReconfiguration;

    @Override
    protected ConfigureQueueCode preconditionCheck() {
        QueueImpl queue = getQueue();
        Uri uri = queue.getUri();
        // Queue should be opened. If queue is closing or closed there is no
        // need to suspend it. If queue is opening then there are two options:
        // - queue is waiting for open response.
        //   When it is received and the host is still unhealthy then configure
        //   request will be skipped and the the queue will go directly to
        //   suspended state.
        // - queue is waiting for configure response.
        //   When it is received and the host is still unhealthy then suspend
        //   request will be sent.
        if (queue.getState() != QueueState.e_OPENED) {
            logger.error("Queue '{}' is not opened to be suspended", uri);
            return ConfigureQueueResult.INVALID_QUEUE;
        }

        // There should be no running strategy. Since we take opened queues,
        // the running strategy could be standalone configure, suspend or
        // resume request. When response is received, corresponding suspend or
        // resume request will be sent if necessary (deferred request).
        if (queue.getStrategy() != null) {
            logger.error("Queue '{}' is in process and busy with other protocol sequences.", uri);
            return ConfigureQueueResult.ALREADY_IN_PROGRESS;
        }

        // Queue is not sensitive to host health changes.
        if (!getQueueOptions().getSuspendsOnBadHostHealth()) {
            logger.error("Queue '{}' is not sensitive to host health.", uri);
            return ConfigureQueueResult.INVALID_QUEUE;
        }

        // Queue may be already suspended if:
        // - while opening queue, host health has changed to unhealthy and
        //   configure request has been skipped
        // - host health has changed to unhealthy during incomplete
        //   unhealthy->healthy transition
        if (queue.getIsSuspendedWithBroker()) {
            logger.error("Queue '{}' is already suspended", uri);
            return ConfigureQueueResult.INVALID_QUEUE;
        }

        return ConfigureQueueResult.SUCCESS;
    }

    @Override
    protected void startSequence() {
        logger.info(
                "Queue '{}' is going to be suspended{}",
                getQueue().getUri(),
                isReconfiguration ? " (reconfiguration)" : "");

        // Increment number of Host Health requests outstanding.
        getHealthManager().onHealthRequest();

        // Send suspend request
        sendConfigureRequest();
    }

    @Override
    protected QueueStreamParameters createParameters(SubQueueIdInfo subQueueIdInfo) {
        // For suspend request we use deconfigure parameters regardless of
        // provided queue options which could be used later by resume request
        // when host health changes to healthy state.
        int qId = getQueue().getQueueId();
        return QueueStreamParameters.createDeconfigureParameters(qId, subQueueIdInfo);
    }

    @Override
    protected void completeConfiguring(GenericResult result) {
        logger.info("Queue '{}' suspension completed: {}", getQueue().getUri(), result);

        // Decrement number of Host Health requests outstanding.
        // A `resume` request may fire a HOST_HEALTH_RESTORED event only
        // when there is no more pending host health requests.
        getHealthManager().onHealthResponse();

        // Save new options in case of success response.
        if (result.isSuccess()) {
            storeOptionsToQueue();
        }

        // Set the 'isSuspendedWithBroker' flag.
        getQueue().setIsSuspendedWithBroker(true);

        // If this is sync suspend strategy then there will be no user event
        // issued so we need to set 'isSuspended' flag here.  When the
        // control returns to user thread, the queue is already suspended.
        if (!getIsAsync()) {
            getQueue().setIsSuspended(true);
        }

        // New PUTs will be blocked to queue upon user receives the queue
        // suspension event
        resultHook(result);

        // Drop the channel and initiate reconnecting in case of error response.
        if (!result.isSuccess() && !result.isCanceled()) {
            getConnection().drop();
        }

        // If the request was canceled, there are no more outstanding requests
        // and the host is healthy, then issue a HOST_HEALTH_RESTORED event.
        if (result.isCanceled()) {
            getHealthManager().checkHostIsStableHealthy();
        }
    }

    @Override
    protected void enqueueEvent(ConfigureQueueCode code) {
        // Enqueue events only if queue is not closing
        if (getIsCanceledOnQueueClosing()) {
            return;
        }

        // Enqueue SUSPENDED event
        super.enqueueEvent(code);

        // If reconfiguration, also enqueue CONFIGURE_RESULT event
        if (isReconfiguration) {
            enqueueEvent(code, QueueControlEvent.Type.e_QUEUE_CONFIGURE_RESULT);
        }
    }

    @Override
    protected QueueControlEvent.Type getQueueControlEventType() {
        // 'QueueHandle.isSuspended()' property will be set to true in
        // 'QueueControlEvent.Type.handleQueueSuspendResult` method
        // when dispatching the event to user
        return QueueControlEvent.Type.e_QUEUE_SUSPEND_RESULT;
    }

    public SuspendQueueStrategy(
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

        isReconfiguration =
                queue.getQueueOptions() != null
                        && !queue.getQueueOptions().getSuspendsOnBadHostHealth()
                        && queueOptions.getSuspendsOnBadHostHealth();
    }
}
