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
import com.bloomberg.bmq.impl.infr.proto.RequestManager;
import com.bloomberg.bmq.impl.intf.BrokerConnection;
import com.bloomberg.bmq.impl.intf.QueueState;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResumeQueueStrategy extends ConfigureQueueStrategy {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final boolean isReconfiguration;

    @Override
    protected ConfigureQueueCode preconditionCheck() {
        QueueImpl queue = getQueue();
        Uri uri = queue.getUri();
        // Queue should be opened. If queue is closing or closed there is no
        // need to resume it. If queue is opening then there are two options:
        // - queue is waiting for open response.
        //   When it is received and the host is still healthy then configure
        //   request will be sent by 'OpenQueueStrategy'.
        // - queue is waiting for configure response.
        //   When it is received and the host is still healthy then there is
        //   no need to send resume request.
        if (queue.getState() != QueueState.e_OPENED) {
            logger.error("Queue '{}' is not opened to be resumed", uri);
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

        // NOTE: we may resume the queue that is not health sensitive in case
        // the host is unhealthy and user is reconfiguring the queue from
        // health sensitive to health insensitive.

        // Queue may be already resumed if host health has changed to healthy
        // during incomplete healthy->unhealthy transition. Also queue suspension
        // could be canceled due to connection lost or close queue operation.
        // In this case we need to check if we need to issue HOST_HEALTH_RESTORED event.
        if (!queue.getIsSuspendedWithBroker()) {
            logger.error("Queue '{}' is not suspended", uri);
            return ConfigureQueueResult.INVALID_QUEUE;
        }

        return ConfigureQueueResult.SUCCESS;
    }

    @Override
    protected void startSequence() {
        logger.info(
                "Queue '{}' is going to be resumed{}",
                getQueue().getUri(),
                isReconfiguration ? " (reconfiguration)" : "");

        // Increment number of Host Health requests outstanding.
        getHealthManager().onHealthRequest();

        // Send resume request
        sendConfigureRequest();
    }

    @Override
    protected void completeConfiguring(GenericResult result) {
        logger.info("Queue '{}' resuming completed: {}", getQueue().getUri(), result);

        // Decrement number of Host Health requests outstanding.
        getHealthManager().onHealthResponse();

        // Save new options in case of success response.
        if (result.isSuccess()) {
            storeOptionsToQueue();
        }

        // Reset the 'isSuspendedWithBroker' flag.
        getQueue().setIsSuspendedWithBroker(false);

        // If this is sync resume strategy then there will be no user event
        // issued so we need to reset 'isSuspended' flag here.  When the
        // control returns to user thread, the queue is already resumed.
        if (!getIsAsync()) {
            getQueue().setIsSuspended(false);
        }

        // New PUTs will be allowed upon user receives the queue resuming event
        resultHook(result);

        // If there are no more outstanding requests, AND the host remains
        // healthy, then issue a HOST_HEALTH_RESTORED event.
        getHealthManager().checkHostIsStableHealthy();
    }

    @Override
    protected void enqueueEvent(ConfigureQueueCode code) {
        // Enqueue events only if queue is not closing
        if (getIsCanceledOnQueueClosing()) {
            return;
        }

        // Enqueue RESUMED event
        super.enqueueEvent(code);

        // If reconfiguration, also enqueue CONFIGURE_RESULT event
        if (isReconfiguration) {
            enqueueEvent(code, QueueControlEvent.Type.e_QUEUE_CONFIGURE_RESULT);
        }
    }

    @Override
    protected QueueControlEvent.Type getQueueControlEventType() {
        // 'QueueHandler.isSuspended()' property will be set to false in
        // 'QueueControlEvent.Type.handleQueueResumedResult` method when
        // dispatching the event to user
        return QueueControlEvent.Type.e_QUEUE_RESUME_RESULT;
    }

    public ResumeQueueStrategy(
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
                        && queue.getQueueOptions().getSuspendsOnBadHostHealth()
                        && !queueOptions.getSuspendsOnBadHostHealth();
    }
}
