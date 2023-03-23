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
import com.bloomberg.bmq.ResultCodes.CloseQueueCode;
import com.bloomberg.bmq.ResultCodes.ConfigureQueueCode;
import com.bloomberg.bmq.ResultCodes.OpenQueueCode;
import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.impl.infr.proto.RequestManager;
import com.bloomberg.bmq.impl.intf.BrokerConnection;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public class QueueControlStrategyFactory {
    private final BrokerConnection brokerConnection;
    private final RequestManager requestManager;
    private final SessionOptions sessionOptions;
    private final QueueStateManager queueStateManager;
    private final BrokerSession.Enqueuer enqueuer;
    private final ScheduledExecutorService scheduler;
    private final BrokerSession.HealthManager healthManager;

    private QueueControlStrategyFactory(
            BrokerConnection brokerConnection,
            RequestManager requestManager,
            SessionOptions sessionOptions,
            QueueStateManager queueStateManager,
            BrokerSession.Enqueuer enqueuer,
            ScheduledExecutorService scheduler,
            BrokerSession.HealthManager healthManager) {
        this.brokerConnection = brokerConnection;
        this.requestManager = requestManager;
        this.sessionOptions = sessionOptions;
        this.queueStateManager = queueStateManager;
        this.enqueuer = enqueuer;
        this.scheduler = scheduler;
        this.healthManager = healthManager;
    }

    public static QueueControlStrategyFactory create(
            BrokerConnection brokerConnection,
            RequestManager requestManager,
            SessionOptions sessionOptions,
            QueueStateManager queueStateManager,
            BrokerSession.Enqueuer enqueuer,
            ScheduledExecutorService scheduler,
            BrokerSession.HealthManager healthManager) {
        return new QueueControlStrategyFactory(
                brokerConnection,
                requestManager,
                sessionOptions,
                queueStateManager,
                enqueuer,
                scheduler,
                healthManager);
    }

    private CloseQueueStrategy createCloseControlSequenceContext(
            QueueImpl queue,
            Duration timeout,
            boolean isAsync,
            CloseQueueStrategy.Scenario scenario,
            CompletableFuture<CloseQueueCode> future) {
        return new CloseQueueStrategy(
                queue,
                timeout,
                isAsync,
                scenario,
                brokerConnection,
                requestManager,
                queueStateManager,
                enqueuer,
                scheduler,
                healthManager,
                future);
    }

    public CloseQueueStrategy createCloseAsyncSequence(
            QueueImpl queue, Duration timeout, CompletableFuture<CloseQueueCode> future) {
        return createCloseControlSequenceContext(
                queue, timeout, true, CloseQueueStrategy.Scenario.DEFAULT, future);
    }

    public CloseQueueStrategy createCloseSyncSequence(
            QueueImpl queue, Duration timeout, CompletableFuture<CloseQueueCode> future) {
        return createCloseControlSequenceContext(
                queue, timeout, false, CloseQueueStrategy.Scenario.DEFAULT, future);
    }

    public CloseQueueStrategy createOnLateOpenConfRespCloseSequence(
            QueueImpl queue, Duration timeout) {
        // User already got open queue operation result (OpenQueueResult.TIMEOUT)
        // via exception or async event, and the queue state is already set to CLOSED.
        // So we set isAsync to false in order to avoid generating of QUEUE_CLOSE_RESULT event.
        return createCloseControlSequenceContext(
                queue,
                timeout,
                false,
                CloseQueueStrategy.Scenario.LATE_TWO_STEPS_CLOSING,
                new CompletableFuture<>());
    }

    public CloseQueueStrategy createOnLateOpenRespCloseSequence(QueueImpl queue) {
        // User already got open queue operation result (OpenQueueResult.TIMEOUT)
        // via exception or async event, and the queue state is already set to CLOSED.
        // So we set isAsync to false in order to avoid generating of QUEUE_CLOSE_RESULT event.
        return createCloseControlSequenceContext(
                queue,
                sessionOptions.closeQueueTimeout(),
                false,
                CloseQueueStrategy.Scenario.LATE_ONE_STEP_CLOSING,
                new CompletableFuture<>());
    }

    public CloseQueueStrategy createOnLateCloseConfRespCloseSequence(QueueImpl queue) {
        // User already got close queue operation result (CloseQueueResult.TIMEOUT)
        // via exception or async event, and the queue state is already set to CLOSED.
        // So we set isAsync to false in order to avoid generating of QUEUE_CLOSE_RESULT event.
        return createCloseControlSequenceContext(
                queue,
                sessionOptions.closeQueueTimeout(),
                false,
                CloseQueueStrategy.Scenario.LATE_ONE_STEP_CLOSING,
                new CompletableFuture<>());
    }

    private SuspendQueueStrategy createSuspendSequence(
            QueueImpl queue,
            Duration timeout,
            QueueOptions queueOptions,
            boolean isAsync,
            CompletableFuture<ConfigureQueueCode> future) {
        return new SuspendQueueStrategy(
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

    private ResumeQueueStrategy createResumeSequence(
            QueueImpl queue,
            Duration timeout,
            QueueOptions queueOptions,
            boolean isAsync,
            CompletableFuture<ConfigureQueueCode> future) {
        return new ResumeQueueStrategy(
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

    private ConfigureQueueStrategy createConfigureSequence(
            QueueImpl queue,
            Duration timeout,
            QueueOptions queueOptions,
            boolean isAsync,
            CompletableFuture<ConfigureQueueCode> future) {

        QueueOptions currentOptions = queue.getQueueOptions();
        // We need to update only those fields which were set by user in
        // 'queueOptions' parameter. For this purpose we create new builder,
        // merge current options and after that merge `queueOptions'.
        // So the result options will contain the following fields:
        //  - explicitly set by user in 'queueOptions'
        //  - explicitly set by user in 'currentOptions' and not overridden
        //    by 'queueOptions'
        //  - default values for unset fields

        QueueOptions updatedOptions = queueOptions;

        // If queue current options are null then queue is not opened yet.
        if (currentOptions != null) {
            updatedOptions =
                    QueueOptions.builder().merge(currentOptions).merge(queueOptions).build();
        }

        // Depending on queue options and state and current host health
        // create and return either resume, suspend or standalone strategy.
        if (queue.getIsSuspendedWithBroker()) {
            if (!updatedOptions.getSuspendsOnBadHostHealth()) {
                // Suspended queue is no longer health sensitive: RESUME.
                return createResumeSequence(queue, timeout, updatedOptions, isAsync, future);
            }
        } else if (updatedOptions.getSuspendsOnBadHostHealth() && !healthManager.isHostHealthy()) {
            // Active queue is now health sensitive on unhealthy host: SUSPEND.
            return createSuspendSequence(queue, timeout, updatedOptions, isAsync, future);
        }

        // Ordinary configure.
        return new StandaloneConfigureQueueStrategy(
                queue,
                timeout,
                updatedOptions,
                isAsync,
                brokerConnection,
                requestManager,
                queueStateManager,
                enqueuer,
                scheduler,
                healthManager,
                future);
    }

    public ConfigureQueueStrategy createConfigureAsyncSequence(
            QueueImpl queue,
            Duration timeout,
            QueueOptions queueOptions,
            CompletableFuture<ConfigureQueueCode> future) {
        return createConfigureSequence(queue, timeout, queueOptions, true, future);
    }

    public ConfigureQueueStrategy createConfigureSyncSequence(
            QueueImpl queue,
            Duration timeout,
            QueueOptions queueOptions,
            CompletableFuture<ConfigureQueueCode> future) {
        return createConfigureSequence(queue, timeout, queueOptions, false, future);
    }

    public SuspendQueueStrategy createSuspendAsyncSequence(
            QueueImpl queue,
            Duration timeout,
            QueueOptions queueOptions,
            CompletableFuture<ConfigureQueueCode> future) {
        return createSuspendSequence(queue, timeout, queueOptions, true, future);
    }

    public ResumeQueueStrategy createResumeAsyncSequence(
            QueueImpl queue,
            Duration timeout,
            QueueOptions queueOptions,
            CompletableFuture<ConfigureQueueCode> future) {
        return createResumeSequence(queue, timeout, queueOptions, true, future);
    }

    private OpenQueueStrategy createOpenSequence(
            QueueImpl queue,
            Duration timeout,
            QueueOptions queueOptions,
            boolean isAsync,
            boolean isReopen,
            CompletableFuture<OpenQueueCode> future) {
        return new OpenQueueStrategy(
                queue,
                timeout,
                queueOptions,
                isAsync,
                isReopen,
                brokerConnection,
                requestManager,
                queueStateManager,
                enqueuer,
                scheduler,
                healthManager,
                future);
    }

    public OpenQueueStrategy createOpenAsyncSequence(
            QueueImpl queue,
            Duration timeout,
            QueueOptions queueOptions,
            CompletableFuture<OpenQueueCode> future) {
        return createOpenSequence(queue, timeout, queueOptions, true, false, future);
    }

    public OpenQueueStrategy createOpenSyncSequence(
            QueueImpl queue,
            Duration timeout,
            QueueOptions queueOptions,
            CompletableFuture<OpenQueueCode> future) {
        return createOpenSequence(queue, timeout, queueOptions, false, false, future);
    }

    public OpenQueueStrategy createReopenAsyncSequence(
            QueueImpl queue,
            Duration timeout,
            QueueOptions queueOptions,
            CompletableFuture<OpenQueueCode> future) {
        return createOpenSequence(queue, timeout, queueOptions, true, true, future);
    }
}
