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

import com.bloomberg.bmq.QueueFlags;
import com.bloomberg.bmq.QueueOptions;
import com.bloomberg.bmq.ResultCodes.GenericResult;
import com.bloomberg.bmq.ResultCodes.OpenQueueCode;
import com.bloomberg.bmq.ResultCodes.OpenQueueResult;
import com.bloomberg.bmq.impl.events.QueueControlEvent;
import com.bloomberg.bmq.impl.infr.msg.ControlMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.QueueHandleParameters;
import com.bloomberg.bmq.impl.infr.msg.StatusCategory;
import com.bloomberg.bmq.impl.infr.msg.Subscription;
import com.bloomberg.bmq.impl.infr.proto.RequestManager;
import com.bloomberg.bmq.impl.intf.BrokerConnection;
import com.bloomberg.bmq.impl.intf.QueueState;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenQueueStrategy extends QueueControlStrategy<OpenQueueCode> {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final boolean isReopen;

    private RequestManager.Request createOpenQueueRequest() {
        logger.trace("createOpenQueueRequest");
        QueueImpl queue = getQueue();
        RequestManager.Request req = getRequestManager().createRequest();
        ControlMessageChoice msg = new ControlMessageChoice();
        msg.makeOpenQueue();
        req.setRequest(msg);
        QueueHandleParameters handleParams = queue.getParameters();
        req.request().openQueue().setHandleParameters(handleParams);
        return req;
    }

    private GenericResult sendOpenRequest() {
        generateNextQueueIdForQueueIfNeeded();
        RequestManager.Request req = createOpenQueueRequest();
        req.setAsyncNotifier(this::onOpenQueueResponse);
        return sendRawRequest(req);
    }

    private void sendConfigureRequest() {
        RequestManager.Request req = createConfigureQueueRequest();
        req.setAsyncNotifier(this::onConfigureQueueResponse);
        GenericResult genericResult = sendRawRequest(req);
        if (genericResult.isFailure()) {
            logger.error("Failed to send configureQueue request. RC={}", genericResult);

            // Remove the queue from active list and set state to CLOSED
            getQueueStateManager().onOpenQueueFailed(getQueue());

            // Decrement substreamcount
            decrementSubStreamCount();

            resultHook(genericResult);
        }
    }

    private void onOpenQueueResponse(RequestManager.Request opReq) {
        logger.debug("Async notifier for response received: {}", opReq.response());
        StatusCategory statusCategory = extractResponseStatus(opReq);
        if (statusCategory.isFailure()) {
            logger.error("Open queue failed: {}", opReq.response());
            onOpenQueueFailed(statusCategory);
            resultHook(statusCategory);
            return;
        }
        logger.debug(
                "Ready to send configure request after open queue response: {}", opReq.response());

        // If queue is sensitive to host health and the host is unhealthy,
        // then transition directly to suspended state. There will be no
        // QUEUE_SUSPENDED event generated.
        QueueImpl queue = getQueue();
        queue.setIsSuspendedWithBroker(
                getQueueOptions().getSuspendsOnBadHostHealth()
                        && !getHealthManager().isHostHealthy());
        queue.setIsSuspended(queue.getIsSuspendedWithBroker());

        // If host is unhealthy, skip configure step and go directly to
        // OPENED state.  Also configure queue currently only makes sense for a
        // reader, and is therefore a no-op for a not reading queue.
        if (queue.getIsSuspendedWithBroker() || !queue.isReader()) {
            logger.debug(
                    "Skip configure request for {} queue [uri: '{}']",
                    !queue.isReader() ? "not reading" : "suspended",
                    queue.getUri());

            storeOptionsToQueue();
            setQueueState(QueueState.e_OPENED);
            resultHook(OpenQueueResult.SUCCESS);
        } else {
            sendConfigureRequest();
        }
    }

    @Override
    protected OpenQueueCode preconditionCheck() {
        QueueImpl queue = getQueue();
        QueueState state = queue.getState();
        QueueImpl alreadyPresentQueue =
                getQueueStateManager().findByQueueId(queue.getFullQueueId());

        if (queue.getStrategy() != null) {
            logger.error("Queue is in process is busy with other protocol sequences.");
            return OpenQueueResult.ALREADY_IN_PROGRESS;
        }

        final boolean isWriter = QueueFlags.isWriter(queue.getParameters().getFlags());
        final boolean hasId = !queue.getUri().id().isEmpty();
        if (isWriter && hasId) {
            logger.error(
                    "AppId should not be specified in the URI if queue is opened in write mode [uri: '{}']",
                    queue.getUri());
            return OpenQueueResult.INVALID_ARGUMENT;
        }

        if (!isReopen) {
            if (alreadyPresentQueue != null) {
                logger.error("Queue already exists and has queueId={}", queue.getQueueId());
                return OpenQueueResult.QUEUE_ID_NOT_UNIQUE;
            }
            if (queue.isOpened()) {
                logger.error("Queue is already opened.");
                return OpenQueueResult.ALREADY_OPENED;
            }
            if (queue.getState() != QueueState.e_CLOSED) {
                logger.error("Queue is in process of either opening, closing or configuring.");
                return OpenQueueResult.ALREADY_IN_PROGRESS;
            }
        } else {
            if (state != QueueState.e_OPENED) return OpenQueueResult.NOT_SUPPORTED;
        }
        return OpenQueueResult.SUCCESS;
    }

    @Override
    protected void startSequence() {
        logger.trace("startSequence");
        GenericResult genericResult = sendOpenRequest();
        if (genericResult.isFailure()) {
            logger.error("Failed to send openQueue request. RC={}", genericResult);
            resultHook(genericResult);
            return;
        }
        startCancelationTimer();
        if (isReopen) {
            onReopenQueueSent();
        } else {
            onOpenQueueSent();
        }

        incrementSubStreamCount();
    }

    @Override
    protected OpenQueueCode upcast(GenericResult genericResult) {
        return OpenQueueResult.upcast(genericResult);
    }

    private void onConfigureQueueResponse(RequestManager.Request r) {
        // Context aliases
        logger.debug("Async notifier for response received: {}", r.response());

        StatusCategory statusCategory = extractResponseStatus(r);
        if (statusCategory.isFailure()) {
            onOpenQueueConfigureFailed(statusCategory);
            resultHook(statusCategory);
            return;
        }
        // Consider stream params as applied on the broker side and store in queue instance
        storeOptionsToQueue();
        storeSubscriptionsToQueue(r.request().configureStream().streamParameters().subscriptions());
        setQueueState(QueueState.e_OPENED);
        getQueueStateManager().onConfigureStreamSent(getQueue());
        resultHook(OpenQueueResult.SUCCESS);
    }

    private void storeSubscriptionsToQueue(Subscription[] subscriptions) {
        getQueue().setSubscriptions(subscriptions);
    }

    private void generateNextQueueIdForQueueIfNeeded() {
        QueueImpl queue = getQueue();
        int qId = queue.getQueueId();
        if (qId != QueueImpl.INVALID_QUEUE_ID) {
            logger.debug("Queue has been already in use with queueId={}", qId);
            return;
        }
        QueueId queueKey = getQueueStateManager().generateNextQueueId(queue.getUri());
        queue.setQueueId(queueKey.getQId()).setSubQueueId(queueKey.getSubQId());
        if (getQueueStateManager().findByQueueId(queueKey) != null) {
            throw new IllegalStateException("Queue already exists among active queues");
        }
    }

    @Override
    protected final QueueControlEvent.Type getQueueControlEventType() {
        if (!isReopen) {
            return QueueControlEvent.Type.e_QUEUE_OPEN_RESULT;
        } else {
            return QueueControlEvent.Type.e_QUEUE_REOPEN_RESULT;
        }
    }

    public OpenQueueStrategy(
            QueueImpl queue,
            Duration timeout,
            QueueOptions queueOptions,
            boolean isAsync,
            boolean isReopen,
            BrokerConnection brokerConnection,
            RequestManager requestManager,
            QueueStateManager queueStateManager,
            BrokerSession.Enqueuer enqueuer,
            ScheduledExecutorService scheduler,
            BrokerSession.HealthManager healthManager,
            CompletableFuture<OpenQueueCode> future) {

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

        this.isReopen = isReopen;
    }
}
