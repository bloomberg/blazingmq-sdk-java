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
import com.bloomberg.bmq.ResultCodes.GenericCode;
import com.bloomberg.bmq.ResultCodes.GenericResult;
import com.bloomberg.bmq.impl.events.QueueControlEvent;
import com.bloomberg.bmq.impl.infr.msg.ControlMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.QueueHandleParameters;
import com.bloomberg.bmq.impl.infr.msg.QueueStreamParameters;
import com.bloomberg.bmq.impl.infr.msg.StatusCategory;
import com.bloomberg.bmq.impl.infr.msg.StreamParameters;
import com.bloomberg.bmq.impl.infr.msg.SubQueueIdInfo;
import com.bloomberg.bmq.impl.infr.proto.RequestManager;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.intf.BrokerConnection;
import com.bloomberg.bmq.impl.intf.QueueState;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link QueueControlStrategy} class is an abstract base class encapsulating general queue control
 * sequence logic.
 *
 * <p>Thread safety. Conditionally thread-safe and thread-enabled class.
 *
 * <p>Subclasses implementing concrete strategies:
 *
 * <ul>
 *   <li>{@link OpenQueueStrategy};
 *   <li>{@link ConfigureQueueStrategy};
 *   <li>{@link CloseQueueStrategy};
 * </ul>
 */
public abstract class QueueControlStrategy<RESULT extends GenericCode> {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final QueueImpl queue;
    private final Duration timeout;
    private final QueueOptions queueOptions;
    private final boolean isAsync;
    private final Object cancellationLock;
    private final BrokerConnection brokerConnection;
    private final RequestManager requestManager;
    private final QueueStateManager queueStateManager;
    private final BrokerSession.Enqueuer enqueuer;
    private final ScheduledExecutorService scheduler;
    private final BrokerSession.HealthManager healthManager;
    private final CompletableFuture<RESULT> completableFuture;

    private RequestManager.Request currentRequest;
    private ScheduledFuture<?> onTimeoutFuture;
    private boolean isCanceledOnQueueClosing = false;

    // CREATORS
    protected QueueControlStrategy(
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
            CompletableFuture<RESULT> completableFuture) {
        this.queue = queue;
        this.timeout = timeout;
        this.queueOptions = queueOptions;
        this.isAsync = isAsync;
        currentRequest = null;
        cancellationLock = new Object();
        this.brokerConnection = brokerConnection;
        this.requestManager = requestManager;
        this.queueStateManager = queueStateManager;
        this.enqueuer = enqueuer;
        this.scheduler = scheduler;
        this.healthManager = healthManager;
        this.completableFuture = completableFuture;
    }

    // ACCESSORS
    protected abstract QueueControlEvent.Type getQueueControlEventType();

    protected boolean canCancelOnQueueClosing() {
        return false;
    }

    protected final QueueImpl getQueue() {
        return queue;
    }

    protected final QueueOptions getQueueOptions() {
        return queueOptions;
    }

    protected final boolean getIsAsync() {
        return isAsync;
    }

    protected final Duration getTimeout() {
        return timeout;
    }

    protected final BrokerConnection getConnection() {
        return brokerConnection;
    }

    protected final RequestManager getRequestManager() {
        return requestManager;
    }

    protected final QueueStateManager getQueueStateManager() {
        return queueStateManager;
    }

    protected final ScheduledExecutorService getScheduler() {
        return scheduler;
    }

    protected final BrokerSession.HealthManager getHealthManager() {
        return healthManager;
    }

    protected boolean getIsCanceledOnQueueClosing() {
        return isCanceledOnQueueClosing;
    }

    // MANIPULATORS
    /**
     * Start configurated strategy, by sending request in USER THREAD, and handling responses in
     * broker session scheduler thread. Given method is agnostic to sync/async mode of the strategy.
     */
    public void start() {
        RESULT code = preconditionCheck();
        if (code.isFailure()) {
            logger.error("Precondition for starting sequence failed.");
            resultHook(code);
            return;
        }
        getQueue().setStrategy(this);
        startSequence();
    }

    protected abstract RESULT preconditionCheck();

    protected abstract void startSequence();

    protected final void setCurrentRequest(RequestManager.Request req) {
        synchronized (cancellationLock) {
            currentRequest = req;
        }
    }

    protected void cancelOnStop() {
        cancel("disconnected", null);
    }

    protected void cancelOnQueueClosing() {
        if (!canCancelOnQueueClosing()) {
            throw new IllegalStateException("Strategy cannot be canceled on queue closing");
        }

        cancel("queue being closed", () -> isCanceledOnQueueClosing = true);
    }

    private void cancel(String reason, Runnable onCanceling) {
        synchronized (cancellationLock) {
            if (currentRequest != null) {
                logger.debug("Cancelling strategy request {}", currentRequest);

                if (onCanceling != null) {
                    onCanceling.run();
                }

                currentRequest.cancel(reason);
            }
        }
    }

    private void cancelOnTimeout() {
        synchronized (cancellationLock) {
            if (currentRequest != null) {
                currentRequest.cancelDueToTimeout();
            }
        }
    }

    protected final void startCancelationTimer() {
        onTimeoutFuture =
                getScheduler()
                        .schedule(
                                this::cancelOnTimeout,
                                getTimeout().toNanos(),
                                TimeUnit.NANOSECONDS);
    }

    protected void setQueueState(QueueState state) {
        queue.setState(state);
    }

    protected void onOpenQueueSent() {
        queueStateManager.onOpenQueueSent(queue);
    }

    protected void onReopenQueueSent() {
        queueStateManager.onReopenQueueSent(queue);
    }

    protected void onOpenQueueFailed(StatusCategory statusCategory) {
        assert statusCategory.isFailure();

        if (statusCategory.isTimeout()) {
            // Expect to get late response from the broker.
            // When received, close queue request will be sent

            queueStateManager.onOpenQueueLocallyTimedOut(queue);
        } else {
            // Consider open queue failed due to server response

            decrementSubStreamCount();
            queueStateManager.onOpenQueueFailed(queue);
        }
    }

    protected void onOpenQueueConfigureFailed(StatusCategory statusCategory) {
        assert statusCategory.isFailure();

        if (statusCategory.isTimeout()) {
            // Expect to get late response from the broker.
            // When received, close queue request will be sent

            queueStateManager.onOpenQueueLocallyTimedOut(queue);
        } else {
            if (!statusCategory.isCanceled()) {
                // If we've got a broker config response with some error we have to
                // send a close queue request, and we don't care about response
                sendCloseRequest();
            }

            decrementSubStreamCount();
            queueStateManager.onOpenQueueFailed(queue);
        }
    }

    protected void onCloseQueueConfigureFailed() {
        queueStateManager.onCloseQueueConfigureFailed(queue);
    }

    protected void onFullyClosed() {
        queueStateManager.onFullyClosed(queue);
    }

    protected RequestManager.Request createCloseQueueRequest() {
        RequestManager.Request req = getRequestManager().createRequest();
        ControlMessageChoice msg = new ControlMessageChoice();
        msg.makeCloseQueue();
        req.setRequest(msg);
        QueueHandleParameters params = getQueue().getParameters();
        req.request().closeQueue().setHandleParameters(params);

        req.request().closeQueue().setIsFinal(getSubStreamCount() == 1);

        return req;
    }

    protected void sendCloseRequest() {
        RequestManager.Request req = createCloseQueueRequest();
        GenericResult genericResult = sendRawRequest(req);
        if (genericResult.isFailure()) {
            logger.error("Failed to send closeQueue request. RC={}", genericResult);
        }
    }

    protected void incrementSubStreamCount() {
        queueStateManager.incrementSubStreamCount(queue);
    }

    protected void decrementSubStreamCount() {
        queueStateManager.decrementSubStreamCount(queue);
    }

    protected int getSubStreamCount() {
        return queueStateManager.getSubStreamCount(queue);
    }

    protected RequestManager.Request createConfigureQueueRequest() {
        QueueImpl q = getQueue();

        SubQueueIdInfo subQueueIdInfo = null;
        if (q.getSubQueueId() != QueueId.k_DEFAULT_SUBQUEUE_ID) {
            subQueueIdInfo = new SubQueueIdInfo(q.getSubQueueId(), q.getUri().id());
        }

        ControlMessageChoice msg = new ControlMessageChoice();
        msg.makeConfigureStream();

        RequestManager.Request req = getRequestManager().createRequest();
        req.setRequest(msg);
        req.request().configureStream().setId(q.getQueueId());
        StreamParameters params = createStreamParameters(subQueueIdInfo);
        req.request().configureStream().setStreamParameters(params);

        return req;
    }

    protected QueueStreamParameters createParameters(SubQueueIdInfo subQueueIdInfo) {
        // TODO: not used, consider removing
        // This function was used for the old style ConfigureQueueStream request (without
        // subscriptions)
        QueueStreamParameters params;
        Long flags = getQueue().getParameters().getFlags();
        int qId = getQueue().getQueueId();
        QueueOptions ops = getQueueOptions();
        if (QueueFlags.isReader(flags)) {
            params = QueueStreamParameters.createParameters(qId, subQueueIdInfo, ops);
        } else {
            params = QueueStreamParameters.createDeconfigureParameters(qId, subQueueIdInfo);
        }
        return params;
    }

    protected StreamParameters createStreamParameters(SubQueueIdInfo subQueueIdInfo) {
        StreamParameters params;
        Long flags = getQueue().getParameters().getFlags();
        QueueOptions ops = getQueueOptions();
        if (QueueFlags.isReader(flags)) {
            params = StreamParameters.createParameters(subQueueIdInfo, ops);
        } else {
            params = StreamParameters.createDeconfigureParameters(subQueueIdInfo);
        }
        return params;
    }

    // TODO: discuss redundancy of StatusCategory/GenericResult
    protected void resultHook(StatusCategory statusCategory) {
        GenericResult genericResult = ResultCodeUtils.toGenericResult(statusCategory);
        resultHook(genericResult);
    }

    protected void resultHook(GenericResult genericResult) {
        RESULT code = upcast(genericResult);
        resultHook(code);
    }

    protected void resultHook(RESULT code) {
        if (onTimeoutFuture != null) {
            onTimeoutFuture.cancel(false);
        }
        if (queue.getStrategy() != null) {
            queue.resetStrategy();
        }
        if (isAsync) {
            enqueueEvent(code);
        }
        completableFuture.complete(code);
    }

    protected void enqueueEvent(RESULT code) {
        enqueueEvent(code, getQueueControlEventType());
    }

    protected void enqueueEvent(RESULT code, QueueControlEvent.Type type) {
        QueueControlEvent queueControlEvent =
                QueueControlEvent.createInstance(type, code, "", queue);
        enqueuer.enqueueEvent(queueControlEvent);
    }

    protected final StatusCategory extractResponseStatus(RequestManager.Request r) {
        Argument.expectNonNull(r, "request");
        Argument.expectNonNull(r.response(), "request response");

        StatusCategory statusCategory = StatusCategory.E_SUCCESS;
        if (r.response().status() != null) {
            statusCategory = r.response().status().category();
        }
        return statusCategory;
    }

    protected void storeOptionsToQueue() {
        queue.setQueueOptions(getQueueOptions());
    }

    protected final GenericResult sendRawRequest(RequestManager.Request r) {
        return sendRawRequest(r, getTimeout());
    }

    protected final GenericResult sendRawRequest(RequestManager.Request r, Duration timeout) {
        setCurrentRequest(r);
        return requestManager.sendRequest(r, getConnection(), timeout);
    }

    protected abstract RESULT upcast(GenericResult genericResult);

    public CompletableFuture<RESULT> getResultFuture() {
        return completableFuture;
    }
}
