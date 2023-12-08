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

import com.bloomberg.bmq.ResultCodes.CloseQueueCode;
import com.bloomberg.bmq.ResultCodes.CloseQueueResult;
import com.bloomberg.bmq.ResultCodes.GenericResult;
import com.bloomberg.bmq.impl.events.QueueControlEvent;
import com.bloomberg.bmq.impl.infr.msg.StatusCategory;
import com.bloomberg.bmq.impl.infr.msg.StreamParameters;
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

public class CloseQueueStrategy extends QueueControlStrategy<CloseQueueCode> {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Scenario scenario;

    public enum Scenario {
        DEFAULT,

        LATE_TWO_STEPS_CLOSING,
        // reaction on late incoming configure
        // response during opening sequence

        LATE_ONE_STEP_CLOSING;
        // reaction on late incoming open queue
        // response during opening sequence or
        // configure queue response during closing
        // sequence

        boolean isDefault() {
            return this == DEFAULT;
        }

        boolean isLateTwoStepsClosing() {
            return this == LATE_TWO_STEPS_CLOSING;
        }

        boolean isLateOneStepClosing() {
            return this == LATE_ONE_STEP_CLOSING;
        }
    }

    @Override
    protected StreamParameters createStreamParameters(SubQueueIdInfo subQueueIdInfo) {
        return StreamParameters.createDeconfigureParameters(subQueueIdInfo);
    }

    @Override
    protected CloseQueueCode upcast(GenericResult genericResult) {
        return CloseQueueResult.upcast(genericResult);
    }

    @Override
    protected CloseQueueCode preconditionCheck() {
        QueueImpl queue = getQueue();
        QueueState state = getQueue().getState();

        ConfigureQueueStrategy strategy = null;

        if (queue.getStrategy() != null) {
            if (scenario.isDefault()
                    && state == QueueState.e_OPENED
                    && queue.getStrategy().canCancelOnQueueClosing()
                    && queue.getStrategy() instanceof ConfigureQueueStrategy) {
                // If the queue is opened and there is a strategy in progress,
                // then this is either standalone, suspend ot resume
                // configuration request.
                strategy = (ConfigureQueueStrategy) queue.getStrategy();
                logger.warn("Queue strategy will be canceled: {}.", strategy);
            } else {
                logger.error("Queue is busy with other protocol sequences.");
                return CloseQueueResult.ALREADY_IN_PROGRESS;
            }
        }
        if (scenario.isDefault()) {
            QueueImpl alreadyPresentQueue =
                    getQueueStateManager().findByQueueId(queue.getFullQueueId());
            if (alreadyPresentQueue == null) {
                logger.error("Queue with QueueId={} doesn't exist", queue.getFullQueueId());
                return CloseQueueResult.UNKNOWN_QUEUE;
            }
            if (state == QueueState.e_CLOSED) {
                logger.error("Queue is already closed.");
                return CloseQueueResult.ALREADY_CLOSED;
            }
            if (state != QueueState.e_OPENED) {
                logger.error("Queue is in process of either opening or closing.");
                return CloseQueueResult.ALREADY_IN_PROGRESS;
            }
        }
        if ((scenario.isLateTwoStepsClosing() && state != QueueState.e_CLOSED)
                || (scenario.isLateOneStepClosing() && state != QueueState.e_CLOSED)) {
            return CloseQueueResult.NOT_SUPPORTED;
        }

        // If we get here and there is a strategy in progress, this should be a
        // pending configuration request which we need to cancel before
        // starting close queue strategy.
        if (strategy != null) {
            // Cancel pending strategy.
            strategy.cancelOnQueueClosing();
            // Check if there is no running strategy.
            if (queue.getStrategy() != null) {
                logger.error("Failed to cancel queue strategy: {}", queue.getStrategy());
                return CloseQueueResult.NOT_SUPPORTED;
            }
        }

        return CloseQueueResult.SUCCESS;
    }

    @Override
    protected void startSequence() {
        if (scenario.isDefault()) {
            startCancelationTimer();
            setQueueState(QueueState.e_CLOSING);
        }

        RequestManager.Request req;
        if (scenario.isLateOneStepClosing()) {
            // Just send close queue request
            req = createCloseQueueRequest();
        } else {
            // Deconfigure queue currently only makes sense for a
            // reader, and is therefore a no-op for a not reading queue.
            if (!getQueue().isReader()) {
                logger.debug(
                        "Skip deconfigure request for not reading queue [uri: '{}']",
                        getQueue().getUri());
                handleConfigureStatus(StatusCategory.E_SUCCESS);
                return;
            }

            req = createConfigureQueueRequest();
            req.setAsyncNotifier(this::onConfigureResponse);
        }

        GenericResult genericResult = sendRawRequest(req, getTimeout());
        if (genericResult.isFailure()) {
            logger.error("Failed to send configureQueue request. RC={}", genericResult);
            // If no connection the queue now is closed
            if (genericResult.isNotConnected()) {
                decrementSubStreamCount();
                onFullyClosed();
                genericResult = GenericResult.SUCCESS;
            } else {
                // According to 'TcpBrokerConnection' implementation, when
                // writing to channel, we may get either SUCCESS or
                // NOT_CONNECTED. If the channel is full, we wait for it to
                // become writable and return SUCCESS before local timeout expires.
                // That means this branch shouldn't be executed.
                // TODO: check the statement above and remove this code
                onCloseQueueConfigureFailed(); // should be called for configure request only?

                decrementSubStreamCount();
                // TODO. In case of failed conf request,
                // do we need to send close queue request?
                // To be fixed later.
            }

            resultHook(genericResult);
            return;
        }

        if (scenario.isLateOneStepClosing()) {
            // Close request successfully sent and since there is no callback,
            // decrement substreamcount here
            decrementSubStreamCount();
            resultHook(GenericResult.SUCCESS);
        }
    }

    private void onCloseResponse(RequestManager.Request r) {
        logger.debug("Async notifier for response received: {}", r.response());
        StatusCategory statusCategory = extractResponseStatus(r);
        // If given sequence is handling late open queue or configuration response during
        // open sequence, then we do not need to remove queue from active queues as
        // far as it is already removed.
        if (scenario.isDefault()) {
            onFullyClosed();
        }

        resultHook(statusCategory);
    }

    private void onConfigureResponse(RequestManager.Request confR) {
        logger.debug("Async notifier for response received: {}", confR.response());
        StatusCategory confStatusCategory = extractResponseStatus(confR);

        handleConfigureStatus(confStatusCategory);
    }

    private void handleConfigureStatus(StatusCategory confStatusCategory) {
        if (confStatusCategory.isFailure()) {
            logger.debug("Configure response error status: {}", confStatusCategory);
            onCloseQueueConfigureFailed();

            if (!confStatusCategory.isCanceled() && !confStatusCategory.isTimeout()) {
                // In case of bad configure response,
                // we try to send close queue request
                logger.debug("Sending close request");
                GenericResult genericResult = sendRawRequest(createCloseQueueRequest());

                if (genericResult.isFailure()) {
                    logger.error("Failed to send closeQueue request. RC={}", genericResult);
                }
            }

            // If not timeout, close the queue.
            // Otherwise keep it CLOSING until late response.
            if (!confStatusCategory.isTimeout()) {
                logger.debug("Cleanup queue data");
                // We decrement substream count
                decrementSubStreamCount();

                // As a result of the above onCloseQueueConfigureFailed() call
                // the queue now is in the CLOSED state. To do a full cleanup
                // we need to call onFullyClosed() but it expects the queue
                // in the CLOSING state. So as a workaround set the queue state
                // to CLOSING here.
                // TODO: review postcondition in the
                //       QueueStateManager.onCloseQueueConfigureFailed()
                setQueueState(QueueState.e_CLOSING);

                onFullyClosed();
            }
            resultHook(confStatusCategory);
            return;
        }

        RequestManager.Request req = createCloseQueueRequest();

        req.setAsyncNotifier(this::onCloseResponse);
        GenericResult genericResult = sendRawRequest(req);

        // Regardless of close request status, we decrement substream count
        decrementSubStreamCount();

        if (genericResult.isFailure()) {
            logger.error("Failed to send closeQueue request. RC={}", genericResult);
            setQueueState(QueueState.e_CLOSED);

            resultHook(genericResult);
        }
    }

    @Override
    protected QueueControlEvent.Type getQueueControlEventType() {
        return QueueControlEvent.Type.e_QUEUE_CLOSE_RESULT;
    }

    public CloseQueueStrategy(
            QueueImpl queue,
            Duration timeout,
            boolean isAsync,
            Scenario scenario,
            BrokerConnection brokerConnection,
            RequestManager requestManager,
            QueueStateManager queueStateManager,
            BrokerSession.Enqueuer enqueuer,
            ScheduledExecutorService scheduler,
            BrokerSession.HealthManager healthManager,
            CompletableFuture<CloseQueueCode> future) {
        super(
                queue,
                timeout,
                null,
                isAsync,
                brokerConnection,
                requestManager,
                queueStateManager,
                enqueuer,
                scheduler,
                healthManager,
                future);
        this.scenario = scenario;
    }
}
