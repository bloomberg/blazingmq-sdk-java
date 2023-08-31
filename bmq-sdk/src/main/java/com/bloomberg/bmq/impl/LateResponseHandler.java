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
import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.impl.infr.msg.ConfigureStream;
import com.bloomberg.bmq.impl.infr.msg.ConfigureStreamResponse;
import com.bloomberg.bmq.impl.infr.msg.ControlMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.OpenQueue;
import com.bloomberg.bmq.impl.infr.msg.OpenQueueResponse;
import com.bloomberg.bmq.impl.infr.msg.QueueHandleParameters;
import com.bloomberg.bmq.impl.infr.msg.StreamParameters;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.intf.QueueState;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LateResponseHandler {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final QueueStateManager queueStateManager;
    private final QueueControlStrategyFactory strategyFactory;
    private final SessionOptions sessionOptions;

    private LateResponseHandler(
            QueueStateManager queueStateManager,
            QueueControlStrategyFactory strategyFactory,
            SessionOptions sessionOptions) {
        this.queueStateManager = queueStateManager;
        this.strategyFactory = strategyFactory;
        this.sessionOptions = sessionOptions;
    }

    public static LateResponseHandler createInstance(
            QueueStateManager queueStateManager,
            QueueControlStrategyFactory strategyFactory,
            SessionOptions sessionOptions) {
        return new LateResponseHandler(queueStateManager, strategyFactory, sessionOptions);
    }

    public void handleLateResponse(ControlMessageChoice controlMessageChoice) {
        Argument.expectNonNull(controlMessageChoice, "controlMessageChoice");
        if (controlMessageChoice.isOpenQueueResponseValue()) {
            handleOpenQueueResponse(controlMessageChoice.openQueueResponse());
        } else if (controlMessageChoice.isConfigureStreamResponseValue()) {
            handleConfigureStreamResponse(controlMessageChoice.configureStreamResponse());
        } else {
            logger.error("Inappropriate late response received: {}", controlMessageChoice);
        }
    }

    private void handleOpenQueueResponse(OpenQueueResponse openQueueResponse) {
        // As far as our current JSON parsing doesn't guarantee mandatory fields in deserialized
        // object
        // we can not consider this case as program error. Should log error and return control from
        // the method.
        QueueId queueId = extractQueueId(openQueueResponse);
        if (queueId == null) {
            logger.error("QueueId can not be extracted from openQueueResponse");
            return;
        }
        QueueImpl queue = queueStateManager.findExpiredByQueueId(queueId);
        if (queue == null) {
            logger.error(
                    "Queue with qId={} subQId={} not found.",
                    queueId.getQId(),
                    queueId.getSubQId());
            return;
        }
        queueStateManager.onLateResponseProcessed(queue);
        strategyFactory.createOnLateOpenRespCloseSequence(queue).start();
    }

    private void handleConfigureStreamResponse(ConfigureStreamResponse configureStreamResponse) {
        if (configureStreamResponse == null) {
            logger.error("Malformed late configureQueueStreamResponse.");
            return;
        }
        logger.debug("Late CONFIGURE response: {}", configureStreamResponse);
        ConfigureStream originalRequest = configureStreamResponse.getOriginalRequest();
        if (originalRequest == null) {
            logger.error("Malformed late configureStreamResponse. Missed OriginalRequest.");
            return;
        }
        StreamParameters parameters = originalRequest.streamParameters();
        if (parameters == null) {
            logger.error("Malformed late configureStreamResponse. Missed QueueHandleParameters.");
            return;
        }

        int qId = originalRequest.id();
        int subQId = 0; // We don't have subId in the ConfigureStream

        QueueId queueId = QueueId.createInstance(qId, subQId); // returns not-null QueueId

        QueueImpl queue = queueStateManager.findByQueueId(queueId);
        QueueImpl expiredQueue = queueStateManager.findExpiredByQueueId(queueId);
        if (queue != null) {
            logger.debug("QueueImpl is among active queues");
            if (expiredQueue == null) {
                // Suppose, that given response corresponds to standalone configure request
                // If queue is among active but not expired queues. It means that queue SHELL not be
                // in
                // CLOSED state, otherwise program error.
                logger.debug("Standalone configure request");
                onStandaloneConfigurationResponse(queue, parameters);
            } else if (queue == expiredQueue) {
                // Suppose, that given response corresponds to close queue configure request
                // If queue is among expired but not active queues. It means that queue SHELL be in
                // CLOSED state, otherwise program error.
                logger.debug("Close queue configure request");
                onCloseConfigurationResponse(queue);
            }
        } else if (expiredQueue != null) {
            // Suppose, that given response corresponds to open queue configure request
            // If queue is among active and not among expired queues. It means that configuration
            // response
            // during queue opening is timed out and queue SHELL be in the CLOSED state,
            // otherwise program error.
            logger.debug("Open queue configure request");
            onOpenConfigurationResponse(expiredQueue);
        }
    }

    private boolean areParametersInSync(
            QueueOptions currentQueueOptions, StreamParameters parameters) {

        if (currentQueueOptions.getSubscriptions().size() != parameters.subscriptions().length) {
            return false;
        }

        return true;
    }

    private void onStandaloneConfigurationResponse(QueueImpl queue, StreamParameters parameters) {
        Argument.expectCondition(
                queue.getState() != QueueState.e_CLOSED, "'queue' must not be closed");
        if (queue.getState() != QueueState.e_OPENED) {
            logger.error(
                    "No need to send sync configuration request on late response, "
                            + "while we are not in OPENED state");
            return;
        }

        // If queue is opened, some queue options MUST be stored.
        QueueOptions currentQueueOptions =
                Argument.expectNonNull(queue.getQueueOptions(), "queue options");

        // Check if parameters applied to server is similar to parameters stored in queue.
        // Build new options (apply values from 'parameters' on top of
        // 'currentQueueOptions' options) and compare with 'currentQueueOptions'.
        boolean parametersInSync = areParametersInSync(currentQueueOptions, parameters);
        if (!parametersInSync) {
            // If actual parameters are not the same as received in late configure response
            // we should send the update to server with current parameters.
            // We will not inform the user about result of this operation.
            final CompletableFuture<ConfigureQueueCode> dummyFuture = new CompletableFuture<>();
            strategyFactory
                    .createConfigureSyncSequence(
                            queue,
                            sessionOptions.configureQueueTimeout(),
                            queue.getQueueOptions(),
                            dummyFuture)
                    .start();
        }
    }

    private void onOpenConfigurationResponse(QueueImpl queue) {
        Argument.expectCondition(queue.getState() == QueueState.e_CLOSED, "'queue' must be closed");

        queueStateManager.onLateResponseProcessed(queue);
        strategyFactory
                .createOnLateOpenConfRespCloseSequence(
                        queue, sessionOptions.configureQueueTimeout())
                .start();
    }

    private void onCloseConfigurationResponse(QueueImpl queue) {
        Argument.expectCondition(queue.getState() == QueueState.e_CLOSED, "'queue' must be closed");

        queueStateManager.onLateResponseProcessed(queue);
        strategyFactory.createOnLateCloseConfRespCloseSequence(queue).start();
    }

    private QueueId extractQueueId(OpenQueueResponse openQueueResponse) {
        if (openQueueResponse == null) {
            logger.error("Malformed late openQueueResponse.");
            return null;
        }
        OpenQueue originalRequest = openQueueResponse.getOriginalRequest();
        if (originalRequest == null) {
            logger.error("Malformed late openQueueResponse. Missed OriginalRequest.");
            return null;
        }
        QueueHandleParameters parameters = originalRequest.getHandleParameters();
        if (parameters == null) {
            logger.error("Malformed late openQueueResponse. Missed QueueHandleParameters.");
            return null;
        }
        int qId = parameters.getQId();
        int subQId = 0;
        if (parameters.getSubIdInfo() != null) {
            subQId = parameters.getSubIdInfo().subId();
        }
        return QueueId.createInstance(qId, subQId);
    }
}
