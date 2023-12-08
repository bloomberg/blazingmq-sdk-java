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
import com.bloomberg.bmq.impl.infr.msg.StatusCategory;
import com.bloomberg.bmq.impl.infr.proto.RequestManager;
import com.bloomberg.bmq.impl.intf.BrokerConnection;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ConfigureQueueStrategy} class is an abstract base class encapsulating general configure
 * queue control sequence logic.
 *
 * <p>Thread safety. Conditionally thread-safe and thread-enabled class.
 *
 * <p>Subclasses implementing concrete strategies:
 *
 * <ul>
 *   <li>{@link StandaloneConfigureQueueStrategy};
 *   <li>{@link SuspendQueueStrategy};
 *   <li>{@link ResumeQueueStrategy};
 * </ul>
 */
public abstract class ConfigureQueueStrategy extends QueueControlStrategy<ConfigureQueueCode> {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    protected void sendConfigureRequest() {
        // Configure queue currently only makes sense for a
        // reader, and is therefore a no-op for a not reading queue.
        if (!getQueue().isReader()) {
            logger.debug(
                    "Skip configure request for not reading queue [uri: '{}']",
                    getQueue().getUri());
            completeConfiguring(GenericResult.SUCCESS);
            return;
        }

        RequestManager.Request req = createConfigureQueueRequest();
        req.setAsyncNotifier(this::onConfigureQueueResponse);
        GenericResult genericResult = sendRawRequest(req);
        if (genericResult.isFailure()) {
            logger.error("Failed to send configureQueue request. RC={}", genericResult);
            completeConfiguring(genericResult);
            return;
        }
        startCancelationTimer();
    }

    @Override
    protected ConfigureQueueCode upcast(GenericResult genericResult) {
        return ConfigureQueueResult.upcast(genericResult);
    }

    @Override
    protected boolean canCancelOnQueueClosing() {
        return true;
    }

    protected void onConfigureQueueResponse(RequestManager.Request r) {
        logger.info("Async notifier for response received: {}", r.response());

        StatusCategory statusCategory = extractResponseStatus(r);

        getQueueStateManager().onConfigureStreamSent(getQueue());

        completeConfiguring(ResultCodeUtils.toGenericResult(statusCategory));
    }

    protected abstract void completeConfiguring(GenericResult result);

    protected ConfigureQueueStrategy(
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
