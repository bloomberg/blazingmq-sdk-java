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
package com.bloomberg.bmq.impl.infr.proto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.bloomberg.bmq.ResultCodes.GenericResult;
import com.bloomberg.bmq.impl.infr.msg.ControlMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.StatusCategory;
import com.bloomberg.bmq.impl.intf.BrokerConnection;
import com.bloomberg.bmq.impl.intf.BrokerConnection.StartCallback;
import com.bloomberg.bmq.impl.intf.BrokerConnection.StartStatus;
import com.bloomberg.bmq.impl.intf.BrokerConnection.StopCallback;
import com.bloomberg.bmq.impl.intf.BrokerConnection.StopStatus;
import com.bloomberg.bmq.impl.intf.SessionEventHandler;
import com.bloomberg.bmq.impl.intf.SessionStatusHandler;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestManagerTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    RequestManager manager;
    Channel channel;
    EventHandler eventHandler;
    Semaphore responseSema;
    Semaphore asyncNotifierSema;
    int numResponses = 0;
    ScheduledExecutorService scheduler;
    AtomicBoolean serverEnabled = new AtomicBoolean(true);
    Consumer<RequestManager.Request> responseCallback = null;
    Consumer<RequestManager.Request> asyncNotifier = null;

    private static void acquireSema(Semaphore sema) {
        try {
            sema.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    void responseCb(RequestManager.Request request) {
        numResponses++;
        logger.info("Response #{} received, requestId: {}", numResponses, request.request().id());
        if (numResponses == 1) {
            RequestManager.Request req = manager.createRequest();
            ControlMessageChoice msg = new ControlMessageChoice();
            req.setRequest(msg);
            Consumer<RequestManager.Request> cb = this::responseCb;
            req.setOnResponseCb(cb);

            logger.info("Sending the second request");
            manager.sendRequest(req, channel);
        }
        if (numResponses == 2) {
            responseSema.release();
        }
    }

    class EventHandler
            implements SessionEventHandler, SessionStatusHandler, StartCallback, StopCallback {

        public EventHandler() {}

        @Override
        public void handleSessionStatus(SessionStatus status) {
            logger.info("Status: {}", status);
        }

        @Override
        public void handleControlEvent(ControlEventImpl controlEvent) {
            handlEvent(controlEvent);
        }

        @Override
        public void handleAckMessage(AckMessageImpl ackMsg) {
            fail();
        }

        @Override
        public void handlePushMessage(PushMessageImpl pushMsg) {
            fail();
        }

        @Override
        public void handlePutEvent(PutEventImpl putEvent) {
            fail();
        }

        @Override
        public void handleConfirmEvent(ConfirmEventImpl confirmEvent) {
            fail();
        }

        public void handlEvent(EventImpl event) {
            logger.info("handleEvent");
            ControlMessageChoice m = new ControlMessageChoice();
            m.setId(numResponses + 1);
            int res = manager.processResponse(m);
            assertEquals(0, res);
        }

        @Override
        public void handleStartCb(StartStatus status) {
            logger.info("handleStartCb: {}", status);
        }

        @Override
        public void handleStopCb(StopStatus status) {
            logger.info("handleStopCb: {}", status);
        }
    }

    class Channel implements BrokerConnection {

        @Override
        public void start(StartCallback startCb) {}

        @Override
        public boolean isStarted() {
            return true;
        }

        @Override
        public void stop(StopCallback stopCb, Duration timeout) {}

        @Override
        public void drop() {}

        @Override
        public GenericResult linger() {
            return GenericResult.SUCCESS;
        }

        @Override
        public boolean isOldStyleMessageProperties() {
            return false;
        }

        @Override
        public GenericResult write(ByteBuffer[] buffers, boolean waitUntilWritable) {
            assertNotNull(eventHandler);
            Runnable task =
                    () -> {
                        if (serverEnabled.get()) {
                            eventHandler.handleControlEvent(null);
                        }
                    };
            new Thread(task).start();
            return GenericResult.SUCCESS;
        }
    }

    @Test
    public void testCallbackReentrance() {

        /*
         * 1. Send one Request using RequestManager and a test Channel.
         * 2. Send another Request from the context of the Response callback.
         * 3. Assure that a Response for the second Request comes.
         */

        logger.info("Started...");

        channel = new Channel();
        eventHandler = new EventHandler();
        responseSema = new Semaphore(0);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        manager = new RequestManager(scheduler);

        RequestManager.Request req = manager.createRequest();
        ControlMessageChoice msg = new ControlMessageChoice();

        logger.info("Connecting...");

        channel.start(eventHandler);

        responseCallback = this::responseCb;
        req.setRequest(msg).setOnResponseCb(responseCallback);

        logger.info("Sending request...");
        manager.sendRequest(req, channel);

        req.waitForResponse();

        logger.info("Waiting for response...");
        acquireSema(responseSema);
    }

    @Test
    public void testRequestTimeout() {
        /*
         * 1. Set request timeout duration.
         * 2. Connect via channel and disable pseudo-server.
         * 3. Try to send a request and start waiting for response.
         * 4. Assure that that the request was canceled because of timeout.
         */

        logger.info("Started...");

        channel = new Channel();
        eventHandler = new EventHandler();
        responseSema = new Semaphore(0);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        manager = new RequestManager(scheduler);

        RequestManager.Request req = manager.createRequest();
        ControlMessageChoice msg = new ControlMessageChoice();

        logger.info("Connecting...");

        channel.start(eventHandler);

        responseCallback = this::responseCb;
        req.setRequest(msg).setOnResponseCb(responseCallback);

        logger.info("Sending request...");
        serverEnabled.set(false);
        manager.sendRequest(req, channel, Duration.ofMillis(100));

        req.waitForResponse();
        assertTrue(req.response().isStatusValue());
        assertSame(StatusCategory.E_TIMEOUT, req.response().status().category());
    }

    @Test
    public void testCancelAllEvents() {
        /*
         * 1. Set request timeout duration.
         * 2. Connect via channel and disable pseudo-server.
         * 3. Try to send a number of requests
         * 4. Cancel all requests via request manager.
         * 5. Assure that that the request was canceled.
         */
        logger.info("Started...");
        final int NUM_REQUESTS = 10;

        channel = new Channel();
        eventHandler = new EventHandler();
        responseSema = new Semaphore(0);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        manager = new RequestManager(scheduler);

        ArrayList<RequestManager.Request> requests = new ArrayList<>();

        logger.info("Connecting...");

        channel.start(eventHandler);

        try {
            serverEnabled.set(false);
            for (int i = 0; i < NUM_REQUESTS; i++) {
                RequestManager.Request req = manager.createRequest();
                responseCallback = this::responseCb;
                req.setRequest(new ControlMessageChoice()).setOnResponseCb(responseCallback);
                logger.info("Sending request...");
                manager.sendRequest(req, channel, Duration.ofSeconds(1000));
                logger.info("rId {}", req.request().id());
                requests.add(req);
            }
            manager.cancelAllRequests();

            requests.forEach(
                    (RequestManager.Request request) -> {
                        assertTrue(request.response().isStatusValue());
                        assertSame(
                                StatusCategory.E_CANCELED, request.response().status().category());
                    });
        } catch (Exception e) {
            logger.error("Exception: ", e);
            fail();
        }
    }

    @Test
    public void testAsyncNotifier() {
        channel = new Channel();
        eventHandler = new EventHandler();
        scheduler = Executors.newSingleThreadScheduledExecutor();
        manager = new RequestManager(scheduler);
        responseSema = new Semaphore(0);

        asyncNotifierSema = new Semaphore(0);

        RequestManager.Request req = manager.createRequest();
        ControlMessageChoice msg = new ControlMessageChoice();

        logger.info("Connecting...");
        channel.start(eventHandler);

        asyncNotifier =
                (request) -> {
                    logger.info("Async notifier callback");
                    asyncNotifierSema.release();
                };
        responseCallback =
                (request) -> {
                    responseSema.release();
                    request.asyncNotify();
                };
        req.setRequest(msg).setOnResponseCb(responseCallback).setAsyncNotifier(asyncNotifier);
        try {
            logger.info("Sending request...");
            manager.sendRequest(req, channel);
            logger.info("Waiting for response...");
            responseSema.acquire();
            asyncNotifierSema.acquire();
        } catch (InterruptedException e) {
            logger.error("Interrupted: ", e);
            Thread.currentThread().interrupt();
            fail();
        }
    }
}
