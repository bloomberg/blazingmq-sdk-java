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

import com.bloomberg.bmq.ResultCodes.GenericResult;
import com.bloomberg.bmq.impl.infr.msg.ControlMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.StatusCategory;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.intf.BrokerConnection;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestManager {
    /*  Thread notation for RequestManager and RequestManager.Request classes.
     *
     *  CONTROL THREAD - thread of 'scheduler' in which given class methods are evaluated
     *  CLIENT THREAD  - thread from which 'waitForResponse' method is called (used in test driver only)
     *
     *  Basic assumption that given class is executed in execution context of BrokerSession.
     *
     */
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final Duration k_TIMEOUT_DEFAULT = Duration.ofSeconds(30);

    public class Request {
        private ControlMessageChoice reqMsg;
        private ControlMessageChoice respMsg;
        private Consumer<Request> onResponse;
        private Consumer<Request> asyncNotifier;
        private ScheduledFuture<?> onTimeoutFuture;
        private FutureTask<Void> onResponseFuture;
        private Duration timeout;

        private volatile Boolean isStarted; // Needed only if assertions are enabled.

        @Override
        public String toString() {
            // CONTRACT
            // Use in the same thread where created, or in any thread after start has been invoked.
            StringBuilder sb = new StringBuilder();
            sb.append("Request message: ")
                    .append(reqMsg.toString())
                    .append("; Timeout duration: ")
                    .append(timeout.toString());
            return sb.toString();
        }

        private Request() {
            reqMsg = null;
            respMsg = null;
            onTimeoutFuture = null;
            timeout = k_TIMEOUT_DEFAULT;
            onResponseFuture =
                    new FutureTask<>(
                            () -> {
                                try {
                                    if (onResponse != null) {
                                        onResponse.accept(this);
                                    } else {
                                        if (asyncNotifier != null) {
                                            asyncNotifier.accept(this);
                                        }
                                    }
                                } catch (Exception e) {
                                    logger.error("Exception:", e);
                                }
                                return null;
                            });
            assert !(isStarted =
                    false); // Wrapped in assert to remove assignment, when asserts are disabled.
        }

        public Request setRequest(ControlMessageChoice msg) {
            // CONTRACT
            // Usage: BEFORE RequestManager.sendRequest invocation only.
            // Thread notation: from CONTROL thread
            // Params: cb should be not null.
            assert !isStarted;
            if (reqMsg != null) {
                throw new IllegalStateException("'reqMsg' is already set");
            }
            reqMsg = Argument.expectNonNull(msg, "msg");
            return this;
        }

        private Request setResponse(ControlMessageChoice msg) {
            assert isStarted;
            if (respMsg != null) {
                throw new IllegalStateException("'respMsg' is already set");
            }
            respMsg = Argument.expectNonNull(msg, "msg");
            return this;
        }

        public Request setOnResponseCb(Consumer<Request> cb) {
            // CONTRACT
            // Usage: BEFORE RequestManager.sendRequest invocation only.
            // Thread notation: from CONTROL thread
            // Params: cb should be not null.
            assert !isStarted;
            if (onResponse != null) {
                throw new IllegalStateException("'onResponse' is already set");
            }
            onResponse = Argument.expectNonNull(cb, "cb");
            return this;
        }

        public Request setAsyncNotifier(Consumer<Request> cb) {
            // CONTRACT
            // Usage: BEFORE RequestManager.sendRequest invocation only.
            // Thread notation: from CONTROL thread
            // Params: cb should be not null.
            assert !isStarted;
            if (asyncNotifier != null) {
                throw new IllegalStateException("'asyncNotifier' is already set");
            }
            asyncNotifier = Argument.expectNonNull(cb, "cb");
            return this;
        }

        private Request setTimeout(Duration duration) {
            assert !isStarted;
            if (timeout != k_TIMEOUT_DEFAULT) {
                throw new IllegalStateException("'timeout' is already set");
            }
            timeout = Argument.expectNonNull(duration, "duration");
            return this;
        }

        private void start() {
            assert !isStarted;
            if (onTimeoutFuture != null) {
                throw new IllegalStateException("Start is already called");
            }
            isStarted = true;
            onTimeoutFuture =
                    RequestManager.this.scheduler.schedule(
                            this::onTimeout, timeout.toNanos(), TimeUnit.NANOSECONDS);
        }

        private void stop() {
            assert isStarted;
            if (onTimeoutFuture == null) {
                throw new IllegalStateException("Stop is called when not started");
            }
            isStarted = false;
            onTimeoutFuture.cancel(false);
        }

        public void waitForResponse() {
            // CONTRACT
            // Usage: AFTER RequestManager.sendRequest invocation only.
            // Thread notation: from CLIENT THREAD only
            assert isStarted;
            try {
                onResponseFuture.get();
            } catch (InterruptedException ex) {
                logger.error("Interrupted: ", ex);
                Thread.currentThread().interrupt();
            } catch (Exception ex) { // CanceledException, ExecutionException
                logger.error("Exception: ", ex);
                throw new RuntimeException(
                        "Runtime exception while expecting for response future.");
            }
        }

        private void onTimeout() {
            assert isStarted;
            logger.info("onTimeout");
            if (!onResponseFuture.isDone()) {
                RequestManager.this.requests.remove(reqMsg.id());
                respMsg = new ControlMessageChoice();
                respMsg.setId(reqMsg.id());
                respMsg.makeStatus();
                respMsg.status().setCategory(StatusCategory.E_TIMEOUT);
                onResponseFuture.run();
            }
        }

        private void cancelWithStatus(StatusCategory statusCategory, String reason) {
            // CONTRACT
            // Usage: AFTER RequestManager.sendRequest invocation ONLY.
            // Thread notation: from CONTROL thread
            assert isStarted;

            String msg = "The request was canceled [reason: " + reason + "]";
            RequestManager.this.requests.remove(reqMsg.id());
            onTimeoutFuture.cancel(false);
            respMsg = new ControlMessageChoice();
            respMsg.setId(reqMsg.id());
            respMsg.makeStatus();
            respMsg.status().setCategory(statusCategory);
            respMsg.status().setMessage(msg);
            onResponseFuture.run();
        }

        public void cancel(String reason) {
            cancelWithStatus(StatusCategory.E_CANCELED, reason);
        }

        public void cancelDueToTimeout() {
            cancelWithStatus(StatusCategory.E_TIMEOUT, "timedout after " + timeout);
        }

        private void onResponse() {
            // CONTRACT
            // Usage:
            // - BEFORE RequestManager.sendRequest invocation
            // - in RequestManager routines
            // - in Request routines under RequestManager synchronization
            // - AFTER Request.waitForResponse return control.
            // Thread notation: from CONTROL thread
            if (!onTimeoutFuture.isDone()) {
                onTimeoutFuture.cancel(false);
                onResponseFuture.run();
            }
        }

        public ControlMessageChoice request() {
            // CONTRACT
            // Usage:
            // - BEFORE RequestManager.sendRequest invocation
            // - in RequestManager routines
            // - in Request routines under RequestManager synchronization
            // - AFTER Request.waitForResponse return control.
            // Thread notation: from CONTROL thread
            return reqMsg;
        }

        public ControlMessageChoice response() {
            return respMsg;
        }

        public void asyncNotify() {
            RequestManager.this.asyncNotify(this);
        }
    }

    public interface Checker {
        boolean check();
    }

    private int nextRequestId = 0;
    private final SchemaEventBuilder schemaEventBuilder;
    private final ScheduledExecutorService scheduler;
    private LinkedHashMap<Integer, Request> requests;
    private final Checker checker;

    public RequestManager(ScheduledExecutorService scheduler) {
        this(scheduler, () -> true);
    }

    public RequestManager(ScheduledExecutorService scheduler, Checker checker) {
        schemaEventBuilder = new SchemaEventBuilder();
        requests = new LinkedHashMap<>();
        this.scheduler = scheduler;
        this.checker = checker;
    }

    public Request createRequest() {
        return new Request();
    }

    public GenericResult sendRequest(Request req, BrokerConnection channel) {
        assert checker.check();
        return sendRequest(req, channel, k_TIMEOUT_DEFAULT);
    }

    public GenericResult sendRequest(
            Request req, BrokerConnection channel, Duration timeoutInterval) {
        assert checker.check();
        Argument.expectNonNull(req, "req");
        Argument.expectNonNull(req.request(), "request()");
        Argument.expectNonNull(channel, "channel");
        Argument.expectNonNull(timeoutInterval, "timeoutInterval");

        int requestId = ++nextRequestId;
        ControlMessageChoice request = req.request();
        request.setId(requestId);
        try {
            schemaEventBuilder.setMessage(request);
        } catch (IOException ex) {
            // Should never happen - program error
            throw new RuntimeException(ex);
        }
        req.setTimeout(timeoutInterval);
        req.start();
        requests.put(requestId, req);
        GenericResult res = channel.write(schemaEventBuilder.build(), true);
        if (res == GenericResult.SUCCESS) {
            logger.info("Message sent: {}", req);
        } else {
            req.stop();
            requests.remove(requestId);
            logger.error("Message not sent(write status = {}): {}", res, req);
        }
        return res;
    }

    public int processResponse(ControlMessageChoice response) {
        assert checker.check();
        if (requests == null) {
            throw new IllegalStateException("'requests' not set");
        }
        Argument.expectNonNull(response, "response");
        logger.debug("Got response: {}", response);
        Request req = requests.get(response.id());
        if (req == null) {
            // The request must have completed at the same time from a
            // different thread while we were waiting.
            logger.info("Empty request");
            return -1;
        }
        requests.remove(response.id());
        req.setResponse(response).onResponse();
        return 0;
    }

    public void cancelAllRequests() {
        assert checker.check();
        LinkedHashMap<Integer, Request> requestsAcquired = this.requests;
        this.requests = new LinkedHashMap<>();
        requestsAcquired.values().forEach(r -> r.cancel("connection lost"));
        requestsAcquired.clear();
    }

    private void asyncNotify(Request request) {
        assert checker.check();
        Argument.expectNonNull(request, "request");
        if (request.asyncNotifier != null) {
            request.asyncNotifier.accept(request);
        }
    }
}
