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

import com.bloomberg.bmq.BMQException;
import com.bloomberg.bmq.HostHealthMonitor;
import com.bloomberg.bmq.HostHealthState;
import com.bloomberg.bmq.QueueOptions;
import com.bloomberg.bmq.ResultCodes.CloseQueueCode;
import com.bloomberg.bmq.ResultCodes.CloseQueueResult;
import com.bloomberg.bmq.ResultCodes.ConfigureQueueCode;
import com.bloomberg.bmq.ResultCodes.ConfigureQueueResult;
import com.bloomberg.bmq.ResultCodes.GenericCode;
import com.bloomberg.bmq.ResultCodes.GenericResult;
import com.bloomberg.bmq.ResultCodes.OpenQueueCode;
import com.bloomberg.bmq.ResultCodes.OpenQueueResult;
import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.Uri;
import com.bloomberg.bmq.impl.events.AckMessageEvent;
import com.bloomberg.bmq.impl.events.BrokerSessionEvent;
import com.bloomberg.bmq.impl.events.Event;
import com.bloomberg.bmq.impl.events.PushMessageEvent;
import com.bloomberg.bmq.impl.infr.net.ConnectionOptions;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnectionFactory;
import com.bloomberg.bmq.impl.infr.proto.AckMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.ConfirmEventBuilder;
import com.bloomberg.bmq.impl.infr.proto.ConfirmEventImpl;
import com.bloomberg.bmq.impl.infr.proto.ConfirmMessage;
import com.bloomberg.bmq.impl.infr.proto.ControlEventImpl;
import com.bloomberg.bmq.impl.infr.proto.EventType;
import com.bloomberg.bmq.impl.infr.proto.PushMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.PutEventImpl;
import com.bloomberg.bmq.impl.infr.proto.PutMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.RequestManager;
import com.bloomberg.bmq.impl.infr.stat.QueuesStats;
import com.bloomberg.bmq.impl.infr.stat.Stats;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.intf.BrokerConnection;
import com.bloomberg.bmq.impl.intf.EventHandler;
import com.bloomberg.bmq.impl.intf.QueueHandle;
import com.bloomberg.bmq.impl.intf.QueueState;
import com.bloomberg.bmq.impl.intf.SessionEventHandler;
import com.bloomberg.bmq.impl.intf.SessionStatusHandler;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BrokerSession
        implements HostHealthMonitor.Handler, SessionEventHandler, SessionStatusHandler {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int EVENT_POLLING_TIMEOUT = 1; // sec
    private static final int TERMINATION_TIMEOUT = EVENT_POLLING_TIMEOUT * 2; // sec

    private static final Duration ADD_MONITOR_HANDLER_TIMEOUT = Duration.ofSeconds(30);

    private final BrokerConnection brokerConnection; // thread-safe
    private final RequestManager requestManager; // thread-safe
    private final InboundEventBuffer inboundEventBuffer; // thread-safe
    private final Semaphore startSema; // thread-safe
    private final EventHandler eventHandler; // immutable
    private final SessionOptions sessionOptions; // immutable
    private final ExecutorService threadPool; // thread-safe
    private final QueueStateManager queueStateManager; // thread-safe
    private final ScheduledExecutorService scheduler; // thread-safe
    private final Stats stats; // thread-safe
    private final QueueControlStrategyFactory strategyFactory;
    private final LateResponseHandler lateResponseHandler;
    private final long threadId;
    private final PutPoster putPoster;

    private final AtomicBoolean isUserStart;
    private final AtomicBoolean isUserStop;
    private final AtomicBoolean isStarting;
    private final AtomicBoolean isStopping;

    private volatile ScheduledFuture<?> onStartTimeoutFuture;
    private volatile ScheduledFuture<?> onStopTimeoutFuture;
    private volatile HostHealthState hostHealthState = HostHealthState.Healthy;

    private int numPendingHostHealthRequests = 0;

    public boolean isInSessionExecutor() {
        return threadId == Thread.currentThread().getId();
    }

    @FunctionalInterface
    public interface Enqueuer {
        void enqueueEvent(Event event);
    }

    public interface HealthManager {
        void onHealthRequest();

        void onHealthResponse();

        boolean isHostHealthy();

        void checkHostIsStableHealthy();
    }

    private class HealthManagerImpl implements HealthManager {
        @Override
        public void onHealthRequest() {
            assert isInSessionExecutor();

            // Increment number of Host Health requests outstanding.
            ++numPendingHostHealthRequests;
            logger.debug(
                    "Incremented num of pending host health requests to {}",
                    numPendingHostHealthRequests);
        }

        @Override
        public void onHealthResponse() {
            assert isInSessionExecutor();

            // Indicate one fewer host-health queue request is pending.
            //
            // A `resume` request may fire a HOST_HEALTH_RESTORED event only
            // when this counter reaches zero.
            if (numPendingHostHealthRequests <= 0) {
                logger.warn("Attempt to decrement num of pending host health requests below zero");
                return;
            }

            --numPendingHostHealthRequests;
            logger.debug(
                    "Decremented num of pending host health requests to {}",
                    numPendingHostHealthRequests);
        }

        @Override
        public boolean isHostHealthy() {
            assert isInSessionExecutor();
            return BrokerSession.this.isHostHealthy();
        }

        @Override
        public void checkHostIsStableHealthy() {
            assert isInSessionExecutor();
            BrokerSession.this.checkHostIsStableHealthy();
        }
    }

    public static BrokerSession createInstance(
            SessionOptions sessionOptions,
            TcpConnectionFactory connectionFactory,
            ScheduledExecutorService scheduler,
            EventHandler eventHandler) {
        CompletableFuture<Long> completableFuture = new CompletableFuture<>();
        scheduler.execute(() -> completableFuture.complete(Thread.currentThread().getId()));
        long threadId = 0;
        try {
            threadId = BmqFuture.get(completableFuture, BmqFuture.s_DEFAULT_TIMEOUT);
        } catch (TimeoutException e) {
            throw new BMQException("Failed to get thread ID: ", e);
        }
        return new BrokerSession(
                sessionOptions, connectionFactory, scheduler, eventHandler, threadId);
    }

    // Shutdown ExecutorService as recommended in JavaDoc
    private static void awaitTermination(ExecutorService pool) {
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(TERMINATION_TIMEOUT, TimeUnit.SECONDS)) {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(TERMINATION_TIMEOUT, TimeUnit.SECONDS))
                    throw new RuntimeException("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    @SuppressWarnings("squid:S2142")
    private BrokerSession(
            SessionOptions sessionOptions,
            TcpConnectionFactory connectionFactory,
            ScheduledExecutorService scheduler,
            EventHandler eventHandler,
            long threadId) {
        this.scheduler = scheduler;
        stats =
                new Stats(
                        this.scheduler,
                        sessionOptions.statsDumpInterval().getSeconds(),
                        this::dumpStats);
        inboundEventBuffer =
                new InboundEventBuffer(
                        sessionOptions.inboundBufferWaterMark(), stats.eventQueueStats());
        startSema = new Semaphore(0);
        this.sessionOptions = sessionOptions;

        // Init futures with dummy tasks
        onStartTimeoutFuture = this.scheduler.schedule(() -> {}, 0, TimeUnit.NANOSECONDS);
        onStopTimeoutFuture = this.scheduler.schedule(() -> {}, 0, TimeUnit.NANOSECONDS);
        this.threadId = threadId;
        requestManager = new RequestManager(scheduler, this::isInSessionExecutor);
        brokerConnection =
                new TcpBrokerConnection(
                        new ConnectionOptions(this.sessionOptions),
                        connectionFactory,
                        scheduler,
                        requestManager,
                        stats.eventsStats(),
                        this, // SessionEventHandler
                        this); // SessionStatusHandler
        putPoster = new PutPoster(brokerConnection, stats.eventsStats());

        this.eventHandler = eventHandler;
        threadPool = Executors.newSingleThreadExecutor();
        queueStateManager = QueueStateManager.createInstance();
        strategyFactory =
                QueueControlStrategyFactory.create(
                        brokerConnection,
                        requestManager,
                        this.sessionOptions,
                        queueStateManager,
                        this::enqueueEvent,
                        this.scheduler,
                        new HealthManagerImpl());
        lateResponseHandler =
                LateResponseHandler.createInstance(
                        queueStateManager, strategyFactory, this.sessionOptions);

        isUserStart = new AtomicBoolean(false);
        isUserStop = new AtomicBoolean(false);
        isStarting = new AtomicBoolean(false);
        isStopping = new AtomicBoolean(false);

        threadPool.submit(
                () -> {
                    logger.debug("Worker thread started");
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            Event event = inboundEventBuffer.poll(EVENT_POLLING_TIMEOUT);
                            if (event != null) {
                                logger.debug("Event received: {}", event);
                                this.eventHandler.handleEvent(event);
                            }
                        } catch (InterruptedException e) {
                            logger.info(
                                    "Worker thread interrupted. isInterrupted() {}",
                                    Thread.currentThread().isInterrupted());
                            break;
                        } catch (Exception e) {
                            logger.error("Worker thread got user exception: ", e);
                        }
                    }
                    logger.info("Worker thread finished");
                });
    }

    @SuppressWarnings("squid:S2142")
    public GenericResult start(Duration timeout) {
        if (!isStarting.compareAndSet(false, true)) {
            logger.info("Starting in progress");
            return GenericResult.NOT_SUPPORTED;
        }

        if (timeout == null) {
            timeout = sessionOptions.startTimeout();
        }

        isUserStart.set(true);

        // Add host health monitor handler
        addHostHealthMonitorHandler();

        brokerConnection.start(
                (BrokerConnection.StartStatus status) -> {
                    logger.debug("Start callback: {}", status);
                    if (status == BrokerConnection.StartStatus.SUCCESS) {
                        startSema.release();

                        // Enabled scheduled stats dumping
                        stats.enableDumping();
                    }
                    // TODO: handle error status
                });

        logger.debug("Broker connection is starting.");
        try {
            if (!startSema.tryAcquire(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
                brokerConnection.stop(
                        (BrokerConnection.StopStatus status) -> {
                            logger.info("Stop callback: {}", status);
                            // TODO: Here looks like here we need to do the best effort
                            // and if connection stop fails choose different from TIMEOUT
                            // return code for BrokerSession.start
                        },
                        sessionOptions.stopTimeout());
                return GenericResult.TIMEOUT;
            }
            logger.debug("Broker connection started.");
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            // Not recoverable
            throw new RuntimeException(ex);
        } finally {
            isStarting.set(false);
        }

        return GenericResult.SUCCESS;
    }

    public void startAsync(Duration timeout) {
        if (!isStarting.compareAndSet(false, true)) {
            scheduler.execute(
                    () -> {
                        logger.info("Starting in progress");
                        enqueueConnectionInProgress();
                    });
            return;
        }

        if (timeout == null) {
            timeout = sessionOptions.startTimeout();
        }

        isUserStart.set(true);

        // Add host health monitor handler
        addHostHealthMonitorHandler();

        onStartTimeoutFuture =
                scheduler.schedule(this::onStartTimeout, timeout.toNanos(), TimeUnit.NANOSECONDS);

        brokerConnection.start(
                (BrokerConnection.StartStatus status) -> {
                    logger.info("Start callback: {}", status);
                    onStartTimeoutFuture.cancel(false);
                    switch (status) {
                        case SUCCESS:
                            enqueueConnected();

                            // Enabled scheduled stats dumping
                            stats.enableDumping();
                            break;
                        case CANCELLED:
                            enqueueCanceled();
                            break;
                        default:
                            enqueueError();
                            break;
                    }
                    isStarting.set(false);
                });
    }

    public boolean isStarted() {
        return brokerConnection.isStarted();
    }

    @SuppressWarnings("squid:S2142")
    public GenericResult stop(Duration timeout) {
        if (!isStopping.compareAndSet(false, true)) {
            logger.info("Stopping in progress");
            return GenericResult.NOT_SUPPORTED;
        }

        if (timeout == null) {
            timeout = sessionOptions.stopTimeout();
        }

        isUserStop.set(true);

        // Remove host health monitor handler
        removeHostHealthMonitorHandler();

        // Future of connection has been stopped.
        final CompletableFuture<Void> connectionStopFuture = new CompletableFuture<>();
        brokerConnection.stop(
                (BrokerConnection.StopStatus status) -> {
                    logger.debug("Stop callback: {}", status);
                    // TODO: handle error status
                    connectionStopFuture.complete(null);
                },
                timeout);

        logger.debug("Broker connection is stopping.");

        try {
            // Stop broker connection first
            connectionStopFuture
                    // Then cancel pending requests
                    .thenCompose(r -> execQueueManagerCleanUp())
                    .get(timeout.toNanos(), TimeUnit.NANOSECONDS);

            logger.debug("Broker connection stopped.");

            // Disable scheduled stats dumping
            stats.disableDumping();
            // Print final statistics
            stats.dumpFinal();
        } catch (TimeoutException ex) {
            logger.debug("Disconnect timeout");
            return GenericResult.TIMEOUT;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            // Not recoverable
            throw new RuntimeException(ex);
        } catch (ExecutionException ex) {
            // Not recoverable
            throw new RuntimeException(ex);
        } finally {
            isStopping.set(false);
        }
        return GenericResult.SUCCESS;
    }

    public void stopAsync(Duration timeout) {
        if (!isStopping.compareAndSet(false, true)) {
            scheduler.execute(
                    () -> {
                        logger.info("Stopping in progress");
                        enqueueDisconnectionInProgress();
                    });
            return;
        }

        final Duration stopTimeout = timeout != null ? timeout : sessionOptions.stopTimeout();

        isUserStop.set(true);

        // Remove host health monitor handler
        removeHostHealthMonitorHandler();

        final CompletableFuture<Void> connectionStopFuture = new CompletableFuture<>();

        onStopTimeoutFuture =
                scheduler.schedule(
                        this::onStopTimeout, stopTimeout.toNanos(), TimeUnit.NANOSECONDS);

        // Stop broker connection first
        CompletableFuture.runAsync(
                        () ->
                                brokerConnection.stop(
                                        (BrokerConnection.StopStatus status) -> {
                                            logger.info("Stop callback: {}", status);
                                            // TODO: handle error status
                                            connectionStopFuture.complete(null);
                                        },
                                        stopTimeout),
                        scheduler)
                // Wait for broker connection to be stopped
                .thenComposeAsync(r -> connectionStopFuture, scheduler)
                // Then cancel pending requests
                .thenComposeAsync(r -> execQueueManagerCleanUp(), scheduler)
                // Finally enqueue event and dump stats
                .thenAcceptAsync(
                        r -> {
                            logger.debug("Broker connection stopped.");

                            onStopTimeoutFuture.cancel(false);
                            enqueueDisconnected();
                            isStopping.set(false);

                            // Disable scheduled stats dumping
                            stats.disableDumping();
                            // Print final statistics
                            stats.dumpFinal();
                        },
                        scheduler);

        logger.info("Broker connection is stopping.");
    }

    private void addHostHealthMonitorHandler() {
        if (sessionOptions.hostHealthMonitor() == null) {
            return;
        }

        // Since host health state handler is executed in the scheduler thread,
        // we also execute the code below using the scheduler. This way if
        // the handler is called by the monitor, we ensure it executes after
        // 'hostHealthState' is set to initial value.
        final CompletableFuture<Void> future = new CompletableFuture<>();
        scheduler.execute(
                () -> {
                    try {
                        boolean added = sessionOptions.hostHealthMonitor().addHandler(this);

                        if (added) {
                            HostHealthState state =
                                    sessionOptions.hostHealthMonitor().hostHealthState();
                            logger.info(
                                    "Added host health monitor handler, current state: {}", state);

                            // Handle the state provided by the monitor.
                            // If the host has become unhealthy, corresponding event
                            // should be issued.
                            handleHostHealthState(state);
                        } else {
                            logger.warn("Host health monitor handler was already added");
                        }
                    } catch (Exception e) {
                        logger.warn("Failed to add host health monitor handler", e);
                    } finally {
                        future.complete(null);
                    }
                });

        try {
            // Use timeout to ensure we do not wait forever
            BmqFuture.get(future, ADD_MONITOR_HANDLER_TIMEOUT);
        } catch (TimeoutException e) {
            logger.error("Add host health monitor handler timeout: ", e);
        }
    }

    private void removeHostHealthMonitorHandler() {
        if (sessionOptions.hostHealthMonitor() == null) {
            return;
        }

        try {
            boolean removed = sessionOptions.hostHealthMonitor().removeHandler(this);

            // After the handler is removed, it should not be called by the
            // monitor anymore so we should be safe to reset 'hostHealthState'.
            if (removed) {
                logger.info("Removed host health monitor handler");
                hostHealthState = HostHealthState.Healthy;
            } else {
                logger.warn("Host health monitor handler was already removed");
            }
        } catch (Exception e) {
            logger.warn("Failed to remove host health monitor handler", e);
        }
    }

    private CompletableFuture<Void> execQueueManagerCleanUp() {
        // Future of session has been cleaned up.
        logger.debug("Called execQueueManagerCleanUp");
        final CompletableFuture<Void> cleanUpFuture = new CompletableFuture<>();
        Runnable cleanUpCompletion =
                () -> {
                    logger.debug("Queue manager cleaning up started");

                    queueStateManager.getAllQueues().stream().forEach(QueueImpl::resetQueue);
                    queueStateManager.reset();
                    cleanUpFuture.complete(null);

                    logger.debug("Queue manager cleaning up completed");
                };

        scheduler.execute(
                () -> {
                    logger.debug("Queue manager cleaning up ...");

                    try {
                        // All state mutations are made in scheduler thread.
                        // Create list of unfinished sequences.
                        Collection<QueueControlStrategy<?>> activeStrategies =
                                queueStateManager.getAllQueues().stream()
                                        .map(QueueImpl::getStrategy)
                                        .filter(Objects::nonNull)
                                        .collect(Collectors.toList());
                        if (!activeStrategies.isEmpty()) {
                            // If given list is not empty, then cancel all unfinished sequences
                            // and set up clean up action as a continuation executed in scheduler.
                            activeStrategies.forEach(QueueControlStrategy::cancelOnStop);
                            activeStrategies.stream()
                                    .map(QueueControlStrategy::getResultFuture)
                                    .forEach(CompletableFuture::join);

                            CompletableFuture.runAsync(cleanUpCompletion, scheduler);

                        } else {
                            // Otherwise immediately perform clean up action.
                            cleanUpCompletion.run();
                        }
                    } catch (Exception e) {
                        logger.error("Failed to cleanup the Queue Manager: ", e);
                    }
                });
        return cleanUpFuture;
    }

    @Override
    public void handleSessionStatus(SessionStatus status) {
        assert isInSessionExecutor();
        if ((status == SessionStatus.SESSION_DOWN && isUserStop.getAndSet(false))
                || (status == SessionStatus.SESSION_UP && isUserStart.getAndSet(false))) {
            logger.debug("Handle session status(user action): {}", status);
            if (status == SessionStatus.SESSION_DOWN) {
                enqueueNegativeAcks();
            }
        } else {
            logger.debug("Handle session status: {}", status);
            if (status == SessionStatus.SESSION_UP) {
                enqueueReconnected();
                Collection<QueueImpl> openedQueues = queueStateManager.getOpenedQueues();
                if (openedQueues.isEmpty()) {
                    enqueueStateRestored();
                    return;
                }

                // Reset opened queues substream count
                openedQueues.stream().forEach(queueStateManager::resetSubStreamCount);

                openedQueues.stream()
                        // QueueImpl -> CompletableFuture<OpenQueueCode> -> CompletableFuture<Void>
                        .map(q -> createReopenFuture(q).thenAccept(c -> {}))
                        // Use completed future as Identity which is returned by reduce() if stream
                        // is empty
                        .reduce(CompletableFuture.completedFuture(null), CompletableFuture::allOf)
                        .thenRunAsync(this::enqueueStateRestored, scheduler);
            } else if (status == SessionStatus.SESSION_DOWN) {
                enqueueConnectionLost();
                enqueueNegativeAcks();
            }
        }
    }

    public GenericResult linger() {
        GenericResult res = brokerConnection.linger();
        threadPool.shutdown();
        scheduler.shutdown();
        awaitTermination(threadPool);
        awaitTermination(scheduler);
        return res;
    }

    private void handleHostHealthState(HostHealthState state) {
        assert isInSessionExecutor();

        // Do nothing if monitor reports null value.
        if (state == null) {
            logger.warn("Host health monitor reported null state");
            return;
        }

        logger.info("Host health monitor reported new state: {}", state);

        // Do nothing if host health hasn't changed
        if (Objects.equals(hostHealthState, state)) {
            return;
        }

        boolean wasHealthy = isHostHealthy();
        hostHealthState = state;

        switch (hostHealthState) {
            case Healthy:
                logger.info("The host has entered healthy state");
                resumeHealthSensitiveQueues();
                break;
            case Unhealthy:
            case Unknown:
                // Enter unhealthy state only if host was healthy before.
                // Otherwise ignore unhealthy <-> unknown transitions.
                if (wasHealthy) {
                    logger.info("The host has entered unhealthy state");
                    suspendHealthSensitiveQueues();
                }
                break;
            default:
                logger.warn("Unexpected host health state: {}", state);
                break;
        }
    }

    @Override
    public void onHostHealthStateChanged(HostHealthState state) {
        // Executed by thread determined by "HostHealthMonitor".

        scheduler.execute(() -> handleHostHealthState(state));
    }

    private boolean isHostHealthy() {
        return hostHealthState == HostHealthState.Healthy;
    }

    @Override
    public void handleControlEvent(ControlEventImpl controlEvent) {
        assert isInSessionExecutor();
        try {
            if (requestManager.processResponse(controlEvent.controlChoice()) != 0) {
                lateResponseHandler.handleLateResponse(controlEvent.controlChoice());
            }
        } catch (JsonSyntaxException exception) {
            throw new RuntimeException(exception);
        }
    }

    @Override
    public void handleAckMessage(AckMessageImpl ackMsg) {
        assert isInSessionExecutor();
        try {
            putPoster.registerAck(ackMsg);
            AckMessageEvent event = AckMessageEvent.createInstance(ackMsg);
            enqueueEvent(event);
        } catch (IllegalStateException e) {
            logger.error("Failed to handle ACK message: ", e);
        }
    }

    @Override
    public void handlePushMessage(PushMessageImpl pushMsg) {
        assert isInSessionExecutor();
        logger.trace("Got push message: {}", pushMsg);
        PushMessageEvent event = PushMessageEvent.createInstance(pushMsg);
        enqueueEvent(event);
    }

    @Override
    public void handleConfirmEvent(ConfirmEventImpl confirmEvent) {
        assert isInSessionExecutor();
        logger.error("Unexpected event type: confirm event {}", confirmEvent);
        assert false : "Unexpected event type: confirm event - failure in debug mode";
    }

    @Override
    public void handlePutEvent(PutEventImpl putEvent) {
        assert isInSessionExecutor();
        logger.error("Unexpected event type: put event {}", putEvent);
        assert false : "Unexpected event type: put event - failure in debug mode";
    }

    /**
     * Returns the size of the inbound event buffer.
     *
     * @return int size of the inbound event buffer
     */
    public int inboundBufferSize() {
        return inboundEventBuffer.size();
    }

    @SuppressWarnings("squid:S2142")
    private void enqueueEvent(Event event) {
        assert isInSessionExecutor();
        try {
            inboundEventBuffer.put(event);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted", e);
        }
    }

    private void enqueueSessionEvent(
            BrokerSessionEvent.Type sessionEventType, String errorDescription) {
        BrokerSessionEvent brokerSessionEvent =
                BrokerSessionEvent.createInstance(sessionEventType, errorDescription);
        enqueueEvent(brokerSessionEvent);
    }

    private void enqueueDisconnected() {
        enqueueSessionEvent(BrokerSessionEvent.Type.e_DISCONNECTED, "");
    }

    private void enqueueReconnected() {
        enqueueSessionEvent(BrokerSessionEvent.Type.e_RECONNECTED, "");
    }

    private void enqueueConnectionLost() {
        enqueueSessionEvent(BrokerSessionEvent.Type.e_CONNECTION_LOST, "");
    }

    private void enqueueStateRestored() {
        enqueueSessionEvent(BrokerSessionEvent.Type.e_STATE_RESTORED, "");
    }

    private void enqueueConnected() {
        enqueueSessionEvent(BrokerSessionEvent.Type.e_CONNECTED, "");
    }

    private void enqueueConnectionTimedOut() {
        enqueueSessionEvent(BrokerSessionEvent.Type.e_CONNECTION_TIMEOUT, "");
    }

    private void enqueueDisconnectionTimedOut() {
        enqueueSessionEvent(BrokerSessionEvent.Type.e_DISCONNECTION_TIMEOUT, "");
    }

    private void enqueueCanceled() {
        enqueueSessionEvent(BrokerSessionEvent.Type.e_CANCELED, "");
    }

    private void enqueueError() {
        enqueueSessionEvent(BrokerSessionEvent.Type.e_ERROR, "");
    }

    private void enqueueConnectionInProgress() {
        enqueueSessionEvent(BrokerSessionEvent.Type.e_CONNECTION_IN_PROGRESS, "");
    }

    private void enqueueDisconnectionInProgress() {
        enqueueSessionEvent(BrokerSessionEvent.Type.e_DISCONNECTION_IN_PROGRESS, "");
    }

    private void enqueueHostUnhealthy() {
        enqueueSessionEvent(BrokerSessionEvent.Type.e_HOST_UNHEALTHY, "");
    }

    private void enqueueHostHealthRestored() {
        enqueueSessionEvent(BrokerSessionEvent.Type.e_HOST_HEALTH_RESTORED, "");
    }

    private void enqueueNegativeAcks() {
        for (AckMessageImpl m : putPoster.createNegativeAcks()) {
            AckMessageEvent event = AckMessageEvent.createInstance(m);
            enqueueEvent(event);
        }
    }

    private CompletableFuture<OpenQueueCode> createReopenFuture(QueueImpl queue) {
        assert isInSessionExecutor();
        QueueId queueId = queue.getFullQueueId();
        logger.error("Queue id={} should be reopened", queueId);
        final CompletableFuture<OpenQueueCode> future = new CompletableFuture<>();
        QueueControlStrategy<OpenQueueCode> strategy =
                strategyFactory.createReopenAsyncSequence(
                        queue, sessionOptions.openQueueTimeout(), queue.getQueueOptions(), future);
        strategy.start();
        return strategy.getResultFuture();
    }

    private void suspendHealthSensitiveQueues() {
        assert isInSessionExecutor();

        // If there are already outstanding suspend/resume requests, then we have
        // moved from (Unhealthy -> Healthy -> Unhealthy) host-health state without
        // having finished resuming all queues in the time between the two
        // unhealthy states. Our session in this case never entered a completely
        // "healthy" state, so we never issued a corresponding "health restored"
        // event. We therefore also elide this intermediate "host unhealthy" event.
        if (numPendingHostHealthRequests == 0) {
            enqueueHostUnhealthy();
        }

        // Suspend health sensitive queues if we are connected
        if (isStarted()) {
            logger.info("Suspend health sensitive queues");
            // Attempt to suspend each queue.
            queueStateManager
                    // Grab opened queues
                    .getOpenedQueues().stream()
                    // Take queues without currently running strategy
                    .filter(q -> q.getStrategy() == null)
                    // Take queues which are sensitive to host health
                    .filter(q -> q.getQueueOptions().getSuspendsOnBadHostHealth())
                    // Take queues which are not already suspended
                    .filter(q -> !q.getIsSuspendedWithBroker())
                    // Run suspend strategy
                    .forEach(this::suspendQueue);
        }
    }

    private void suspendQueue(QueueImpl queue) {
        assert isInSessionExecutor();

        final CompletableFuture<ConfigureQueueCode> future = new CompletableFuture<>();

        SuspendQueueStrategy strategy =
                strategyFactory.createSuspendAsyncSequence(
                        queue,
                        sessionOptions.configureQueueTimeout(),
                        queue.getQueueOptions(),
                        future);

        strategy.start();

        // When the strategy completes we need to check whether the host health
        // has changed and send suspend or resume request if necessary.
        future.thenAccept(code -> alignQueueWithHostHealth(queue, code));
    }

    private void resumeHealthSensitiveQueues() {
        assert isInSessionExecutor();

        // An increment on the number of pending requests acts as a "guard" to
        // ensure that no calls to 'resumeQueue' inadvertently issue a
        // redundant 'HOST_HEALTH_RESUMED' event (which could otherwise occur in
        // certain cases, e.g., write-only queues, request failure, etc).
        ++numPendingHostHealthRequests;
        logger.debug(
                "Incremented (guard) num of pending host health requests to {}",
                numPendingHostHealthRequests);

        // Resume health sensitive queues if we are connected
        if (isStarted()) {
            logger.info("Resume health sensitive queues");
            // Attempt to resume each queue.
            queueStateManager
                    // Grab opened queues
                    .getOpenedQueues().stream()
                    // Take queues without currently running strategy
                    .filter(q -> q.getStrategy() == null)
                    // Take queues which are sensitive to host health
                    .filter(q -> q.getQueueOptions().getSuspendsOnBadHostHealth())
                    // Take queues which are suspended
                    .filter(QueueImpl::getIsSuspendedWithBroker)
                    // Run resume strategy
                    .forEach(this::resumeQueue);
        }

        // Decrement our counter "guard".
        --numPendingHostHealthRequests;
        logger.debug(
                "Decremented (guard) num of pending host health requests to {}",
                numPendingHostHealthRequests);

        // If we aren't waiting for any queues to resume, then just immediately
        // publish the session event ourselves.
        checkHostIsStableHealthy();
    }

    private void resumeQueue(QueueImpl queue) {
        assert isInSessionExecutor();

        final CompletableFuture<ConfigureQueueCode> future = new CompletableFuture<>();

        ResumeQueueStrategy strategy =
                strategyFactory.createResumeAsyncSequence(
                        queue,
                        sessionOptions.configureQueueTimeout(),
                        queue.getQueueOptions(),
                        future);

        strategy.start();

        // When the strategy completes we need to check whether the host health
        // has changed and send suspend or resume request if necessary.
        future.thenAccept(code -> alignQueueWithHostHealth(queue, code));
    }

    private void alignQueueWithHostHealth(QueueImpl queue, GenericCode code) {
        assert isInSessionExecutor();
        Argument.expectNonNull(queue, "queue");

        // Request was canceled. This could happen due to:
        //  - connection lost.
        //  - user has asked to close the queue when there was a pending
        //    configuration request (standalone, suspend or resume).
        // In both cases we do nothing.
        // If connection is restored or user opens the queue again, it will
        // align with host health state during the opening.
        if (code.isCanceled()) {
            return;
        }

        // Queue is not suspendable.
        if (!queue.getQueueOptions().getSuspendsOnBadHostHealth()) {
            return;
        }

        // If the queue is health sensitive and the previous configure request
        // wasn't canceled, then we need to check if the host-health changed and
        // new configuration-request to suspend or resume the queue should be sent.
        if (queue.getIsSuspendedWithBroker() && isHostHealthy()) {
            logger.info("The host is healthy now, need to resume queue: {}", queue.getUri());
            resumeQueue(queue);
        } else if (!queue.getIsSuspendedWithBroker() && !isHostHealthy()) {
            logger.info("The host is unhealthy now, need to suspend queue: {}", queue.getUri());
            suspendQueue(queue);
        }
    }

    private void checkHostIsStableHealthy() {
        assert isInSessionExecutor();

        // TODO: check that we are not stopping the session

        // If there are no more outstanding requests, AND the host remains
        // healthy, then issue a HOST_HEALTH_RESTORED event.
        if (numPendingHostHealthRequests == 0 && isHostHealthy()) {
            enqueueHostHealthRestored();
        }
    }

    private void onStartTimeout() {
        brokerConnection.stop(
                (BrokerConnection.StopStatus status) -> {
                    logger.info("Stop callback: {}", status);
                },
                sessionOptions.stopTimeout());
        enqueueConnectionTimedOut();
        isStarting.set(false);
    }

    private void onStopTimeout() {
        enqueueDisconnectionTimedOut();
        isStopping.set(false);
    }

    private void dumpStats(String stats) {
        logger.info(stats);
    }

    public BmqFuture<OpenQueueCode> openQueueAsync(
            QueueImpl queue, QueueOptions ops, Duration timeout) {
        final Duration timeOut = timeout == null ? sessionOptions.openQueueTimeout() : timeout;
        final CompletableFuture<OpenQueueCode> future = new CompletableFuture<>();
        scheduler.execute(
                () -> {
                    try {
                        strategyFactory
                                .createOpenAsyncSequence(queue, timeOut, ops, future)
                                .start();
                    } catch (Exception e) {
                        logger.error("Failed to execute OpenQueue strategy: ", e);
                    }
                });

        // Add the queue to queues stats and send suspend/resume request if needed
        CompletableFuture<OpenQueueCode> statsFuture =
                future.thenApply(
                        res -> {
                            if (res.isSuccess()) {
                                stats.queuesStats()
                                        .onQueueOpen(queue.getFullQueueId(), queue.getUri());

                                // If queue is opened and not suspended that means the host was
                                // healthy and configure request was sent. We need to check
                                // whether the host is unhealthy now and send suspend request.
                                // If queue is opened and suspended that means the host was
                                // unhealthy and configure request was not sent. We need to
                                // check whether the host is healthy now and send resume request.
                                alignQueueWithHostHealth(queue, res);
                            }

                            return res;
                        });

        return new BmqFuture<>(statsFuture);
    }

    public OpenQueueCode openQueue(QueueImpl queue, QueueOptions ops, Duration timeout) {
        final Duration timeOut = timeout == null ? sessionOptions.openQueueTimeout() : timeout;
        final CompletableFuture<OpenQueueCode> future = new CompletableFuture<>();
        OpenQueueCode res = OpenQueueResult.UNKNOWN;
        scheduler.execute(
                () -> {
                    try {
                        strategyFactory.createOpenSyncSequence(queue, timeOut, ops, future).start();
                    } catch (IllegalStateException e) {
                        logger.error("Failed to open queue: ", e);
                        future.complete(OpenQueueResult.ALREADY_OPENED);
                    } catch (Exception e) {
                        logger.error("Failed to open queue for unknown reason: ", e);
                    }
                });
        try {
            res = BmqFuture.get(future, timeOut.plus(timeOut));
        } catch (TimeoutException e) {
            logger.error("Open queue future timeout: ", e);
            res = OpenQueueResult.TIMEOUT;
        }

        // Add queue into queues stats
        if (res.isSuccess()) {
            stats.queuesStats().onQueueOpen(queue.getFullQueueId(), queue.getUri());

            // If queue is opened and not suspended that means the host was
            // healthy and configure request was sent. We need to check
            // whether the host is unhealthy now and send suspend request.
            // If queue is opened and suspended that means the host was
            // unhealthy and configure request was not sent. We need to
            // check whether the host is healthy now and send resume request.
            final GenericCode code = res;
            scheduler.execute(() -> alignQueueWithHostHealth(queue, code));
        }

        return res;
    }

    public BmqFuture<ConfigureQueueCode> configureQueueAsync(
            QueueImpl queue, QueueOptions ops, Duration timeout) {
        final Duration timeOut = timeout == null ? sessionOptions.configureQueueTimeout() : timeout;
        final CompletableFuture<ConfigureQueueCode> future = new CompletableFuture<>();
        scheduler.execute(
                () -> {
                    try {
                        strategyFactory
                                .createConfigureAsyncSequence(queue, timeOut, ops, future)
                                .start();

                        // When the strategy completes, we need to check whether
                        // the host health has changed and send suspend or resume
                        // request if necessary.
                        future.thenAccept(code -> alignQueueWithHostHealth(queue, code));

                    } catch (Exception e) {
                        logger.error("Failed to configure queue async: ", e);
                    }
                });

        return new BmqFuture<>(future);
    }

    public ConfigureQueueCode configureQueue(QueueImpl queue, QueueOptions ops, Duration timeout) {
        final Duration timeOut = timeout == null ? sessionOptions.configureQueueTimeout() : timeout;
        final CompletableFuture<ConfigureQueueCode> future = new CompletableFuture<>();
        ConfigureQueueCode res = ConfigureQueueResult.UNKNOWN;
        scheduler.execute(
                () -> {
                    try {
                        strategyFactory
                                .createConfigureSyncSequence(queue, timeOut, ops, future)
                                .start();

                        // When the strategy completes, we need to check whether
                        // the host health has changed and send suspend or resume
                        // request if necessary.
                        future.thenAccept(code -> alignQueueWithHostHealth(queue, code));

                    } catch (Exception e) {
                        logger.error("Failed to configure queue: ", e);
                    }
                });
        try {
            res = BmqFuture.get(future, timeOut.plus(timeOut));
        } catch (TimeoutException e) {
            logger.error("Configure queue future timeout: ", e);
            res = ConfigureQueueResult.TIMEOUT;
        }
        return res;
    }

    public BmqFuture<CloseQueueCode> closeQueueAsync(QueueImpl queue, Duration timeout) {
        final Duration timeOut = timeout == null ? sessionOptions.closeQueueTimeout() : timeout;
        final CompletableFuture<CloseQueueCode> future = new CompletableFuture<>();
        scheduler.execute(
                () -> {
                    try {
                        strategyFactory.createCloseAsyncSequence(queue, timeOut, future).start();
                    } catch (Exception e) {
                        logger.error("Failed to close queue async: ", e);
                    }
                });

        // Update the queue status in queues stats
        CompletableFuture<CloseQueueCode> statsFuture =
                future.thenApply(
                        res -> {
                            if (res.isSuccess()) {
                                stats.queuesStats().onQueueClose(queue.getFullQueueId());
                            }

                            return res;
                        });

        return new BmqFuture<>(statsFuture);
    }

    public CloseQueueCode closeQueue(QueueImpl queue, Duration timeout) {
        final Duration timeOut = timeout == null ? sessionOptions.closeQueueTimeout() : timeout;
        final CompletableFuture<CloseQueueCode> future = new CompletableFuture<>();
        CloseQueueCode res = CloseQueueResult.UNKNOWN;
        scheduler.execute(
                () -> {
                    try {
                        strategyFactory.createCloseSyncSequence(queue, timeOut, future).start();
                    } catch (Exception e) {
                        logger.error("Failed to close queue: ", e);
                    }
                });
        try {
            res = BmqFuture.get(future, timeOut.plus(timeOut));
        } catch (TimeoutException e) {
            logger.error("Close queue future timeout: ", e);
            res = CloseQueueResult.TIMEOUT;
        }

        // Update queue in queues stats
        if (res.isSuccess()) {
            stats.queuesStats().onQueueClose(queue.getFullQueueId());
        }

        return res;
    }

    public QueueHandle lookupQueue(Uri uri) {
        return queueStateManager.findByUri(uri);
    }

    public QueueHandle lookupQueue(QueueId queueId) {
        return queueStateManager.findByQueueId(queueId);
    }

    public void post(QueueHandle queueHandle, PutMessageImpl... msgs) throws BMQException {
        Argument.expectNonNull(queueHandle, "queueHandle");
        Argument.expectNonNull(msgs, "msgs");
        Argument.expectPositive(msgs.length, "message array length");

        // Queue state guard
        QueueState state = queueHandle.getState();

        logger.trace("queue handle: {} state: {}", queueHandle, state);

        if (state != QueueState.e_OPENED) {
            throw new IllegalStateException(
                    "Not supported operation for state " + state + " - attempt to post.");
        }
        for (PutMessageImpl m : msgs) {
            m.setQueueId(queueHandle.getQueueId());
        }

        putPoster.post(msgs);

        // Update queues stats
        QueueId queueId = queueHandle.getFullQueueId();
        for (PutMessageImpl msg : msgs) {
            int appDataSize = msg.appData().unpackedSize();
            double ratio = msg.appData().compressionRatio();

            stats.queuesStats().onPutMessage(queueId, appDataSize, ratio);
        }
    }

    public GenericResult confirm(QueueHandle queueHandle, PushMessageImpl... messages) {
        Argument.expectNonNull(queueHandle, "queueHandle");
        Argument.expectNonNull(messages, "messages");

        // Queue state guard
        QueueState state = queueHandle.getState();
        if (state != QueueState.e_OPENED) {
            logger.error("Not supported operation for state {} - attempt to confirm", state);
            return GenericResult.NOT_SUPPORTED;
        }

        ConfirmEventBuilder ceb = new ConfirmEventBuilder();
        try {
            for (PushMessageImpl m : messages) {
                ConfirmMessage confMsg = new ConfirmMessage();
                confMsg.setQueueId(m.queueId());
                confMsg.setSubQueueId(queueHandle.getSubQueueId());
                confMsg.setMessageGUID(m.messageGUID());
                ceb.packMessage(confMsg);
            }
        } catch (IOException e) {
            logger.error("Failed to confirm PUSH event: ", e);
            return GenericResult.INVALID_ARGUMENT;
        }

        int eventLength = ceb.eventLength();
        int msgCount = ceb.messageCount();

        ByteBuffer[] message = ceb.build();

        GenericResult res = brokerConnection.write(message, true);

        // update stat
        if (res == GenericResult.SUCCESS) {
            stats.eventsStats().onEvent(EventType.CONFIRM, eventLength, msgCount);
        }

        return res;
    }

    public QueuesStats queuesStats() {
        return stats.queuesStats();
    }
}
