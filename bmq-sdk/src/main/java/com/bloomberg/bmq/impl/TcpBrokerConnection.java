/*
 * Copyright 2022-2025 Bloomberg Finance L.P.
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

import com.bloomberg.bmq.ResultCodes.GenericResult;
import com.bloomberg.bmq.impl.infr.msg.BrokerResponse;
import com.bloomberg.bmq.impl.infr.msg.ClientIdentity;
import com.bloomberg.bmq.impl.infr.msg.ClientLanguage;
import com.bloomberg.bmq.impl.infr.msg.ClientType;
import com.bloomberg.bmq.impl.infr.msg.ControlMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.NegotiationMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.StatusCategory;
import com.bloomberg.bmq.impl.infr.net.ConnectionOptions;
import com.bloomberg.bmq.impl.infr.net.intf.ChannelStatusHandler;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.ConnectCallback;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.ConnectStatus;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.DisconnectCallback;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.DisconnectStatus;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.ReadCallback;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.WriteStatus;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnectionFactory;
import com.bloomberg.bmq.impl.infr.proto.AckEventImpl;
import com.bloomberg.bmq.impl.infr.proto.ControlEventImpl;
import com.bloomberg.bmq.impl.infr.proto.EventImpl;
import com.bloomberg.bmq.impl.infr.proto.EventType;
import com.bloomberg.bmq.impl.infr.proto.Protocol;
import com.bloomberg.bmq.impl.infr.proto.PushEventImpl;
import com.bloomberg.bmq.impl.infr.proto.RequestManager;
import com.bloomberg.bmq.impl.infr.proto.SchemaEventBuilder;
import com.bloomberg.bmq.impl.infr.scm.VersionUtil;
import com.bloomberg.bmq.impl.infr.stat.EventsStats;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.infr.util.SystemUtil;
import com.bloomberg.bmq.impl.intf.BrokerConnection;
import com.bloomberg.bmq.impl.intf.BrokerConnectionFSM;
import com.bloomberg.bmq.impl.intf.BrokerConnectionFSM.FSMWorker;
import com.bloomberg.bmq.impl.intf.BrokerConnectionFSM.Inputs;
import com.bloomberg.bmq.impl.intf.BrokerConnectionFSM.States;
import com.bloomberg.bmq.impl.intf.SessionEventHandler;
import com.bloomberg.bmq.impl.intf.SessionStatusHandler;
import com.bloomberg.bmq.impl.intf.SessionStatusHandler.SessionStatus;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpBrokerConnection
        implements BrokerConnection,
                ChannelStatusHandler,
                ConnectCallback,
                DisconnectCallback,
                ReadCallback,
                FSMWorker {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final String MPS_EX_FEATURE = "MPS:MESSAGE_PROPERTIES_EX";
    private static AtomicInteger counter = new AtomicInteger(0);

    private final EventsStats eventsStats; // thread-safe

    private final RequestManager requestManager;

    private ConnectionOptions connectionOptions;
    private TcpConnection connection;

    private SessionEventHandler sessionEventHandler;
    private SessionStatusHandler sessionStatusHandler;

    private volatile StartCallback startCallback;
    private volatile StopCallback stopCallback;

    private volatile Duration stopTimeout;
    private volatile boolean isOldStyleMessageProperties = false;

    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> onNegotiationTimeoutFuture;
    private Duration negotiationTimeout;
    private ClientIdentity brokerIdentity;
    private ProtocolEventTcpReader protocolEventTcpReader;
    private LinkedBlockingQueue<EventImpl> bmqEvents;
    private BrokerConnectionFSM fsm;

    private AtomicBoolean isLingered = new AtomicBoolean(false);
    private final int sessionId = counter.incrementAndGet();
    private NegotiationMessageChoice negotiationMsg;

    public static BrokerConnection createInstance(
            ConnectionOptions options,
            TcpConnectionFactory connectionFactory,
            ScheduledExecutorService scheduler,
            RequestManager requestManager,
            EventsStats eventsStats,
            SessionEventHandler sessionEventHandler,
            SessionStatusHandler sessionStatusHandler) {
        return new TcpBrokerConnection(
                options,
                connectionFactory,
                scheduler,
                requestManager,
                eventsStats,
                sessionEventHandler,
                sessionStatusHandler);
    }

    TcpBrokerConnection(
            ConnectionOptions options,
            TcpConnectionFactory connectionFactory,
            ScheduledExecutorService scheduler,
            RequestManager requestManager,
            EventsStats eventsStats,
            SessionEventHandler sessionEventHandler,
            SessionStatusHandler sessionStatusHandler) {

        connectionOptions = Argument.expectNonNull(options, "options");
        connection =
                Argument.expectNonNull(connectionFactory, "connectionFactory").createConnection();
        this.scheduler = Argument.expectNonNull(scheduler, "scheduler");
        this.requestManager = Argument.expectNonNull(requestManager, "requestManager");
        this.eventsStats = Argument.expectNonNull(eventsStats, "eventsStats");
        this.sessionEventHandler =
                Argument.expectNonNull(sessionEventHandler, "sessionEventHandler");
        this.sessionStatusHandler =
                Argument.expectNonNull(sessionStatusHandler, "sessionStatusHandler");

        connection.setChannelStatusHandler(this);
        startCallback = null;
        stopCallback = null;
        bmqEvents = new LinkedBlockingQueue<>();
        negotiationTimeout = options.startAttemptTimeout();

        protocolEventTcpReader = new ProtocolEventTcpReader(new TcpBrokerConnection.EventHandler());
        fsm = BrokerConnectionFSMImpl.createInstance(this);
    }

    public ClientIdentity brokerIdentity() {
        return brokerIdentity;
    }

    private GenericResult addFsmInput(Inputs ev) {
        GenericResult res = GenericResult.REFUSED;
        try {
            scheduler.execute(
                    () -> {
                        try {
                            fsm.handleInput(ev);
                        } catch (Exception e) {
                            logger.error("Failed to handle FSM {} input: {}", ev, e);
                        }
                    });
            res = GenericResult.SUCCESS;
        } catch (RejectedExecutionException e) {
            logger.error("Failed to add FSM input: ", e);
        }
        return res;
    }

    @SuppressWarnings("squid:S2142")
    private GenericResult addBmqEvent(EventImpl ev) {
        GenericResult res = GenericResult.REFUSED;
        try {
            bmqEvents.put(ev);
            res = addFsmInput(Inputs.BMQ_EVENT);
        } catch (InterruptedException e) {
            logger.error("Failed to add BlazingMQ event: ", e);
            Thread.currentThread().interrupt();
        }
        return res;
    }

    private GenericResult connect() {
        int rc = connection.connect(connectionOptions, this, this, Protocol.PACKET_MIN_SIZE);
        if (0 != rc) {
            // Failure means that start or stop is in progress.  Invoking
            // 'start' in this scenario is not supported.
            logger.error("Failed to connect: {}", rc);
            return GenericResult.NOT_SUPPORTED;
        }
        return GenericResult.SUCCESS;
    }

    @Override
    public void doConnect() {
        GenericResult rc = connect();
        if (rc != GenericResult.SUCCESS) {
            addFsmInput(Inputs.CONNECT_STATUS_FAILURE);
        }
    }

    @Override
    public void doNegotiation() {
        // Proceed with negotiation. Setup negotiation response timeout.
        onNegotiationTimeoutFuture =
                scheduler.schedule(
                        () -> {
                            logger.error("Negotiation timed out");
                            addFsmInput(Inputs.NEGOTIATION_TIMEOUT);
                        },
                        negotiationTimeout.toNanos(),
                        TimeUnit.NANOSECONDS);

        // Start negotiation
        try {
            WriteStatus rc = negotiate();
            if (rc != WriteStatus.SUCCESS) {
                logger.error("Failed to send negotiation message");
                addFsmInput(Inputs.NEGOTIATION_FAILURE);
            }
        } catch (IOException e) {
            logger.error("Failed to negotiate: ", e);
            addFsmInput(Inputs.NEGOTIATION_FAILURE);
        }
    }

    @Override
    public void doDisconnectSession() {
        // Send disconnect request
        try {
            GenericResult rc = sendDisconnect();
            if (rc != GenericResult.SUCCESS) {
                logger.error("Failed to send Disconnect message");
                addFsmInput(Inputs.DISCONNECT_BROKER_FAILURE);
            }
        } catch (Exception e) {
            logger.error("Failed to request disconnect: ", e);
            addFsmInput(Inputs.DISCONNECT_BROKER_FAILURE);
        }
    }

    private void onDisconnectResponse(RequestManager.Request r) {
        logger.debug("Broker disconnect response decoded:\n {}", r.response());

        if (r.response().isDisconnectResponseValue()) {
            addFsmInput(Inputs.DISCONNECT_BROKER_RESPONSE);

        } else if (r.response().isStatusValue()
                && r.response().status().category() == StatusCategory.E_TIMEOUT) {

            logger.error("Disconnecting timed out");
            addFsmInput(Inputs.DISCONNECT_BROKER_TIMEOUT);
        } else {
            logger.error("Invalid disconnect response");
            addFsmInput(Inputs.DISCONNECT_BROKER_FAILURE);
        }
    }

    @Override
    public void doDisconnectChannel() {
        int rc = connection.disconnect(this);
        if (0 != rc) {
            logger.error("Failed to disconnect channel: {}", rc);
            addFsmInput(Inputs.DISCONNECT_CHANNEL_FAILURE);
        }
    }

    @Override
    public void handleNegotiationResponse() {
        if (onNegotiationTimeoutFuture.isDone()) {
            logger.warn("Negotiation timeout expired");
            return;
        }
        EventImpl bmqEv = bmqEvents.poll();
        if (bmqEv == null) {
            logger.error("No BlazingMQ events");
            return;
        }
        if (bmqEv.type() != EventType.CONTROL) {
            logger.error("Unexpected BlazingMQ event: {}", bmqEv);
            return;
        }
        try {
            NegotiationMessageChoice negoMsg = ((ControlEventImpl) bmqEv).negotiationChoice();
            logger.debug("Broker negotiation response decoded:\n {}", negoMsg);
            onNegotiationTimeoutFuture.cancel(false);
            if (validateBrokerResponse(negoMsg.brokerResponse())) {
                addFsmInput(Inputs.NEGOTIATION_RESPONSE);
            } else {
                addFsmInput(Inputs.NEGOTIATION_FAILURE);
            }
        } catch (JsonSyntaxException e) {
            logger.error("Failed to decode Negotiation message: ", e);
            addFsmInput(Inputs.NEGOTIATION_FAILURE);
        }
    }

    @Override
    public void handleBmqEvent() {
        EventImpl bmqEv = bmqEvents.poll();
        if (bmqEv == null) {
            logger.error("No BlazingMQ events");
            return;
        }

        bmqEv.dispatch(sessionEventHandler);
        eventsStats.onEvent(bmqEv.type(), bmqEv.eventLength(), bmqEv.messageCount());
    }

    @Override
    public void handleChannelDown() {
        protocolEventTcpReader.reset();
        requestManager.cancelAllRequests();
    }

    @Override
    public void handleStop() {
        if (onNegotiationTimeoutFuture != null && !onNegotiationTimeoutFuture.isDone()) {
            onNegotiationTimeoutFuture.cancel(false);
        }
    }

    @Override
    public void reportSessionStatus(SessionStatus status) {
        sessionStatusHandler.handleSessionStatus(status);
    }

    @Override
    public void reportStartStatus(StartStatus status) {
        if (startCallback == null) {
            throw new IllegalStateException("Start callback is not set");
        }
        startCallback.handleStartCb(status);
    }

    @Override
    public void reportStopStatus(StopStatus status) {
        if (stopCallback != null) {
            stopCallback.handleStopCb(status);
        }
    }

    private NegotiationMessageChoice createNegoMsg() {
        // ==============================
        // Prepare a negotiation message
        // ==============================
        NegotiationMessageChoice negoMsgChoice = new NegotiationMessageChoice();

        negoMsgChoice.makeClientIdentity();

        ClientIdentity clientIdentity = negoMsgChoice.clientIdentity();

        clientIdentity.setProtocolversion(Protocol.VERSION);
        clientIdentity.setSdkVersion(VersionUtil.getSdkVersion());
        clientIdentity.setClientType(ClientType.E_TCPCLIENT);
        clientIdentity.setProcessName(SystemUtil.getProcessName());
        clientIdentity.setHostName(SystemUtil.getHostName());
        clientIdentity.setPid(SystemUtil.getProcessId());
        clientIdentity.setSessionId(sessionId);
        clientIdentity.setFeatures("PROTOCOL_ENCODING:JSON;" + MPS_EX_FEATURE);
        clientIdentity.setClusterName("");
        clientIdentity.setClusterNodeId(-1);
        clientIdentity.setSdkLanguage(ClientLanguage.E_JAVA);
        clientIdentity.setUserAgent(constructUserAgent());
        return negoMsgChoice;
    }

    private String constructUserAgent() {
        String prefix = "";
        if (connectionOptions.userAgentPrefix().length() > 0) {
            prefix = connectionOptions.userAgentPrefix() + " ";
        }
        String javaVersion = "java" + SystemUtil.getJavaVersionString();
        String sdkVersion = VersionUtil.getJarVersion();
        return prefix + "com.bloomberg.bmq(" + javaVersion + "):" + sdkVersion;
    }

    private WriteStatus negotiate() throws IOException {
        if (negotiationMsg == null) {
            negotiationMsg = createNegoMsg();
        }

        SchemaEventBuilder schemaBuilder = new SchemaEventBuilder();
        schemaBuilder.setMessage(negotiationMsg);

        // ===========================================
        // Send the nogotiation message to the Broker
        // ===========================================
        logger.info("Sending negotiation message:\n {}", negotiationMsg);
        return connection.write(schemaBuilder.build());
    }

    private GenericResult sendDisconnect() {
        // ============================
        // Create a Disconnect message
        // ============================
        RequestManager.Request req = requestManager.createRequest();
        ControlMessageChoice ctrlMsg = new ControlMessageChoice();

        req.setRequest(ctrlMsg);
        ctrlMsg.makeDisconnect();

        req.setAsyncNotifier(this::onDisconnectResponse);
        // ==========================================
        // Send the Disconnect message to the Broker
        // ==========================================
        logger.info("Sending disconnect message: {}", ctrlMsg);
        return requestManager.sendRequest(req, this, stopTimeout);
    }

    private boolean validateBrokerResponse(BrokerResponse resp) {
        boolean isValid = false;
        if (resp != null
                && resp.result() != null
                && resp.result().category() == StatusCategory.E_SUCCESS
                && resp.getOriginalRequest() != null) {

            brokerIdentity = resp.getOriginalRequest();

            // TODO: remove after 2nd rollout of "new style" brokers
            String brokerFeatures = brokerIdentity.features();
            isOldStyleMessageProperties =
                    brokerFeatures == null
                            || brokerFeatures.isEmpty()
                            || !brokerFeatures.toUpperCase().contains(MPS_EX_FEATURE);
            logger.info(
                    "Broker supports new style message properties: {}",
                    !isOldStyleMessageProperties);
            isValid = true;
        } else {
            logger.error("Broker response is invalid");
        }
        return isValid;
    }

    private class EventHandler implements ProtocolEventTcpReader.EventHandler {

        public void handleEvent(EventType eventType, ByteBuffer[] bbuf) {
            logger.debug("Handle EventImpl {} with size: {}", eventType, bbuf.length);
            EventImpl reportedEvent;
            switch (eventType) {
                case CONTROL:
                    reportedEvent = new ControlEventImpl(bbuf);
                    break;
                case PUSH:
                    reportedEvent = new PushEventImpl(bbuf);
                    break;
                case ACK:
                    reportedEvent = new AckEventImpl(bbuf);
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected event type: " + eventType);
            }

            // Todo: cache iteration over event data
            logger.debug("Incoming Event: {}", reportedEvent);

            addBmqEvent(reportedEvent);
        }
    }

    @Override
    public void start(StartCallback startCb) {
        startCallback = Argument.expectNonNull(startCb, "startCb");
        if (isLingered.get()) {
            throw new IllegalStateException("The connection has been lingered");
        }

        addFsmInput(Inputs.START_REQUEST);
    }

    @Override
    public boolean isStarted() {
        return fsm.currentState() == States.CONNECTED;
    }

    @Override
    public void stop(StopCallback stopCb, Duration timeout) {
        stopCallback = Argument.expectNonNull(stopCb, "stopCb");
        stopTimeout = Argument.expectNonNull(timeout, "timeout");
        Argument.expectCondition(
                !(timeout.isNegative() || timeout.isZero()), "'timeout' must be positive");

        if (isLingered.get()) {
            throw new IllegalStateException("The connection has been lingered");
        }

        addFsmInput(Inputs.STOP_REQUEST);
    }

    @Override
    public void drop() {
        // Drop the channel and initiate reconnecting.

        scheduler.execute(
                () -> {
                    if (isLingered.get()) {
                        logger.warn("The connection has been lingered");
                        return;
                    }

                    if (!isStarted()) {
                        logger.warn("The connection is not started");
                        return;
                    }

                    logger.info("Drop the channel and initiate reconnecting");
                    int rc = connection.disconnect(null);
                    if (rc != 0) {
                        logger.warn("Failed to disconnect the channel: {} code", rc);
                    }
                });
    }

    @Override
    public GenericResult linger() {
        if (isLingered.compareAndSet(false, true)) {
            if (connection.linger() != 0) {
                // TODO: elaborate return codes inappropriate state vs shutdown timeout
                return GenericResult.UNKNOWN;
            }
        }
        return GenericResult.SUCCESS;
    }

    @Override
    public boolean isOldStyleMessageProperties() {
        return isOldStyleMessageProperties;
    }

    @Override
    public GenericResult write(ByteBuffer[] buffers, boolean waitUntilWritable) {

        if (isLingered.get()) {
            throw new IllegalStateException("The connection has been lingered");
        }

        // TBD: in the absence of a 'BW_LIMIT' return code, we return REFUSED
        // for now, in case of channel is not writable.

        while (true) {

            if (fsm.currentState() == States.CONNECTION_LOST) {
                return GenericResult.NOT_CONNECTED;
            }

            WriteStatus wrc = connection.write(buffers);
            if (wrc == WriteStatus.CLOSED) {
                return GenericResult.NOT_CONNECTED;
            }

            if (wrc == WriteStatus.WRITE_BUFFER_FULL) {
                if (!waitUntilWritable) {
                    return GenericResult.REFUSED;
                }

                connection.waitUntilWritable();

            } else {
                return GenericResult.SUCCESS;
            }
        }
    }

    @Override
    public void handleChannelStatus(ChannelStatus status) {
        Inputs ev;
        switch (status) {
            case CHANNEL_UP:
                ev = Inputs.CHANNEL_STATUS_UP;
                logger.info("Channel is up");
                break;
            case CHANNEL_DOWN:
                ev = Inputs.CHANNEL_STATUS_DOWN;
                logger.info("Channel is down.");
                break;
            case CHANNEL_WRITABLE:
                ev = Inputs.CHANNEL_STATUS_WRITABLE;
                logger.info("Channel is writable.");
                break;
            default:
                throw new IllegalArgumentException("Unexpected channel status: " + status);
        }
        addFsmInput(ev);
    }

    @Override
    public void handleConnectCb(ConnectStatus status) {
        logger.debug("Received connection callback, result: {}", status);
        Inputs ev;
        switch (status) {
            case SUCCESS:
                ev = Inputs.CONNECT_STATUS_SUCCESS;
                break;
            case FAILURE:
                ev = Inputs.CONNECT_STATUS_FAILURE;
                break;
            case CANCELLED:
                ev = Inputs.CONNECT_STATUS_CANCELLED;
                break;
            default:
                throw new IllegalArgumentException("Unexpected connect status: " + status);
        }
        addFsmInput(ev);
    }

    @Override
    public void handleDisconnectCb(DisconnectStatus status) {
        logger.debug("Received disconnection callback, result: {}", status);
        Inputs ev;
        switch (status) {
            case SUCCESS:
                ev = Inputs.DISCONNECT_CHANNEL_SUCCESS;
                break;
            case FAILURE:
                ev = Inputs.DISCONNECT_CHANNEL_FAILURE;
                break;
            default:
                throw new IllegalArgumentException("Unexpected disconnect status: " + status);
        }
        addFsmInput(ev);
    }

    @Override
    public void handleReadCb(ReadCompletionStatus completionStatus, ByteBuffer[] data) {
        logger.debug("handleReadCb");
        try {
            protocolEventTcpReader.read(completionStatus, data);
        } catch (IOException ex) {
            // TODO: reconnect logic or some proper handling
            throw new RuntimeException("Read failed for TcpBrokerConnection", ex);
        }
    }
}
