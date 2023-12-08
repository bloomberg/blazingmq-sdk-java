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
import com.bloomberg.bmq.QueueFlags;
import com.bloomberg.bmq.QueueOptions;
import com.bloomberg.bmq.ResultCodes.CloseQueueCode;
import com.bloomberg.bmq.ResultCodes.ConfigureQueueCode;
import com.bloomberg.bmq.ResultCodes.OpenQueueCode;
import com.bloomberg.bmq.Uri;
import com.bloomberg.bmq.impl.events.AckMessageHandler;
import com.bloomberg.bmq.impl.events.PushMessageHandler;
import com.bloomberg.bmq.impl.events.QueueControlEvent;
import com.bloomberg.bmq.impl.events.QueueControlEventHandler;
import com.bloomberg.bmq.impl.infr.msg.QueueHandleParameters;
import com.bloomberg.bmq.impl.infr.msg.SubQueueIdInfo;
import com.bloomberg.bmq.impl.infr.proto.AckMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.PushMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.PutMessageImpl;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.intf.QueueHandle;
import com.bloomberg.bmq.impl.intf.QueueState;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Thread-safe, guarded by 'lock' file
//  TODO: consider synchronisation relaxation
public class QueueImpl implements QueueHandle {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static final int INVALID_QUEUE_ID = -1;

    // Immutable fields
    private final BrokerSession brokerSession;
    private final Uri uri; // immutable object
    private final Object lock;
    private final QueueControlEventHandler queueEventHandler;
    private final AckMessageHandler ackMessageHandler;
    private final PushMessageHandler pushMessageHandler;
    private final boolean isReader;

    // Fields exposed to user thread
    private final QueueHandleParameters parameters; // mutable object and final field
    private volatile QueueState state;
    private final ArrayList<PutMessageImpl> putMessages = new ArrayList<>();
    private volatile boolean isSuspended = false;
    // Whether the queue is suspended.
    // While suspended, a queue receives no
    // messages from the broker, and
    // attempts to pack a message destined
    // for the queue will be rejected.

    // Fields modified in BrokerSession scheduler thread only
    private QueueOptions queueOptions;
    private QueueControlStrategy<?> strategy = null;
    private boolean isSuspendedWithBroker = false;
    // Whether the queue is suspended from
    // the perspective of the broker. When queue
    // is suspended or resumed, this field is
    // updated immediately while 'isSuspended'
    // is updated only when corresponding user
    // event is being dispatching.

    private final Map<Integer, com.bloomberg.bmq.Subscription> subscriptionIdMap = new HashMap<>();

    public QueueImpl(
            BrokerSession brokerSession,
            Uri uri,
            long flags,
            QueueControlEventHandler queueEventHandler,
            AckMessageHandler ackMessageHandler,
            PushMessageHandler pushMessageHandler) {

        this.brokerSession = Argument.expectNonNull(brokerSession, "brokerSession");
        this.uri = Argument.expectNonNull(uri, "uri");

        state = QueueState.e_CLOSED;
        parameters = new QueueHandleParameters();
        parameters.setFlags(flags).setQId(INVALID_QUEUE_ID).setUri(this.uri);
        queueOptions = null;
        this.queueEventHandler = queueEventHandler;
        this.ackMessageHandler = ackMessageHandler;
        this.pushMessageHandler = pushMessageHandler;
        lock = new Object();
        isReader = QueueFlags.isReader(flags);

        logger.debug("Created queue, uri: {}, flags: {}", uri, flags);
    }

    public void setStrategy(QueueControlStrategy<?> strategy) {
        assert brokerSession.isInSessionExecutor();
        if (this.strategy != null) {
            throw new IllegalStateException("'strategy' is already set");
        }
        this.strategy = Argument.expectNonNull(strategy, "strategy");
    }

    public void resetStrategy() {
        if (this.strategy == null) {
            throw new IllegalStateException("'strategy' is already empty");
        }
        strategy = null;
    }

    public QueueControlStrategy<?> getStrategy() {
        return strategy;
    }

    public QueueOptions getQueueOptions() {
        // TODO: immutable object, why in session executor thread required?
        //        assert brokerSession.isInSessionExecutor();
        return queueOptions; // expose immutable object(thread-safe)
    }

    public QueueImpl setQueueOptions(QueueOptions queueOptions) {
        assert brokerSession.isInSessionExecutor();
        this.queueOptions = queueOptions;
        return this;
    }

    public QueueImpl setState(QueueState state) {
        assert brokerSession.isInSessionExecutor();
        logger.debug("Changing state [{}] -> [{}] for queue with uri: {}", this.state, state, uri);
        this.state = state;
        return this;
    }

    public boolean getIsSuspendedWithBroker() {
        assert brokerSession.isInSessionExecutor();
        return isSuspendedWithBroker;
    }

    public void setIsSuspendedWithBroker(boolean value) {
        assert brokerSession.isInSessionExecutor();
        isSuspendedWithBroker = value;
    }

    @Override
    public Map<Integer, com.bloomberg.bmq.Subscription> getSubscriptionIdMap() {
        return subscriptionIdMap;
    }

    @Override
    public boolean getIsSuspended() {
        // Expose primitive type value(thread-safe).
        // Accessed in user thread (Session.QueueAdapter.post() and
        // Session.QueueAdapter.pack() methods).
        return isSuspended;
    }

    @Override
    public void setIsSuspended(boolean value) {
        // May be set in BrokerSession scheduler thread (OpenQueueStrategy,
        // SuspendQueueStrategy and ResumeQueueStrategy) and in threadpool's
        // thread when dispatching 'impl.events.QueueControlEvent'
        // to 'impl.events.QueueControlEventHandler'.
        isSuspended = value;
    }

    // Evaluating only in BrokerSession scheduler thread
    public QueueImpl setQueueId(int qId) {
        parameters.setQId(qId);
        return this;
    }

    public QueueImpl setSubQueueId(int subQId) {
        synchronized (lock) {
            if (!uri.id().isEmpty()) {
                SubQueueIdInfo subInfo = new SubQueueIdInfo(subQId, uri.id());
                parameters.setSubIdInfo(subInfo);
            }
            return this;
        }
    }

    public int getQueueId() {
        synchronized (lock) {
            return parameters.getQId(); // expose primitive type value(thread-safe)
        }
    }

    public int getSubQueueId() {
        synchronized (lock) {
            if (parameters.getSubIdInfo() == null) {
                return QueueId.k_DEFAULT_SUBQUEUE_ID;
            }
            return parameters.getSubIdInfo().subId();
            // expose primitive type value(thread-safe)
        }
    }

    public QueueId getFullQueueId() {
        int queueId;
        int subQueueId;
        synchronized (lock) {
            queueId = parameters.getQId();
            if (parameters.getSubIdInfo() == null) {
                subQueueId = QueueId.k_DEFAULT_SUBQUEUE_ID;
            } else {
                subQueueId = parameters.getSubIdInfo().subId();
            }
        }
        return QueueId.createInstance(queueId, subQueueId);
    }

    public QueueState getState() {
        return state; // expose immutable enum(thread-safe)
    }

    public boolean isOpened() {
        return getState() == QueueState.e_OPENED;
    }

    public Uri getUri() {
        return uri; // expose immutable object(thread-safe)
    }

    public QueueHandleParameters getParameters() {
        synchronized (lock) {
            return new QueueHandleParameters(
                    parameters); // defensive copy - probably need make type to immutable
        }
    }

    public boolean isReader() {
        // Expose final field (thread-safe)
        return isReader;
    }

    @Nonnull
    public OpenQueueCode open(QueueOptions ops, Duration timeout) {
        return brokerSession.openQueue(this, ops, timeout);
    }

    public BmqFuture<OpenQueueCode> openAsync(QueueOptions ops, Duration timeout) {
        return brokerSession.openQueueAsync(this, ops, timeout);
    }

    public ConfigureQueueCode configure(QueueOptions ops, Duration timeout) {
        return brokerSession.configureQueue(this, ops, timeout);
    }

    public BmqFuture<ConfigureQueueCode> configureAsync(QueueOptions ops, Duration timeout) {
        return brokerSession.configureQueueAsync(this, ops, timeout);
    }

    public CloseQueueCode close(Duration timeout) {
        return brokerSession.closeQueue(this, timeout);
    }

    public BmqFuture<CloseQueueCode> closeAsync(Duration timeout) {
        return brokerSession.closeQueueAsync(this, timeout);
    }

    public void pack(PutMessageImpl message) throws BMQException {
        synchronized (lock) {
            putMessages.add(message);
        }
    }

    public PutMessageImpl[] flush() throws BMQException {
        PutMessageImpl[] msgs;
        synchronized (lock) {
            msgs = new PutMessageImpl[putMessages.size()];
            msgs = putMessages.toArray(msgs);
            putMessages.clear();
        }
        brokerSession.post(this, msgs);
        return msgs;
    }

    @Override
    public void handleAckMessage(AckMessageImpl msg) {
        if (ackMessageHandler != null) {
            ackMessageHandler.handleAckMessage(msg);
        }
    }

    @Override
    public void handlePushMessage(PushMessageImpl msg) {
        // Update queues stats
        QueueId queueId = getFullQueueId();
        int appDataSize = msg.appData().unpackedSize();

        brokerSession.queuesStats().onPushMessage(queueId, appDataSize);

        if (pushMessageHandler != null) {
            pushMessageHandler.handlePushMessage(msg);
        }
    }

    @Override
    public void handleQueueEvent(QueueControlEvent ev) {
        if (queueEventHandler != null) {
            ev.dispatch(queueEventHandler);
        }
    }

    // Clean up queue state
    public void resetQueue() {
        assert brokerSession.isInSessionExecutor();
        if (strategy != null) {
            resetStrategy();
        }
        setState(QueueState.e_CLOSED);
    }
}
