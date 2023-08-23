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
import com.bloomberg.bmq.CorrelationId;
import com.bloomberg.bmq.MessageGUID;
import com.bloomberg.bmq.ResultCodes.AckResult;
import com.bloomberg.bmq.ResultCodes.GenericResult;
import com.bloomberg.bmq.impl.infr.proto.AckMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.EventBuilderResult;
import com.bloomberg.bmq.impl.infr.proto.EventHeader;
import com.bloomberg.bmq.impl.infr.proto.EventType;
import com.bloomberg.bmq.impl.infr.proto.PutEventBuilder;
import com.bloomberg.bmq.impl.infr.proto.PutMessageImpl;
import com.bloomberg.bmq.impl.infr.stat.EventsStats;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.intf.BrokerConnection;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PutPoster {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final BrokerConnection brokerConnection;
    private final EventsStats eventsStats;

    private final LinkedBlockingDeque<PutMessageImpl> putMessages = new LinkedBlockingDeque<>();

    private final ConcurrentHashMap<CorrelationId, PutMessageImpl> unacknowledgedPuts =
            new ConcurrentHashMap<>();

    private int maxEventSize = EventHeader.MAX_SIZE_SOFT;

    private boolean hasMessages() {
        return putMessages.peekFirst() != null;
    }

    public PutPoster(BrokerConnection con, EventsStats eventsStats) {
        brokerConnection = Argument.expectNonNull(con, "connection");
        this.eventsStats = Argument.expectNonNull(eventsStats, "eventStats");
    }

    void setMaxEventSize(int val) {
        maxEventSize = Argument.expectNotGreater(val, EventHeader.MAX_SIZE_SOFT, "max event size");
    }

    public void pack(PutMessageImpl... msgs) {
        Argument.expectNonNull(msgs, "msgs");
        Argument.expectPositive(msgs.length, "message array length");
        for (PutMessageImpl m : msgs) {
            Argument.expectNonNull(m, "put message");

            if (m.correlationId() != null && unacknowledgedPuts.containsKey(m.correlationId())) {
                throw new IllegalStateException("PUT message with duplicated CorrelationId " + m);
            }
            putMessages.add(m);
        }
    }

    public void flush() {
        while (hasMessages()) {
            sendEvent();
        }
    }

    public void post(PutMessageImpl... msgs) {
        pack(msgs);
        flush();
    }

    private void sendEvent() {
        PutEventBuilder putBuilder = new PutEventBuilder();
        putBuilder.setMaxEventSize(maxEventSize);
        Collection<PutMessageImpl> packedMsgs = new ArrayList<>();
        while (hasMessages()) {
            PutMessageImpl msgImpl = putMessages.pollFirst();
            if (msgImpl == null) {
                continue;
            }

            try {
                EventBuilderResult packResult =
                        putBuilder.packMessage(
                                msgImpl, brokerConnection.isOldStyleMessageProperties());
                if (packResult == EventBuilderResult.EVENT_TOO_BIG) {
                    // Put the current message back to the deque
                    putMessages.addFirst(msgImpl);
                    break;
                } else {
                    Argument.expectCondition(
                            packResult == EventBuilderResult.SUCCESS,
                            "Failed to build PUT event: ",
                            packResult);
                }
                packedMsgs.add(msgImpl);
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to build PUT event: " + e);
            }
        }

        int eventLength = putBuilder.eventLength();
        int msgCount = putBuilder.messageCount();
        logger.debug("Sending {} PUT messages...", msgCount);

        // TODO: if there is an exception in 'writeBuffer', clean unackedPuts
        // that were added here but were not sent
        registerUnackedPuts(packedMsgs);
        
        writeBuffer(putBuilder.build());

        // Update statistics
        eventsStats.onEvent(EventType.PUT, eventLength, msgCount);
    }

    private void writeBuffer(ByteBuffer[] payload) throws BMQException {
        GenericResult postResult =
                brokerConnection.write(Argument.expectNonNull(payload, "payload"), true);
        if (postResult != GenericResult.SUCCESS) {
            throw new BMQException("Failed to post PUT event: " + postResult);
        }
    }

    private void registerUnackedPuts(Collection<PutMessageImpl> msgs) {
        for (PutMessageImpl p : msgs) {
            if (p.correlationId() != null) {
                unacknowledgedPuts.put(p.correlationId(), p);
            }
        }
        logger.debug("Unacknowledged PUT messages {}", unacknowledgedPuts.size());
    }

    public void registerAck(AckMessageImpl ackMsg) {
        Argument.expectNonNull(ackMsg, "ackMsg");

        // In some cases broker may send ACK messages with UNKNOWN status and null correlation Id.
        // Since there is no unacknowledged PUT message with such correlation Id,
        // we just log such ACK message and return
        if (CorrelationIdImpl.NULL_CORRELATION_ID.equals(ackMsg.correlationId())) {
            logger.warn("Got ACK message with NULL correlation Id: {}", ackMsg);
            return;
        }

        CorrelationIdImpl ackId = ackMsg.correlationId();
        PutMessageImpl putMsg = unacknowledgedPuts.remove(ackId);
        if (putMsg == null) {
            throw new IllegalStateException("Correlation ID not found " + ackId);
        }

        ackMsg.setCorrelationId((CorrelationIdImpl) putMsg.correlationId());
    }

    public Collection<AckMessageImpl> createNegativeAcks() {
        Collection<PutMessageImpl> unackedPuts = new ArrayList<>();
        Set<Map.Entry<CorrelationId, PutMessageImpl>> set = unacknowledgedPuts.entrySet();
        for (Map.Entry<CorrelationId, PutMessageImpl> item : set) {
            unackedPuts.add(item.getValue());
        }
        ArrayList<AckMessageImpl> res = new ArrayList<>();
        for (PutMessageImpl putMsg : unackedPuts) {
            AckMessageImpl m =
                    new AckMessageImpl(
                            AckResult.UNKNOWN,
                            (CorrelationIdImpl) putMsg.correlationId(),
                            MessageGUID.createEmptyGUID(),
                            putMsg.queueId());
            res.add(m);
        }
        logger.info("Created {} NACK messages", res.size());
        unacknowledgedPuts.clear();
        return res;
    }
}
