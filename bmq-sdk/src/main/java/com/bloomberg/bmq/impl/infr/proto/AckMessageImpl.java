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

import com.bloomberg.bmq.MessageGUID;
import com.bloomberg.bmq.ResultCodes;
import com.bloomberg.bmq.impl.CorrelationIdImpl;
import com.bloomberg.bmq.impl.ResultCodeUtils;
import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.proto.intf.Streamable;
import com.bloomberg.bmq.impl.infr.util.BitUtil;
import java.io.IOException;

public final class AckMessageImpl implements Streamable {
    // This class defines the (repeated) payload following the 'AckHeader'
    // struct.

    // AckMessageImpl structure datagram [24 bytes]:
    // ..
    //   +---------------+---------------+---------------+---------------+
    //   |0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
    //   +---------------+---------------+---------------+---------------+
    //   |   R   |Status |            CorrelationId                      |
    //   +---------------+---------------+---------------+---------------+
    //   |                          MessageGUID                          |
    //   +---------------+---------------+---------------+---------------+
    //   |                      MessageGUID (cont.)                      |
    //   +---------------+---------------+---------------+---------------+
    //   |                      MessageGUID (cont.)                      |
    //   +---------------+---------------+---------------+---------------+
    //   |                      MessageGUID (cont.)                      |
    //   +---------------+---------------+---------------+---------------+
    //   |                            QueueId                            |
    //   +---------------+---------------+---------------+---------------+
    //       R.: Reserved
    //
    //  Reserved (R)..: For alignment and extension ~ must be 0
    //  Status........: Status of this message.
    //  CorrelationId.: Id specified by the producer client to correlate this
    //                  message
    //  MessageGUID...: MessageGUID associated to this message by the broker
    //  QueueId.......: Id of the queue (as anounced during open queue by the
    //                  the producer of the Put event this Ack is a response
    //                  of)
    // ..

    private int statusAndCorrelationId = 0;
    // Status code and associated correlation Id.

    private byte[] messageGUID;
    // MessageGUID associated to this message.

    private int queueId;
    // Queue Id.

    private CorrelationIdImpl correlationId;

    private static final int STATUS_NUM_BITS = 4;
    private static final int CORRID_NUM_BITS = 24;

    private static final int STATUS_START_IDX = 24;
    private static final int CORRID_START_IDX = 0;

    private static final int STATUS_MASK = BitUtil.oneMask(STATUS_START_IDX, STATUS_NUM_BITS);

    private static final int CORRID_MASK = BitUtil.oneMask(CORRID_START_IDX, CORRID_NUM_BITS);

    public static final int MAX_STATUS = (1 << STATUS_NUM_BITS) - 1;
    // Highest possible value for the status.

    public static final int NULL_CORRELATION_ID = 0;
    // Constant to indicate no correlation Id.

    public static final int MESSAGE_SIZE = 24;
    // Current size (bytes) of the message.

    public AckMessageImpl() {
        messageGUID = new byte[MessageGUID.SIZE_BINARY];
        correlationId = CorrelationIdImpl.NULL_CORRELATION_ID;
    }

    public AckMessageImpl(
            ResultCodes.AckResult status,
            CorrelationIdImpl correlationId,
            MessageGUID guid,
            int queueId) {
        this();
        setStatus(status);
        setCorrelationId(correlationId);
        setMessageGUID(guid);
        setQueueId(queueId);
    }

    public void setStatus(ResultCodes.AckResult status) {
        int value = ResultCodeUtils.intFromAckResult(status);
        statusAndCorrelationId =
                (statusAndCorrelationId & CORRID_MASK) | (value << STATUS_START_IDX);
    }

    public void setCorrelationId(CorrelationIdImpl value) {
        int id = value.toInt();
        statusAndCorrelationId = (statusAndCorrelationId & STATUS_MASK) | (id & CORRID_MASK);
        correlationId = value;
    }

    public void setQueueId(int value) {
        queueId = value;
    }

    public void setMessageGUID(final MessageGUID value) {
        value.toBinary(messageGUID);
    }

    public ResultCodes.AckResult status() {
        int status = (statusAndCorrelationId & STATUS_MASK) >>> STATUS_START_IDX;
        return ResultCodeUtils.ackResultFromInt(status);
    }

    public CorrelationIdImpl correlationId() {
        return correlationId;
    }

    public int queueId() {
        return queueId;
    }

    public MessageGUID messageGUID() {
        return MessageGUID.fromBinary(messageGUID);
    }

    @Override
    public void streamIn(ByteBufferInputStream bbis) throws IOException {
        statusAndCorrelationId = bbis.readInt();
        correlationId = CorrelationIdImpl.restoreId(statusAndCorrelationId & CORRID_MASK);
        for (int i = 0; i < MessageGUID.SIZE_BINARY; i++) {
            messageGUID[i] = bbis.readByte();
        }
        queueId = bbis.readInt();
    }

    public void streamOut(ByteBufferOutputStream bbos) throws IOException {
        bbos.writeInt(statusAndCorrelationId);
        for (int i = 0; i < MessageGUID.SIZE_BINARY; i++) {
            bbos.writeByte(messageGUID[i]);
        }
        bbos.writeInt(queueId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ AckMessage [")
                .append(" QueueId=")
                .append(queueId())
                .append(" MessageGUID=")
                .append(messageGUID())
                .append(" Status=")
                .append(status())
                .append(" CorrelationId=")
                .append(correlationId())
                .append(" ] ]");
        return sb.toString();
    }
}
