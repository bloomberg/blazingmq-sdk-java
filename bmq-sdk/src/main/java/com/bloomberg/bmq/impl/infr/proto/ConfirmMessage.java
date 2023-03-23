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
import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.proto.intf.Streamable;
import java.io.IOException;

public class ConfirmMessage implements Streamable {
    // This class defines the (repeated) payload following the 'ConfirmHeader'
    // datagram.

    // ConfirmMessage structure datagram [24 bytes]:
    // ..
    //   +---------------+---------------+---------------+---------------+
    //   |0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
    //   +---------------+---------------+---------------+---------------+
    //   |                            QueueId                            |
    //   +---------------+---------------+---------------+---------------+
    //   |                          MessageGUID                          |
    //   +---------------+---------------+---------------+---------------+
    //   |                      MessageGUID (cont.)                      |
    //   +---------------+---------------+---------------+---------------+
    //   |                      MessageGUID (cont.)                      |
    //   +---------------+---------------+---------------+---------------+
    //   |                      MessageGUID (cont.)                      |
    //   +---------------+---------------+---------------+---------------+
    //   |                          SubQueueId                           |
    //   +---------------+---------------+---------------+---------------+
    //
    //  QueueId.......: Id of the queue (as announced during open queue by the
    //                  consumer of this message)
    //  MessageGUID...: MessageGUID associated to this message by the broker
    //  SubQueueId....: Id of the view associated to this message and queueId
    //                  , i.e. "SubQueue" (as announced during open queue by
    //                  the consumer of this message).
    // ..
    //

    private int queueId;
    private byte[] messageGUID;
    private int subQueueId;

    public static final int MESSAGE_SIZE = 24;
    // Current size (bytes) of the message.

    public ConfirmMessage() {
        messageGUID = new byte[MessageGUID.SIZE_BINARY];
    }

    public void setQueueId(int value) {
        queueId = value;
    }

    public void setSubQueueId(int value) {
        subQueueId = value;
    }

    public void setMessageGUID(final MessageGUID value) {
        value.toBinary(messageGUID);
    }

    public int queueId() {
        return queueId;
    }

    public int subQueueId() {
        return subQueueId;
    }

    public MessageGUID messageGUID() {
        return MessageGUID.fromBinary(messageGUID);
    }

    @Override
    public void streamIn(ByteBufferInputStream bbis) throws IOException {
        queueId = bbis.readInt();
        for (int i = 0; i < MessageGUID.SIZE_BINARY; i++) {
            messageGUID[i] = bbis.readByte();
        }
        subQueueId = bbis.readInt();
    }

    public void streamOut(ByteBufferOutputStream bbos) throws IOException {
        bbos.writeInt(queueId);
        for (int i = 0; i < MessageGUID.SIZE_BINARY; i++) {
            bbos.writeByte(messageGUID[i]);
        }
        bbos.writeInt(subQueueId);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ConfirmMessage]")
                .append("\n\t")
                .append("QueueId    : ")
                .append(queueId())
                .append("\n\t")
                .append("MessageGUID: ")
                .append(messageGUID())
                .append("\n\t")
                .append("SubQueueId : ")
                .append(subQueueId());
        return sb.toString();
    }
}
