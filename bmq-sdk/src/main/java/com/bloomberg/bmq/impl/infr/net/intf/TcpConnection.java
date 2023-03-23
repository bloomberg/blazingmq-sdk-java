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
package com.bloomberg.bmq.impl.infr.net.intf;

import com.bloomberg.bmq.impl.infr.net.ConnectionOptions;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public interface TcpConnection {

    enum WriteStatus {
        SUCCESS, // Message has been enqueued to be sent.
        CLOSED, // Channel is down.
        WRITE_BUFFER_FULL; // Channel is not writable due to flow control.
    }

    enum ConnectStatus {
        SUCCESS,
        FAILURE,
        CANCELLED; // Occurs when 'disconnect' is invoked while connection
        // has been scheduled (retry etc.)
    }

    enum DisconnectStatus {
        SUCCESS,
        FAILURE;
    }

    interface ReadCallback {
        class ReadCompletionStatus {
            private int numNeeded;

            public ReadCompletionStatus() {
                numNeeded = 0;
            }

            public int numNeeded() {
                return numNeeded;
            }

            public void setNumNeeded(int value) {
                this.numNeeded = value;
            }
        }

        void handleReadCb(ReadCompletionStatus completionStatus, ByteBuffer[] data);
    }

    interface ConnectCallback {
        void handleConnectCb(ConnectStatus status);
    }

    interface DisconnectCallback {
        void handleDisconnectCb(DisconnectStatus status);
    }

    int connect(
            ConnectionOptions options,
            ConnectCallback connectCb,
            ReadCallback readCb,
            int initialMinNumBytes);

    int disconnect(DisconnectCallback disconnectCb);

    int linger();

    boolean isConnected();

    void setChannelStatusHandler(ChannelStatusHandler handler);

    WriteStatus write(ByteBuffer[] data);

    boolean isWritable();

    void waitUntilWritable();
    // Cannot be invoked from I/O thread.

    InetSocketAddress localAddress();

    InetSocketAddress remoteAddress();
}
