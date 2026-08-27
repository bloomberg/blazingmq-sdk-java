/*
 * Copyright 2026 Bloomberg Finance L.P.
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
package com.bloomberg.bmq.fuzz;

import com.bloomberg.bmq.impl.ProtocolEventTcpReader;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection;
import com.bloomberg.bmq.impl.infr.proto.EventType;
import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Fuzz target for {@link ProtocolEventTcpReader}, the framing state machine that turns the raw
 * {@code ByteBuffer[]} handed up by Netty into whole BlazingMQ events.
 *
 * <p>The input is split into a sequence of {@code read()} calls, each carrying a small array of
 * variable-length buffers, so that the partial-header and partial-event resumption paths -- which
 * carry state across calls -- are covered as well as the happy path.
 *
 * <p>{@link IOException} is the reader's documented rejection of malformed input and is swallowed.
 * Anything else escapes and is reported by Jazzer.
 */
public final class ProtocolEventTcpReaderFuzzer {

    private static final int MAX_READ_CALLS = 8;
    private static final int MAX_BUFFERS_PER_CALL = 4;
    private static final int MAX_BUFFER_SIZE = 512;

    private ProtocolEventTcpReaderFuzzer() {
        throw new IllegalStateException("Utility class");
    }

    public static void fuzzerTestOneInput(FuzzedDataProvider data) {
        CollectingEventHandler handler = new CollectingEventHandler();
        ProtocolEventTcpReader reader = new ProtocolEventTcpReader(handler);
        TcpConnection.ReadCallback.ReadCompletionStatus status =
                new TcpConnection.ReadCallback.ReadCompletionStatus();

        final int numReadCalls = data.consumeInt(1, MAX_READ_CALLS);
        for (int i = 0; i < numReadCalls && data.remainingBytes() > 0; ++i) {
            final int numBuffers = data.consumeInt(1, MAX_BUFFERS_PER_CALL);
            ByteBuffer[] buffers = new ByteBuffer[numBuffers];
            for (int j = 0; j < numBuffers; ++j) {
                buffers[j] =
                        ByteBuffer.wrap(data.consumeBytes(data.consumeInt(0, MAX_BUFFER_SIZE)));
            }

            try {
                reader.read(status, buffers);
            } catch (IOException e) {
                // Keep feeding the reader: recovery after a rejected event is worth covering.
            }
        }
    }

    /**
     * Reads through each delivered event: a buffer with a bad position/limit pair throws here, and
     * an empty event is reported instead of passing silently.
     */
    private static final class CollectingEventHandler
            implements ProtocolEventTcpReader.EventHandler {

        @Override
        public void handleEvent(EventType eventType, ByteBuffer[] bbuf) {
            int totalBytes = 0;
            for (ByteBuffer b : bbuf) {
                totalBytes += b.remaining();
                b.duplicate().get(new byte[b.remaining()]);
            }

            if (totalBytes <= 0) {
                throw new IllegalStateException(
                        "Framer delivered an empty event of type " + eventType);
            }
        }
    }
}
