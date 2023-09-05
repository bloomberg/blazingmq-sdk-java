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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bloomberg.bmq.MessageGUID;
import com.bloomberg.bmq.ResultCodes.AckResult;
import com.bloomberg.bmq.ResultCodes.GenericResult;
import com.bloomberg.bmq.impl.infr.proto.AckMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.CompressionAlgorithmType;
import com.bloomberg.bmq.impl.infr.proto.EventType;
import com.bloomberg.bmq.impl.infr.proto.MessagePropertiesImpl;
import com.bloomberg.bmq.impl.infr.proto.PutEventBuilder;
import com.bloomberg.bmq.impl.infr.proto.PutHeader;
import com.bloomberg.bmq.impl.infr.proto.PutHeaderFlags;
import com.bloomberg.bmq.impl.infr.proto.PutMessageImpl;
import com.bloomberg.bmq.impl.infr.stat.EventsStats;
import com.bloomberg.bmq.impl.infr.stat.EventsStatsTest;
import com.bloomberg.bmq.impl.intf.BrokerConnection;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PutPosterTest {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testConstructor() {
        try {
            new PutPoster(null, null);
            fail(); // Should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'connection' must be non-null", e.getMessage());
        }

        BrokerConnection c = mock(BrokerConnection.class);
        try {
            new PutPoster(c, null);
            fail(); // Should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'eventStats' must be non-null", e.getMessage());
        }

        try {
            new PutPoster(null, new EventsStats());
            fail(); // Should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'connection' must be non-null", e.getMessage());
        }

        new PutPoster(c, new EventsStats());
    }

    @Test
    public void testPostEmptyArray() {
        BrokerConnection mockedConnection = mock(BrokerConnection.class);
        PutPoster poster = new PutPoster(mockedConnection, new EventsStats());

        // NULL array
        try {
            PutMessageImpl[] m = null;
            poster.post(m);
            fail(); // Should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'msgs' must be non-null", e.getMessage());
        }

        // NULL message
        try {
            PutMessageImpl m = null;
            poster.post(m);
            fail(); // Should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'put message' must be non-null", e.getMessage());
        }

        // Empty array
        try {
            poster.post();
            fail(); // Should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'message array length' must be positive", e.getMessage());
        }
    }

    @Test
    public void testPostFailed() throws IOException {
        BrokerConnection mockedConnection = mock(BrokerConnection.class);
        PutPoster poster = new PutPoster(mockedConnection, new EventsStats());

        // Empty payload
        PutMessageImpl msg = new PutMessageImpl();
        try {
            poster.post(msg);
            fail(); // Should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to build PUT event: PAYLOAD_EMPTY", e.getMessage());
        }

        // Payload too big
        msg = new PutMessageImpl();
        msg.appData().setPayload(ByteBuffer.allocate(PutHeader.MAX_PAYLOAD_SIZE_SOFT + 1));

        // Set compression to none in order to get too big payload
        msg.setCompressionType(CompressionAlgorithmType.E_NONE);

        try {
            poster.post(msg);
            fail(); // Should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to build PUT event: PAYLOAD_TOO_BIG", e.getMessage());
        }

        // Missing correlation ID
        msg = new PutMessageImpl();
        msg.appData().setPayload(ByteBuffer.allocate(10));
        msg.setFlags(PutHeaderFlags.setFlag(0, PutHeaderFlags.ACK_REQUESTED));

        try {
            poster.post(msg);
            fail(); // Should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("Failed to build PUT event: MISSING_CORRELATION_ID", e.getMessage());
        }
    }

    @Test
    public void testPostValidMessages() throws IOException {
        for (boolean isOldStyleProperties : new boolean[] {false, true}) {
            BrokerConnection mockedConnection = mock(BrokerConnection.class);
            when(mockedConnection.isOldStyleMessageProperties()).thenReturn(isOldStyleProperties);
            when(mockedConnection.write(any(ByteBuffer[].class), anyBoolean()))
                    .thenReturn(GenericResult.SUCCESS);

            EventsStats eventsStats = new EventsStats();
            PutPoster poster = new PutPoster(mockedConnection, eventsStats);

            final MessagePropertiesImpl props = new MessagePropertiesImpl();
            props.setPropertyAsInt32("id", 3);
            props.setPropertyAsBinary("data", new byte[] {1, 2, 3, 4, 5});

            PutMessageImpl bigMsg1 = new PutMessageImpl();
            bigMsg1.appData().setPayload(ByteBuffer.allocate(PutHeader.MAX_PAYLOAD_SIZE_SOFT));
            bigMsg1.setCompressionType(CompressionAlgorithmType.E_NONE);

            PutMessageImpl smallMsg1 = new PutMessageImpl();
            smallMsg1.appData().setPayload(ByteBuffer.allocate(10000));
            smallMsg1.appData().setProperties(props);

            PutMessageImpl bigMsg2 = new PutMessageImpl();
            bigMsg2.appData().setPayload(ByteBuffer.allocate(PutHeader.MAX_PAYLOAD_SIZE_SOFT));
            bigMsg2.setCompressionType(CompressionAlgorithmType.E_NONE);

            PutMessageImpl smallMsg2 = new PutMessageImpl();
            smallMsg2.appData().setProperties(props);
            smallMsg2.appData().setPayload(ByteBuffer.allocate(10001));

            PutMessageImpl compressedMsg = new PutMessageImpl();
            compressedMsg
                    .appData()
                    .setPayload(ByteBuffer.allocate(PutHeader.MAX_PAYLOAD_SIZE_SOFT));
            compressedMsg.appData().setProperties(props);
            compressedMsg.setCompressionType(CompressionAlgorithmType.E_ZLIB);

            poster.post(bigMsg1, smallMsg1, bigMsg2, smallMsg2, compressedMsg);

            assertEquals(isOldStyleProperties, bigMsg1.appData().isOldStyleProperties());
            assertEquals(isOldStyleProperties, smallMsg1.appData().isOldStyleProperties());
            assertEquals(isOldStyleProperties, bigMsg2.appData().isOldStyleProperties());
            assertEquals(isOldStyleProperties, smallMsg2.appData().isOldStyleProperties());
            assertEquals(isOldStyleProperties, compressedMsg.appData().isOldStyleProperties());

            assertEquals(0, bigMsg1.header().schemaWireId());
            assertEquals(isOldStyleProperties ? 0 : 1, smallMsg1.header().schemaWireId());
            assertEquals(0, bigMsg2.header().schemaWireId());
            assertEquals(isOldStyleProperties ? 0 : 1, smallMsg2.header().schemaWireId());
            assertEquals(isOldStyleProperties ? 0 : 1, compressedMsg.header().schemaWireId());

            // Build data to check
            PutEventBuilder builder = new PutEventBuilder();
            EventsStats expectedStats = new EventsStats();

            builder.packMessage(bigMsg1, isOldStyleProperties);
            builder.packMessage(smallMsg1, isOldStyleProperties);
            expectedStats.onEvent(EventType.PUT, builder.eventLength(), builder.messageCount());
            ByteBuffer[] data1 = builder.build();

            builder.reset();
            builder.packMessage(bigMsg2, isOldStyleProperties);
            builder.packMessage(smallMsg2, isOldStyleProperties);
            builder.packMessage(compressedMsg, isOldStyleProperties);
            expectedStats.onEvent(EventType.PUT, builder.eventLength(), builder.messageCount());
            ByteBuffer[] data2 = builder.build();

            // write method can be verified using two lines below,
            // but for clarity we at first check number of invocations and
            // after that we check arguments
            // verify(mockedConnection, times(1)).write(data1, true);
            // verify(mockedConnection, times(1)).write(data2, true);

            // Special classes to capture arguments passed to the write method
            ArgumentCaptor<ByteBuffer[]> bbCaptor = ArgumentCaptor.forClass(ByteBuffer[].class);
            ArgumentCaptor<Boolean> boolCaptor = ArgumentCaptor.forClass(Boolean.class);

            // Verify that the write method has been called twice
            verify(mockedConnection, times(2)).write(bbCaptor.capture(), boolCaptor.capture());

            // Get captured arguments
            List<ByteBuffer[]> allData = bbCaptor.getAllValues();
            List<Boolean> allBooleans = boolCaptor.getAllValues();

            // Verify first argument
            assertArrayEquals(data1, allData.get(0));
            assertArrayEquals(data2, allData.get(1));

            // Verify second argument
            assertEquals(true, allBooleans.get(0));
            assertEquals(true, allBooleans.get(1));

            StringBuilder expectedBuilder = new StringBuilder();
            EventsStatsTest.dump(expectedStats, expectedBuilder, false);
            String expectedStr = expectedBuilder.toString();
            logger.info("Expected stats:\n{}", expectedStr);

            StringBuilder actualBuilder = new StringBuilder();
            EventsStatsTest.dump(eventsStats, actualBuilder, false);
            String actualStr = actualBuilder.toString();
            logger.info("Actual stats:\n{}", actualStr);

            assertEquals(expectedStr, actualStr);
        }
    }

    @Test
    public void testPostNoInfiniteLoop()
            throws IOException, TimeoutException, InterruptedException {

        for (boolean isOldStyleProperties : new boolean[] {false, true}) {
            BrokerConnection connection = mock(BrokerConnection.class);
            when(connection.isOldStyleMessageProperties()).thenReturn(isOldStyleProperties);
            when(connection.write(any(ByteBuffer[].class), anyBoolean()))
                    .thenReturn(GenericResult.SUCCESS);

            PutPoster poster = new PutPoster(connection, new EventsStats());

            // Update max event size
            final int MAX_EVENT_SIZE = 1024;

            poster.setMaxEventSize(MAX_EVENT_SIZE);

            // Create a msg with payload = max event size
            PutMessageImpl msg1 = new PutMessageImpl();
            msg1.appData().setPayload(ByteBuffer.allocate(MAX_EVENT_SIZE));

            // Post async
            ExecutorService es = Executors.newSingleThreadExecutor();
            Future<?> f = es.submit(() -> poster.post(msg1));

            // Get the result
            try {
                f.get(1, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();

                assertNotNull(cause);
                assertEquals("Failed to build PUT event: PAYLOAD_TOO_BIG", cause.getMessage());
            }

            es.shutdownNow();

            // Post a msg with payload = max payload size
            PutMessageImpl msg2 = new PutMessageImpl();
            msg2.appData()
                    .setPayload(ByteBuffer.allocate(MAX_EVENT_SIZE - PutHeader.HEADER_SIZE - 4));

            poster.post(msg2);
        }
    }

    @Test
    public void testRegisterAck() throws Exception {
        for (boolean isOldStyleProperties : new boolean[] {false, true}) {
            BrokerConnection connection = mock(BrokerConnection.class);
            when(connection.isOldStyleMessageProperties()).thenReturn(isOldStyleProperties);
            when(connection.write(any(ByteBuffer[].class), anyBoolean()))
                    .thenReturn(GenericResult.SUCCESS);

            PutPoster poster = new PutPoster(connection, new EventsStats());

            // try to register null ACK message
            try {
                poster.registerAck(null);
                fail(); // Should not get here
            } catch (IllegalArgumentException e) {
                assertEquals("'ackMsg' must be non-null", e.getMessage());
            }

            // Register ACK with null correlation Id
            // When AckMessageImpl is being streamed in, its `correlationId()
            // is initialized to some value by creating CorrelationImpl instance.
            // Here we use 'restoreId' method to create new instance of
            // CorrelationIdImpl with zero id to ensure its reference differs
            // from CorrelationIdImpl.NULL_CORRELATION_ID object.
            AckMessageImpl ackMsg =
                    new AckMessageImpl(
                            AckResult.UNKNOWN,
                            CorrelationIdImpl.restoreId(0),
                            MessageGUID.createEmptyGUID(),
                            0);
            poster.registerAck(ackMsg); // should be just logged and ignored

            // Post PUT message and then register ACK message
            Object userData = new Object();
            CorrelationIdImpl cId = CorrelationIdImpl.nextId(userData);

            PutMessageImpl msg = new PutMessageImpl();
            msg.appData().setPayload(ByteBuffer.allocate(10));
            msg.setupCorrelationId(cId);

            poster.post(msg);

            ackMsg =
                    new AckMessageImpl(
                            AckResult.SUCCESS,
                            CorrelationIdImpl.restoreId(cId.toInt()),
                            MessageGUID.createEmptyGUID(),
                            0);

            poster.registerAck(ackMsg);

            assertEquals(cId, ackMsg.correlationId());
            assertEquals(userData, ackMsg.correlationId().userData());

            // Try to register the same ACK message again
            ackMsg =
                    new AckMessageImpl(
                            AckResult.SUCCESS,
                            CorrelationIdImpl.restoreId(cId.toInt()),
                            ackMsg.messageGUID(),
                            0);
            poster.registerAck(ackMsg); // should be just logged and ignored
        }
    }
}
