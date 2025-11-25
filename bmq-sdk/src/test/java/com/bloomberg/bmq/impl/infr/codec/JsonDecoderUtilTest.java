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
package com.bloomberg.bmq.impl.infr.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.msg.BrokerResponse;
import com.bloomberg.bmq.impl.infr.msg.ClientIdentity;
import com.bloomberg.bmq.impl.infr.msg.ControlMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.MessagesTestSamples;
import com.bloomberg.bmq.impl.infr.msg.NegotiationMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.OpenQueue;
import com.bloomberg.bmq.impl.infr.msg.OpenQueueResponse;
import com.bloomberg.bmq.impl.infr.msg.QueueHandleParameters;
import com.bloomberg.bmq.impl.infr.msg.Status;
import com.bloomberg.bmq.impl.infr.msg.StatusCategory;
import com.bloomberg.bmq.impl.infr.proto.EventHeader;
import com.bloomberg.bmq.impl.infr.proto.EventType;
import com.bloomberg.bmq.util.TestHelpers;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JsonDecoderUtilTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    void testDecodeClientIdentity() throws IOException {
        ByteBuffer buf = TestHelpers.readFile(MessagesTestSamples.CLIENT_IDENTITY_JSON.filePath());

        ByteBufferInputStream bbis = new ByteBufferInputStream(buf);
        EventHeader header = new EventHeader();

        header.streamIn(bbis);
        assertEquals(0, header.fragmentBit());
        assertEquals(MessagesTestSamples.CLIENT_IDENTITY_JSON.length(), header.length());
        assertEquals(1, header.protocolVersion());
        assertEquals(2, header.headerWords());
        assertNotNull(header.type());
        assertEquals(EventType.CONTROL, header.type());

        NegotiationMessageChoice negoMsg =
                JsonDecoderUtil.decode(bbis, NegotiationMessageChoice.class);

        assertTrue(negoMsg.isClientIdentityValue());
        assertFalse(negoMsg.isBrokerResponseValue());
        assertNotNull(negoMsg.clientIdentity());
        assertNull(negoMsg.brokerResponse());

        ClientIdentity ci = negoMsg.clientIdentity();
        assertEquals(999999, ci.sdkVersion().intValue());
        assertEquals(123, ci.sessionId().intValue());
        assertEquals(-1, ci.clusterNodeId().intValue());
        assertEquals(1, ci.protocolVersion().intValue());
        assertEquals("E_TCPCLIENT", ci.clientType().toString());
        assertEquals("proc_name", ci.processName());
        assertEquals(9999, ci.pid().intValue());
        assertEquals("host_name", ci.hostName());
        assertEquals("", ci.features());
        assertEquals("", ci.clusterName());
        assertEquals("E_JAVA", ci.sdkLanguage().toString());
    }

    @Test
    void testDecodeBrokerResponse() throws IOException {
        ByteBuffer buf = TestHelpers.readFile(MessagesTestSamples.BROKER_RESPONSE_JSON.filePath());

        ByteBufferInputStream bbis = new ByteBufferInputStream(buf);
        EventHeader header = new EventHeader();

        header.streamIn(bbis);
        assertEquals(0, header.fragmentBit());
        assertEquals(MessagesTestSamples.BROKER_RESPONSE_JSON.length(), header.length());
        assertEquals(1, header.protocolVersion());
        assertEquals(2, header.headerWords());
        assertNotNull(header.type());
        assertEquals(EventType.CONTROL, header.type());
        NegotiationMessageChoice negoMsg =
                JsonDecoderUtil.decode(bbis, NegotiationMessageChoice.class);

        assertFalse(negoMsg.isClientIdentityValue());
        assertTrue(negoMsg.isBrokerResponseValue());
        assertNull(negoMsg.clientIdentity());
        assertNotNull(negoMsg.brokerResponse());

        BrokerResponse br = negoMsg.brokerResponse();
        assertEquals(0, br.result().code());
        assertEquals("", br.result().message());
        assertEquals(StatusCategory.E_SUCCESS, br.result().category());
        assertEquals(1, br.protocolVersion().intValue());
        assertEquals(3, br.brokerVersion().intValue());
        assertFalse(br.isDeprecatedSdk());

        ClientIdentity ci = br.getOriginalRequest();
        assertEquals(999999, ci.sdkVersion().intValue());
        assertEquals(123, ci.sessionId().intValue());
        assertEquals(-1, ci.clusterNodeId().intValue());
        assertEquals(1, ci.protocolVersion().intValue());
        assertEquals("E_TCPCLIENT", ci.clientType().toString());
        assertEquals("proc_name", ci.processName());
        assertEquals(9999, ci.pid().intValue());
        assertEquals("host_name", ci.hostName());
        assertEquals("", ci.features());
        assertEquals("cluster_name", ci.clusterName());
        assertEquals("E_JAVA", ci.sdkLanguage().toString());
    }

    @Test
    void testDecodeOpenQueue() throws IOException {
        ByteBuffer buf = TestHelpers.readFile(MessagesTestSamples.OPEN_QUEUE_JSON.filePath());

        ByteBufferInputStream bbis = new ByteBufferInputStream(buf);
        EventHeader header = new EventHeader();

        header.streamIn(bbis);
        assertEquals(0, header.fragmentBit());
        assertEquals(MessagesTestSamples.OPEN_QUEUE_JSON.length(), header.length());
        assertEquals(1, header.protocolVersion());
        assertEquals(2, header.headerWords());
        assertNotNull(header.type());
        assertEquals(EventType.CONTROL, header.type());

        ControlMessageChoice ctrlChoice = JsonDecoderUtil.decode(bbis, ControlMessageChoice.class);

        assertNotNull(ctrlChoice);
        assertFalse(ctrlChoice.isStatusValue());
        assertTrue(ctrlChoice.isOpenQueueValue());
        assertNull(ctrlChoice.status());
        OpenQueue openQueue = ctrlChoice.openQueue();
        assertNotNull(openQueue);
        QueueHandleParameters handleParameters = openQueue.getHandleParameters();
        assertNotNull(handleParameters);
        final String uri = "bmq://bmq.tutorial.hello/test-queue";
        assertEquals(uri, handleParameters.getUri().toString());
        assertEquals(12, handleParameters.getFlags().intValue());
        assertEquals(0, handleParameters.getQId());
        assertEquals(0, handleParameters.getReadCount());
        assertEquals(1, handleParameters.getWriteCount());
        assertEquals(0, handleParameters.getAdminCount());
        assertNull(handleParameters.getSubIdInfo());
    }

    @Test
    void testDecodeOpenQueueResponse() throws IOException {
        ByteBuffer buf =
                TestHelpers.readFile(MessagesTestSamples.OPEN_QUEUE_RESPONSE_JSON.filePath());

        ByteBufferInputStream bbis = new ByteBufferInputStream(buf);
        EventHeader header = new EventHeader();

        header.streamIn(bbis);
        assertEquals(0, header.fragmentBit());
        assertEquals(MessagesTestSamples.OPEN_QUEUE_RESPONSE_JSON.length(), header.length());
        assertEquals(1, header.protocolVersion());
        assertEquals(2, header.headerWords());
        assertNotNull(header.type());
        assertEquals(EventType.CONTROL, header.type());

        ControlMessageChoice ctrlChoice = JsonDecoderUtil.decode(bbis, ControlMessageChoice.class);

        assertNotNull(ctrlChoice);
        assertFalse(ctrlChoice.isStatusValue());
        assertFalse(ctrlChoice.isOpenQueueValue());
        assertTrue(ctrlChoice.isOpenQueueResponseValue());
        assertNull(ctrlChoice.status());
        OpenQueueResponse openQueueResponse = ctrlChoice.openQueueResponse();
        assertNotNull(openQueueResponse);
        assertEquals(1, openQueueResponse.getRoutingConfiguration().getFlags().intValue());
        OpenQueue openQueue = openQueueResponse.getOriginalRequest();
        assertNotNull(openQueue);
        QueueHandleParameters handleParameters = openQueue.getHandleParameters();
        assertNotNull(handleParameters);
        final String uri = "bmq://bmq.tutorial.hello/test-queue";
        assertEquals(uri, handleParameters.getUri().toString());
        assertEquals(12, handleParameters.getFlags().intValue());
        assertEquals(0, handleParameters.getQId());
        assertEquals(0, handleParameters.getReadCount());
        assertEquals(1, handleParameters.getWriteCount());
        assertEquals(0, handleParameters.getAdminCount());
        assertNull(handleParameters.getSubIdInfo());
    }

    @Test
    void testDecodeStatus() throws IOException {
        ByteBuffer buf = TestHelpers.readFile(MessagesTestSamples.STATUS_MSG_JSON.filePath());

        ByteBufferInputStream bbis = new ByteBufferInputStream(buf);
        EventHeader header = new EventHeader();
        header.streamIn(bbis);
        assertEquals(0, header.fragmentBit());
        assertEquals(MessagesTestSamples.STATUS_MSG_JSON.length(), header.length());
        assertEquals(1, header.protocolVersion());
        assertEquals(2, header.headerWords());
        assertNotNull(header.type());
        assertEquals(EventType.CONTROL, header.type());

        ControlMessageChoice ctrlChoice = JsonDecoderUtil.decode(bbis, ControlMessageChoice.class);
        assertNotNull(ctrlChoice);
        assertTrue(ctrlChoice.isStatusValue());
        assertFalse(ctrlChoice.isOpenQueueValue());
        assertFalse(ctrlChoice.isOpenQueueResponseValue());
        Status status = ctrlChoice.status();
        assertNotNull(status);
        assertEquals(StatusCategory.E_SUCCESS, status.category());
        assertEquals(123, status.code());
        assertEquals("Test", status.message());
    }

    @Test
    void testDecodeFromJsonFail() {
        JsonDecoderUtil.decodeFromJson("\"test_string\"", String.class);
        try {
            JsonDecoderUtil.decodeFromJson("\"test_string", String.class);
            fail();
        } catch (JsonSyntaxException e) {
            // OK
        }

        JsonDecoderUtil.decodeFromJson("{\"test_map\":678,\"value\":678098}", Map.class);
        try {
            JsonDecoderUtil.decodeFromJson("{\"test_map\":678      \"value\":678098}", Map.class);
            fail();
        } catch (JsonSyntaxException e) {
            // OK
        }
    }
}
