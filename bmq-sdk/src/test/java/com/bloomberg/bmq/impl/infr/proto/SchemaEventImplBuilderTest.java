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
package com.bloomberg.bmq.impl.infr.proto;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.bloomberg.bmq.Uri;
import com.bloomberg.bmq.impl.infr.msg.BrokerResponse;
import com.bloomberg.bmq.impl.infr.msg.ClientIdentity;
import com.bloomberg.bmq.impl.infr.msg.ClientLanguage;
import com.bloomberg.bmq.impl.infr.msg.ClientType;
import com.bloomberg.bmq.impl.infr.msg.ControlMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.MessagesTestSamples;
import com.bloomberg.bmq.impl.infr.msg.NegotiationMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.OpenQueue;
import com.bloomberg.bmq.impl.infr.msg.OpenQueueResponse;
import com.bloomberg.bmq.impl.infr.msg.QueueHandleParameters;
import com.bloomberg.bmq.impl.infr.msg.RoutingConfiguration;
import com.bloomberg.bmq.impl.infr.msg.Status;
import com.bloomberg.bmq.impl.infr.scm.VersionUtil;
import com.bloomberg.bmq.util.TestHelpers;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SchemaEventImplBuilderTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    void testBuildClientIdentityJson() throws IOException {
        NegotiationMessageChoice negoMsgChoice = new NegotiationMessageChoice();
        negoMsgChoice.makeClientIdentity();

        ClientIdentity clientIdentity = negoMsgChoice.clientIdentity();

        clientIdentity.setProtocolversion(Protocol.VERSION);
        clientIdentity.setSdkVersion(VersionUtil.getSdkVersion());
        clientIdentity.setClientType(ClientType.E_TCPCLIENT);
        clientIdentity.setProcessName("proc_name");
        clientIdentity.setPid(9999);
        clientIdentity.setSessionId(123);
        clientIdentity.setHostName("host_name");
        clientIdentity.setFeatures("");
        clientIdentity.setClusterName("");
        clientIdentity.setClusterNodeId(-1);
        clientIdentity.setSdkLanguage(ClientLanguage.E_JAVA);

        ByteBuffer[] message;
        SchemaEventBuilder schemaBuilder = new SchemaEventBuilder();
        schemaBuilder.setMessage(negoMsgChoice);
        message = schemaBuilder.build();

        TestHelpers.compareWithFileContent(message, MessagesTestSamples.CLIENT_IDENTITY_JSON);
    }

    @Test
    void testBuildBrokerResponseJson() throws IOException {
        NegotiationMessageChoice negoMsgChoice = new NegotiationMessageChoice();
        negoMsgChoice.makeBrokerResponse();
        BrokerResponse br = negoMsgChoice.brokerResponse();
        Status result = new Status();
        result.setCode(0);
        br.setResult(result);
        br.setProtocolversion(1);
        br.setBrokerVersion(3);
        br.setIsDeprecatedSdk(false);

        ClientIdentity clientIdentity = br.getOriginalRequest();
        clientIdentity.setProtocolversion(Protocol.VERSION);
        clientIdentity.setSdkVersion(VersionUtil.getSdkVersion());
        clientIdentity.setClientType(ClientType.E_TCPCLIENT);
        clientIdentity.setProcessName("proc_name");
        clientIdentity.setPid(9999);
        clientIdentity.setSessionId(123);
        clientIdentity.setHostName("host_name");
        clientIdentity.setClusterNodeId(-1);
        clientIdentity.setClusterName("cluster_name");
        clientIdentity.setSdkLanguage(ClientLanguage.E_JAVA);

        ByteBuffer[] message;
        SchemaEventBuilder schemaBuilder = new SchemaEventBuilder();
        schemaBuilder.setMessage(negoMsgChoice);
        message = schemaBuilder.build();

        TestHelpers.compareWithFileContent(message, MessagesTestSamples.BROKER_RESPONSE_JSON);
    }

    @Test
    void testBuildStatusJson() throws IOException {
        ControlMessageChoice msgChoice = new ControlMessageChoice();
        msgChoice.makeStatus();

        Status status = msgChoice.status();
        assertNotNull(status);
        status.setCode(123);
        status.setMessage("Test");

        SchemaEventBuilder schemaBuilder = new SchemaEventBuilder();
        ByteBuffer[] message;
        schemaBuilder.setMessage(msgChoice);
        message = schemaBuilder.build();

        TestHelpers.compareWithFileContent(message, MessagesTestSamples.STATUS_MSG_JSON);

        msgChoice.setId(7);
        schemaBuilder.setMessage(msgChoice);
        message = schemaBuilder.build();
        TestHelpers.compareWithFileContent(message, MessagesTestSamples.STATUS_MSG_ID_JSON);
    }

    @Test
    void testBuildOpenQueueJson() throws IOException {
        ControlMessageChoice msgChoice = new ControlMessageChoice();

        msgChoice.makeOpenQueue();
        OpenQueue openQueue = msgChoice.openQueue();
        assertNotNull(openQueue);

        QueueHandleParameters params = new QueueHandleParameters();
        params.setUri(new Uri("bmq://bmq.tutorial.hello/test-queue"));
        params.setFlags(12L);
        params.setQId(0);
        params.setReadCount(0);
        params.setWriteCount(1);
        params.setAdminCount(0);

        openQueue.setHandleParameters(params);

        SchemaEventBuilder schemaBuilder = new SchemaEventBuilder();

        ByteBuffer[] message;
        schemaBuilder.setMessage(msgChoice);
        message = schemaBuilder.build();

        TestHelpers.compareWithFileContent(message, MessagesTestSamples.OPEN_QUEUE_JSON);

        msgChoice.setId(1);
        schemaBuilder.setMessage(msgChoice);
        message = schemaBuilder.build();
        TestHelpers.compareWithFileContent(message, MessagesTestSamples.OPEN_QUEUE_ID_JSON);
    }

    @Test
    void testBuildOpenQueueResponseJson() throws IOException {
        ControlMessageChoice msgChoice = new ControlMessageChoice();

        msgChoice.makeOpenQueueResponse();
        OpenQueueResponse openQueueResponse = msgChoice.openQueueResponse();
        assertNotNull(openQueueResponse);

        QueueHandleParameters params = new QueueHandleParameters();
        params.setUri(new Uri("bmq://bmq.tutorial.hello/test-queue"));
        params.setFlags(12L);
        params.setQId(0);
        params.setReadCount(0);
        params.setWriteCount(1);
        params.setAdminCount(0);

        openQueueResponse.setOriginalRequest(new OpenQueue());
        openQueueResponse.getOriginalRequest().setHandleParameters(params);

        RoutingConfiguration configuration = new RoutingConfiguration();
        configuration.setFlags(0x01L);
        openQueueResponse.setRoutingConfiguration(configuration);

        SchemaEventBuilder schemaBuilder = new SchemaEventBuilder();

        ByteBuffer[] message;
        schemaBuilder.setMessage(msgChoice);
        message = schemaBuilder.build();

        TestHelpers.compareWithFileContent(message, MessagesTestSamples.OPEN_QUEUE_RESPONSE_JSON);

        msgChoice.setId(1);
        schemaBuilder.setMessage(msgChoice);
        message = schemaBuilder.build();

        TestHelpers.compareWithFileContent(
                message, MessagesTestSamples.OPEN_QUEUE_RESPONSE_ID_JSON);
    }
}
