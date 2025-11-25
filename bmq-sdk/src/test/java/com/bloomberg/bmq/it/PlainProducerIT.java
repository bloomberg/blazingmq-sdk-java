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
package com.bloomberg.bmq.it;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.bloomberg.bmq.QueueFlags;
import com.bloomberg.bmq.Uri;
import com.bloomberg.bmq.impl.infr.codec.JsonDecoderUtil;
import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.msg.ClientIdentity;
import com.bloomberg.bmq.impl.infr.msg.ClientLanguage;
import com.bloomberg.bmq.impl.infr.msg.ClientType;
import com.bloomberg.bmq.impl.infr.msg.CloseQueue;
import com.bloomberg.bmq.impl.infr.msg.ControlMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.NegotiationMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.OpenQueue;
import com.bloomberg.bmq.impl.infr.msg.QueueHandleParameters;
import com.bloomberg.bmq.impl.infr.msg.Status;
import com.bloomberg.bmq.impl.infr.proto.AckEventImpl;
import com.bloomberg.bmq.impl.infr.proto.AckHeader;
import com.bloomberg.bmq.impl.infr.proto.AckMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.EventBuilderResult;
import com.bloomberg.bmq.impl.infr.proto.EventHeader;
import com.bloomberg.bmq.impl.infr.proto.MessagePropertiesImpl;
import com.bloomberg.bmq.impl.infr.proto.Protocol;
import com.bloomberg.bmq.impl.infr.proto.PutEventBuilder;
import com.bloomberg.bmq.impl.infr.proto.PutHeaderFlags;
import com.bloomberg.bmq.impl.infr.proto.PutMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.SchemaEventBuilder;
import com.bloomberg.bmq.impl.infr.scm.VersionUtil;
import com.bloomberg.bmq.impl.infr.util.PrintUtil;
import com.bloomberg.bmq.impl.infr.util.SystemUtil;
import com.bloomberg.bmq.it.util.BmqBroker;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlainProducerIT {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void sendMessage(
            String msgPayload, int port, Uri uri, boolean isOldStyleProperties)
            throws IOException, InterruptedException {

        // ===============================
        // Step 1. Connect to the Broker
        // ===============================

        InetSocketAddress serverAddress = new InetSocketAddress("localhost", port);

        SocketChannel schannel = SocketChannel.open();

        assertNotNull(schannel);

        if (!schannel.connect(serverAddress)) {
            logger.info("connect returned FALSE");
            return;
        }

        InetSocketAddress localAddress;
        InetSocketAddress remoteAddress;

        Socket socket = schannel.socket();
        localAddress = new InetSocketAddress(socket.getLocalAddress(), socket.getLocalPort());
        remoteAddress = new InetSocketAddress(socket.getInetAddress(), socket.getPort());

        logger.info("localAddress: {}", localAddress);
        logger.info("remoteAddress: {}", remoteAddress);

        // Disable nagle's algorithm
        socket.setTcpNoDelay(true);

        // =======================================
        // Step 2. Prepare a negotiation message
        // =======================================

        NegotiationMessageChoice negoMsgChoice = new NegotiationMessageChoice();

        negoMsgChoice.makeClientIdentity();

        ClientIdentity clientIdentity = negoMsgChoice.clientIdentity();

        clientIdentity.setProtocolversion(Protocol.VERSION);
        clientIdentity.setSdkVersion(VersionUtil.getSdkVersion());
        clientIdentity.setClientType(ClientType.E_TCPCLIENT);
        clientIdentity.setProcessName(SystemUtil.getProcessName());
        clientIdentity.setPid(SystemUtil.getProcessId());
        clientIdentity.setSessionId(0);
        clientIdentity.setFeatures("PROTOCOL_ENCODING:JSON;MPS:MESSAGE_PROPERTIES_EX");
        clientIdentity.setClusterName("");
        clientIdentity.setClusterNodeId(-1);
        clientIdentity.setSdkLanguage(ClientLanguage.E_JAVA);
        clientIdentity.setUserAgent("com.bloomberg.bmq.it.PlainProducerIT");

        SchemaEventBuilder schemaBuilder = new SchemaEventBuilder();

        schemaBuilder.setMessage(negoMsgChoice);
        ByteBuffer[] message = schemaBuilder.build();

        // ====================================================
        // Step 3. Send the negotiation message to the Broker
        // ====================================================

        long numWritten;
        numWritten = schannel.write(message);

        if (numWritten == 0) {
            logger.info("Failed to write on channel");
            return;
        }

        // =========================================
        // Step 4. Read a response from the Broker
        // =========================================

        ByteBuffer readBuffer = ByteBuffer.allocate(1024);

        long numRead = schannel.read(readBuffer);
        readBuffer.flip();

        logger.info("Read from channel: {}", numRead);
        logger.info("Content: {}", PrintUtil.hexDump(readBuffer));

        // =============================
        // Step 5. Decode the response
        // =============================

        ByteBufferInputStream bbis = new ByteBufferInputStream(readBuffer);
        EventHeader header = new EventHeader();

        header.streamIn(bbis);

        NegotiationMessageChoice negoMsg =
                JsonDecoderUtil.decode(bbis, NegotiationMessageChoice.class);

        logger.info("Broker response decoded!!!");

        // =================================
        // Step 6. Prepare Open Queue request
        // =================================

        ControlMessageChoice msgChoice = new ControlMessageChoice();
        msgChoice.setId(1);

        msgChoice.makeOpenQueue();

        OpenQueue openQueue = msgChoice.openQueue();

        long flags = 0;

        flags = QueueFlags.setWriter(flags);
        flags = QueueFlags.setAck(flags);

        QueueHandleParameters params = new QueueHandleParameters();

        params.setUri(uri);
        params.setFlags(flags);
        params.setQId(0);
        params.setReadCount(0);
        params.setWriteCount(1);
        params.setAdminCount(0);

        openQueue.setHandleParameters(params);

        schemaBuilder.reset();

        schemaBuilder.setMessage(msgChoice);
        message = schemaBuilder.build();

        // ====================================================
        // Step 7. Send the Open Queue message to the Broker
        // ====================================================

        numWritten = schannel.write(message);

        if (numWritten == 0) {
            logger.info("Open Queue: Failed to write on channel");
            return;
        }

        // =========================================
        // Step 8. Read a response from the Broker
        // =========================================

        readBuffer = ByteBuffer.allocate(1024);

        numRead = schannel.read(readBuffer);
        readBuffer.flip();

        logger.info("Read from channel: {}", numRead);
        logger.info("Content: {}", PrintUtil.hexDump(readBuffer));

        // =============================
        // Step 9. Decode the response
        // =============================

        bbis = new ByteBufferInputStream(readBuffer);
        header = new EventHeader();

        header.streamIn(bbis);

        ControlMessageChoice openQueueCtrlResponse =
                JsonDecoderUtil.decode(bbis, ControlMessageChoice.class);

        if (openQueueCtrlResponse.isStatusValue()) {
            // 'Status' is sent in case of failure.

            final Status status = openQueueCtrlResponse.status();

            logger.info(
                    "Received failed open-queue response: [category: {}, code: {}, message: '{}'].",
                    status.category(),
                    status.code(),
                    status.message());
            schannel.close();
            fail();
            return;
        }

        logger.info("Broker OpenQueueResponse decoded.");

        // =============================
        // Step 10. Send PUT message
        // =============================

        ByteBuffer b = ByteBuffer.wrap(msgPayload.getBytes());

        ByteBufferOutputStream bbos = new ByteBufferOutputStream();
        bbos.writeAscii(msgPayload);

        int putFlags = 0;
        putFlags = PutHeaderFlags.setFlag(putFlags, PutHeaderFlags.ACK_REQUESTED);

        // Add 2 properties to the message.
        MessagePropertiesImpl mp = new MessagePropertiesImpl();
        assertTrue(mp.setPropertyAsString("routingId", "abcd-efgh-ijkl"));
        assertTrue(mp.setPropertyAsInt64("timestamp", 123456789L));

        PutMessageImpl putMsg = new PutMessageImpl();
        putMsg.setQueueId(0);
        putMsg.appData().setProperties(mp);
        putMsg.appData().setPayload(b);

        putMsg.setFlags(putFlags);
        putMsg.setCorrelationId();

        PutEventBuilder putBuilder = new PutEventBuilder();
        EventBuilderResult res = putBuilder.packMessage(putMsg, isOldStyleProperties);

        assertSame(EventBuilderResult.SUCCESS, res);

        message = putBuilder.build();

        numWritten = schannel.write(message);

        if (numWritten == 0) {
            logger.info("PUT: Failed to write on channel");
            return;
        }

        logger.info("Put message has been sent!");

        // =========================================
        // Step 11. Read a ACK from the Broker
        // =========================================

        readBuffer = ByteBuffer.allocate(1024);

        numRead = schannel.read(readBuffer);
        readBuffer.flip();

        logger.info("Read from channel: {}", numRead);
        logger.info("Content: {}", PrintUtil.hexDump(readBuffer));

        // ===========================================
        // Step 12. Decode ACK header and message
        // ===========================================

        AckEventImpl ackEvent = new AckEventImpl(new ByteBuffer[] {readBuffer});

        assertTrue(ackEvent.isValid());

        Iterator<AckMessageImpl> ackIt = ackEvent.iterator();

        AckHeader ackHeader = ackEvent.ackHeader();

        logger.info(ackHeader.toString());

        while (ackIt.hasNext()) {
            AckMessageImpl ackMsg = ackIt.next();
            logger.info(ackMsg.toString());
        }

        // ===========================
        // Step 13. Create CloseQueue
        // ===========================

        msgChoice.reset();
        msgChoice.setId(2);

        msgChoice.makeCloseQueue();

        CloseQueue closeMsg = msgChoice.closeQueue();
        closeMsg.setHandleParameters(params);
        closeMsg.setIsFinal(true);

        schemaBuilder.setMessage(msgChoice);
        message = schemaBuilder.build();

        // ===================================================
        // Step 14. Send the CloseQueue message to the Broker
        // ===================================================

        numWritten = schannel.write(message);

        if (numWritten == 0) {
            throw new IOException("CloseQueue: Failed to write on channel");
        }
        logger.info("CloseQueue: sent {}", numWritten);

        // ===================================================
        // Step 15. Read a CloseQueueResponse from the Broker
        // ===================================================

        readBuffer = ByteBuffer.allocate(1024);

        numRead = schannel.read(readBuffer);
        readBuffer.flip();

        logger.info("Read from channel: {}", numRead);
        logger.info("Content: {}", PrintUtil.hexDump(readBuffer));

        // =======================================
        // Step 16. Decode the CloseQueueResponse
        // =======================================

        bbis = new ByteBufferInputStream(readBuffer);
        header = new EventHeader();

        header.streamIn(bbis);

        msgChoice = JsonDecoderUtil.decode(bbis, ControlMessageChoice.class);

        logger.info("Broker CloseQueueResponse decoded.");

        assertTrue(msgChoice.isCloseQueueResponseValue());

        // =========================================
        // Step 17. Create a Disconnect message
        // =========================================

        msgChoice.reset();
        msgChoice.setId(3);

        msgChoice.makeDisconnect();

        schemaBuilder.setMessage(msgChoice);
        message = schemaBuilder.build();

        // =============================================================
        // Step 18. Send the Disconnect message to the Broker
        // =============================================================

        numWritten = schannel.write(message);

        assertTrue(numWritten > 0);

        logger.info("Disconnect: sent {}", numWritten);

        // ===================================================
        // Step 19. Read a DisconnectResponse from the Broker
        // ===================================================

        readBuffer = ByteBuffer.allocate(1024);

        numRead = schannel.read(readBuffer);
        readBuffer.flip();

        logger.info("Read from channel: {}", numRead);
        logger.info("Content: {}", PrintUtil.hexDump(readBuffer));

        // =======================================
        // Step 20. Decode the DisconnectResponse
        // =======================================

        bbis = new ByteBufferInputStream(readBuffer);
        header = new EventHeader();

        header.streamIn(bbis);

        msgChoice = JsonDecoderUtil.decode(bbis, ControlMessageChoice.class);

        logger.info("Broker DisconnectResponse decoded.");

        assertTrue(msgChoice.isDisconnectResponseValue());

        Thread.sleep(1000);

        schannel.close();
    }

    @Test
    @Disabled("Disable this test because it uses plain sockets and read BMQ events unreliably")
    public void testProducer() throws IOException, InterruptedException {
        logger.info("==================================================");
        logger.info("BEGIN Testing PlainProducerIT sending a message.");
        logger.info("==================================================");

        try (BmqBroker broker = BmqBroker.createStartedBroker()) {

            final String MSG = "Hello from BlazingMQ Java SDK!";
            final int PORT = broker.sessionOptions().brokerUri().getPort();
            final Uri QUEUE_URI = BmqBroker.Domains.Priority.generateQueueUri();

            sendMessage(MSG, PORT, QUEUE_URI, broker.isOldStyleMessageProperties());

            broker.setDropTmpFolder();
        }

        logger.info("================================================");
        logger.info("END Testing PlainProducerIT sending a message.");
        logger.info("================================================");
    }
}
