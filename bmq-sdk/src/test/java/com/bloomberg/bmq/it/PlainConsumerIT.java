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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bloomberg.bmq.QueueFlags;
import com.bloomberg.bmq.Uri;
import com.bloomberg.bmq.impl.infr.codec.JsonDecoderUtil;
import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.msg.ClientIdentity;
import com.bloomberg.bmq.impl.infr.msg.ClientLanguage;
import com.bloomberg.bmq.impl.infr.msg.ClientType;
import com.bloomberg.bmq.impl.infr.msg.CloseQueue;
import com.bloomberg.bmq.impl.infr.msg.ConfigureQueueStream;
import com.bloomberg.bmq.impl.infr.msg.ControlMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.NegotiationMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.OpenQueue;
import com.bloomberg.bmq.impl.infr.msg.QueueHandleParameters;
import com.bloomberg.bmq.impl.infr.proto.ConfirmEventBuilder;
import com.bloomberg.bmq.impl.infr.proto.ConfirmMessage;
import com.bloomberg.bmq.impl.infr.proto.EventHeader;
import com.bloomberg.bmq.impl.infr.proto.MessagePropertiesImpl;
import com.bloomberg.bmq.impl.infr.proto.PropertyType;
import com.bloomberg.bmq.impl.infr.proto.Protocol;
import com.bloomberg.bmq.impl.infr.proto.PushEventImpl;
import com.bloomberg.bmq.impl.infr.proto.PushMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.PushMessageIterator;
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
import java.util.Map;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlainConsumerIT {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static ByteBuffer[] getLastMessage(int port, Uri uri)
            throws IOException, InterruptedException {
        ByteBuffer[] msg = null;

        // ===============================
        // Step 1. Connect to the Broker
        // ===============================

        InetSocketAddress serverAddress = new InetSocketAddress("localhost", port);

        SocketChannel schannel = SocketChannel.open();

        assertNotNull(schannel);

        if (!schannel.connect(serverAddress)) {
            throw new IOException("connect returned FALSE");
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
        clientIdentity.setUserAgent("com.bloomberg.bmq.it.PlainConsumerIT");

        SchemaEventBuilder schemaBuilder = new SchemaEventBuilder();

        schemaBuilder.setMessage(negoMsgChoice);
        ByteBuffer[] message = schemaBuilder.build();

        // ====================================================
        // Step 3. Send the negotiation message to the Broker
        // ====================================================

        long numWritten;
        numWritten = schannel.write(message);

        if (numWritten == 0) {
            throw new IOException("Failed to write on channel");
        }

        // =========================================
        // Step 4. Read a response from the Broker
        // =========================================

        ByteBuffer readBuffer = ByteBuffer.allocate(1024);

        long numRead;
        numRead = schannel.read(readBuffer);
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

        ControlMessageChoice ctrlMsg = new ControlMessageChoice();
        ctrlMsg.setId(1);

        ctrlMsg.makeOpenQueue();

        OpenQueue openQueue = ctrlMsg.openQueue();

        long flags = 0;

        flags = QueueFlags.setReader(flags);

        QueueHandleParameters params = new QueueHandleParameters();

        params.setUri(uri);
        params.setFlags(flags);
        params.setQId(1);
        params.setReadCount(1);
        params.setWriteCount(0);
        params.setAdminCount(0);

        openQueue.setHandleParameters(params);

        schemaBuilder.setMessage(ctrlMsg);
        message = schemaBuilder.build();

        // ====================================================
        // Step 7. Send the Open Queue message to the Broker
        // ====================================================

        logger.info("Sending open-queue message");

        numWritten = schannel.write(message);

        if (numWritten == 0) {
            throw new IOException("Open Queue: Failed to write on channel");
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

        JsonDecoderUtil.decode(bbis, ControlMessageChoice.class);

        logger.info("Broker OpenQueueResponse decoded.");

        // ==============================================
        // Step 10. Prepare ConfigureQueueStream message
        // ==============================================

        ctrlMsg.reset();
        ctrlMsg.setId(2);

        ctrlMsg.makeConfigureQueueStream();

        ConfigureQueueStream cfg = ctrlMsg.configureQueueStream();
        cfg.setId(1);
        cfg.streamParameters().setMaxUnconfirmedMessages(1024L);
        cfg.streamParameters().setMaxUnconfirmedBytes(33554432L);
        cfg.streamParameters().setConsumerPriority(1);
        cfg.streamParameters().setConsumerPriorityCount(1);

        schemaBuilder.setMessage(ctrlMsg);
        message = schemaBuilder.build();

        // =============================================================
        // Step 11. Send the ConfigureQueueStream message to the Broker
        // =============================================================

        numWritten = schannel.write(message);

        if (numWritten == 0) {
            throw new IOException("ConfigureQueueStream: Failed to write on channel");
        }
        logger.info("ConfigureQueueStream: sent {}", numWritten);

        // =========================================
        // Step 12. Read a response from the Broker
        // =========================================

        readBuffer = ByteBuffer.allocate(1024);

        numRead = schannel.read(readBuffer);
        readBuffer.flip();

        logger.info("Read from channel: {}", numRead);
        logger.info("Content: {}", PrintUtil.hexDump(readBuffer));

        // =================================================
        // Step 13. Decode the ConfigureQueueStreamResponse
        // =================================================

        bbis = new ByteBufferInputStream(readBuffer);
        header = new EventHeader();

        header.streamIn(bbis);

        JsonDecoderUtil.decode(bbis, ControlMessageChoice.class);

        logger.info("Broker ConfigureQueueStreamResponse decoded.");

        // =========================================
        // Step 14. Read a queue message
        // =========================================

        readBuffer = ByteBuffer.allocate(1024);

        numRead = schannel.read(readBuffer);
        readBuffer.flip();

        logger.info("Read from channel: {}", numRead);
        logger.info("Content: {}", PrintUtil.hexDump(readBuffer));

        // ============================================
        // Step 15. Decode push messages, confirm them
        // ============================================

        PushEventImpl pushEvent = new PushEventImpl(new ByteBuffer[] {readBuffer});
        ConfirmEventBuilder ceb = new ConfirmEventBuilder();

        assertTrue(pushEvent.isValid());

        PushMessageIterator pushIt = pushEvent.iterator();

        assertTrue(pushIt.isValid());

        while (pushIt.hasNext()) {
            PushMessageImpl pushMsg = pushIt.next();

            logger.info(pushMsg.toString());

            MessagePropertiesImpl props = pushMsg.appData().properties();
            if (props != null) {
                // We expect 2 properties in the message.
                assertEquals(2, props.numProperties());

                // Note that per contract, order of message properties is
                // undefined.

                boolean routingIdPropFound = false;
                boolean timestampPropFound = false;

                Iterator<Map.Entry<String, com.bloomberg.bmq.impl.infr.proto.MessageProperty>> it =
                        props.iterator();
                while (it.hasNext()) {
                    final com.bloomberg.bmq.impl.infr.proto.MessageProperty p =
                            it.next().getValue();
                    if (p.name().equals("routingId")) {
                        assertFalse(routingIdPropFound);
                        routingIdPropFound = true;
                        assertEquals(PropertyType.STRING, p.type());
                        assertEquals("abcd-efgh-ijkl", p.getValueAsString());
                    } else if (p.name().equals("timestamp")) {
                        assertFalse(timestampPropFound);
                        timestampPropFound = true;
                        assertEquals(PropertyType.INT64, p.type());
                        assertEquals(123456789L, p.getValueAsInt64());
                    }
                }

                assertTrue(routingIdPropFound);
                assertTrue(timestampPropFound);
            }

            // Add confirmation
            ConfirmMessage confMsg = new ConfirmMessage();
            confMsg.setQueueId(pushMsg.queueId());
            confMsg.setMessageGUID(pushMsg.messageGUID());
            ceb.packMessage(confMsg);

            // Pick the last message
            msg = pushMsg.appData().applicationData();
        }

        // =========================================
        // Step 16. Send confirmation
        // =========================================

        message = ceb.build();
        numWritten = schannel.write(message);

        if (numWritten == 0) {
            throw new IOException("ConfirmEventImpl: Failed to write on channel");
        }
        logger.info("ConfirmEventImpl: sent {}", numWritten);

        // ============================================================
        // Step 17. Prepare ConfigureQueueStream message befor closing
        // ============================================================

        ctrlMsg.reset();
        ctrlMsg.setId(3);

        ctrlMsg.makeConfigureQueueStream();

        cfg = ctrlMsg.configureQueueStream();
        cfg.setId(1);
        cfg.streamParameters().setMaxUnconfirmedMessages(0L);
        cfg.streamParameters().setMaxUnconfirmedBytes(0L);
        cfg.streamParameters().setConsumerPriority(Integer.MIN_VALUE);
        cfg.streamParameters().setConsumerPriorityCount(0);

        schemaBuilder.setMessage(ctrlMsg);
        message = schemaBuilder.build();

        // =============================================================
        // Step 18. Send the ConfigureQueueStream message to the Broker
        // =============================================================

        numWritten = schannel.write(message);

        if (numWritten == 0) {
            throw new IOException("ConfigureQueueStream: Failed to write on channel");
        }
        logger.info("ConfigureQueueStream: sent {}", numWritten);

        // =========================================
        // Step 19. Read a response from the Broker
        // =========================================

        readBuffer = ByteBuffer.allocate(1024);

        numRead = schannel.read(readBuffer);
        readBuffer.flip();

        logger.info("Read from channel: {}", numRead);
        logger.info("Content: {}", PrintUtil.hexDump(readBuffer));

        // =================================================
        // Step 20. Decode the ConfigureQueueStreamResponse
        // =================================================

        bbis = new ByteBufferInputStream(readBuffer);
        header = new EventHeader();

        header.streamIn(bbis);

        JsonDecoderUtil.decode(bbis, ControlMessageChoice.class);

        logger.info("Broker ConfigureQueueStreamResponse decoded.");

        // ===========================
        // Step 21. Create CloseQueue
        // ===========================

        ctrlMsg.reset();
        ctrlMsg.setId(4);

        ctrlMsg.makeCloseQueue();

        CloseQueue closeMsg = ctrlMsg.closeQueue();
        closeMsg.setHandleParameters(params);
        closeMsg.setIsFinal(true);

        schemaBuilder.setMessage(ctrlMsg);
        message = schemaBuilder.build();

        // ===================================================
        // Step 22. Send the CloseQueue message to the Broker
        // ===================================================

        numWritten = schannel.write(message);

        if (numWritten == 0) {
            throw new IOException("CloseQueue: Failed to write on channel");
        }
        logger.info("CloseQueue: sent {}", numWritten);

        // ===================================================
        // Step 23. Read a CloseQueueResponse from the Broker
        // ===================================================

        readBuffer = ByteBuffer.allocate(1024);

        numRead = schannel.read(readBuffer);
        readBuffer.flip();

        logger.info("Read from channel: {}", numRead);
        logger.info("Content: {}", PrintUtil.hexDump(readBuffer));

        // =======================================
        // Step 24. Decode the CloseQueueResponse
        // =======================================

        bbis = new ByteBufferInputStream(readBuffer);
        header = new EventHeader();

        header.streamIn(bbis);

        ctrlMsg = JsonDecoderUtil.decode(bbis, ControlMessageChoice.class);

        logger.info("Broker CloseQueueResponse decoded.");

        assertTrue(ctrlMsg.isCloseQueueResponseValue());

        // =====================================
        // Step 25. Create a Disconnect message
        // =====================================

        ctrlMsg.reset();
        ctrlMsg.setId(5);
        ctrlMsg.makeDisconnect();

        schemaBuilder.setMessage(ctrlMsg);
        message = schemaBuilder.build();

        // =============================================================
        // Step 26. Send the Disconnect message to the Broker
        // =============================================================

        numWritten = schannel.write(message);

        if (numWritten == 0) {
            throw new IOException("Disconnect: Failed to write on channel");
        }
        logger.info("Disconnect: sent {}", numWritten);

        // ===================================================
        // Step 27. Read a DisconnectResponse from the Broker
        // ===================================================

        readBuffer = ByteBuffer.allocate(1024);

        numRead = schannel.read(readBuffer);
        readBuffer.flip();

        logger.info("Read from channel: {}", numRead);
        logger.info("Content: {}", PrintUtil.hexDump(readBuffer));

        // =======================================
        // Step 28. Decode the DisconnectResponse
        // =======================================

        bbis = new ByteBufferInputStream(readBuffer);
        header = new EventHeader();

        header.streamIn(bbis);

        ctrlMsg = JsonDecoderUtil.decode(bbis, ControlMessageChoice.class);

        logger.info("Broker DisconnectResponse decoded.");

        assertTrue(ctrlMsg.isDisconnectResponseValue());

        Thread.sleep(1000);

        schannel.close();

        return msg;
    }

    @Test
    @Disabled("Disable this test because it uses plain sockets and read BMQ events unreliably")
    public void testConsumer() throws IOException, InterruptedException {

        logger.info("====================================================================");
        logger.info("BEGIN Testing PlainConsumerIT getting message from the BlazingMQ Broker.");
        logger.info("====================================================================");

        try (BmqBroker broker = BmqBroker.createStartedBroker()) {
            final String MSG = "Hello from BlazingMQ Java SDK! I'm consumer";
            final int PORT = broker.sessionOptions().brokerUri().getPort();

            final Uri QUEUE_URI = BmqBroker.Domains.Priority.generateQueueUri();

            PlainProducerIT.sendMessage(MSG, PORT, QUEUE_URI, broker.isOldStyleMessageProperties());

            getLastMessage(PORT, QUEUE_URI);

            broker.setDropTmpFolder();
        }

        logger.info("==================================================================");
        logger.info("END Testing PlainConsumerIT getting message from the BlazingMQ Broker.");
        logger.info("==================================================================");
    }
}
