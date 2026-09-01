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
package com.bloomberg.bmq.it.util;

import com.bloomberg.bmq.MessageGUID;
import com.bloomberg.bmq.impl.ProtocolEventTcpReader;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.msg.BrokerResponse;
import com.bloomberg.bmq.impl.infr.msg.ClientIdentity;
import com.bloomberg.bmq.impl.infr.msg.ConfigureStream;
import com.bloomberg.bmq.impl.infr.msg.ConfigureStreamResponse;
import com.bloomberg.bmq.impl.infr.msg.ControlMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.NegotiationMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.OpenQueue;
import com.bloomberg.bmq.impl.infr.msg.OpenQueueResponse;
import com.bloomberg.bmq.impl.infr.msg.RoutingConfiguration;
import com.bloomberg.bmq.impl.infr.msg.Status;
import com.bloomberg.bmq.impl.infr.msg.StatusCategory;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection;
import com.bloomberg.bmq.impl.infr.proto.ControlEventImpl;
import com.bloomberg.bmq.impl.infr.proto.EventHeader;
import com.bloomberg.bmq.impl.infr.proto.EventType;
import com.bloomberg.bmq.impl.infr.proto.Protocol;
import com.bloomberg.bmq.impl.infr.proto.PushEventBuilder;
import com.bloomberg.bmq.impl.infr.proto.PushMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.SchemaEventBuilder;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.infr.util.SystemUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.EventExecutor;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BmqBrokerSimulator implements TestTcpServer {

    static final Duration DEFAULT_SERVER_STOP_TIMEOUT = Duration.ofSeconds(10);
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Immutable
    public static class ResponseItem {
        final StatusCategory status;
        final Semaphore semaphore;
        final Duration timeout;

        public ResponseItem(StatusCategory status, Semaphore semaphore, Duration timeout) {
            this.status = Argument.expectNonNull(status, "status");
            this.semaphore = semaphore;
            this.timeout = timeout;
        }

        public ResponseItem(StatusCategory status, Semaphore semaphore) {
            this(status, semaphore, null);
        }

        public ResponseItem(StatusCategory status) {
            this(status, null, null);
        }

        StatusCategory getStatus() {
            return status;
        }

        public Duration getTimeout() {
            return timeout;
        }

        void release() {
            if (semaphore != null) {
                semaphore.release();
            }
        }
    }

    @Sharable
    private class NettyServerHandler extends ChannelInboundHandlerAdapter
            implements ProtocolEventTcpReader.EventHandler {
        private CompositeByteBuf readBuffer;
        private TcpConnection.ReadCallback.ReadCompletionStatus readBytesStatus;
        private ProtocolEventTcpReader reader = new ProtocolEventTcpReader(this);

        public NettyServerHandler() {
            readBuffer = Unpooled.compositeBuffer();
            readBytesStatus = new TcpConnection.ReadCallback.ReadCompletionStatus();
            readBytesStatus.setNumNeeded(0);
        }

        private ResponseItem nextItem() {
            switch (serverMode) {
                case BMQ_MANUAL_MODE:
                    return userDefinedResponses.poll();
                case BMQ_AUTO_MODE:
                    return new ResponseItem(StatusCategory.E_SUCCESS);
                default:
                    return null;
            }
        }

        void processControlMessageChoice(ControlMessageChoice controlMessageChoice) {
            Argument.expectNonNull(controlMessageChoice, "controlMessageChoice");
            logger.info("Process control message choice: {}", controlMessageChoice);
            // Store received request.
            try {
                receivedRequests.put(controlMessageChoice);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Unsupported interruption.");
            }
            ControlMessageChoice ctrlResp = null;
            int rId = controlMessageChoice.id();
            if (controlMessageChoice.isOpenQueueValue()) {
                ctrlResp = makeOpenQueueResponse(controlMessageChoice.openQueue(), rId);
            } else if (controlMessageChoice.isConfigureStreamValue()) {
                ctrlResp = makeConfigureStreamResponse(controlMessageChoice.configureStream(), rId);
            } else if (controlMessageChoice.isCloseQueueValue()) {
                ctrlResp = makeCloseQueueResponse(rId);
            } else if (controlMessageChoice.isDisconnectValue()) {
                ctrlResp = makeDisconnectResponse(rId);
            }
            if (ctrlResp != null) {
                ResponseItem item = nextItem();
                if (item == null || item.getStatus() == StatusCategory.E_TIMEOUT) {
                    logger.info("Do nothing for ctrl message: {}", item);
                    return; // Do nothing
                }
                if (item.getStatus() != StatusCategory.E_SUCCESS) {
                    ctrlResp = makeStatusResponse(rId, item.getStatus());
                }
                if (item.getTimeout() != null) {
                    try {
                        Thread.sleep(item.getTimeout().toMillis());
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Unsupported interruption.");
                    }
                }
                writeResponse(ctrlResp);
                item.release();
            } else {
                // MESSAGE UNKNOWN
                if (serverMode == Mode.BMQ_AUTO_MODE) {
                    logger.info("Echo response.");
                    writeResponse(controlMessageChoice);
                } else {
                    logger.info("Do nothing for unknown ctrl message");
                }
            }
        }

        void processNegoMessageChoice(NegotiationMessageChoice negotiationMessageChoice) {
            Argument.expectNonNull(negotiationMessageChoice, "negotiationMessageChoice");
            NegotiationMessageChoice negoResp;
            logger.info("Processing negotiation message choice");
            if (negotiationMessageChoice.isClientIdentityValue()) {
                logger.info("Processing client identity");
                ResponseItem item = nextItem();
                if (item == null) {
                    logger.info("Do nothing for nego message.");
                    return; // Do nothing
                }
                switch (item.getStatus()) {
                    case E_TIMEOUT:
                        logger.info("Due to timeout no response will be sent");
                        break;
                    case E_NOT_READY:
                        logger.info("Special request to shutdown");
                        closeChannels();
                        break;
                    default:
                        negoResp =
                                makeBrokerResponse(
                                        negotiationMessageChoice.clientIdentity(),
                                        item.getStatus());
                        writeResponse(negoResp);
                        break;
                }
                item.release();
            } else {
                // MESSAGE UNKNOWN
                if (serverMode == Mode.BMQ_AUTO_MODE) {
                    logger.info("Echo response.");
                    writeResponse(negotiationMessageChoice);
                } else {
                    logger.info("Do nothing for unknown nego message");
                }
            }
        }

        void echo(Object msg) {
            logger.info("Sending echo response: {}", msg);
            // Echo the received data.
            channelContext.write(msg);
            channelContext.flush();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf byteBuf = (ByteBuf) msg;
            if (serverMode == Mode.ECHO_MODE) {
                echo(msg);
            } else if (serverMode == Mode.SILENT_MODE) {
                byteBuf.release();
                return;
            } else {
                readBuffer.addComponent(true, byteBuf);

                // Check if there are enough bytes in 'readBuffer'.
                final int numNeeded = readBytesStatus.numNeeded();
                if (readBuffer.isReadable(numNeeded)) {
                    logger.trace("Enough bytes: {}", byteBuf.toString(Charset.defaultCharset()));
                    // There are enough bytes.  Notify client.

                    final ByteBuffer[] byteBuffers = readBuffer.nioBuffers();
                    final int numConsumed = readBuffer.readableBytes();

                    try {
                        reader.read(readBytesStatus, byteBuffers);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    // Update read-index of the read-buffer to the amount consumed
                    // by the client, then discard all ByteBufs that have been
                    // read.  Updating the read-index is required because we
                    // specify a ByteBuffer in the read callback, and manipulating
                    // the ByteBuffer does not affect the read/write indices of the
                    // netty ByteBuf.

                    readBuffer.readerIndex(numConsumed);
                    readBuffer.discardReadComponents();
                }
            }
            logger.trace("Channel read is done;");
        }

        public void handleEvent(EventType eventType, ByteBuffer[] bbuf) {
            logger.info("Handle event {} with size: {}", eventType, bbuf.length);
            receivedEventTypes.add(eventType);
            if (eventType == EventType.CONTROL) {
                ControlEventImpl controlEvent = new ControlEventImpl(bbuf);
                ControlMessageChoice controlMessageChoice = controlEvent.tryControlChoice();
                NegotiationMessageChoice negotiationMessageChoice =
                        controlEvent.tryNegotiationChoice();
                logger.info("Control event received: {}", controlEvent);
                if (negotiationMessageChoice != null) {
                    processNegoMessageChoice(negotiationMessageChoice);
                } else if (controlMessageChoice != null) {
                    processControlMessageChoice(controlMessageChoice);
                } else {
                    logger.error("Control message parsing failed.");
                }
            } else {
                logger.info("Non-control event received: {}", eventType);
            }
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) {
            Argument.expectNonNull(ctx, "ctx");
            synchronized (lock) {
                channelContext = ctx;
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // Close the connection when an exception is raised.
            logger.error("Server failed: ", cause);
            ctx.close();
        }
    }

    public enum Mode {
        SILENT_MODE, // let tcp connection be established, but never respond anything
        ECHO_MODE, // simply echo whatever bytes are received
        BMQ_AUTO_MODE, // always respond successful responses
        BMQ_MANUAL_MODE // test driver defines what should be responded to the BlazingMQ client
    }

    private final int port;
    private final URI uri;
    private Mode serverMode;

    /**
     * Guards {@code eventLoop}, {@code channelFuture} and {@code channelContext}, which are
     * accessed both by the test driver thread and by the server event loop. Only held for
     * non-blocking work, never while awaiting the event loop.
     */
    private final Object lock = new Object();

    private EventLoopGroup eventLoop;
    private ChannelFuture channelFuture;
    private ConcurrentLinkedQueue<ResponseItem> userDefinedResponses;
    private volatile ChannelHandlerContext channelContext = null;
    private LinkedBlockingQueue<ControlMessageChoice> receivedRequests =
            new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<EventType> receivedEventTypes = new LinkedBlockingQueue<>();

    public BmqBrokerSimulator(Mode serverMode) {
        this(SystemUtil.getEphemeralPort(), serverMode);
    }

    public BmqBrokerSimulator(int port, Mode serverMode) {
        this.port = Argument.expectPositive(port, "port");
        uri = URI.create("tcp://localhost:" + port);
        this.serverMode = serverMode;
        if (this.serverMode == Mode.BMQ_MANUAL_MODE) {
            userDefinedResponses = new ConcurrentLinkedQueue<>();
        }
        logger.info("Port: {} Server mode: {}", this.port, serverMode);
    }

    public void write(ByteBuffer[] message) {
        ByteBuf bufPushRequest = Unpooled.wrappedBuffer(message);
        channelContext.write(bufPushRequest);
        channelContext.flush();
    }

    public void writePushRequest(int qId) throws IOException {
        final String PAYLOAD = "abcdefghijklmnopqrstuvwxyz";
        final String GUID = "ABCDEF0123456789ABCDEF0123456789";

        MessageGUID guid = MessageGUID.fromHex(GUID);

        PushMessageImpl pushMsg = new PushMessageImpl();
        pushMsg.setQueueId(qId);
        pushMsg.setMessageGUID(guid);
        pushMsg.appData().setPayload(ByteBuffer.wrap(PAYLOAD.getBytes()));

        PushEventBuilder builder = new PushEventBuilder();
        builder.packMessage(pushMsg, isOldStyleMessageProperties());

        write(builder.build());
    }

    public void writeHeartbeatRequest() throws IOException {
        EventHeader header = new EventHeader();
        header.setType(EventType.HEARTBEAT_REQ);
        ByteBufferOutputStream bbos = new ByteBufferOutputStream(EventHeader.HEADER_SIZE);
        header.streamOut(bbos);
        write(bbos.reset());
    }

    public void writeDisconnectResponse(int rId) {
        writeResponse(makeDisconnectResponse(rId));
    }

    public void writeCloseQueueResponse(int rId) {
        writeResponse(makeCloseQueueResponse(rId));
    }

    @Override
    public void start() {
        final int NUM_IO_THREADS = 1;
        final NettyServerHandler handler = new NettyServerHandler();
        final EventLoopGroup group =
                new MultiThreadIoEventLoopGroup(NUM_IO_THREADS, NioIoHandler.newFactory());
        synchronized (lock) {
            eventLoop = group;
        }

        ServerBootstrap b = new ServerBootstrap();
        b.group(group)
                .channel(NioServerSocketChannel.class)
                .childHandler(
                        new ChannelInitializer<SocketChannel>() {
                            @Override
                            public void initChannel(SocketChannel ch) {
                                ch.pipeline().addLast(handler);
                            }
                        })
                .option(ChannelOption.SO_BACKLOG, 128)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        final ChannelFuture bound;
        try {
            bound = b.bind(port).syncUninterruptibly();
        } catch (Exception e) {
            shutdownEventLoop();
            logger.error("Failed to start server on port '{}'", port);
            throw new RuntimeException(e);
        }
        synchronized (lock) {
            channelFuture = bound;
        }
        logger.info("Server bound to port {}", port);
    }

    /**
     * Stops the server and waits until all its resources are released.
     *
     * <p>When this method returns, both the listening socket and the accepted client connection are
     * closed and the port is free to be bound again. Must be called from outside the server event
     * loop, use {@code closeChannels} to bring the server down from a channel handler.
     */
    @Override
    public void stop() {
        logger.info("Server stopping...");
        closeChannels();
        shutdownEventLoop();
        logger.info("Server stopped");
    }

    /**
     * Closes the client connection and the listening socket, may be called from the event loop.
     *
     * <p>Concurrent callers are safe: the channels are claimed under the lock, so exactly one
     * caller observes them and closes them.
     */
    private void closeChannels() {
        final ChannelHandlerContext ctx;
        final ChannelFuture bound;
        synchronized (lock) {
            ctx = channelContext;
            channelContext = null;
            bound = channelFuture;
            channelFuture = null;
        }
        if (ctx != null) {
            ctx.close();
        }
        if (bound != null) {
            bound.channel().close();
        }
    }

    private void shutdownEventLoop() {
        final EventLoopGroup group;
        synchronized (lock) {
            group = eventLoop;
            if (group != null) {
                for (EventExecutor executor : group) {
                    if (executor.inEventLoop()) {
                        throw new IllegalStateException(
                                "Server must not be stopped from its event loop");
                    }
                }
                eventLoop = null;
            }
        }
        if (group == null) {
            return;
        }
        group.shutdownGracefully(0, DEFAULT_SERVER_STOP_TIMEOUT.getSeconds(), TimeUnit.SECONDS)
                .syncUninterruptibly();
    }

    @Override
    public void enableRead() {
        setAutoRead(true);
    }

    @Override
    public void disableRead() {
        setAutoRead(false);
    }

    private void setAutoRead(boolean autoRead) {
        final ChannelFuture bound;
        synchronized (lock) {
            bound = channelFuture;
        }
        if (bound != null) {
            bound.channel().config().setAutoRead(autoRead);
        }
    }

    public void pushSuccess(int number) {
        for (int i = 0; i < number; i++) {
            pushSuccess();
        }
    }

    public void pushSuccess() {
        pushItem(StatusCategory.E_SUCCESS);
    }

    public void pushItem(StatusCategory status) {
        if (serverMode != Mode.BMQ_MANUAL_MODE) {
            throw new IllegalStateException("Expected BMQ_MANUAL_MODE");
        }
        this.userDefinedResponses.add(new ResponseItem(status));
    }

    public void pushItem(StatusCategory status, Semaphore semaphore) {
        if (serverMode != Mode.BMQ_MANUAL_MODE) {
            throw new IllegalStateException("Expected BMQ_MANUAL_MODE");
        }
        this.userDefinedResponses.add(new ResponseItem(status, semaphore));
    }

    public void pushItem(StatusCategory status, Duration timeout) {
        if (serverMode != Mode.BMQ_MANUAL_MODE) {
            throw new IllegalStateException("Expected BMQ_MANUAL_MODE");
        }
        this.userDefinedResponses.add(new ResponseItem(status, null, timeout));
    }

    public void pushItem(StatusCategory status, Semaphore semaphore, Duration timeout) {
        if (serverMode != Mode.BMQ_MANUAL_MODE) {
            throw new IllegalStateException("Expected BMQ_MANUAL_MODE");
        }
        this.userDefinedResponses.add(new ResponseItem(status, semaphore, timeout));
    }

    public ControlMessageChoice nextClientRequest() {
        return nextClientRequest(5);
    }

    public ControlMessageChoice nextClientRequest(int sec) {
        Argument.expectPositive(sec, "sec");

        ControlMessageChoice res = null;
        try {
            res = receivedRequests.poll(sec, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Interrupted: ", e);
            Thread.currentThread().interrupt();
        }
        return res;
    }

    public EventType nextEventType(int sec) {
        Argument.expectPositive(sec, "sec");

        EventType res = null;
        try {
            res = receivedEventTypes.poll(sec, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Interrupted: ", e);
            Thread.currentThread().interrupt();
        }
        return res;
    }

    @Override
    public String toString() {
        return "BmqBrokerSimulator [" + serverMode + " on port " + port + "]";
    }

    public int getPort() {
        return port;
    }

    public URI getURI() {
        // Immutable according to documentation
        return uri;
    }

    void writeResponse(Object rsp) {
        SchemaEventBuilder builder = new SchemaEventBuilder();
        try {
            builder.setMessage(rsp);
            write(builder.build());
        } catch (IOException ex) {
            logger.error("Failed to build resp");
        }
    }

    NegotiationMessageChoice makeBrokerResponse(
            ClientIdentity clientIdentity, StatusCategory category) {
        NegotiationMessageChoice negMsg = new NegotiationMessageChoice();
        negMsg.makeBrokerResponse();
        BrokerResponse brokerResponse = negMsg.brokerResponse();
        brokerResponse.setBrokerVersion(1);
        brokerResponse.setIsDeprecatedSdk(false);
        brokerResponse.setProtocolversion(Protocol.VERSION);
        Status status = new Status();
        status.setCategory(category);
        brokerResponse.setResult(status);
        brokerResponse.setOriginalRequest(clientIdentity);
        return negMsg;
    }

    ControlMessageChoice makeOpenQueueResponse(OpenQueue openQueue, int rId) {
        logger.info("Process OpenQueue request");
        ControlMessageChoice ctrlMsg = new ControlMessageChoice();
        ctrlMsg.setId(rId);
        ctrlMsg.makeOpenQueueResponse();
        OpenQueueResponse openQueueResponse = ctrlMsg.openQueueResponse();
        RoutingConfiguration routingConfiguration = new RoutingConfiguration();
        routingConfiguration.setFlags(0L);
        openQueueResponse.setRoutingConfiguration(routingConfiguration);
        openQueueResponse.setOriginalRequest(openQueue);
        return ctrlMsg;
    }

    ControlMessageChoice makeConfigureStreamResponse(ConfigureStream configureStream, int rId) {
        logger.info("Process ConfigureStream request");
        ControlMessageChoice ctrlMsg = new ControlMessageChoice();
        ctrlMsg.setId(rId);
        ctrlMsg.makeConfigureStreamResponse();
        ConfigureStreamResponse configureStreamResponse = ctrlMsg.configureStreamResponse();
        RoutingConfiguration routingConfiguration = new RoutingConfiguration();
        routingConfiguration.setFlags(0L);
        configureStreamResponse.setOriginalRequest(configureStream);
        return ctrlMsg;
    }

    ControlMessageChoice makeCloseQueueResponse(int rId) {
        logger.info("Process CloseQueue request");
        ControlMessageChoice ctrlMsg = new ControlMessageChoice();
        ctrlMsg.setId(rId);
        ctrlMsg.makeCloseQueueResponse();
        return ctrlMsg;
    }

    ControlMessageChoice makeStatusResponse(int rId, StatusCategory status) {
        logger.info("Reply error status");
        ControlMessageChoice ctrlMsg = new ControlMessageChoice();
        ctrlMsg.setId(rId);
        ctrlMsg.makeStatus();
        ctrlMsg.status().setCategory(status);
        return ctrlMsg;
    }

    ControlMessageChoice makeDisconnectResponse(int rId) {
        logger.info("Process Disconnect message");
        ControlMessageChoice ctrlMsg = new ControlMessageChoice();
        ctrlMsg.setId(rId);
        ctrlMsg.makeDisconnectResponse();
        return ctrlMsg;
    }
}
