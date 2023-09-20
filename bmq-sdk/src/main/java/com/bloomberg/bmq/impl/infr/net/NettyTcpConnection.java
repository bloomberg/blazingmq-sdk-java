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
package com.bloomberg.bmq.impl.infr.net;

import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.net.intf.ChannelStatusHandler;
import com.bloomberg.bmq.impl.infr.net.intf.ChannelStatusHandler.ChannelStatus;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection;
import com.bloomberg.bmq.impl.infr.net.intf.TcpConnection.ReadCallback.ReadCompletionStatus;
import com.bloomberg.bmq.impl.infr.util.Argument;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link NettyTcpConnection} class is netty-based implementation of {@link TcpConnection}
 * interface.
 *
 * <p>Thread safety. Conditionally thread-safe and thread-enabled class.
 *
 * <p>A {@code ChannelHandler} in netty needs to have its 'Sharable' trait, which indicates to netty
 * that same ChannelHandler instance can be used across multiple channels (which can occur for us
 * when a channel is closed and a new one is opened).
 */
@Sharable
public final class NettyTcpConnection extends ChannelInboundHandlerAdapter
        implements TcpConnection, ChannelFutureListener {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * {@link ClientChannelAdapter} class is devoted to express interface to a part of {@link
     * NettyTcpConnection} implementation detail that is directly responsible for interaction with
     * Netty {@link io.netty.bootstrap.Bootstrap} class. We want to add the possibility to test
     * client-server interactions inside a unit test without creating actual network connection. For
     * that purpose, we need to inject a mechanism to replace TCP-connection logic with our
     * test-specific logic, via the constructor which accepts a test instance of {@link
     * ClientChannelAdapter}.
     *
     * <p>Other goals are to inject server responses and as well as to keep the threading model same
     * in the test code. The best candidate is {@link io.netty.channel.embedded.EmbeddedChannel}
     * class that lets us inject responses. The main problem with given class is the fact that it is
     * "threadless" and unfortunately does not allow us to inject custom eventloop. But we can
     * inject it in our channel adapter and execute every embedded channel routine in IO thread. So,
     * it helps to achieve thread safety for plain embedded channel class and at the same time,
     * reproduce the threading model for tested class.
     */
    public interface ClientChannelAdapter {
        /**
         * Create {@code SocketChannel} with proper parameters and subscribe {@code
         * NettyTcpConnection} instance to its events.
         *
         * @param connection {@code NettyTcpConnection} to be subscribed on creating {@code
         *     SocketChannel}.
         * @param loop {@code EventLoop} for handling IO events.
         */
        void configure(NettyTcpConnection connection, EventLoopGroup loop);

        /**
         * Establish connection with server via created channel in {@link
         * ClientChannelAdapter#configure(NettyTcpConnection, EventLoopGroup)} method.
         *
         * @return {@link ChannelFuture} as the result an asynchronous connection establishment.
         */
        ChannelFuture connect();

        /** Reset connection. */
        void reset();

        /**
         * Write {@code data} to channel via channel context instance {@code ctx}.
         *
         * @param ctx channel context.
         * @param data data to write.
         */
        void write(ChannelHandlerContext ctx, ByteBuffer[] data);
    }

    // Nested class to encapsulate part of NettyTcpConnection logic
    // responsible for direct interaction with Netty 'Bootstrap'.
    private static final class BootstrapClientChannelAdapter implements ClientChannelAdapter {
        private NettyTcpConnection connection = null;

        @Override
        public void configure(NettyTcpConnection connection, EventLoopGroup loop) {
            this.connection = connection;
            this.connection.bootstrap = new Bootstrap();
            this.connection
                    .bootstrap
                    .group(loop)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.AUTO_READ, true) // 'true' by default
                    .option(
                            ChannelOption.WRITE_BUFFER_WATER_MARK,
                            new WriteBufferWaterMark(
                                    this.connection.options.writeBufferWaterMark().lowWaterMark(),
                                    this.connection.options.writeBufferWaterMark().highWaterMark()))
                    .option(
                            ChannelOption.CONNECT_TIMEOUT_MILLIS,
                            Math.toIntExact(
                                    this.connection.options.startAttemptTimeout().toMillis()))
                    .handler(
                            new ChannelInitializer<SocketChannel>() {
                                @Override
                                public void initChannel(SocketChannel ch) {
                                    ch.pipeline()
                                            .addLast(BootstrapClientChannelAdapter.this.connection);
                                }
                            });
        }

        @Override
        public ChannelFuture connect() {
            URI hostUri = connection.hostResolver.getNextUri();
            logger.info("Connecting to {}", hostUri);
            return connection.bootstrap.connect(hostUri.getHost(), hostUri.getPort());
        }

        @Override
        public void reset() {
            if (connection != null) {
                connection.bootstrap = null;
                connection = null;
            }
        }

        @Override
        public void write(ChannelHandlerContext ctx, ByteBuffer[] data) {
            if (logger.isDebugEnabled()) {
                int size = 0;
                for (ByteBuffer b : data) {
                    size += b.remaining();
                }
                logger.debug("Writing {} bytes from {} buffers", size, data.length);
            }
            ctx.writeAndFlush(Unpooled.wrappedBuffer(data));
        }
    }

    // If this class gets big, other helper class(s) can implement and/or
    // extend ChannelFutureListener and/or ChannelInboundHandlerAdapter
    // classes.

    private enum ChannelState {
        CONNECTED,
        CONNECTING,
        DISCONNECTED,
        DISCONNECTING;
    }

    private static final int NUM_IO_THREADS = 1;
    private static final int DEFAULT_LINGER_TIMEOUT_MILLISECONDS = 2000;
    private static final int DEFAULT_SHUTDOWN_ATTEMPTS = 3;
    private static final int MAX_IO_DUMP_FILE_SIZE = 1024 * 1024 * 1024; // One Gb

    private static boolean dumpMode = false; // todo: move dump mode routines out of this class

    private static FileOutputStream blackBox;
    private static Writer blackBoxIndex;

    private ByteBufferOutputStream readBuffer;

    private final Object lock;
    private long ioThreadId;
    private ChannelState state;
    private boolean wasOnceConnected;
    private int numRemainingInitialTries;
    private ConnectionOptions options;
    private NetResolver hostResolver;
    private ReadCompletionStatus readBytesStatus;
    private EventLoopGroup eventLoop;
    private Bootstrap bootstrap;
    private ChannelHandlerContext channelContext;
    private ReadCallback readCallback;
    private ConnectCallback connectCallback;
    private DisconnectCallback disconnectCallback;
    private ChannelStatusHandler channelStatusHandler;
    private Semaphore channelWaterMarkSema;
    private ClientChannelAdapter clientChannelAdapter;
    private final long lingerTimeout;

    static {
        String s = System.getenv("BMQ_IO_DUMP_DIR");
        if (s != null) {
            File dumpDir = new File(s);
            if (dumpDir.exists() && dumpDir.isDirectory() && dumpDir.canWrite()) {
                try {
                    String filePref = "bmq_io_dump_" + System.currentTimeMillis();
                    File binFile = new File(dumpDir, filePref + ".bin");
                    File idxFile = new File(dumpDir, filePref + ".idx");

                    blackBox = new FileOutputStream(binFile);
                    blackBoxIndex =
                            new OutputStreamWriter(
                                    new FileOutputStream(idxFile), StandardCharsets.US_ASCII);
                    dumpMode = true;
                    logger.info("Dump mode data  file: {}", binFile.getAbsolutePath());
                    logger.info("Dump mode index file: {}", idxFile.getAbsolutePath());
                } catch (IOException e) {
                    logger.error("Failed to enable IO dump mode: ", e);
                }
            } else {
                logger.error(
                        "Failed to enable IO dump mode. No such directory or not writable: {}", s);
            }
        }
    }

    /**
     * Create {@link NettyTcpConnection} instance.
     *
     * @return NettyTcpConnection instance
     */
    public static TcpConnection createInstance() {
        return new NettyTcpConnection(
                new BootstrapClientChannelAdapter(), DEFAULT_LINGER_TIMEOUT_MILLISECONDS);
    }

    /**
     * Create {@link NettyTcpConnection} with custom {@code lingerTimeout}.
     *
     * @param lingerTimeout timeout in ms
     * @return TcpConnection implementation instance
     */
    public static TcpConnection createTestedInstance(long lingerTimeout) {
        return new NettyTcpConnection(new BootstrapClientChannelAdapter(), lingerTimeout);
    }

    /**
     * Create {@link NettyTcpConnection} with custom instance of {@link ClientChannelAdapter}.
     *
     * @param injectedAdapter instance of {@code ClientChannelAdapter} implementation
     * @return TcpConnection implementation instance
     */
    public static TcpConnection createTestedInstance(ClientChannelAdapter injectedAdapter) {
        return new NettyTcpConnection(injectedAdapter, DEFAULT_LINGER_TIMEOUT_MILLISECONDS);
    }

    /**
     * Create {@link NettyTcpConnection} with custom {@code lingerTimeout} and custom instance of
     * {@link ClientChannelAdapter}.
     *
     * @param injectedAdapter instance of {@code ClientChannelAdapter} implementation
     * @param lingerTimeout timeout in ms
     * @return TcpConnection implementation instance
     */
    public static TcpConnection createTestedInstance(
            ClientChannelAdapter injectedAdapter, long lingerTimeout) {
        return new NettyTcpConnection(injectedAdapter, lingerTimeout);
    }

    private NettyTcpConnection(ClientChannelAdapter injectedAdapter, long lingerTimeout) {
        lock = new Object();
        ioThreadId = -1;
        state = ChannelState.DISCONNECTED;
        wasOnceConnected = false;
        numRemainingInitialTries = 0;
        options = null;
        readBuffer = null;
        readBytesStatus = null;
        eventLoop = null;
        bootstrap = null;
        channelContext = null;
        readCallback = null;
        connectCallback = null;
        disconnectCallback = null;
        this.lingerTimeout = lingerTimeout;
        clientChannelAdapter = injectedAdapter;
        hostResolver = new NetResolver();
    }

    private void dump(ByteBuffer[] byteBuffers) {
        try {
            if (blackBox.getChannel().size() >= MAX_IO_DUMP_FILE_SIZE) {
                logger.warn("Dump exceeded max size: {}", blackBox.getChannel().size());
                closeDumps();
                dumpMode = false;
                return;
            }
            blackBoxIndex.write(Integer.toString(byteBuffers.length));
            for (ByteBuffer b : byteBuffers) {
                blackBoxIndex.write(" " + b.remaining());
                ByteBuffer dup = b.duplicate();
                while (dup.hasRemaining()) {
                    blackBox.write(dup.get());
                }
            }
            blackBoxIndex.write("\n");
        } catch (IOException e) {
            logger.error("Failed to do IO dump: ", e);
            closeDumps();
            dumpMode = false;
        }
    }

    private void closeDumps() {
        try {
            if (blackBox != null) {
                blackBox.close();
            }
            if (blackBoxIndex != null) {
                blackBoxIndex.close();
            }
        } catch (IOException e) {
            logger.error("Failed to close dumps: ", e);
        }
    }

    private boolean stopEventLoop(EventLoopGroup eventLoop, long lingerTimeout) {
        if (eventLoop == null) return true;
        Future<?> f =
                eventLoop.shutdownGracefully(lingerTimeout, lingerTimeout, TimeUnit.MILLISECONDS);
        for (int i = 0; i < DEFAULT_SHUTDOWN_ATTEMPTS && !f.isDone(); i++) {
            f.awaitUninterruptibly(lingerTimeout, TimeUnit.MILLISECONDS);
        }
        return f.isSuccess();
    }

    /**
     * Asynchronously establish connection with tcp server.
     *
     * <p>Initiate connection with tcp server with proper session options (provided by corresponding
     * parameter) and set callbacks for connection and read events. TODO: should return type be
     * something equivalent to {@code bmqt::GenericResultCode}?
     *
     * <p>Thread model: executed in any thread.
     *
     * @param options non-null session options to configure session
     * @param connectCb non-null callback to handle connect event.
     * @param readCb non-null callback to handle read event.
     * @param initialMinNumBytes positive number of minimum bytes needed to trigger read event.
     * @throws IllegalArgumentException if {@code initialMinNumBytes} is non-positive.
     * @return 0 on success, -1 on failure.
     */
    @Override
    public int connect(
            ConnectionOptions options,
            ConnectCallback connectCb,
            ReadCallback readCb,
            int initialMinNumBytes) {
        Argument.expectNonNull(options, "options");
        Argument.expectNonNull(options.brokerUri().getHost(), "host");
        Argument.expectPositive(options.brokerUri().getPort(), "port");
        Argument.expectNonNull(connectCb, "connectCb");
        Argument.expectNonNull(readCb, "readCb");
        Argument.expectPositive(initialMinNumBytes, "initialMinNumBytes");

        EventLoopGroup el;
        long timeout;
        synchronized (lock) {
            try {
                hostResolver.setUri(options.brokerUri());
            } catch (IllegalArgumentException e) {
                logger.error("Failed resolve broker URI: ", e);
                return -1;
            }
            if (state != ChannelState.DISCONNECTED) {
                logger.error("failed to connect - connection or disconnection in progress.");
                return -1; // connection or disconnection in progress
            }
            timeout = lingerTimeout;
            el = eventLoop;
            state = ChannelState.CONNECTING;
            eventLoop = null; // null event loop means that given connection is being stopped.
        }
        if (!stopEventLoop(el, timeout)) return -1;
        synchronized (lock) {
            this.options = options;
            numRemainingInitialTries = this.options.startNumRetries();
            eventLoop = new NioEventLoopGroup(NUM_IO_THREADS);
            readCallback = readCb;
            connectCallback = connectCb;
            clientChannelAdapter.reset();
            clientChannelAdapter.configure(this, this.eventLoop);
            readBuffer = new ByteBufferOutputStream();
            readBytesStatus = new ReadCompletionStatus();
            readBytesStatus.setNumNeeded(initialMinNumBytes);
            channelWaterMarkSema = new Semaphore(0);
            doConnect();
        }

        return 0;
    }

    /**
     * Asynchronously initiate disconnection with tcp server.
     *
     * <p>Initiate disconnection with tcp server and set callback for disconnection event.
     *
     * <p>If callback is provided then regular disconnect procedure is executed. Otherwise the
     * channel is dropped and the reconnecting is initiated.
     *
     * <p>Thread model: executed in any thread.
     *
     * <p>Netty notes:
     *
     * <ul>
     *   <li>For TCP, {@code Channel.disconnect()} is equivalent to {@code Channel.close()}.
     *   <li>Channel.close() will invoke ChannelHandler.close() of *all* channel handlers registered
     *       with the channel pipeline. On the other hand, {@code ChannelHandlerA.close()} will
     *       invoke {@code close()} only on those channel handlers which appear after {@code
     *       ChannelHandlerA} in the channel pipeline. We register only one channel handler so we
     *       for us, both approaches are same. However, {@code disconnect} is called by the user, so
     *       to be complete, we invoke {@code Channel.close()}.
     * </ul>
     *
     * @param disconnectCb Callback, If non-null then it is used to handle disconnect event.
     * @return 0 on success, -1 on failure.
     */
    @Override
    public int disconnect(DisconnectCallback disconnectCb) {
        synchronized (lock) {
            // Unblock any thread waiting for the channel to become writable
            // (client could invoke 'disconnect' from one thread, while another
            // thread is blocked on 'waitUntilWritable').
            channelWaterMarkSema.release();

            logger.debug("disconnect in state {}", state);

            if (state == ChannelState.DISCONNECTED || state == ChannelState.DISCONNECTING) {

                // Already disconnected or disconnection in progress.
                logger.error(
                        "failed to disconnect - already disconnected or disconnection in progress");
                return -1;
            }

            final boolean isToBeClosed = (state == ChannelState.CONNECTED);
            if (isToBeClosed) {
                if (channelContext == null) {
                    throw new IllegalStateException("Channel context is not set");
                }
            } else {
                if (state != ChannelState.CONNECTING) {
                    throw new IllegalStateException(
                            "Expecting channel state: " + ChannelState.CONNECTING);
                }
                // Connection is in progress (which could mean 'doConnect' has
                // been invoked or has been scheduled to be invoked later).
            }

            disconnectCallback = disconnectCb;

            // If the callback is not null, then we are going to do full
            // disconnect procedure. Otherwise we just drop the channel and
            // schedule reconnecting (see 'channelCloseFutureComplete' method).
            if (disconnectCb != null) {
                state = ChannelState.DISCONNECTING;
            }

            if (isToBeClosed) {
                // Initiate async close of the channel.
                channelContext.channel().close();
            }
        }

        return 0;
    }

    /**
     * Gracefully clean up connection.
     *
     * <p>Set all fields to null and gracefully finish associated event loop. Do nothing if
     * connection is not in {@code ChannelState.DISCONNECTED} state.
     *
     * <p>Thread model: executed in any thread, except netty-io thread.
     *
     * @throws IllegalStateException when executed in netty-io thread.
     */
    @Override
    public int linger() {
        EventLoopGroup el;
        long timeout;
        synchronized (lock) {
            if (Thread.currentThread().getId() == ioThreadId) {
                throw new IllegalStateException("Cannot invoke 'linger' from the IO thread.");
            }

            logger.debug("Calling linger in state {}", state);

            if (state != ChannelState.DISCONNECTED) {
                // Someone could invoke 'connect' after 'disconnect' is
                // complete.
                logger.error("Failed to linger - inappropriate state {}", state);
                return -1;
            }

            options = null;
            connectCallback = null;
            readCallback = null;
            readBuffer = null;
            readBytesStatus = null;
            disconnectCallback = null;
            channelContext = null;
            wasOnceConnected = false;
            ioThreadId = -1;
            timeout = lingerTimeout;
            el = eventLoop;
            this.eventLoop = null; // null event loop means that given connection is being stopped
            clientChannelAdapter.reset();
            if (dumpMode) {
                closeDumps();
            }
        }
        if (!stopEventLoop(el, timeout)) {
            logger.error("Failed to linger - event loop");
            return -1;
        }
        return 0;
    }

    /**
     * Verify if connection is in connected state.
     *
     * <p>Thread model: executed in any thread.
     */
    @Override
    public boolean isConnected() {
        synchronized (lock) {
            return state == ChannelState.CONNECTED;
        }
    }

    /**
     * Set non-null channel status handler for given connection.
     *
     * <p>Thread model: executed in any thread.
     *
     * @param handler non-null {@link ChannelStatusHandler} instance.
     */
    @Override
    public void setChannelStatusHandler(ChannelStatusHandler handler) {
        Argument.expectNonNull(handler, "handler");
        synchronized (lock) {
            channelStatusHandler = handler;
        }
    }

    /**
     * Write data to channel.
     *
     * <p>Thread model: executed in any thread.
     *
     * @param data data to be written in channel.
     */
    @Override
    public WriteStatus write(ByteBuffer[] data) {
        synchronized (lock) {
            if (state != ChannelState.CONNECTED) {
                logger.error("Attempt to write in a closed channel");
                return WriteStatus.CLOSED;
            }

            if (channelContext == null) {
                throw new IllegalStateException("Channel context is not set");
            }

            if (!channelContext.channel().isWritable()) {
                // Channel is not writable.  Cannot invoke 'waitUntilWritable'
                // here for 2 reasons:
                // 1) Client could invoke 'write' from the IO thread, and
                //    invoking 'waitUntilWritable' from the IO thread leads to
                //    deadlock.
                // 2) Client may not like the fact that we blocked the thread
                //    which invoked 'write'.

                logger.error("Buffer is full");
                return WriteStatus.WRITE_BUFFER_FULL;
            }
            clientChannelAdapter.write(this.channelContext, data);
            return WriteStatus.SUCCESS;
        }
    }

    /**
     * Verify if associated channel is writable.
     *
     * <p>TODO: this can be removed if needed.
     *
     * <p>Thread model: executed in any thread.
     */
    public boolean isWritable() {

        synchronized (lock) {
            if (state != ChannelState.CONNECTED) {
                return false;
            }
            if (channelContext == null) {
                throw new IllegalStateException("Channel context is not set");
            }

            return channelContext.channel().isWritable();
        }
    }

    /**
     * Wait until channel becomes writable.
     *
     * <p>Thread model: executed in any thread except I/O thread.
     *
     * @throws IllegalStateException when executed in I/O thread.
     */
    @SuppressWarnings("squid:S2142")
    public void waitUntilWritable() {

        Semaphore sema = null;

        synchronized (lock) {
            if (Thread.currentThread().getId() == ioThreadId) {
                throw new IllegalStateException(
                        "Cannot invoke 'waitUntilWritable' from the IO thread.");
            }

            if (state == ChannelState.CONNECTED && !channelContext.channel().isWritable()) {
                sema = channelWaterMarkSema;
            } // else: channel is not connected, or is writable.
        }

        // Wait indefinitely on the semaphore outside the lock.
        if (sema != null) {
            try {
                sema.acquire();
            } catch (InterruptedException e) {
                logger.info("InterruptedException: ", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Return local address of associated channel.
     *
     * <p>Thread model: executed in any thread.
     */
    public InetSocketAddress localAddress() {
        synchronized (lock) {
            if (channelContext != null) {
                Object localAddress = channelContext.channel().localAddress();
                if (localAddress instanceof InetSocketAddress)
                    return (InetSocketAddress) localAddress;
            }
            return null;
        }
    }

    /**
     * Return remote address of associated channel.
     *
     * <p>Thread model: executed in any thread.
     *
     * @return Remote address.
     */
    public InetSocketAddress remoteAddress() {
        synchronized (lock) {
            if (channelContext != null) {
                Object remoteAddress = channelContext.channel().remoteAddress();
                if (remoteAddress instanceof InetSocketAddress)
                    return (InetSocketAddress) remoteAddress;
            }
            return null;
        }
    }

    /**
     * Invoked when the current Channel of the ChannelHandlerContext has become active.
     *
     * <p>Save non-null ChannelHandlerContext to the corresponding instance field.
     *
     * <p>Thread model: executed in netty-io thread.
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        Argument.expectNonNull(ctx, "ctx");
        logger.debug("channelActive");

        ChannelStatusHandler channelHandler;
        synchronized (lock) {
            if (eventLoop == null) {
                logger.error("Stop is in progress, let the channel go down");
                return;
            }
            channelContext = ctx;
            channelHandler = channelStatusHandler;
        }
        if (channelHandler != null) {
            channelHandler.handleChannelStatus(ChannelStatus.CHANNEL_UP);
        }
    }

    /**
     * Invoked when the current Channel of the ChannelHandlerContext has read a message from the
     * peer.
     *
     * <p>Save non-null ChannelHandlerContext in the corresponding instance field.
     *
     * <p>Thread model: executed in netty-io thread.
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // TBD: should read callback be invoked while the lock is held?
        Argument.expectNonNull(msg, "msg");

        synchronized (lock) {
            if (state != ChannelState.CONNECTED) {
                throw new IllegalStateException("Expected CONNECTED channel state, got: " + state);
            }

            ByteBuf byteBuf = (ByteBuf) msg;
            try {
                // Make a copy of bytes present in 'byteBuf'.  This is needed
                // so that the lifetime of 'readBuffer' (and ByteBuffer[]
                // obtained from it via 'readBuffer.peek()') can be
                // decoupled with the associated 'byteBuf'.  As per netty docs,
                // a ByteBuf object is refcounted, and user must invoke
                // 'ByteBuf.release' to decrement the counter when done.  Since
                // there is no place in the logic where we can confidentally
                // invoke 'ByteBuf.release' (BlazingMQ user may keep a PUSH message
                // for ever), we make a copy of bytes present in 'ByteBuf' and
                // invoke 'ByteBuf.release' right away.  This copying is
                // unfortunate, and we will investigate ways to avoid this
                // copy later.
                ByteBuffer nioBuf = ByteBuffer.allocate(byteBuf.readableBytes());
                byteBuf.readBytes(nioBuf);
                readBuffer.writeBuffer(nioBuf);
            } catch (IOException e) {
                logger.error("Failed to write data: ", e);
            }
            byteBuf.release();

            // Check if there are enough bytes in 'readBuffer'.
            final int numNeeded = readBytesStatus.numNeeded();
            logger.debug("Read {} bytes, need at least {}", readBuffer.size(), numNeeded);

            if (readBuffer.size() >= numNeeded) {
                // There are enough bytes.  Notify client.
                final ByteBuffer[] byteBuffers = readBuffer.reset();

                readBuffer = new ByteBufferOutputStream();

                if (dumpMode) {
                    dump(byteBuffers);
                }

                readCallback.handleReadCb(readBytesStatus, byteBuffers);
            }
        }
    }

    /**
     * Invoked when the last message read by the current read operation has been consumed by
     * channelRead.
     *
     * <p>Do nothing.
     *
     * <p>Thread model: executed in netty-io thread.
     */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {}

    /**
     * The Channel of the ChannelHandlerContext was registered is now inactive and reached its end
     * of lifetime.
     *
     * <p>Do nothing.
     *
     * <p>Thread model: executed in netty-io thread.
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // Note that if a listener is registered with the channel-close-future,
        // that is triggered *before* 'channelInactive'.
    }

    /**
     * Gets called once the writable state of a Channel changed.
     *
     * <p>If channel has become writable notify threads expecting channel writability.
     *
     * <p>Thread model: executed in netty-io thread.
     */
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        // If channel has become writable, post on the semaphore on which
        // 'write' might be waiting, and also send CHANNEL_WRITABLE status.
        // Else, simply log.

        logger.info(
                "channelWritabilityChanged. isWritable: {}, BytesBeforeUnwritable: {}, BytesBeforeWritable: {}",
                ctx.channel().isWritable(),
                ctx.channel().bytesBeforeUnwritable(),
                ctx.channel().bytesBeforeWritable());

        if (ctx.channel().isWritable()) {
            ChannelStatusHandler channelHandler = null;
            synchronized (lock) {
                channelHandler = channelStatusHandler;
            }
            channelWaterMarkSema.release();
            if (channelHandler != null) {
                channelHandler.handleChannelStatus(ChannelStatus.CHANNEL_WRITABLE);
            }
        }
    }

    /**
     * Gets called if a Throwable was thrown.
     *
     * <p>TODO: Can we get more info about the exception, depending upon which we might be able to
     * decide whether to close the channel or not...?
     *
     * <p>Thread model: executed in netty-io thread.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.info("exceptionCaught: [{}]", cause.getMessage());
    }

    /**
     * Invoked when IO channel operation is completed.
     *
     * <p>Thread model: executed in netty-io thread.
     *
     * @param future completed {@link ChannelFuture}.
     */
    @Override
    public void operationComplete(ChannelFuture future) {
        // We install listeners on channel futures of 2 types: connect future,
        // and close future.
        if (!future.isDone()) {
            throw new IllegalStateException("Expected 'future' state: is done");
        }

        if (future.channel() != null && future.channel().closeFuture() == future) {
            // This is the result of channel-close-future.
            channelCloseFutureComplete();
        } else {
            // This is the result of connect-future.
            connectFutureComplete(future);
        }
    }

    private void doConnect() {

        // Executed in any thread.

        // This routine can be invoked by 'connect' as well as by retry attempt
        // scheduled event.

        ConnectCallback connectCb = null;
        DisconnectCallback disconnectCb = null;

        synchronized (lock) {
            if (state == ChannelState.DISCONNECTING) {
                logger.warn("Try to connect while disconnecting");
                // While this connect attempt was scheduled, 'disconnect' was
                // invoked.  Since channel is not up, we don't need to do
                // anything other than invoking the disconnect callback.
                state = ChannelState.DISCONNECTED;
                connectCb = connectCallback;
                disconnectCb = disconnectCallback;
            } else {
                clientChannelAdapter.connect().addListener(this);
            }
        }

        // No need to fire 'channel down' event because we are not connected at
        // this time.

        if (connectCb != null) {
            // 'disconnect' was invoked while connection was in progress.  Fire
            // connection callback with 'CANCELLED' status code.
            connectCb.handleConnectCb(ConnectStatus.CANCELLED);
        }

        if (disconnectCb != null) {
            disconnectCb.handleDisconnectCb(DisconnectStatus.SUCCESS);
        }
    }

    private void channelCloseFutureComplete() {

        // Executed in netty-io thread.

        ChannelStatusHandler csHandler = null;
        DisconnectCallback disconnectCb = null;

        synchronized (lock) {
            csHandler = channelStatusHandler;

            logger.debug("channelCloseFutureComplete {}", state);

            if (state == ChannelState.DISCONNECTING) {
                // Disconnection complete.

                // TBD: Can this future fail?  Should we check
                // 'future.isSuccess' or 'future.isCancelled'?  What to do in
                // case of failure or cancellation?  For now, assuming that its
                // a success.

                disconnectCb = disconnectCallback;
                wasOnceConnected = false;
                state = ChannelState.DISCONNECTED;
            } else {
                scheduleConnect();
            }
        }

        if (csHandler != null) {
            csHandler.handleChannelStatus(ChannelStatus.CHANNEL_DOWN);
        }

        if (disconnectCb != null) {
            disconnectCb.handleDisconnectCb(DisconnectStatus.SUCCESS);
        }
    }

    private void connectFutureComplete(ChannelFuture future) {
        // Executed in netty-io thread.

        ConnectCallback connectCb = null;
        ConnectStatus connectRc = ConnectStatus.FAILURE;
        boolean scheduleConnect = false;

        synchronized (lock) {

            // If 'disconnect' was invoked while connection was in progress,
            // we need to abort the connection.

            if (state == ChannelState.DISCONNECTING) {
                if (future.isSuccess()) {
                    future.channel().closeFuture().addListener(this);
                    future.channel().close();
                    // We will post on the disconnecting-semaphore in the
                    // channel-close future.
                }
                // else: future is cancelled or failure, in which case, there
                // is nothing to do.

                return;
            }

            if (future.isCancelled()) {
                // Nothing to do.. (re)connection attempt cancelled.

                logger.debug("connect() attempt cancelled.");

                scheduleConnect = true;
            } else if (future.isSuccess()) {
                state = ChannelState.CONNECTED;
                ioThreadId = Thread.currentThread().getId(); // ok to rewrite

                ChannelFuture closeFuture = future.channel().closeFuture();

                // Install self as listener for close-future event.
                closeFuture.addListener(this);

                if (!wasOnceConnected) {
                    wasOnceConnected = true;
                    connectCb = connectCallback;
                    connectRc = ConnectStatus.SUCCESS;
                }
            } else {
                // Failed to connect.

                if (!wasOnceConnected) {

                    if (0 == numRemainingInitialTries) {
                        logger.info("Exhausted all connection attempts.");

                        connectCb = connectCallback;
                        connectRc = ConnectStatus.FAILURE;
                        state = ChannelState.DISCONNECTED;
                    } else {
                        --numRemainingInitialTries;
                        scheduleConnect = true;
                    }
                } else {
                    // A reconnection attempt failed.  Schedule a new attempt.
                    scheduleConnect = true;
                }
            }
        }

        // Outside the lock, invoke connectCb if needed.

        if (connectCb != null) {
            connectCb.handleConnectCb(connectRc);
        }

        if (scheduleConnect) {
            scheduleConnect();
        }
    }

    private void scheduleConnect() {
        synchronized (lock) {
            if (eventLoop == null) return; // connection is being stopped.
            state = ChannelState.CONNECTING;
            eventLoop.schedule(
                    this::doConnect,
                    options.startRetryInterval().toMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }
}
