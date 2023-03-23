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
package com.bloomberg.bmq.benchmark;

import com.bloomberg.bmq.AbstractSession;
import com.bloomberg.bmq.AckMessage;
import com.bloomberg.bmq.AckMessageHandler;
import com.bloomberg.bmq.BMQException;
import com.bloomberg.bmq.CompressionAlgorithm;
import com.bloomberg.bmq.MessageProperties;
import com.bloomberg.bmq.PushMessage;
import com.bloomberg.bmq.PushMessageHandler;
import com.bloomberg.bmq.PutMessage;
import com.bloomberg.bmq.Queue;
import com.bloomberg.bmq.QueueControlEvent;
import com.bloomberg.bmq.QueueEventHandler;
import com.bloomberg.bmq.QueueFlags;
import com.bloomberg.bmq.QueueOptions;
import com.bloomberg.bmq.Session;
import com.bloomberg.bmq.SessionEvent;
import com.bloomberg.bmq.SessionEventHandler;
import com.bloomberg.bmq.Uri;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.it.util.BmqBroker;
import com.bloomberg.bmq.it.util.TestTools;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Date;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Session memory benchmarks
 *
 * <p>Since JMH can pass options to forked JVM processes, we may use it to run benchmarks with
 * limited amount of heap memory.
 *
 * <p>Usually the goal of benchmarks is to measure performance, but in this situation the benchmark
 * score is not of great interest.<br>
 * If the benchmark test completes and there are no unexpected errors in log, we may assume that SDK
 * works fine in the test scenario with provided amount of heap memory
 *
 * <p>There are two types of benchmark methods here:
 *
 * <ul>
 *   <li>single message methods<br>
 *       They send one PUT message and receive one PUSH message at a time
 *   <li>batch messages methods<br>
 *       They send batch of PUT messages and receive batch of PUSH messages at a time
 * </ul>
 *
 * <p>Each benchmark has its own specific heap memory limit.<br>
 * When benchmark is running, benchmark test method is called multiple times.<br>
 * When benchmark completes, the benchmark score is produced. Here the score is an approximate
 * number of times the test method can be called per second (ops/sec)
 *
 * <p>Our goal is to ensure that test methods work properly when JVM heap memory is limited.<br>
 * Each benchmark in `SessionBenchmarkIT` is configured with specific approximate minimum amount of
 * memory required to execute corresponding test method.
 *
 * <p>For instance, let's look at {@code sendReceive512KiB} benchmark.
 *
 * <p>The test method sends PUT message with some payload of size 512 KiB without compression, waits
 * for PUSH message and sends CONFIRM.<br>
 * In the benchmark `@Fork` annotation a JVM option is used to limit heap memory to 16 MiB.<br>
 * If benchmark runs properly and there are no errors in log, it means the test method works fine
 * with provided heap memory limit.
 *
 * <p>Default annotations for benchmarks:
 *
 * <ul>
 *   <li>measure the number of operations per second
 *   <li>use one warmup fork and one measurement fork
 *   <li>for each fork:
 *       <ul>
 *         <li>use max-heap JVM parameter (see benchmarks 'Fork' annotations below)
 *         <li>do one 1m warmup benchmark iteration
 *         <li>do 30 measurement iterations 1m each
 *         <li>use 2m timeout for each iteration
 *       </ul>
 * </ul>
 */
// Use "Scope.Thread" here in order to use one state object
// per thread (here, one thread per fork)
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.MINUTES)
@Measurement(iterations = 30, time = 1, timeUnit = TimeUnit.MINUTES)
@Timeout(time = 2, timeUnit = TimeUnit.MINUTES)
public class SessionBenchmark {

    private static class TestSession
            implements SessionEventHandler,
                    QueueEventHandler,
                    AckMessageHandler,
                    PushMessageHandler {

        private static final Logger logger =
                LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

        private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(20);

        private final Uri uri = createUniqueUri();

        private final Consumer<PushMessage> pushConsumer;
        private final Consumer<AckMessage> ackConsumer;

        private long counter = 0;

        private BmqBroker broker;
        private AbstractSession session;
        private Queue queue;

        public TestSession(Consumer<PushMessage> pushConsumer, Consumer<AckMessage> ackConsumer) {
            this.pushConsumer = Argument.expectNonNull(pushConsumer, "pushConsumer");
            this.ackConsumer = Argument.expectNonNull(ackConsumer, "ackConsumer");
        }

        @Override
        public void handleSessionEvent(SessionEvent event) {
            // Uncomment to see additional information in log
            // logger.info("handleSessionEvent: {}", event);
        }

        @Override
        public void handleQueueEvent(QueueControlEvent event) {
            // Uncomment to see additional information in log
            // logger.info("handleQueueEvent: {}", event);
        }

        @Override
        public void handleAckMessage(AckMessage msg) {
            // Uncomment to see additional information in log
            // logger.info("*** Got ack message from the Broker, that it got our message with ***");
            // logger.info("*** GUID: {}", msg.messageGUID());
            // logger.info("*** Queue URI: {}", msg.queue().uri());
            // logger.info("*** Ack status: {}", msg.status());

            ackConsumer.accept(msg);
        }

        @Override
        public void handlePushMessage(PushMessage msg) {
            // Uncomment to see additional information in log
            // logger.info("handlePushMessage: {}", msg);

            // logger.info("*** Got new message ***");
            // logger.info("GUID: {}", msg.messageGUID());
            // logger.info("Queue URI: {}", msg.queue().uri());

            pushConsumer.accept(msg);
        }

        public void startBroker() throws IOException {
            logger.info("Starting the broker");

            if (broker != null) {
                throw new IllegalStateException("Broker already started");
            }

            broker = BmqBroker.createStartedBroker();
            logger.info("Broker successfully started");
        }

        public void stopBroker() throws IOException {
            logger.info("Stopping the broker");

            if (broker == null) {
                throw new IllegalStateException("Broker already stopped");
            }

            broker.setDropTmpFolder();
            broker.close();
            broker = null;

            logger.info("Broker successfully stopped");
        }

        public void startSession() {
            logger.info("Starting the session");

            if (session != null) {
                throw new IllegalStateException("Session already started");
            }

            session = new Session(broker.sessionOptions(), this);
            session.start(DEFAULT_TIMEOUT);

            logger.info("Session successfully started");
        }

        public void stopSession() {
            logger.info("Stopping the session");

            if (session == null) {
                throw new IllegalStateException("Session already stopped");
            }

            try {
                session.stop(DEFAULT_TIMEOUT);
                logger.info("Session successfully stopped");
            } catch (BMQException e) {
                logger.warn("BMQException when stopping the session:", e);
            }

            logger.info("Lingering the session");
            session.linger();
            logger.info("Session linger complete");
        }

        public void openQueue(long flags) {
            logger.info("Opening the queue [{}]", uri);

            if (queue != null) {
                throw new IllegalStateException("The queue already opened");
            }

            queue = session.getQueue(uri, flags, this, this, this);

            QueueOptions queueOptions =
                    QueueOptions.builder()
                            .setMaxUnconfirmedMessages(1000) // 1,000 messages
                            .setMaxUnconfirmedBytes(64 * 1024 * 1024) // 64 MiB
                            .build();

            queue.open(queueOptions, DEFAULT_TIMEOUT);
            logger.info("Successfully opened the queue [{}]", uri);
        }

        public void openReaderWriterAckQueue() {
            long flags = 0;
            flags = QueueFlags.setWriter(flags);
            flags = QueueFlags.setReader(flags);
            flags = QueueFlags.setAck(flags);

            openQueue(flags);
        }

        public void closeQueue() {
            logger.info("Closing the queue [{}]", uri);

            if (queue == null || !queue.isOpen()) {
                throw new IllegalStateException("The queue already closed");
            }

            try {
                queue.close(DEFAULT_TIMEOUT);
                logger.info("Successfully closed the queue [{}]", uri);
            } catch (BMQException e) {
                logger.warn("BMQException when closing the queue [{}]:", uri, e);
            }

            queue = null;
        }

        public void post(int size, CompressionAlgorithm compression) {
            counter++;

            ByteBuffer data = ByteBuffer.allocate(size);
            data.putLong(size / 2, counter);

            PutMessage msg = queue.createPutMessage(data);
            msg.setCorrelationId();

            msg.setCompressionAlgorithm(compression);

            MessageProperties mp = msg.messageProperties();
            mp.setPropertyAsString("routingId", "42");
            mp.setPropertyAsInt64("timestamp", new Date().getTime());

            queue.post(msg);
        }

        private static Uri createUniqueUri() {
            return BmqBroker.Domains.Priority.generateQueueUri();
        }
    }

    private static final Logger logger =
            LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int SIZE_512_B = 512;
    private static final int SIZE_512_KIB = 512 * 1024;
    private static final int SIZE_5_MIB = 5 * 1024 * 1024;
    private static final int SIZE_60_MIB = 60 * 1024 * 1024;

    private static final int PUSH_PROCESS_TIMEOUT = 100; // ms

    private final Semaphore batchSema = new Semaphore(0);
    private final TestSession session;

    private Blackhole blackhole;
    private ArrayDeque<PushMessage> pushMessages;

    public SessionBenchmark() {
        session = new TestSession(this::onPushMessage, this::onAckMessage);
    }

    // "Setup" the state object before each benchmark
    @Setup(Level.Trial)
    public void setupTrial(final Blackhole bh) throws IOException {
        session.startBroker();
        session.startSession();
        session.openReaderWriterAckQueue();

        blackhole = bh;
    }

    // "TearDown" the state object after each benchmark
    @TearDown(Level.Trial)
    public void stop() throws IOException {
        session.closeQueue();
        session.stopSession();
        session.stopBroker();
    }

    // Single message benchmarks

    @Benchmark
    @Fork(jvmArgsAppend = "-Xmx16m")
    public void sendReceive512B() {
        sendReceive(SIZE_512_B, CompressionAlgorithm.None);
    }

    @Benchmark
    @Fork(jvmArgsAppend = "-Xmx16m")
    public void sendReceive512B_Zlib() {
        sendReceive(SIZE_512_B, CompressionAlgorithm.Zlib);
    }

    @Benchmark
    @Fork(jvmArgsAppend = "-Xmx16m")
    public void sendReceive512KiB() {
        sendReceive(SIZE_512_KIB, CompressionAlgorithm.None);
    }

    @Benchmark
    @Fork(jvmArgsAppend = "-Xmx16m")
    public void sendReceive512KiB_Zlib() {
        sendReceive(SIZE_512_KIB, CompressionAlgorithm.Zlib);
    }

    @Benchmark
    @Fork(jvmArgsAppend = "-Xmx128m")
    public void sendReceive5MiB() {
        sendReceive(SIZE_5_MIB, CompressionAlgorithm.None);
    }

    @Benchmark
    @Fork(jvmArgsAppend = "-Xmx128m")
    public void sendReceive5MiB_Zlib() {
        sendReceive(SIZE_5_MIB, CompressionAlgorithm.Zlib);
    }

    @Benchmark
    @Fork(jvmArgsAppend = "-Xmx512m")
    public void sendReceive60MiB() {
        sendReceive(SIZE_60_MIB, CompressionAlgorithm.None);
    }

    @Benchmark
    @Fork(jvmArgsAppend = "-Xmx512m")
    public void sendReceive60MiB_Zlib() {
        sendReceive(SIZE_60_MIB, CompressionAlgorithm.Zlib);
    }

    // Batch messages benchmarks

    @Benchmark
    // It can work with 16mb of heap memory but since we are sending
    // PUT messages first we should be prepared to buffer them in case
    // of the broker sends ACKs with some delay
    @Fork(jvmArgsAppend = "-Xmx128m")
    public void sendReceiveBatch100ConfirmNow() {
        sendReceiveBatch(100, SIZE_512_KIB, CompressionAlgorithm.None, false);
    }

    // It can work with 16mb of heap memory but since we are sending
    // PUT messages first we should be prepared to buffer them in case
    // of the broker sends ACKs with some delay
    @Benchmark
    @Fork(jvmArgsAppend = "-Xmx128m")
    public void sendReceiveBatch100ZlibConfirmNow() {
        sendReceiveBatch(100, SIZE_512_KIB, CompressionAlgorithm.Zlib, false);
    }

    // It can work with 16mb of heap memory but since we are sending
    // PUT messages first we should be prepared to buffer them in case
    // of the broker sends ACKs with some delay
    @Benchmark
    @Fork(jvmArgsAppend = "-Xmx768m")
    public void sendReceiveBatch1000ZlibConfirmNow() {
        sendReceiveBatch(1000, SIZE_512_KIB, CompressionAlgorithm.Zlib, false);
    }

    @Benchmark
    @Fork(jvmArgsAppend = "-Xmx128m")
    public void sendReceiveBatch100ConfirmLater() {
        sendReceiveBatch(100, SIZE_512_KIB, CompressionAlgorithm.None, true);
    }

    @Benchmark
    @Fork(jvmArgsAppend = "-Xmx600m")
    public void sendReceiveBatch800ZlibConfirmLater() {
        sendReceiveBatch(800, SIZE_512_KIB, CompressionAlgorithm.Zlib, true);
    }

    @Benchmark
    @Fork(jvmArgsAppend = "-Xmx768m")
    public void sendReceiveBatch1000ZlibConfirmLater() {
        sendReceiveBatch(1000, SIZE_512_KIB, CompressionAlgorithm.Zlib, true);
    }

    private void sendReceive(int rawMsgSize, CompressionAlgorithm compression) {
        session.post(rawMsgSize, compression);

        // Wait for PUSH message
        TestTools.acquireSema(batchSema, PUSH_PROCESS_TIMEOUT);
    }

    private void sendReceiveBatch(
            int batchSize, int rawMsgSize, CompressionAlgorithm compression, boolean confirmLater) {

        if (confirmLater) {
            // Create queue of messages which will be confirmed
            // after all PUSH messages have been received
            pushMessages = new ArrayDeque<>(batchSize);
        }

        // Post PUT messages
        for (int i = 0; i < batchSize; i++) {
            session.post(rawMsgSize, compression);
            TestTools.sleepForMilliSeconds(10);
        }

        logger.info("Sent {} PUT messages", batchSize);

        // Wait for PUSH messages
        int semaphoreTimeout = batchSize * PUSH_PROCESS_TIMEOUT;
        TestTools.acquireSema(batchSema, batchSize, semaphoreTimeout);

        if (confirmLater) {
            logger.info("Confirm received PUSH messages: {}", pushMessages.size());

            // This should reproduce a corner case when all incoming
            // are in the memory. The next step is to confirm them
            while (!pushMessages.isEmpty()) {
                PushMessage msg = pushMessages.remove();
                confirm(msg);
            }
        }
    }

    private void confirm(PushMessage msg) {
        msg.confirm();
        // Uncomment to see additional information in log
        // logger.info("Confirm status: {}", confirmResult);

        blackhole.consume(msg.payload());
    }

    private void onPushMessage(PushMessage msg) {
        if (pushMessages != null) {
            // Put incoming message into the buffer
            // which will be processed later
            pushMessages.add(msg);
        } else {
            // Otherwise, confirm the message right here
            confirm(msg);
        }

        // Notify 'post' method
        batchSema.release();
    }

    private void onAckMessage(AckMessage msg) {
        // Uncomment to see additional information in log
        // logger.info("ACK message with {} status", msg.status());
    }
}
