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
package com.bloomberg.bmq.impl.infr.stat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.bloomberg.bmq.Uri;
import com.bloomberg.bmq.impl.QueueId;
import com.bloomberg.bmq.impl.infr.msg.MessagesTestSamples;
import com.bloomberg.bmq.util.TestHelpers;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueuesStatsTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /** Helper class to simulate broker session events using different executors */
    private static class Simulator {
        final ExecutorService sessionScheduler = Executors.newSingleThreadExecutor();
        final ExecutorService userThreadPool = Executors.newFixedThreadPool(4);

        final QueuesStats stats;

        public Simulator(QueuesStats stats) {
            this.stats = stats;
        }

        public void openQueue(QueueId id, Uri uri) {
            stats.onQueueOpen(id, uri);
        }

        public void closeQueue(QueueId id) {
            stats.onQueueClose(id);
        }

        public CompletableFuture<Void> put(
                QueueId queueId, int appDataSize, int msgCount, double ratio) {
            return CompletableFuture.runAsync(
                    () -> {
                        sleep(100);
                        logger.info("PUT from thread {}", Thread.currentThread().getId());

                        for (int i = 0; i < msgCount; i++) {
                            stats.onPutMessage(queueId, appDataSize, ratio);
                        }
                    },
                    userThreadPool);
        }

        public CompletableFuture<Void> push(QueueId queueId, int appDataSize, int msgCount) {
            return CompletableFuture.runAsync(
                    () -> {
                        sleep(100);
                        logger.info("PUSH from thread {}", Thread.currentThread().getId());

                        for (int i = 0; i < msgCount; i++) {
                            stats.onPushMessage(queueId, appDataSize);
                        }
                    },
                    sessionScheduler);
        }

        private void sleep(int ms) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                logger.error("Interrupted: ", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    public void testOnQueueOpen() {
        QueuesStats stats = new QueuesStats();
        Uri uri = new Uri("bmq://test/queue");

        // Invalid arguments
        try {
            stats.onQueueOpen(null, uri);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            logger.info("Caught expected exception: ", e);
        }

        try {
            stats.onQueueOpen(QueueId.createInstance(0, 0), null);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            logger.info("Caught expected exception: ", e);
        }

        // Proper arguments
        QueueId id = QueueId.createInstance(1, 0);

        stats.onQueueOpen(id, uri);

        // Try to open again
        try {
            stats.onQueueOpen(id, uri);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("Queue with the same id has been already opened", e.getMessage());
            logger.info("Caught expected exception: ", e);
        }

        // Close and reopen the queue
        stats.onQueueClose(id);
        stats.onQueueOpen(id, uri);
    }

    @Test
    public void testOnQueueClose() {
        QueuesStats stats = new QueuesStats();

        // Open queues
        QueueId id1 = QueueId.createInstance(1, 0);
        QueueId id2 = QueueId.createInstance(1, 1);
        Uri uri = new Uri("bmq://test/queue");

        stats.onQueueOpen(id1, uri);
        stats.onQueueOpen(id2, uri);

        // Try to close with null id
        try {
            stats.onQueueClose(null);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            logger.info("Caught expected exception: ", e);
        }

        // Try to close with non-existing queue id
        try {
            stats.onQueueClose(QueueId.createInstance(0, 0));
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("There is no queue with provided id: [0,0]", e.getMessage());
            logger.info("Caught expected exception: ", e);
        }

        // Close queues
        stats.onQueueClose(id1);
        stats.onQueueClose(id2);
    }

    @Test
    public void testOnPushMessage() {
        QueuesStats stats = new QueuesStats();

        QueueId queueId1 = QueueId.createInstance(1, 0);
        Uri uri1 = new Uri("bmq://test/queue1");

        stats.onQueueOpen(queueId1, uri1);

        QueueId queueId2 = QueueId.createInstance(2, 0);

        // Try to call with null queue
        try {
            stats.onPushMessage(null, 1);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            logger.info("Caught expected error: ", e);
        }

        // Try to call with non-positive size
        try {
            stats.onPushMessage(queueId2, 0);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            logger.info("Caught expected error: ", e);
        }

        // Try to call on queue which has not been opened
        try {
            stats.onPushMessage(queueId2, 1);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("There is no queue with provided id: " + queueId2, e.getMessage());
            logger.info("Caught expected error: ", e);
        }

        stats.onQueueClose(queueId1);
        // Try to call on queue which has been closed
        try {
            stats.onPushMessage(queueId1, 1);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("Queue with provided id is closed", e.getMessage());
            logger.info("Caught expected error: ", e);
        }

        // Open second queue
        stats.onQueueOpen(queueId2, new Uri("bmq://test/queue2"));

        // Log push event
        stats.onPushMessage(queueId2, 1);

        // Reopen first queue
        stats.onQueueOpen(queueId1, uri1);

        // Log push event
        stats.onPushMessage(queueId1, 1);
    }

    @Test
    public void testOnPutEvent() {
        QueuesStats stats = new QueuesStats();

        QueueId queueId1 = QueueId.createInstance(1, 0);
        Uri uri1 = new Uri("bmq://test/queue1");

        stats.onQueueOpen(queueId1, uri1);

        QueueId queueId2 = QueueId.createInstance(2, 0);

        // Try to call with null queue
        try {
            stats.onPutMessage(null, 1, 1);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            logger.info("Caught expected error: ", e);
        }

        // Try to call with non-positive size
        try {
            stats.onPutMessage(queueId2, 0, 1);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            logger.info("Caught expected error: ", e);
        }

        // Try to call with non-positive ratio
        try {
            stats.onPutMessage(queueId2, 1, 0);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            logger.info("Caught expected error: ", e);
        }

        // Try to call on queue which has not been opened
        try {
            stats.onPutMessage(queueId2, 1, 1);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("There is no queue with provided id: " + queueId2, e.getMessage());
            logger.info("Caught expected error: ", e);
        }

        stats.onQueueClose(queueId1);
        // Try to call on queue which has been closed
        try {
            stats.onPutMessage(queueId1, 1, 1);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("Queue with provided id is closed", e.getMessage());
            logger.info("Caught expected error: ", e);
        }

        // Open second queue
        stats.onQueueOpen(queueId2, new Uri("bmq://test/queue2"));

        // Log put event
        stats.onPutMessage(queueId2, 1, 1);

        // Reopen first queue
        stats.onQueueOpen(queueId1, uri1);

        // Log put event
        stats.onPutMessage(queueId1, 1, 1);
    }

    @Test
    public void testEmptyOutput() throws IOException {
        QueuesStats stats = new QueuesStats();

        StringBuilder builder = new StringBuilder();
        stats.dump(builder, false);

        String expected = emptyOutput();
        String actual = builder.toString();
        logger.info("Expected:\n{}", expected);
        logger.info("Actual:\n{}", actual);

        assertEquals(expected, actual);

        // Check final output
        builder = new StringBuilder();
        stats.dump(builder, true);

        expected = emptyFinalOutput();
        actual = builder.toString();
        logger.info("Expected:\n{}", expected);
        logger.info("Actual:\n{}", actual);

        assertEquals(expected, actual);
    }

    @Test
    public void testSampleOutput() throws Exception {
        QueuesStats stats = new QueuesStats();
        Simulator simulator = new Simulator(stats);

        QueueId queueId1 = QueueId.createInstance(1, 0);
        QueueId queueId2 = QueueId.createInstance(2, 0);

        simulator.openQueue(queueId1, new Uri("bmq://test/queue1"));
        simulator.openQueue(queueId2, new Uri("bmq://test/queue2?id=foo"));

        // First group of activities
        CompletableFuture.allOf(
                        simulator.put(queueId1, 200, 2, 1),
                        simulator.put(queueId1, 804, 3, 3.908),
                        simulator.put(queueId1, 1024 * 1024, 1248, 1),
                        simulator.put(queueId1, 124 * 1024, 12483, 1),
                        simulator.put(queueId1, 124, 1248323, 7.985),
                        simulator.put(queueId1, 124, 1248376, 1),
                        simulator.put(queueId1, 1024, 248372, 1.801),
                        simulator.put(queueId1, 124 * 1024, 12483, 1),
                        simulator.put(queueId1, 124, 1248323, 1),
                        simulator.put(queueId1, 124, 1248376, 4.823),
                        simulator.put(queueId1, 1024, 248372, 1),
                        simulator.push(queueId1, 4023, 10),
                        simulator.put(queueId2, 142, 1232345, 1.789),
                        simulator.push(queueId2, 1042 * 10, 12323))
                .get();

        StringBuilder builder = new StringBuilder();
        stats.dump(builder, false);

        // Second group of activities
        simulator.closeQueue(queueId1);

        QueueId queueId3 = QueueId.createInstance(3, 0);
        simulator.openQueue(queueId3, new Uri("bmq://test/queue1"));

        CompletableFuture.allOf(
                        simulator.put(queueId3, 1023, 1, 1.556), simulator.push(queueId3, 345, 654))
                .get();

        builder = new StringBuilder();
        stats.dump(builder, false);

        String expected = sampleOutput();
        String actual = builder.toString();
        logger.info("Expected:\n{}", expected);
        logger.info("Actual:\n{}", actual);

        assertEquals(expected, actual);

        // Check final output
        builder = new StringBuilder();
        stats.dump(builder, true);

        String finalExpected = sampleFinalOutput();
        String finalActual = builder.toString();

        logger.info("Final expected:\n{}", finalExpected);
        logger.info("Final actual:\n{}", finalActual);

        assertEquals(finalExpected, finalActual);

        // Check output is empty
        builder = new StringBuilder();
        stats.dump(builder, false);

        String emptyExpected = emptyOutput();
        String emptyActual = builder.toString();

        logger.info("Empty expected:\n{}", emptyExpected);
        logger.info("Empty actual:\n{}", emptyActual);

        assertEquals(emptyExpected, emptyActual);
    }

    private String emptyOutput() throws IOException {
        return readFileContent(MessagesTestSamples.STATS_QUEUES_EMPTY.filePath());
    }

    private String emptyFinalOutput() throws IOException {
        return readFileContent(MessagesTestSamples.STATS_QUEUES_EMPTY_FINAL.filePath());
    }

    private String sampleOutput() throws IOException {
        return readFileContent(MessagesTestSamples.STATS_QUEUES_SAMPLE.filePath());
    }

    private String sampleFinalOutput() throws IOException {
        return readFileContent(MessagesTestSamples.STATS_QUEUES_SAMPLE_FINAL.filePath());
    }

    private String readFileContent(String fileName) throws IOException {
        ByteBuffer bb = TestHelpers.readFile(fileName);
        return StandardCharsets.UTF_8.decode(bb).toString();
    }
}
