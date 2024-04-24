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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bloomberg.bmq.impl.infr.msg.MessagesTestSamples;
import com.bloomberg.bmq.impl.infr.proto.EventType;
import com.bloomberg.bmq.util.TestHelpers;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventsStatsTest {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /** Helper class to simulate broker session events using different executors */
    private static class Simulator {
        final ExecutorService sessionScheduler = Executors.newSingleThreadExecutor();
        final ExecutorService userThreadPool = Executors.newFixedThreadPool(4);

        final EventsStats stats;

        public Simulator(EventsStats stats) {
            this.stats = stats;
        }

        public CompletableFuture<Void> put(int size, int msgCount) {
            return CompletableFuture.runAsync(
                    () -> {
                        sleep(100);
                        logger.info("PUT from thread {}", Thread.currentThread().getId());
                        stats.onEvent(EventType.PUT, size, msgCount);
                    },
                    userThreadPool);
        }

        public CompletableFuture<Void> control(int size) {
            return CompletableFuture.runAsync(
                    () -> {
                        sleep(100);
                        logger.info("CONTROL from thread {}", Thread.currentThread().getId());
                        stats.onEvent(EventType.CONTROL, size, 0);
                    },
                    sessionScheduler);
        }

        public CompletableFuture<Void> ack(int size, int msgCount) {
            return CompletableFuture.runAsync(
                    () -> {
                        sleep(100);
                        logger.info("ACK from thread {}", Thread.currentThread().getId());
                        stats.onEvent(EventType.ACK, size, msgCount);
                    },
                    sessionScheduler);
        }

        public CompletableFuture<Void> push(int size, int msgCount) {
            return CompletableFuture.runAsync(
                    () -> {
                        sleep(100);
                        logger.info("PUSH from thread {}", Thread.currentThread().getId());
                        stats.onEvent(EventType.PUSH, size, msgCount);
                    },
                    sessionScheduler);
        }

        public CompletableFuture<Void> confirm(int size, int msgCount) {
            return CompletableFuture.runAsync(
                    () -> {
                        sleep(100);
                        logger.info("CONFIRM from thread {}", Thread.currentThread().getId());
                        stats.onEvent(EventType.CONFIRM, size, msgCount);
                    },
                    userThreadPool);
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

    // Is used by StatsTest class
    public static void dump(EventsStats eventsStats, StringBuilder builder, boolean isFinal) {
        eventsStats.dump(builder, isFinal);
    }

    @Test
    void testEmptyOutput() throws IOException {
        EventsStats stats = new EventsStats();

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
    void testSampleOutput() throws Exception {
        EventsStats stats = new EventsStats();

        Simulator simulator = new Simulator(stats);

        StringBuilder builder1 = new StringBuilder();

        // First group of activity
        CompletableFuture.allOf(
                        simulator.control(70),
                        simulator.control(70),
                        simulator.control(38),
                        simulator.control(38),
                        simulator.control(70),
                        simulator.control(70),
                        simulator.put(84, 2),
                        simulator.put(84, 2),
                        simulator.put(84, 2),
                        simulator.put(84, 2),
                        simulator.put(84, 2),
                        simulator.put(84, 2),
                        simulator.put(840, 10),
                        simulator.push(652, 14),
                        simulator.confirm(36, 1),
                        simulator.confirm(36, 1),
                        simulator.confirm(36, 1),
                        simulator.confirm(36, 1),
                        simulator.confirm(36, 1),
                        simulator.put(4302425, 100),
                        simulator.ack(72, 2),
                        simulator.ack(108, 3),
                        simulator.ack(3600, 100))
                .get(); // wait for completion

        stats.dump(builder1, false);

        StringBuilder builder2 = new StringBuilder();

        // Second group of activity
        CompletableFuture.allOf(
                        simulator.put(84, 2),
                        simulator.ack(144, 4),
                        simulator.control(70),
                        simulator.control(70),
                        simulator.control(70),
                        simulator.control(70),
                        simulator.control(70),
                        simulator.control(70),
                        simulator.control(70),
                        simulator.control(70),
                        simulator.control(70),
                        simulator.control(70),
                        simulator.put(2000000000, 20000000),
                        simulator.put(1538298000, 40000000),
                        simulator.ack(1440000000, 40000000),
                        simulator.ack(720000000, 20000000))
                .thenRunAsync(() -> stats.dump(builder2, false)) // dump stats
                .get(); // wait for completion

        String expected = sampleOutput();
        String actual = builder2.toString();
        logger.info("Expected:\n{}", expected);
        logger.info("Actual:\n{}", actual);

        assertEquals(expected, actual);

        // Check final output
        StringBuilder builder3 = new StringBuilder();
        stats.dump(builder3, true);

        String finalExpected = sampleFinalOutput();
        String finalActual = builder3.toString();

        logger.info("Final expected:\n{}", finalExpected);
        logger.info("Final actual:\n{}", finalActual);

        assertEquals(finalExpected, finalActual);

        // Check output is empty
        StringBuilder builder4 = new StringBuilder();
        stats.dump(builder4, false);

        String emptyExpected = emptyOutput();
        String emptyActual = builder4.toString();

        logger.info("Empty expected:\n{}", emptyExpected);
        logger.info("Empty actual:\n{}", emptyActual);

        assertEquals(emptyExpected, emptyActual);
    }

    private String emptyOutput() throws IOException {
        return readFileContent(MessagesTestSamples.STATS_EVENTS_EMPTY.filePath());
    }

    private String emptyFinalOutput() throws IOException {
        return readFileContent(MessagesTestSamples.STATS_EVENTS_EMPTY_FINAL.filePath());
    }

    private String sampleOutput() throws IOException {
        return readFileContent(MessagesTestSamples.STATS_EVENTS_SAMPLE.filePath());
    }

    private String sampleFinalOutput() throws IOException {
        return readFileContent(MessagesTestSamples.STATS_EVENTS_SAMPLE_FINAL.filePath());
    }

    private String readFileContent(String fileName) throws IOException {
        ByteBuffer bb = TestHelpers.readFile(fileName);
        return StandardCharsets.UTF_8.decode(bb).toString();
    }
}
