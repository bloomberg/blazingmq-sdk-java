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

import com.bloomberg.bmq.impl.infr.msg.MessagesTestSamples;
import com.bloomberg.bmq.util.TestHelpers;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventQueueStatsTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testOnDequeue() {
        EventQueueStats stats = new EventQueueStats();

        // Invalid arguments
        try {
            stats.onDequeue(null);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            logger.info("Caught expected exception: ", e);
        }

        try {
            stats.onDequeue(Duration.ofNanos(-1));
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'queuedTime' must be non-negative", e.getMessage());
            logger.info("Caught expected exception: ", e);
        }

        // Proper duration
        stats.onEnqueue();
        stats.onDequeue(Duration.ofNanos(50));

        // Try dequeue when size is zero
        try {
            stats.onDequeue(Duration.ofNanos(50));
            fail(); // should not get here
        } catch (IllegalStateException e) {
            assertEquals("Queue size is zero", e.getMessage());
            logger.info("Caught expected exception: ", e);
        }

        // Zero duration (is unlikely, but possible)
        stats.onEnqueue();
        stats.onDequeue(Duration.ZERO);
    }

    @Test
    public void testEmptyOutput() throws IOException {
        EventQueueStats stats = new EventQueueStats();

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
        EventQueueStats stats = new EventQueueStats();

        // First group of activities
        stats.onEnqueue();
        stats.onDequeue(Duration.ofNanos(40059));
        stats.onEnqueue();
        stats.onEnqueue();
        stats.onEnqueue();
        stats.onEnqueue();
        stats.onEnqueue();
        stats.onEnqueue();
        stats.onEnqueue();
        stats.onEnqueue();
        stats.onEnqueue();
        stats.onEnqueue();
        stats.onDequeue(Duration.ofNanos(80123));
        stats.onDequeue(Duration.ofNanos(70543));
        stats.onDequeue(Duration.ofNanos(72098));
        stats.onDequeue(Duration.ofNanos(73354));
        stats.onDequeue(Duration.ofNanos(74486));
        stats.onDequeue(Duration.ofNanos(75958));
        stats.onDequeue(Duration.ofNanos(71395));
        stats.onDequeue(Duration.ofNanos(77123));
        stats.onDequeue(Duration.ofNanos(78111));
        stats.onDequeue(Duration.ofNanos(76236));

        StringBuilder builder = new StringBuilder();
        stats.dump(builder, false);

        // Second group of activities
        stats.onEnqueue();
        stats.onDequeue(Duration.ofNanos(35132));
        stats.onEnqueue();
        stats.onDequeue(Duration.ofNanos(43354));
        stats.onEnqueue();
        stats.onDequeue(Duration.ofNanos(39946));
        stats.onEnqueue();
        stats.onEnqueue();
        stats.onEnqueue();
        stats.onEnqueue();
        stats.onDequeue(Duration.ofNanos(50051));

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
        return readFileContent(MessagesTestSamples.STATS_EVENTQUEUE_EMPTY.filePath());
    }

    private String emptyFinalOutput() throws IOException {
        return readFileContent(MessagesTestSamples.STATS_EVENTQUEUE_EMPTY_FINAL.filePath());
    }

    private String sampleOutput() throws IOException {
        return readFileContent(MessagesTestSamples.STATS_EVENTQUEUE_SAMPLE.filePath());
    }

    private String sampleFinalOutput() throws IOException {
        return readFileContent(MessagesTestSamples.STATS_EVENTQUEUE_SAMPLE_FINAL.filePath());
    }

    private String readFileContent(String fileName) throws IOException {
        ByteBuffer bb = TestHelpers.readFile(fileName);
        return StandardCharsets.UTF_8.decode(bb).toString();
    }
}
