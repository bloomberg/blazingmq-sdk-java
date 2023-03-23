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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.bloomberg.bmq.Uri;
import com.bloomberg.bmq.impl.QueueId;
import com.bloomberg.bmq.impl.infr.proto.EventType;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatsTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testConstructor() {
        Consumer<String> emptyConsumer = s -> {};

        try {
            new Stats(null, 0, emptyConsumer);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'scheduler' must be non-null", e.getMessage());
            logger.info("Caught expected error: ", e);
        }

        try {
            new Stats(Executors.newSingleThreadScheduledExecutor(), -1, emptyConsumer);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'dumpInterval' must be non-negative", e.getMessage());
            logger.info("Caught expected error: ", e);
        }

        try {
            new Stats(Executors.newSingleThreadScheduledExecutor(), 0, null);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'handler' must be non-null", e.getMessage());
            logger.info("Caught expected error: ", e);
        }

        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

        new Stats(scheduler, 0, emptyConsumer);
        verify(scheduler, never())
                .scheduleWithFixedDelay(
                        any(Runnable.class), anyInt(), anyInt(), any(TimeUnit.class));
    }

    // Non-generic interface to avoid compilator warnings
    private interface StringConsumer extends Consumer<String> {}

    @Test
    public void testDump() {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);

        Consumer<String> onDump = mock(StringConsumer.class);
        InOrder inOrder = inOrder(onDump);

        Stats stats = new Stats(scheduler, 10, onDump);
        stats.enableDumping();

        verify(scheduler, times(1))
                .scheduleWithFixedDelay(captor.capture(), eq(10L), eq(10L), eq(TimeUnit.SECONDS));
        Runnable internalDump = captor.getAllValues().get(0);

        QueueId queueId = QueueId.createInstance(1, 0);
        Uri uri = new Uri("bmq://hello/queue1");

        // Accumulate stats
        stats.eventsStats().onEvent(EventType.CONTROL, 70, 0);
        stats.eventsStats().onEvent(EventType.PUT, 1024, 2);
        stats.eventsStats().onEvent(EventType.ACK, 72, 2);

        stats.queuesStats().onQueueOpen(queueId, uri);
        stats.queuesStats().onPutMessage(queueId, 500, 1);
        stats.queuesStats().onPutMessage(queueId, 1400, 1);
        stats.queuesStats().onPutMessage(queueId, 4000, 2.5);

        stats.eventQueueStats().onEnqueue();
        stats.eventQueueStats().onEnqueue();
        stats.eventQueueStats().onDequeue(Duration.ofNanos(35));
        stats.eventQueueStats().onEnqueue();
        stats.eventQueueStats().onEnqueue();
        stats.eventQueueStats().onDequeue(Duration.ofNanos(41));
        stats.eventQueueStats().onEnqueue();
        stats.eventQueueStats().onDequeue(Duration.ofNanos(45));

        // Check scheduled dumping

        // 1. Prepare data
        StringBuilder builder = new StringBuilder();
        builder.append("#### stats [delta = last 10 seconds] ####\n");

        EventsStats eventsStats = new EventsStats();
        eventsStats.onEvent(EventType.CONTROL, 70, 0);
        eventsStats.onEvent(EventType.PUT, 1024, 2);
        eventsStats.onEvent(EventType.ACK, 72, 2);
        eventsStats.dump(builder, false);

        QueuesStats queuesStats = new QueuesStats();
        queuesStats.onQueueOpen(queueId, uri);
        queuesStats.onPutMessage(queueId, 500, 1);
        queuesStats.onPutMessage(queueId, 1400, 1);
        queuesStats.onPutMessage(queueId, 4000, 2.5);

        builder.append("\n");
        queuesStats.dump(builder, false);

        EventQueueStats eventQueueStats = new EventQueueStats();
        eventQueueStats.onEnqueue();
        eventQueueStats.onEnqueue();
        eventQueueStats.onDequeue(Duration.ofNanos(35));
        eventQueueStats.onEnqueue();
        eventQueueStats.onEnqueue();
        eventQueueStats.onDequeue(Duration.ofNanos(41));
        eventQueueStats.onEnqueue();
        eventQueueStats.onDequeue(Duration.ofNanos(45));

        builder.append("\n");
        eventQueueStats.dump(builder, false);

        // 2. Dump
        internalDump.run();

        // 3. Verify
        inOrder.verify(onDump, times(1)).accept(builder.toString());

        logger.info("Regular stats: {}", builder);

        // Disable dumping
        stats.disableDumping();

        // Verify there is no dumping
        internalDump.run();
        inOrder.verify(onDump, never()).accept(any(String.class));

        // Dump final stats
        stats.dumpFinal();
        builder = new StringBuilder();
        builder.append("#### final stats ####\n");
        eventsStats.dump(builder, true);
        builder.append("\n");
        queuesStats.dump(builder, true);
        builder.append("\n");
        eventQueueStats.dump(builder, true);
        inOrder.verify(onDump, times(1)).accept(builder.toString());

        logger.info("Final stats: {}", builder);

        // Enable dumping
        stats.enableDumping();

        // Verify statistics is empty
        internalDump.run();
        builder = new StringBuilder();
        builder.append("#### stats [delta = last 10 seconds] ####\n");
        eventsStats.dump(builder, false);
        builder.append("\n");
        queuesStats.dump(builder, false);
        builder.append("\n");
        eventQueueStats.dump(builder, false);
        inOrder.verify(onDump, times(1)).accept(builder.toString());

        logger.info("Empty stats: {}", builder);
    }
}
