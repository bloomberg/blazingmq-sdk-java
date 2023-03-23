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

import com.bloomberg.bmq.impl.infr.util.Argument;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Stats {

    private final EventsStats eventsStats = new EventsStats();
    private final QueuesStats queuesStats = new QueuesStats();
    private final EventQueueStats eventQueueStats = new EventQueueStats();

    private final long dumpInterval;
    private final Consumer<String> handler;

    private volatile boolean isDumpingEnabled;

    public Stats(ScheduledExecutorService scheduler, long dumpInterval, Consumer<String> handler) {
        Argument.expectNonNull(scheduler, "scheduler");
        this.dumpInterval = Argument.expectNonNegative(dumpInterval, "dumpInterval");
        this.handler = Argument.expectNonNull(handler, "handler");

        if (dumpInterval > 0) {
            scheduler.scheduleWithFixedDelay(
                    this::dumpStats, dumpInterval, dumpInterval, TimeUnit.SECONDS);
        }
    }

    public void enableDumping() {
        isDumpingEnabled = true;
    }

    public void disableDumping() {
        isDumpingEnabled = false;
    }

    public EventsStats eventsStats() {
        return eventsStats;
    }

    public QueuesStats queuesStats() {
        return queuesStats;
    }

    public EventQueueStats eventQueueStats() {
        return eventQueueStats;
    }

    public void dumpFinal() {
        dumpStats(true);
    }

    private void dumpStats() {
        if (!isDumpingEnabled) {
            return;
        }

        dumpStats(false);
    }

    private void dumpStats(boolean isFinal) {
        StringBuilder builder = new StringBuilder();

        if (isFinal) {
            builder.append("#### final stats ####\n");
        } else {
            builder.append(
                    String.format("#### stats [delta = last %d seconds] ####%n", dumpInterval));
        }

        eventsStats.dump(builder, isFinal);
        builder.append("\n");
        queuesStats.dump(builder, isFinal);
        builder.append("\n");
        eventQueueStats.dump(builder, isFinal);

        handler.accept(builder.toString());
    }
}
