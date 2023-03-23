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

import static com.bloomberg.bmq.impl.infr.stat.StatsUtil.formatCenter;
import static com.bloomberg.bmq.impl.infr.stat.StatsUtil.formatNum;
import static com.bloomberg.bmq.impl.infr.stat.StatsUtil.numWidth;
import static com.bloomberg.bmq.impl.infr.stat.StatsUtil.repeatCharacter;

import com.bloomberg.bmq.impl.infr.stat.StatsUtil.Format;
import com.bloomberg.bmq.impl.infr.util.Argument;
import java.time.Duration;

public class EventQueueStats {

    // Event queue statistics:
    //
    // : o !Queue::EnqueueDelta!: number of events that were enqueued since the last
    // :   print
    // :
    // : o !Queue::DequeueDelta!: number of events that were dequeued since the last
    // :   print
    // :
    // : o !Queue::Size!: current size of the queue (at time of print)
    // :
    // : o !Queue::Max!: maximum size reached by the queue in the interval between
    // :   the previous print and the current print
    // :
    // : o !Queue::Abs.Max!: maximum size ever reached by the queue
    // :
    // : o !QueueTime::Min!: minimum time spent in the queue by an event, in the
    // :   interval between the previous print and the current print
    // :
    // : o !QueueTime::Avg!: average time spent in the queue by an event, in the
    // :   interval between the previous print and the current print
    // :
    // : o !QueueTime::Max!: maximum time spent in the queue by an event, in the
    // :   interval between the previous print and the current print
    // :
    // : o !QueueTime::Abs.Max!: maximum time ever spent in the queue by an event

    private static final int ENQUEUE_DELTA = 0;
    private static final int DEQUEUE_DELTA = 1;
    private static final int SIZE = 2;
    private static final int SIZE_MAX = 3;
    private static final int SIZE_ABS_MAX = 4;
    private static final int TIME_MIN = 5;
    private static final int TIME_AVG = 6;
    private static final int TIME_MAX = 7;
    private static final int TIME_ABS_MAX = 8;

    private long enqueueDelta;
    private long dequeueDelta;
    private long size;
    private long sizeMax;
    private long sizeAbsMax;
    private long timeMin;
    private long timeMax;
    private long timeAbsMax;

    private long timeSum;

    public EventQueueStats() {
        reset(true);
    }

    public synchronized void onEnqueue() {
        enqueueDelta++;
        size++;

        sizeMax = Math.max(sizeMax, size);
        sizeAbsMax = Math.max(sizeMax, sizeAbsMax);
    }

    public synchronized void onDequeue(Duration queuedTime) {
        // No guarantees are made that provided duration is always positive.
        // It's rather unlikely but it could be zero.

        Argument.expectNonNull(queuedTime, "queuedTime");
        Argument.expectCondition(!queuedTime.isNegative(), "'queuedTime' must be non-negative");

        if (size == 0) {
            throw new IllegalStateException("Queue size is zero");
        }

        dequeueDelta++;
        size--;

        long time = queuedTime.toNanos();
        timeSum += time;

        timeMin = timeMin > 0 ? Math.min(timeMin, time) : time;
        timeMax = Math.max(timeMax, time);
        timeAbsMax = Math.max(timeMax, timeAbsMax);
    }

    public synchronized void dump(StringBuilder builder, boolean isFinal) {
        builder.append("::::: Event Queue >>\n");

        // Calculate avg time
        long timeAvg = 0;

        if (dequeueDelta > 0) {
            timeAvg = timeSum / dequeueDelta;
        }

        // Determine columns widths
        int[] widths =
                new int[] {
                    Math.max(numWidth(enqueueDelta, Format.NUM, true), 13),
                    Math.max(numWidth(dequeueDelta, Format.NUM, true), 13),
                    Math.max(numWidth(size, Format.NUM, true), 4),
                    Math.max(numWidth(sizeMax, Format.NUM, true), 3),
                    Math.max(numWidth(sizeAbsMax, Format.NUM, true), 8),
                    Math.max(numWidth(timeMin, Format.TIME_PERIOD, false), 3),
                    Math.max(numWidth(timeAvg, Format.TIME_PERIOD, false), 3),
                    Math.max(numWidth(timeMax, Format.TIME_PERIOD, false), 3),
                    Math.max(numWidth(timeAbsMax, Format.TIME_PERIOD, true), 8)
                };

        if (!isFinal) { // Print with delta

            // Print group headers
            int queueColumnsWidth =
                    widths[ENQUEUE_DELTA]
                            + widths[DEQUEUE_DELTA]
                            + widths[SIZE]
                            + widths[SIZE_MAX]
                            + widths[SIZE_ABS_MAX];
            String queueGroup = formatCenter("Queue", queueColumnsWidth + 8);

            int timeColumnsWidth =
                    widths[TIME_MIN] + widths[TIME_AVG] + widths[TIME_MAX] + widths[TIME_ABS_MAX];
            String timeGroup = formatCenter("Queue Time", timeColumnsWidth + 6);

            builder.append(String.format("%s| %s%n", queueGroup, timeGroup));

            // Print headers
            Object[] headers =
                    new Object[] {
                        formatCenter("Enqueue Delta", widths[ENQUEUE_DELTA]),
                        formatCenter("Dequeue Delta", widths[DEQUEUE_DELTA]),
                        formatCenter("Size", widths[SIZE]),
                        formatCenter("Max", widths[SIZE_MAX]),
                        formatCenter("Abs. Max", widths[SIZE_ABS_MAX]),
                        formatCenter("Min", widths[TIME_MIN]),
                        formatCenter("Avg", widths[TIME_AVG]),
                        formatCenter("Max", widths[TIME_MAX]),
                        formatCenter("Abs. Max", widths[TIME_ABS_MAX])
                    };

            builder.append(String.format("%s| %s| %s| %s| %s| %s| %s| %s| %s%n", headers));

            repeatCharacter(builder, '-', widths[ENQUEUE_DELTA]);
            for (int i = DEQUEUE_DELTA; i < widths.length; i++) {
                builder.append('+');
                repeatCharacter(builder, '-', widths[i] + 1);
            }
            builder.append("\n");

            // Print rows
            Object[] objWidths = new Object[widths.length];
            for (int i = 0; i < widths.length; i++) {
                objWidths[i] = widths[i];
            }

            String rowFormat =
                    String.format(
                            "%%%ds| %%%ds| %%%ds| %%%ds| %%%ds| %%%ds| %%%ds| %%%ds| %%%ds%n",
                            objWidths);

            // Print statistics
            Object[] values =
                    new Object[] {
                        formatNum(enqueueDelta, Format.NUM, true),
                        formatNum(dequeueDelta, Format.NUM, true),
                        formatNum(size, Format.NUM, true),
                        formatNum(sizeMax, Format.NUM, true),
                        formatNum(sizeAbsMax, Format.NUM, true),
                        formatNum(timeMin, Format.TIME_PERIOD),
                        formatNum(timeAvg, Format.TIME_PERIOD),
                        formatNum(timeMax, Format.TIME_PERIOD),
                        formatNum(timeAbsMax, Format.TIME_PERIOD, true)
                    };

            builder.append(String.format(rowFormat, values));

            // Clear delta values after dumping
            reset(false);

        } else {

            // Print group headers
            builder.append(
                    String.format(
                            "%s| %s%n",
                            formatCenter("Queue", widths[SIZE_ABS_MAX]),
                            formatCenter("Queue Time", widths[TIME_ABS_MAX])));

            // Print headers
            Object[] headers =
                    new Object[] {
                        formatCenter("Abs. Max", widths[SIZE_ABS_MAX]),
                        formatCenter("Abs. Max", widths[TIME_ABS_MAX])
                    };

            builder.append(String.format("%s| %s%n", headers));

            repeatCharacter(builder, '-', widths[SIZE_ABS_MAX]);
            builder.append('+');
            repeatCharacter(builder, '-', widths[TIME_ABS_MAX] + 1);
            builder.append("\n");

            // Print rows
            String rowFormat =
                    String.format("%%%ds| %%%ds%n", widths[SIZE_ABS_MAX], widths[TIME_ABS_MAX]);

            // Print statistics
            Object[] values =
                    new Object[] {
                        formatNum(sizeAbsMax, Format.NUM, true),
                        formatNum(timeAbsMax, Format.TIME_PERIOD, true)
                    };

            builder.append(String.format(rowFormat, values));

            // Clear all values after dumping
            reset(true);
        }
    }

    private void reset(boolean resetTotalValues) {
        enqueueDelta = 0;
        dequeueDelta = 0;
        sizeMax = 0;

        timeMin = 0;
        timeSum = 0;
        timeMax = 0;

        if (resetTotalValues) {
            size = 0;
            sizeAbsMax = 0;
            timeAbsMax = 0;
        }
    }
}
