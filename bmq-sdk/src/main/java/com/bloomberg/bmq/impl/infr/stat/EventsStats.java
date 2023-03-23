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

import static com.bloomberg.bmq.impl.infr.stat.StatsUtil.columnWidth;
import static com.bloomberg.bmq.impl.infr.stat.StatsUtil.formatCenter;
import static com.bloomberg.bmq.impl.infr.stat.StatsUtil.formatNum;
import static com.bloomberg.bmq.impl.infr.stat.StatsUtil.repeatCharacter;

import com.bloomberg.bmq.impl.infr.proto.EventType;
import com.bloomberg.bmq.impl.infr.stat.StatsUtil.Format;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class EventsStats {

    private static final int NUM_VALUES = 6;

    private static final int MESSAGES_DELTA = 0;
    private static final int EVENTS_DELTA = 1;
    private static final int BYTES_DELTA = 2;
    private static final int MESSAGES_TOTAL = 3;
    private static final int EVENTS_TOTAL = 4;
    private static final int BYTES_TOTAL = 5;

    private Map<EventType, long[]> stats;

    public EventsStats() {
        init();
    }

    public synchronized void onEvent(EventType type, int eventSize, int msgCount) {
        long[] eventStat = stats.get(type);

        eventStat[MESSAGES_DELTA] += msgCount;
        eventStat[EVENTS_DELTA] += 1;
        eventStat[BYTES_DELTA] += eventSize;
        eventStat[MESSAGES_TOTAL] += msgCount;
        eventStat[EVENTS_TOTAL] += 1;
        eventStat[BYTES_TOTAL] += eventSize;
    }

    synchronized void dump(StringBuilder builder, boolean isFinal) {
        // Determine columns widths
        Collection<long[]> table = stats.values();

        int[] widths =
                new int[] {
                    columnWidth(table, MESSAGES_DELTA, 8, Format.NUM),
                    columnWidth(table, EVENTS_DELTA, 6, Format.NUM),
                    columnWidth(table, BYTES_DELTA, 5, Format.MEMORY),
                    columnWidth(table, MESSAGES_TOTAL, 8, Format.NUM),
                    columnWidth(table, EVENTS_TOTAL, 6, Format.NUM),
                    columnWidth(table, BYTES_TOTAL, 5, Format.MEMORY)
                };

        builder.append("::::: Events >>\n");

        if (!isFinal) { // Print with delta

            // Print group headers
            int deltaColumnsWidth =
                    widths[MESSAGES_DELTA] + widths[EVENTS_DELTA] + widths[BYTES_DELTA];
            String deltaGroup = formatCenter("delta", deltaColumnsWidth + 4);

            int absoluteColumnsWidth =
                    widths[MESSAGES_TOTAL] + widths[EVENTS_TOTAL] + widths[BYTES_TOTAL];
            String absoluteGroup = formatCenter("absolute", absoluteColumnsWidth + 4);

            builder.append(String.format("  Type   | %s| %s%n", deltaGroup, absoluteGroup));

            // Print headers
            Object[] headers =
                    new Object[] {
                        formatCenter("messages", widths[MESSAGES_DELTA]),
                        formatCenter("events", widths[EVENTS_DELTA]),
                        formatCenter("bytes", widths[BYTES_DELTA]),
                        formatCenter("messages", widths[MESSAGES_TOTAL]),
                        formatCenter("events", widths[EVENTS_TOTAL]),
                        formatCenter("bytes", widths[BYTES_TOTAL])
                    };
            builder.append(String.format("         | %s| %s| %s| %s| %s| %s%n", headers));

            builder.append("---------");
            for (int w : widths) {
                builder.append('+');
                repeatCharacter(builder, '-', w + 1);
            }
            builder.append("\n");

            // Print rows
            Object[] cellWidths =
                    new Object[] {
                        columnWidth(table, MESSAGES_DELTA, 8, Format.NUM),
                        columnWidth(table, EVENTS_DELTA, 6, Format.NUM),
                        columnWidth(table, BYTES_DELTA, 5, Format.MEMORY),
                        columnWidth(table, MESSAGES_TOTAL, 8, Format.NUM),
                        columnWidth(table, EVENTS_TOTAL, 6, Format.NUM),
                        columnWidth(table, BYTES_TOTAL, 5, Format.MEMORY)
                    };
            String rowFormat =
                    String.format(
                            "  %%-7s| %%%ds| %%%ds| %%%ds| %%%ds| %%%ds| %%%ds%n", cellWidths);

            for (Map.Entry<EventType, long[]> pair : stats.entrySet()) {
                EventType type = pair.getKey();
                long[] row = pair.getValue();

                Object[] cellValues =
                        new Object[] {
                            type.toString(),
                            formatNum(row[MESSAGES_DELTA], Format.NUM),
                            formatNum(row[EVENTS_DELTA], Format.NUM),
                            formatNum(row[BYTES_DELTA], Format.MEMORY),
                            formatNum(row[MESSAGES_TOTAL], Format.NUM),
                            formatNum(row[EVENTS_TOTAL], Format.NUM),
                            formatNum(row[BYTES_TOTAL], Format.MEMORY)
                        };

                builder.append(String.format(rowFormat, cellValues));
            }

            // Clear delta values after dumping
            clearDelta();

        } else { // Print without delta

            // Print headers
            Object[] headers =
                    new Object[] {
                        formatCenter("messages", widths[MESSAGES_TOTAL]),
                        formatCenter("events", widths[EVENTS_TOTAL]),
                        formatCenter("bytes", widths[BYTES_TOTAL])
                    };
            builder.append(String.format("         | %s| %s| %s%n", headers));

            builder.append("---------");
            for (int i = MESSAGES_TOTAL; i < widths.length; i++) {
                builder.append('+');
                repeatCharacter(builder, '-', widths[i] + 1);
            }
            builder.append("\n");

            // Print rows
            Object[] cellWidths =
                    new Object[] {
                        columnWidth(table, MESSAGES_TOTAL, 8, Format.NUM),
                        columnWidth(table, EVENTS_TOTAL, 6, Format.NUM),
                        columnWidth(table, BYTES_TOTAL, 5, Format.MEMORY)
                    };
            String rowFormat = String.format("  %%-7s| %%%ds| %%%ds| %%%ds%n", cellWidths);

            for (Map.Entry<EventType, long[]> pair : stats.entrySet()) {
                EventType type = pair.getKey();
                long[] row = pair.getValue();

                Object[] cellValues =
                        new Object[] {
                            type.toString(),
                            formatNum(row[MESSAGES_TOTAL], Format.NUM),
                            formatNum(row[EVENTS_TOTAL], Format.NUM),
                            formatNum(row[BYTES_TOTAL], Format.MEMORY)
                        };

                builder.append(String.format(rowFormat, cellValues));
            }

            // Clear all values after dumping
            init();
        }
    }

    private void init() {
        stats = new LinkedHashMap<>(5);

        stats.put(EventType.PUT, new long[NUM_VALUES]);
        stats.put(EventType.CONTROL, new long[NUM_VALUES]);
        stats.put(EventType.ACK, new long[NUM_VALUES]);
        stats.put(EventType.PUSH, new long[NUM_VALUES]);
        stats.put(EventType.CONFIRM, new long[NUM_VALUES]);
    }

    private void clearDelta() {
        for (long[] eventStat : stats.values()) {
            eventStat[MESSAGES_DELTA] = 0;
            eventStat[EVENTS_DELTA] = 0;
            eventStat[BYTES_DELTA] = 0;
        }
    }
}
