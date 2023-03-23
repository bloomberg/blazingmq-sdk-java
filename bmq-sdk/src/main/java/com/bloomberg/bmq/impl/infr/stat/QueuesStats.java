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

import com.bloomberg.bmq.Uri;
import com.bloomberg.bmq.impl.QueueId;
import com.bloomberg.bmq.impl.infr.stat.StatsUtil.Format;
import com.bloomberg.bmq.impl.infr.util.Argument;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueuesStats {

    private static final Logger logger =
            LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int NUM_VALUES = 10;

    private static final int IN_BYTES_DELTA = 0;
    private static final int IN_BYTES_TOTAL = 1;
    private static final int IN_MESSAGES_DELTA = 2;
    private static final int IN_MESSAGES_TOTAL = 3;
    private static final int OUT_BYTES_DELTA = 4;
    private static final int OUT_BYTES_TOTAL = 5;
    private static final int OUT_MESSAGES_DELTA = 6;
    private static final int OUT_MESSAGES_TOTAL = 7;
    private static final int OUT_RATIO_DELTA = 8;
    private static final int OUT_RATIO_TOTAL = 9;

    private static final int COMPRESSION_PRECISION_FACTOR = 1000;

    /** Contains queue uri, stats and open/close flag */
    private static class QueueStats {
        private final Uri uri;
        private final long[] stats;
        private boolean isOpen;

        private long ratioX1000SumDelta = 0;
        private long ratioX1000SumTotal = 0;
        private int compressedMsgDelta = 0;
        private int compressedMsgTotal = 0;

        public QueueStats(Uri uri) {
            this.uri = uri;
            stats = new long[NUM_VALUES];
            isOpen = true;
        }

        public Uri uri() {
            return uri;
        }

        public long[] stats() {
            return stats;
        }

        public boolean isOpen() {
            return isOpen;
        }

        public void close() {
            isOpen = false;
        }

        public void open() {
            isOpen = true;
        }

        public void addInputBytes(int num) {
            stats[IN_BYTES_DELTA] += num;
            stats[IN_BYTES_TOTAL] += num;
            stats[IN_MESSAGES_DELTA] += 1;
            stats[IN_MESSAGES_TOTAL] += 1;
        }

        public void addOutputBytes(int num) {
            stats[OUT_BYTES_DELTA] += num;
            stats[OUT_BYTES_TOTAL] += num;
            stats[OUT_MESSAGES_DELTA] += 1;
            stats[OUT_MESSAGES_TOTAL] += 1;
        }

        public void addCompressionRatio(double ratio) {
            // Compression ratio for the queue is computed as average
            // compression ratio for all compressed messages in this queue

            int ratioX1000 = (int) (ratio * COMPRESSION_PRECISION_FACTOR);

            ratioX1000SumDelta += ratioX1000;
            ratioX1000SumTotal += ratioX1000;

            compressedMsgDelta += 1;
            compressedMsgTotal += 1;

            stats[OUT_RATIO_DELTA] = ratioX1000SumDelta / compressedMsgDelta;

            stats[OUT_RATIO_TOTAL] = ratioX1000SumTotal / compressedMsgTotal;
        }

        public void resetDelta() {
            ratioX1000SumDelta = 0;
            compressedMsgDelta = 0;

            stats[IN_BYTES_DELTA] = 0;
            stats[IN_MESSAGES_DELTA] = 0;
            stats[OUT_BYTES_DELTA] = 0;
            stats[OUT_MESSAGES_DELTA] = 0;
            stats[OUT_RATIO_DELTA] = 0;
        }
    }

    private final long[] summary;
    private final Map<QueueId, QueueStats> stats;

    private long summaryRatioX1000SumDelta = 0;
    private long summaryRatioX1000SumTotal = 0;
    private int summaryCompressedMsgDelta = 0;
    private int summaryCompressedMsgTotal = 0;

    public QueuesStats() {
        summary = new long[NUM_VALUES];
        stats = new LinkedHashMap<>();
    }

    public synchronized void onQueueOpen(QueueId id, Uri uri) {
        Argument.expectNonNull(id, "id");
        Argument.expectNonNull(uri, "uri");

        QueueStats s = stats.get(id);

        if (s != null) { // Reopen the queue
            Argument.expectCondition(!s.isOpen(), "Queue with the same id has been already opened");
            Argument.expectCondition(
                    uri.equals(s.uri()),
                    "Mismatch between 'uri' provided and stored in queue stats");

            s.open();
        } else { // Open new queue
            s = new QueueStats(uri);
            stats.put(id, s);
        }
    }

    public synchronized void onQueueClose(QueueId id) {
        QueueStats s = stats.get(Argument.expectNonNull(id, "id"));
        Argument.expectCondition(s != null, "There is no queue with provided id: ", id);
        s.close();
    }

    public synchronized void onPushMessage(QueueId id, int size) {
        QueueStats queueStats = stats.get(Argument.expectNonNull(id, "id"));

        // Note. Temporarily disabled, see internal-ticket I88.
        // Argument.expectCondition(queueStats != null, "There is no queue with provided id: ", id);
        // TODO. Refactor the code, move stats updating to scheduler thread, update queue stats in
        // queue control strategies etc.

        if (queueStats == null) {
            logger.warn("There is no queue with provided id '{}' yet", id);
            return;
        }

        Argument.expectCondition(queueStats.isOpen(), "Queue with provided id is closed");
        Argument.expectPositive(size, "size");

        // update summary item
        summary[IN_BYTES_DELTA] += size;
        summary[IN_BYTES_TOTAL] += size;
        summary[IN_MESSAGES_DELTA] += 1;
        summary[IN_MESSAGES_TOTAL] += 1;

        // update queue stats
        queueStats.addInputBytes(size);
    }

    public synchronized void onPutMessage(QueueId id, int size, double ratio) {
        Argument.expectPositive(size, "size");
        Argument.expectCondition(ratio > 0, "'ratio' must be positive");

        QueueStats queueStats = stats.get(Argument.expectNonNull(id, "id"));

        Argument.expectCondition(queueStats != null, "There is no queue with provided id: ", id);
        Argument.expectCondition(queueStats.isOpen(), "Queue with provided id is closed");

        // update summary item
        summary[OUT_BYTES_DELTA] += size;
        summary[OUT_BYTES_TOTAL] += size;
        summary[OUT_MESSAGES_DELTA] += 1;
        summary[OUT_MESSAGES_TOTAL] += 1;

        // update queue stats
        queueStats.addOutputBytes(size);

        if (ratio != 1) {
            queueStats.addCompressionRatio(ratio);

            // Summary compression ratio is computed as average
            // compression ratio for all compressed messages in all queues
            int ratioX1000 = (int) (ratio * COMPRESSION_PRECISION_FACTOR);

            summaryRatioX1000SumDelta += ratioX1000;
            summaryRatioX1000SumTotal += ratioX1000;

            summaryCompressedMsgDelta += 1;
            summaryCompressedMsgTotal += 1;

            summary[OUT_RATIO_DELTA] = summaryRatioX1000SumDelta / summaryCompressedMsgDelta;

            summary[OUT_RATIO_TOTAL] = summaryRatioX1000SumTotal / summaryCompressedMsgTotal;
        }
    }

    public synchronized void dump(StringBuilder builder, boolean isFinal) {
        builder.append("::::: Queues >>\n");

        Collection<long[]> table =
                stats.values().stream().map(QueueStats::stats).collect(Collectors.toList());

        // Determine data columns widths
        int[] widths =
                new int[] {
                    columnWidth(table, summary, IN_BYTES_DELTA, 13, Format.MEMORY),
                    columnWidth(table, summary, IN_BYTES_TOTAL, 5, Format.MEMORY),
                    columnWidth(table, summary, IN_MESSAGES_DELTA, 16, Format.NUM),
                    columnWidth(table, summary, IN_MESSAGES_TOTAL, 8, Format.NUM),
                    columnWidth(table, summary, OUT_BYTES_DELTA, 13, Format.MEMORY),
                    columnWidth(table, summary, OUT_BYTES_TOTAL, 5, Format.MEMORY),
                    columnWidth(table, summary, OUT_MESSAGES_DELTA, 16, Format.NUM),
                    columnWidth(table, summary, OUT_MESSAGES_TOTAL, 8, Format.NUM),
                    columnWidth(table, summary, OUT_RATIO_DELTA, 5, Format.DECIMAL),
                    columnWidth(table, summary, OUT_RATIO_TOTAL, 5, Format.DECIMAL),
                };

        int queueWidth = queueColumnWidth(6);

        if (!isFinal) { // Print with delta

            // Print group headers
            int inputColumnsWidth =
                    widths[IN_BYTES_DELTA]
                            + widths[IN_BYTES_TOTAL]
                            + widths[IN_MESSAGES_DELTA]
                            + widths[IN_MESSAGES_TOTAL];
            String inputGroup = formatCenter("In", inputColumnsWidth + 6);

            int outputColumnsWidth =
                    widths[OUT_BYTES_DELTA]
                            + widths[OUT_BYTES_TOTAL]
                            + widths[OUT_MESSAGES_DELTA]
                            + widths[OUT_MESSAGES_TOTAL];
            String outputGroup = formatCenter("Out", outputColumnsWidth + 6);

            int ratioColumnsWidth = widths[OUT_RATIO_DELTA] + widths[OUT_RATIO_TOTAL];
            String ratioGroup = formatCenter("Compression Ratio", ratioColumnsWidth + 2);

            repeatCharacter(builder, ' ', queueWidth);
            builder.append(String.format("| %s| %s| %s%n", inputGroup, outputGroup, ratioGroup));

            // Print headers
            Object[] headers =
                    new Object[] {
                        formatCenter("bytes (delta)", widths[IN_BYTES_DELTA]),
                        formatCenter("bytes", widths[IN_BYTES_TOTAL]),
                        formatCenter("messages (delta)", widths[IN_MESSAGES_DELTA]),
                        formatCenter("messages", widths[IN_MESSAGES_TOTAL]),
                        formatCenter("bytes (delta)", widths[OUT_BYTES_DELTA]),
                        formatCenter("bytes", widths[OUT_BYTES_TOTAL]),
                        formatCenter("messages (delta)", widths[OUT_MESSAGES_DELTA]),
                        formatCenter("messages", widths[OUT_MESSAGES_TOTAL]),
                        formatCenter("delta", widths[OUT_RATIO_DELTA]),
                        formatCenter("total", widths[OUT_RATIO_TOTAL])
                    };

            repeatCharacter(builder, ' ', queueWidth);
            for (Object header : headers) {
                builder.append("| ").append(header);
            }
            builder.append("\n");

            repeatCharacter(builder, '-', queueWidth);
            for (int w : widths) {
                builder.append('+');
                repeatCharacter(builder, '-', w + 1);
            }
            builder.append("\n");

            // Print rows
            Object[] allCellWidths =
                    new Object[] {
                        queueWidth,
                        widths[IN_BYTES_DELTA],
                        widths[IN_BYTES_TOTAL],
                        widths[IN_MESSAGES_DELTA],
                        widths[IN_MESSAGES_TOTAL],
                        widths[OUT_BYTES_DELTA],
                        widths[OUT_BYTES_TOTAL],
                        widths[OUT_MESSAGES_DELTA],
                        widths[OUT_MESSAGES_TOTAL],
                        widths[OUT_RATIO_DELTA],
                        widths[OUT_RATIO_TOTAL]
                    };
            String rowFormat =
                    String.format(
                            "%%-%ds"
                                    + "| %%%ds| %%%ds| %%%ds| %%%ds"
                                    + "| %%%ds| %%%ds| %%%ds| %%%ds| %%%ds| %%%ds%n",
                            allCellWidths);

            // Print summary row
            Object[] summaryValues =
                    new Object[] {
                        "queues",
                        formatNum(summary[IN_BYTES_DELTA], Format.MEMORY),
                        formatNum(summary[IN_BYTES_TOTAL], Format.MEMORY),
                        formatNum(summary[IN_MESSAGES_DELTA], Format.NUM),
                        formatNum(summary[IN_MESSAGES_TOTAL], Format.NUM),
                        formatNum(summary[OUT_BYTES_DELTA], Format.MEMORY),
                        formatNum(summary[OUT_BYTES_TOTAL], Format.MEMORY),
                        formatNum(summary[OUT_MESSAGES_DELTA], Format.NUM),
                        formatNum(summary[OUT_MESSAGES_TOTAL], Format.NUM),
                        formatNum(summary[OUT_RATIO_DELTA], Format.DECIMAL),
                        formatNum(summary[OUT_RATIO_TOTAL], Format.DECIMAL)
                    };

            builder.append(String.format(rowFormat, summaryValues));

            for (QueueStats qs : stats.values()) {
                long[] row = qs.stats();
                String uri =
                        qs.isOpen()
                                ? "  " + qs.uri().toString()
                                : String.format("  (%s)", qs.uri().toString());

                Object[] cellValues =
                        new Object[] {
                            uri,
                            formatNum(row[IN_BYTES_DELTA], Format.MEMORY),
                            formatNum(row[IN_BYTES_TOTAL], Format.MEMORY),
                            formatNum(row[IN_MESSAGES_DELTA], Format.NUM),
                            formatNum(row[IN_MESSAGES_TOTAL], Format.NUM),
                            formatNum(row[OUT_BYTES_DELTA], Format.MEMORY),
                            formatNum(row[OUT_BYTES_TOTAL], Format.MEMORY),
                            formatNum(row[OUT_MESSAGES_DELTA], Format.NUM),
                            formatNum(row[OUT_MESSAGES_TOTAL], Format.NUM),
                            formatNum(row[OUT_RATIO_DELTA], Format.DECIMAL),
                            formatNum(row[OUT_RATIO_TOTAL], Format.DECIMAL)
                        };

                builder.append(String.format(rowFormat, cellValues));
            }

            // Clear delta values after dumping
            clearDelta();

        } else { // Print without delta

            // Update compression ratio header width
            widths[OUT_RATIO_TOTAL] = columnWidth(table, summary, OUT_RATIO_TOTAL, 17, Format.NUM);

            // Print group headers
            int inputColumnsWidth = widths[IN_BYTES_TOTAL] + widths[IN_MESSAGES_TOTAL];
            String inputGroup = formatCenter("In", inputColumnsWidth + 2);

            int outputColumnsWidth =
                    widths[OUT_BYTES_TOTAL] + widths[OUT_MESSAGES_TOTAL] + widths[OUT_RATIO_TOTAL];
            String outputGroup = formatCenter("Out", outputColumnsWidth + 4);

            repeatCharacter(builder, ' ', queueWidth);
            builder.append(String.format("| %s| %s%n", inputGroup, outputGroup));

            // Print headers
            Object[] headers =
                    new Object[] {
                        formatCenter("bytes", widths[IN_BYTES_TOTAL]),
                        formatCenter("messages", widths[IN_MESSAGES_TOTAL]),
                        formatCenter("bytes", widths[OUT_BYTES_TOTAL]),
                        formatCenter("messages", widths[OUT_MESSAGES_TOTAL]),
                        formatCenter("compression ratio", widths[OUT_RATIO_TOTAL])
                    };

            repeatCharacter(builder, ' ', queueWidth);
            builder.append(String.format("| %s| %s| %s| %s| %s%n", headers));

            repeatCharacter(builder, '-', queueWidth);
            for (int i = IN_BYTES_TOTAL; i < widths.length; i += 2) {
                builder.append('+');
                repeatCharacter(builder, '-', widths[i] + 1);
            }
            builder.append("\n");

            // Print rows
            Object[] allCellWidths =
                    new Object[] {
                        queueWidth,
                        widths[IN_BYTES_TOTAL],
                        widths[IN_MESSAGES_TOTAL],
                        widths[OUT_BYTES_TOTAL],
                        widths[OUT_MESSAGES_TOTAL],
                        widths[OUT_RATIO_TOTAL]
                    };
            String rowFormat =
                    String.format("%%-%ds| %%%ds| %%%ds| %%%ds| %%%ds| %%%ds%n", allCellWidths);

            // Print summary row
            Object[] summaryValues =
                    new Object[] {
                        "queues",
                        formatNum(summary[IN_BYTES_TOTAL], Format.MEMORY),
                        formatNum(summary[IN_MESSAGES_TOTAL], Format.NUM),
                        formatNum(summary[OUT_BYTES_TOTAL], Format.MEMORY),
                        formatNum(summary[OUT_MESSAGES_TOTAL], Format.NUM),
                        formatNum(summary[OUT_RATIO_TOTAL], Format.DECIMAL)
                    };

            builder.append(String.format(rowFormat, summaryValues));

            for (QueueStats qs : stats.values()) {
                long[] row = qs.stats();
                String uri =
                        qs.isOpen()
                                ? "  " + qs.uri().toString()
                                : String.format("  (%s)", qs.uri().toString());

                Object[] cellValues =
                        new Object[] {
                            uri,
                            formatNum(row[IN_BYTES_TOTAL], Format.MEMORY),
                            formatNum(row[IN_MESSAGES_TOTAL], Format.NUM),
                            formatNum(row[OUT_BYTES_TOTAL], Format.MEMORY),
                            formatNum(row[OUT_MESSAGES_TOTAL], Format.NUM),
                            formatNum(row[OUT_RATIO_TOTAL], Format.DECIMAL)
                        };

                builder.append(String.format(rowFormat, cellValues));
            }

            // Clear all data
            clear();
        }
    }

    private void clear() {
        // Clear summary data
        Arrays.fill(summary, 0);

        summaryRatioX1000SumDelta = 0;
        summaryCompressedMsgDelta = 0;
        summaryRatioX1000SumTotal = 0;
        summaryCompressedMsgTotal = 0;

        // Clear queues
        stats.clear();
    }

    private void clearDelta() {
        // Clear summary delta
        summary[IN_BYTES_DELTA] = 0;
        summary[IN_MESSAGES_DELTA] = 0;
        summary[OUT_BYTES_DELTA] = 0;
        summary[OUT_MESSAGES_DELTA] = 0;
        summary[OUT_RATIO_DELTA] = 0;

        summaryRatioX1000SumDelta = 0;
        summaryCompressedMsgDelta = 0;

        // Clear queues delta
        for (QueueStats qs : stats.values()) {
            qs.resetDelta();
        }
    }

    private int queueColumnWidth(int summaryItemWidth) {
        int width = summaryItemWidth;

        for (QueueStats qs : stats.values()) {
            int uriLength = qs.uri().toString().length();
            int extra = 2 + (qs.isOpen() ? 0 : 2); // indent + parentheses

            width = Math.max(width, uriLength + extra);
        }

        return width;
    }
}
