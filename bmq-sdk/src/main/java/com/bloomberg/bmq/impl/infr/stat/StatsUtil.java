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
import java.util.Collection;

class StatsUtil {

    private static final int KiB = 1024;

    // List of supported time interval units
    private static final String[] TIME_INTERVAL_UNITS =
            new String[] {"ns", "us", "ms", "s", "m", "h", "d", "w"};

    // List of unit values expressed as a number of smaller units e.g. 1us=1000ns, 1ms=1000us etc.
    private static final int[] TIME_INTERVAL_SIZES = {1, 1000, 1000, 1000, 60, 60, 24, 7};

    enum Format {
        NUM,
        DECIMAL,
        MEMORY,
        TIME_PERIOD
    }

    static int columnWidth(Collection<long[]> table, int index, int headerWidth, Format format) {
        return columnWidth(table, index, headerWidth, format, 3);
    }

    static int columnWidth(
            Collection<long[]> table,
            int index,
            int headerWidth,
            Format format,
            int decimalPlaces) {
        Argument.expectNonNull(table, "table");
        Argument.expectNonNegative(index, "index");
        Argument.expectNonNegative(headerWidth, "headerWidth");

        int maxWidth = headerWidth;

        for (long[] row : table) {
            long value = row[index];

            int width = numWidth(value, format, false, decimalPlaces);
            maxWidth = Math.max(maxWidth, width);
        }

        return maxWidth;
    }

    static int columnWidth(
            Collection<long[]> table, long[] summary, int index, int headerWidth, Format format) {
        return columnWidth(table, summary, index, headerWidth, format, 3);
    }

    static int columnWidth(
            Collection<long[]> table,
            long[] summary,
            int index,
            int headerWidth,
            Format format,
            int decimalPlaces) {
        Argument.expectNonNull(summary, "summary");
        Argument.expectCondition(
                index < summary.length, "'index' must be less than 'summary' length");

        int summaryCellWidth = numWidth(summary[index], format, false, decimalPlaces);

        return Math.max(
                summaryCellWidth, columnWidth(table, index, headerWidth, format, decimalPlaces));
    }

    static String formatCenter(String s, int width) {
        Argument.expectNonNull(s, "string");
        Argument.expectPositive(width, "width");

        int strlen = s.length();

        if (width > strlen + 1) {
            int extra = (width - strlen) / 2;
            int min = width - extra;

            String format = String.format("%%%ds%%%ds", min, extra);

            return String.format(format, s, "");
        } else {
            String format = String.format("%%%ds", width);

            return String.format(format, s);
        }
    }

    static String formatNum(long num, Format format) {
        return formatNum(num, format, false, 3);
    }

    static String formatNum(long num, Format format, boolean printZeroValue) {
        return formatNum(num, format, printZeroValue, 3);
    }

    static String formatNum(long num, Format format, boolean printZeroValue, int decimalPlaces) {
        Argument.expectNonNegative(num, "number");
        Argument.expectNonNull(format, "format");

        switch (format) {
            case NUM:
                return formatNum(num, printZeroValue);
            case DECIMAL:
                return formatNumAsDecimal(num, printZeroValue, decimalPlaces);
            case MEMORY:
                return formatNumAsMemory(num, printZeroValue);
            case TIME_PERIOD:
                return formatNumAsTimePeriod(num, printZeroValue);
            default:
                throw new IllegalArgumentException(format.toString() + " - unknown format");
        }
    }

    private static String formatNum(long num, boolean printZeroValue) {
        if (num == 0 && !printZeroValue) {
            return "";
        }

        return String.format("%,d", num);
    }

    private static String formatNumAsDecimal(long num, boolean printZeroValue, int decimalPlaces) {
        Argument.expectPositive(decimalPlaces, "decimalPlaces");

        if (num == 0 && !printZeroValue) {
            return "";
        }

        double d = num / Math.pow(10, decimalPlaces);
        String format = String.format("%%,.%df", decimalPlaces);

        return String.format(format, d);
    }

    private static String formatNumAsMemory(long num, boolean printZeroValue) {
        if (num == 0 && !printZeroValue) {
            return "";
        }

        if (num < KiB) {
            return num + " B";
        }

        // Format as '0000.00 XB' value
        int k = (63 - Long.numberOfLeadingZeros(num)) / 10;
        long div = 1L << (k * 10);
        double v = (double) num / div;
        return String.format("%.2f %siB", v, " KMGTPE".charAt(k));
    }

    private static String formatNumAsTimePeriod(long num, boolean printZeroValue) {
        if (num == 0 && !printZeroValue) {
            return "";
        }

        String unit = TIME_INTERVAL_UNITS[0]; // "ns"
        long div = TIME_INTERVAL_SIZES[0]; // "1"

        for (int nextLevel = 1; nextLevel < TIME_INTERVAL_UNITS.length; ++nextLevel) {
            int nextLevelSize = TIME_INTERVAL_SIZES[nextLevel];

            if (num < div * nextLevelSize) {
                break;
            }

            unit = TIME_INTERVAL_UNITS[nextLevel];
            div *= nextLevelSize;
        }

        if (div == 1) {
            return num + " " + unit;
        }

        double d = (double) num / div;

        return String.format("%,.2f %s", d, unit);
    }

    static int numWidth(long num, Format format, boolean spaceForZeroValue) {
        return numWidth(num, format, spaceForZeroValue, 3);
    }

    static int numWidth(long num, Format format, boolean spaceForZeroValue, int decimalPlaces) {
        Argument.expectNonNegative(num, "number");
        Argument.expectNonNull(format, "format");

        switch (format) {
            case NUM:
                return numWidth(num, spaceForZeroValue);
            case DECIMAL:
                return numWidthAsDecimal(num, spaceForZeroValue, decimalPlaces);
            case MEMORY:
                return numWidthAsMemory(num, spaceForZeroValue);
            case TIME_PERIOD:
                return numWidthAsTimePeriod(num, spaceForZeroValue);
            default:
                throw new IllegalArgumentException(format.toString() + " - unknown format");
        }
    }

    private static int numWidth(long num, boolean spaceForZeroValue) {
        if (num == 0) {
            if (spaceForZeroValue) {
                return 1; // "0"
            } else {
                return 0;
            }
        }

        int numDigits = numDigits(num);
        return numDigits + (numDigits - 1) / 3;
    }

    private static int numWidthAsDecimal(long num, boolean spaceForZeroValue, int decimalPlaces) {
        Argument.expectPositive(decimalPlaces, "decimalPlaces");

        if (num == 0) {
            if (spaceForZeroValue) {
                return decimalPlaces + 2; // "0." + decimal places
            } else {
                return 0;
            }
        }

        int numDigits = numDigits(num);

        // Compute number of thousand separators in integer part
        int numSeparators = 0;
        if (numDigits > decimalPlaces) {
            numSeparators = (numDigits - decimalPlaces - 1) / 3;
        }

        // + 2 for "0."; +1 for decimal separator
        return Math.max(decimalPlaces + 2, numDigits + numSeparators + 1);
    }

    private static int numWidthAsMemory(long num, boolean spaceForZeroValue) {
        if (num == 0) {
            if (spaceForZeroValue) {
                return 3; // "0 B"
            } else {
                return 0;
            }
        }

        int k = (63 - Long.numberOfLeadingZeros(num)) / 10;
        long div = 1L << (k * 10);

        long l = num / div;
        int width = numDigits(l) + 2; // num + ' B'

        // +3 for '.XX'
        // +2 for 'XiB'
        if (num >= KiB) {
            width += 5;
        }

        return width;
    }

    private static int numWidthAsTimePeriod(long num, boolean spaceForZeroValue) {
        if (num == 0) {
            if (spaceForZeroValue) {
                return 4; // "0 ns"
            } else {
                return 0;
            }
        }

        String unit = TIME_INTERVAL_UNITS[0]; // "ns"
        long div = TIME_INTERVAL_SIZES[0]; // "1"

        for (int nextLevel = 1; nextLevel < TIME_INTERVAL_UNITS.length; ++nextLevel) {
            int nextLevelSize = TIME_INTERVAL_SIZES[nextLevel];

            if (num < div * nextLevelSize) {
                break;
            }

            unit = TIME_INTERVAL_UNITS[nextLevel];
            div *= nextLevelSize;
        }

        long n = num / div;

        int numDigits = numDigits(n);

        int width = numDigits + 1 + unit.length(); // num + space + unit;

        if (div > 1) {
            width += (numDigits - 1) / 3 + 3; // separators + ".00"
        }

        return width;
    }

    private static int numDigits(long number) {
        if (number == 0) {
            return 1;
        }

        return (int) (Math.log10(number) + 1);
    }

    static void repeatCharacter(StringBuilder builder, char c, int n) {
        Argument.expectNonNull(builder, "builder");
        Argument.expectNonNegative(n, "number");

        for (int i = 0; i < n; i++) {
            builder.append(c);
        }
    }
}
