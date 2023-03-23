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
import static com.bloomberg.bmq.impl.infr.stat.StatsUtil.numWidth;
import static com.bloomberg.bmq.impl.infr.stat.StatsUtil.repeatCharacter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.bloomberg.bmq.impl.infr.stat.StatsUtil.Format;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatsUtilTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testColumnWidth() {
        // Null table
        try {
            columnWidth(null, -1, -1, Format.NUM);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'table' must be non-null", e.getMessage());
            logger.info("Caught expected exception: ", e);
        }

        // Negative index
        try {
            columnWidth(new ArrayList<>(), -1, -1, Format.NUM);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'index' must be non-negative", e.getMessage());
            logger.info("Caught expected exception: ", e);
        }

        // Negative header width
        try {
            columnWidth(new ArrayList<>(), 0, -1, Format.NUM);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'headerWidth' must be non-negative", e.getMessage());
            logger.info("Caught expected exception: ", e);
        }

        Collection<long[]> table = new ArrayList<>();
        table.add(new long[] {0, 100});
        table.add(new long[] {0, 505});
        table.add(new long[] {0, 1023});
        table.add(new long[] {0, 90454});
        table.add(new long[] {0, 9023});

        assertEquals(3, columnWidth(table, 0, 3, Format.NUM));
        assertEquals(3, columnWidth(table, 0, 3, Format.DECIMAL));
        assertEquals(3, columnWidth(table, 0, 3, Format.MEMORY));
        assertEquals(3, columnWidth(table, 0, 3, Format.TIME_PERIOD));

        assertEquals(6, columnWidth(table, 1, 3, Format.NUM));
        assertEquals(6, columnWidth(table, 1, 3, Format.DECIMAL));
        assertEquals(9, columnWidth(table, 1, 3, Format.MEMORY));
        assertEquals(8, columnWidth(table, 1, 3, Format.TIME_PERIOD));
    }

    @Test
    public void testColumnWidthWithSummary() {
        // Null summary
        try {
            columnWidth(new ArrayList<>(), null, -1, -1, Format.NUM);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'summary' must be non-null", e.getMessage());
            logger.info("Caught expected exception: ", e);
        }

        // Invalid summary length
        try {
            columnWidth(new ArrayList<>(), new long[2], 2, -1, Format.NUM);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'index' must be less than 'summary' length", e.getMessage());
            logger.info("Caught expected exception: ", e);
        }

        Collection<long[]> table = new ArrayList<>();
        table.add(new long[] {0, 100});
        table.add(new long[] {0, 505});
        table.add(new long[] {0, 1023});
        table.add(new long[] {0, 97454});
        table.add(new long[] {0, 9023});

        long[] summary = new long[] {0, 108105};

        assertEquals(3, columnWidth(table, summary, 0, 3, Format.NUM));
        assertEquals(3, columnWidth(table, summary, 0, 3, Format.DECIMAL));
        assertEquals(3, columnWidth(table, summary, 0, 3, Format.MEMORY));
        assertEquals(3, columnWidth(table, summary, 0, 3, Format.TIME_PERIOD));

        assertEquals(7, columnWidth(table, summary, 1, 3, Format.NUM));
        assertEquals(7, columnWidth(table, summary, 1, 3, Format.DECIMAL));
        assertEquals(10, columnWidth(table, summary, 1, 3, Format.MEMORY));
        assertEquals(9, columnWidth(table, summary, 1, 3, Format.TIME_PERIOD));
    }

    @Test
    public void testFormatCenter() {
        // Null string
        try {
            formatCenter(null, 0);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'string' must be non-null", e.getMessage());
            logger.info("Caught expected exception: ", e);
        }

        // Non-positive width
        try {
            formatCenter("s", 0);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'width' must be positive", e.getMessage());
            logger.info("Caught expected exception: ", e);
        }

        // Empty string
        assertEquals(" ", formatCenter("", 1));
        assertEquals("   ", formatCenter("", 3));

        // String length >= width
        assertEquals("str", formatCenter("str", 1));
        assertEquals("str", formatCenter("str", 3));

        // String length < width
        assertEquals(" str", formatCenter("str", 4));
        assertEquals("  str ", formatCenter("str", 6));
    }

    @Test
    public void testFormatNum() {
        // Negative number
        try {
            formatNum(-1, Format.NUM, false);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'number' must be non-negative", e.getMessage());
            logger.info("Caught expected exception: ", e);
        }

        // Null format
        try {
            formatNum(0, null, false);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'format' must be non-null", e.getMessage());
            logger.info("Caught expected exception: ", e);
        }

        // Non-positive decimal places fro Decimal format
        try {
            formatNum(0, Format.DECIMAL, false, 0);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'decimalPlaces' must be positive", e.getMessage());
            logger.info("Caught expected exception: ", e);
        }

        // Format.Num
        assertEquals("", formatNum(0, Format.NUM, false));
        assertEquals("0", formatNum(0, Format.NUM, true));
        assertEquals("123", formatNum(123, Format.NUM, false));
        assertEquals("123,456", formatNum(123456, Format.NUM, false));

        // Format.Decimal
        assertEquals("", formatNum(0, Format.DECIMAL, false));
        assertEquals("0.000", formatNum(0, Format.DECIMAL, true));
        assertEquals("0.00", formatNum(0, Format.DECIMAL, true, 2));
        assertEquals("0.0000", formatNum(0, Format.DECIMAL, true, 4));
        assertEquals("0.123", formatNum(123, Format.DECIMAL, false));
        assertEquals("1.23", formatNum(123, Format.DECIMAL, false, 2));
        assertEquals("0.0123", formatNum(123, Format.DECIMAL, false, 4));
        assertEquals("123.456", formatNum(123456, Format.DECIMAL, false));
        assertEquals("1,234.56", formatNum(123456, Format.DECIMAL, false, 2));
        assertEquals("12.3456", formatNum(123456, Format.DECIMAL, false, 4));

        // Format.Memory
        assertEquals("", formatNum(0, Format.MEMORY, false));
        assertEquals("0 B", formatNum(0, Format.MEMORY, true));
        assertEquals("1023 B", formatNum(1023, Format.MEMORY, false));
        assertEquals("1023.45 KiB", formatNum(1048012, Format.MEMORY, false));

        // Format.TimePeriod
        assertEquals("", formatNum(0, Format.TIME_PERIOD, false));
        assertEquals("0 ns", formatNum(0, Format.TIME_PERIOD, true));
        assertEquals("123 ns", formatNum(123, Format.TIME_PERIOD, false));
        assertEquals("1.02 us", formatNum(1023, Format.TIME_PERIOD, false));
        assertEquals("135.45 ms", formatNum(135450000, Format.TIME_PERIOD, false));
        assertEquals("123.45 w", formatNum(74662560000000000L, Format.TIME_PERIOD, false));
    }

    @Test
    public void testNumWidth() {
        // Negative number
        try {
            numWidth(-1, Format.NUM, false);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'number' must be non-negative", e.getMessage());
            logger.info("Caught expected exception: ", e);
        }

        // Null format
        try {
            numWidth(0, null, false);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'format' must be non-null", e.getMessage());
            logger.info("Caught expected exception: ", e);
        }

        // Non-positive decimal places for Decimal format
        try {
            numWidth(0, Format.DECIMAL, false, 0);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'decimalPlaces' must be positive", e.getMessage());
            logger.info("Caught expected exception: ", e);
        }

        // Format.Num
        assertEquals(0, numWidth(0, Format.NUM, false));
        assertEquals(1, numWidth(0, Format.NUM, true));
        assertEquals(3, numWidth(123, Format.NUM, false));
        assertEquals(7, numWidth(123456, Format.NUM, false));

        // Format.Decimal
        assertEquals(0, numWidth(0, Format.DECIMAL, false));
        assertEquals(5, numWidth(0, Format.DECIMAL, true));
        assertEquals(4, numWidth(0, Format.DECIMAL, true, 2));
        assertEquals(6, numWidth(0, Format.DECIMAL, true, 4));
        assertEquals(5, numWidth(123, Format.DECIMAL, false));
        assertEquals(4, numWidth(123, Format.DECIMAL, false, 2));
        assertEquals(6, numWidth(123, Format.DECIMAL, false, 4));
        assertEquals(7, numWidth(123456, Format.DECIMAL, false));
        assertEquals(8, numWidth(123456, Format.DECIMAL, false, 2));
        assertEquals(7, numWidth(123456, Format.DECIMAL, false, 4));

        // Format.Memory
        assertEquals(0, numWidth(0, Format.MEMORY, false));
        assertEquals(3, numWidth(0, Format.MEMORY, true));
        assertEquals(6, numWidth(1023, Format.MEMORY, false));
        assertEquals(11, numWidth(1048012, Format.MEMORY, false));

        // Format.TimePeriod
        assertEquals(0, numWidth(0, Format.TIME_PERIOD, false));
        assertEquals(4, numWidth(0, Format.TIME_PERIOD, true));
        assertEquals(6, numWidth(123, Format.TIME_PERIOD, false));
        assertEquals(7, numWidth(1023, Format.TIME_PERIOD, false));
        assertEquals(9, numWidth(135450000, Format.TIME_PERIOD, false));
        assertEquals(8, numWidth(74662560000000000L, Format.TIME_PERIOD, false));
    }

    @Test
    public void testRepeatCharacter() {
        // Null builder
        try {
            repeatCharacter(null, 'c', -1);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'builder' must be non-null", e.getMessage());
            logger.info("Caught expected exception: ", e);
        }

        // Negative number
        try {
            repeatCharacter(new StringBuilder(), 'c', -1);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'number' must be non-negative", e.getMessage());
            logger.info("Caught expected exception: ", e);
        }

        StringBuilder builder = new StringBuilder();

        // Zero number
        repeatCharacter(builder, 'c', 0);
        assertEquals("", builder.toString());

        // Positive number
        repeatCharacter(builder, 'c', 3);
        assertEquals("ccc", builder.toString());
    }
}
