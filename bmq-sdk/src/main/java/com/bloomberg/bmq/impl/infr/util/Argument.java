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
package com.bloomberg.bmq.impl.infr.util;

/**
 * Provides simple and uniform way to check function arguments.
 *
 * <p>This utility class introduce methods for checking function arguments: (1) integer, (2) long,
 * (3) references and (4) complex conditions. The usage of this utility routines should be limited
 * to argument checks only because if any of the provided check functions fails, the
 * 'IllegalArgumentException' is thrown.
 */
public class Argument {
    public static int expectPositive(int value, String varname) {
        if (value <= 0) {
            StringBuilder sb = new StringBuilder();
            sb.append("'").append(varname).append("'");
            sb.append(" must be positive");
            throw new IllegalArgumentException(sb.toString());
        }
        return value;
    }

    public static int expectNonNegative(int value, String varname) {
        return (int) expectNonNegative((long) value, varname);
    }

    public static long expectNonNegative(long value, String varname) {
        if (value < 0) {
            StringBuilder sb = new StringBuilder();
            sb.append("'").append(varname).append("'");
            sb.append(" must be non-negative");
            throw new IllegalArgumentException(sb.toString());
        }
        return value;
    }

    public static int expectNotGreater(int value, int border, String varname) {
        if (value > border) {
            StringBuilder sb = new StringBuilder();
            sb.append("'").append(varname).append("'");
            sb.append(" must not exceed ").append(border);
            throw new IllegalArgumentException(sb.toString());
        }
        return value;
    }

    public static <T> T expectNonNull(T value, String varname) {
        if (value == null) {
            StringBuilder sb = new StringBuilder();
            sb.append("'").append(varname).append("'");
            sb.append(" must be non-null");
            throw new IllegalArgumentException(sb.toString());
        }
        return value;
    }

    public static void expectCondition(boolean condition, Object... failMessage)
            throws IllegalArgumentException {
        if (!condition) {
            StringBuilder sb = new StringBuilder();
            for (Object s : failMessage) {
                sb.append(s);
            }
            throw new IllegalArgumentException(sb.toString());
        }
    }

    private Argument() {
        throw new IllegalStateException("Utility class");
    }
}
