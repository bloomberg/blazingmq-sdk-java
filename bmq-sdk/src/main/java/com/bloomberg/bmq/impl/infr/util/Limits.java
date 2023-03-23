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

import java.math.BigInteger;

public class Limits {
    // Sizes of various basic types in bytes
    public static final byte BYTE_SIZE = 1;
    public static final byte SHORT_SIZE = 2; // (Short.SIZE / Byte.SIZE);
    public static final byte CHAR_SIZE = 2; // (Character.SIZE / Byte.SIZE);
    public static final byte INT_SIZE = 4; // (Integer.SIZE / Byte.SIZE);
    public static final byte LONG_SIZE = 8; // (Long.SIZE / Byte.SIZE);
    public static final byte FLOAT_SIZE = 4; // (Float.SIZE / Byte.SIZE);
    public static final byte DOUBLE_SIZE = 8; // (Double.SIZE / Byte.SIZE);

    public static final int BITS_PER_OCTET = 8;

    // Num bits
    public static final byte INTEGER_NUM_BITS = INT_SIZE * BITS_PER_OCTET;
    public static final byte LONG_NUM_BITS = LONG_SIZE * BITS_PER_OCTET;

    public static final short MAX_UNSIGNED_BYTE = 0xFF;
    public static final int MAX_UNSIGNED_SHORT = 0xFFFF;
    public static final long MAX_UNSIGNED_INT = 0xFFFFFFFFL;
    public static final BigInteger MAX_UNSIGNED_LONG = new BigInteger("FFFFFFFFFFFFFFFF", 16);

    private Limits() {
        throw new IllegalStateException("Utility class");
    }
}
