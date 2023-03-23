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

public class UnsignedUtil {
    private static final BigInteger BIGINT_ZERO = BigInteger.valueOf(0);

    public static byte shortToUint8(short s) throws NumberFormatException {
        if (s > Limits.MAX_UNSIGNED_BYTE || s < 0) throw new NumberFormatException();

        return (byte) s;
    }

    public static short intToUint16(int i) throws NumberFormatException {
        if (i > Limits.MAX_UNSIGNED_SHORT || i < 0) throw new NumberFormatException();

        return (short) i;
    }

    public static int longToUint32(long l) throws NumberFormatException {
        if (l > Limits.MAX_UNSIGNED_INT || l < 0) throw new NumberFormatException();

        return (int) l;
    }

    public static long bigIntToUint64(BigInteger bigInt) throws NumberFormatException {
        if (bigInt.compareTo(Limits.MAX_UNSIGNED_LONG) > 0 || bigInt.compareTo(BIGINT_ZERO) < 0) {
            throw new NumberFormatException();
        }
        return bigInt.longValue();
    }

    public static short uint8ToShort(byte b) {
        return (short) (b & Limits.MAX_UNSIGNED_BYTE);
    }

    public static int uint16ToInt(short s) {
        return (s & Limits.MAX_UNSIGNED_SHORT);
    }

    public static long uint32ToLong(int i) {
        return (i & Limits.MAX_UNSIGNED_INT);
    }

    public static BigInteger uint64ToBigInteger(long l) {
        return BigInteger.valueOf(l).and(Limits.MAX_UNSIGNED_LONG);
    }

    public static short checkUnsignedByte(short s) throws IllegalArgumentException {
        Argument.expectCondition(
                0 <= s && s <= Limits.MAX_UNSIGNED_BYTE, "Out of unsigned byte range");

        return s;
    }

    public static int checkUnsignedShort(int i) throws IllegalArgumentException {
        Argument.expectCondition(
                0 <= i && i <= Limits.MAX_UNSIGNED_SHORT, "Out of unsigned short range");

        return i;
    }

    public static long checkUnsignedInt(long l) throws IllegalArgumentException {
        Argument.expectCondition(
                0 <= l && l <= Limits.MAX_UNSIGNED_INT, "Out of unsigned int range");

        return l;
    }

    private UnsignedUtil() {
        throw new IllegalStateException("Utility class");
    }
}
