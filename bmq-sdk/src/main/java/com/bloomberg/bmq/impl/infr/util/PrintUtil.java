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

import java.nio.ByteBuffer;

public class PrintUtil {

    private static final char[] HEX_DUMP_CHARS = {
        '.', //   0   0
        '.', //   1   1
        '.', //   2   2
        '.', //   3   3
        '.', //   4   4
        '.', //   5   5
        '.', //   6   6
        '.', //   7   7
        '.', //   8   8 - BACKSPACE
        '.', //   9   9 - TAB
        '.', //  10   a - LF
        '.', //  11   b
        '.', //  12   c
        '.', //  13   d - CR
        '.', //  14   e
        '.', //  15   f
        '.', //  16  10
        '.', //  17  11
        '.', //  18  12
        '.', //  19  13
        '.', //  20  14
        '.', //  21  15
        '.', //  22  16
        '.', //  23  17
        '.', //  24  18
        '.', //  25  19
        '.', //  26  1a
        '.', //  27  1b
        '.', //  28  1c
        '.', //  29  1d
        '.', //  30  1e
        '.', //  31  1f
        ' ', //  32  20 - SPACE
        '!', //  33  21 - !
        '"', //  34  22 - "
        '#', //  35  23 - #
        '$', //  36  24 - $
        '%', //  37  25 - %
        '&', //  38  26 - &
        '\'', //  39  27 - '
        '(', //  40  28 - (
        ')', //  41  29 - )
        '*', //  42  2a - *
        '+', //  43  2b - +
        ',', //  44  2c - ,
        '-', //  45  2d - -
        '.', //  46  2e - .
        '/', //  47  2f - /
        '0', //  48  30 - 0
        '1', //  49  31 - 1
        '2', //  50  32 - 2
        '3', //  51  33 - 3
        '4', //  52  34 - 4
        '5', //  53  35 - 5
        '6', //  54  36 - 6
        '7', //  55  37 - 7
        '8', //  56  38 - 8
        '9', //  57  39 - 9
        ':', //  58  3a - :
        ';', //  59  3b - ;
        '<', //  60  3c - <
        '=', //  61  3d - =
        '>', //  62  3e - >
        '?', //  63  3f - ?
        '@', //  64  40 - @
        'A', //  65  41 - A
        'B', //  66  42 - B
        'C', //  67  43 - C
        'D', //  68  44 - D
        'E', //  69  45 - E
        'F', //  70  46 - F
        'G', //  71  47 - G
        'H', //  72  48 - H
        'I', //  73  49 - I
        'J', //  74  4a - J
        'K', //  75  4b - K
        'L', //  76  4c - L
        'M', //  77  4d - M
        'N', //  78  4e - N
        'O', //  79  4f - O
        'P', //  80  50 - P
        'Q', //  81  51 - Q
        'R', //  82  52 - R
        'S', //  83  53 - S
        'T', //  84  54 - T
        'U', //  85  55 - U
        'V', //  86  56 - V
        'W', //  87  57 - W
        'X', //  88  58 - X
        'Y', //  89  59 - Y
        'Z', //  90  5a - Z
        '[', //  91  5b - [
        '\\', //  92  5c - '\'
        ']', //  93  5d - ]
        '^', //  94  5e - ^
        '_', //  95  5f - _
        '`', //  96  60 - `
        'a', //  97  61 - a
        'b', //  98  62 - b
        'c', //  99  63 - c
        'd', // 100  64 - d
        'e', // 101  65 - e
        'f', // 102  66 - f
        'g', // 103  67 - g
        'h', // 104  68 - h
        'i', // 105  69 - i
        'j', // 106  6a - j
        'k', // 107  6b - k
        'l', // 108  6c - l
        'm', // 109  6d - m
        'n', // 110  6e - n
        'o', // 111  6f - o
        'p', // 112  70 - p
        'q', // 113  71 - q
        'r', // 114  72 - r
        's', // 115  73 - s
        't', // 116  74 - t
        'u', // 117  75 - u
        'v', // 118  76 - v
        'w', // 119  77 - w
        'x', // 120  78 - x
        'y', // 121  79 - y
        'z', // 122  7a - z
        '{', // 123  7b - {
        '|', // 124  7c - |
        '}', // 125  7d - }
        '~', // 126  7e - ~
        '.', // 127  7f - DEL
        '.', // 128  80
        '.', // 129  81
        '.', // 130  82
        '.', // 131  83
        '.', // 132  84
        '.', // 133  85
        '.', // 134  86
        '.', // 135  87
        '.', // 136  88
        '.', // 137  89
        '.', // 138  8a
        '.', // 139  8b
        '.', // 140  8c
        '.', // 141  8d
        '.', // 142  8e
        '.', // 143  8f
        '.', // 144  90
        '.', // 145  91
        '.', // 146  92
        '.', // 147  93
        '.', // 148  94
        '.', // 149  95
        '.', // 150  96
        '.', // 151  97
        '.', // 152  98
        '.', // 153  99
        '.', // 154  9a
        '.', // 155  9b
        '.', // 156  9c
        '.', // 157  9d
        '.', // 158  9e
        '.', // 159  9f
        '.', // 160  a0
        '.', // 161  a1
        '.', // 162  a2
        '.', // 163  a3
        '.', // 164  a4
        '.', // 165  a5
        '.', // 166  a6
        '.', // 167  a7
        '.', // 168  a8
        '.', // 169  a9
        '.', // 170  aa
        '.', // 171  ab
        '.', // 172  ac
        '.', // 173  ad
        '.', // 174  ae
        '.', // 175  af
        '.', // 176  b0
        '.', // 177  b1
        '.', // 178  b2
        '.', // 179  b3
        '.', // 180  b4
        '.', // 181  b5
        '.', // 182  b6
        '.', // 183  b7
        '.', // 184  b8
        '.', // 185  b9
        '.', // 186  ba
        '.', // 187  bb
        '.', // 188  bc
        '.', // 189  bd
        '.', // 190  be
        '.', // 191  bf
        '.', // 192  c0
        '.', // 193  c1
        '.', // 194  c2
        '.', // 195  c3
        '.', // 196  c4
        '.', // 197  c5
        '.', // 198  c6
        '.', // 199  c7
        '.', // 200  c8
        '.', // 201  c9
        '.', // 202  ca
        '.', // 203  cb
        '.', // 204  cc
        '.', // 205  cd
        '.', // 206  ce
        '.', // 207  cf
        '.', // 208  d0
        '.', // 209  d1
        '.', // 210  d2
        '.', // 211  d3
        '.', // 212  d4
        '.', // 213  d5
        '.', // 214  d6
        '.', // 215  d7
        '.', // 216  d8
        '.', // 217  d9
        '.', // 218  da
        '.', // 219  db
        '.', // 220  dc
        '.', // 221  dd
        '.', // 222  de
        '.', // 223  df
        '.', // 224  e0
        '.', // 225  e1
        '.', // 226  e2
        '.', // 227  e3
        '.', // 228  e4
        '.', // 229  e5
        '.', // 230  e6
        '.', // 231  e7
        '.', // 232  e8
        '.', // 233  e9
        '.', // 234  ea
        '.', // 235  eb
        '.', // 236  ec
        '.', // 237  ed
        '.', // 238  ee
        '.', // 239  ef
        '.', // 240  f0
        '.', // 241  f1
        '.', // 242  f2
        '.', // 243  f3
        '.', // 244  f4
        '.', // 245  f5
        '.', // 246  f6
        '.', // 247  f7
        '.', // 248  f8
        '.', // 249  f9
        '.', // 250  fa
        '.', // 251  fb
        '.', // 252  fc
        '.', // 253  fd
        '.', // 254  fe
        '.' // 255  ff
    };

    public static final String LINE_SEPARATOR = System.getProperty("line.separator", "\n");

    public static final int MAX_DUMP_BYTES = 512;

    public static String hexDump(ByteBuffer... bb) {
        StringBuilder sb = new StringBuilder();
        for (ByteBuffer b : bb) {
            int pos = b.position();
            // The current position is set to zero
            b.rewind();

            byte[] ar = new byte[b.limit()];

            // While reading the current position is moved to the end
            b.get(ar);
            // The current position is set to the initial state
            b.position(pos);
            hexDump(sb, ar, 0, b.limit());
        }
        return sb.toString();
    }

    public static String hexDump(byte[] b) {
        StringBuilder sb = new StringBuilder();
        hexDump(sb, b);
        return sb.toString();
    }

    public static void hexDump(StringBuilder sb, ByteBuffer bb) {
        byte[] buffer = null;
        if (bb.hasArray()) {
            buffer = bb.array();
        } else {
            ByteBuffer t = ByteBuffer.allocate(bb.limit());
            t.put(bb);
            buffer = t.array();
        }
        hexDump(sb, buffer, 0, buffer.length);
    }

    public static void hexDump(StringBuilder sb, byte[] buffer) {
        hexDump(sb, buffer, 0, buffer.length);
    }

    public static void hexAppend(StringBuilder sb, ByteBuffer[] bb) {
        int size = 0;
        for (ByteBuffer b : bb) {
            size += b.limit();
        }
        ByteBuffer buf = ByteBuffer.allocate(size);
        for (ByteBuffer b : bb) {
            buf.put(b);
        }
        hexAppend(sb, buf);
    }

    public static void hexAppend(StringBuilder sb, ByteBuffer bb) {
        byte[] buffer = null;
        if (bb.hasArray()) {
            buffer = bb.array();
        } else {
            ByteBuffer t = ByteBuffer.allocate(bb.limit());
            t.put(bb);
            buffer = t.array();
        }
        hexAppend(sb, buffer);
    }

    public static void hexAppend(StringBuilder sb, byte[] buffer) {
        if (sb == null || buffer == null) {
            return;
        }
        int end = buffer.length;
        boolean truncated = false;
        if (end > MAX_DUMP_BYTES) {
            end = MAX_DUMP_BYTES;
            truncated = true;
        }
        for (int i = 0; i < end; i++) {
            sb.append(HEX_DUMP_CHARS[UnsignedUtil.uint8ToShort(buffer[i])]);
        }
        if (truncated) {
            sb.append("***");
        }
    }

    public static void hexDump(StringBuilder sb, byte[] buffer, int start, int end) {
        if (sb == null || buffer == null) {
            return;
        }

        if ((end - start) > MAX_DUMP_BYTES) {
            end = start + MAX_DUMP_BYTES;
            sb.append("DUMPING MAX_DUMP_BYTES: " + MAX_DUMP_BYTES).append(LINE_SEPARATOR);
        }

        final int BLOCK_SIZE = 4;
        final int BYTES_PER_LINE = 16;

        for (int i = start, byteNo = 0; i < end; i += BYTES_PER_LINE, byteNo += BYTES_PER_LINE) {
            int j;
            appendUnsignedStringValue(sb, byteNo, 6, ' ');
            sb.append(":   ");

            for (j = 0; j < BYTES_PER_LINE; ++j) {
                if ((i + j) < end) {
                    appendHexByteValue(sb, buffer[i + j]);
                } else {
                    sb.append("  ");
                }

                if (j % BLOCK_SIZE == 3) {
                    sb.append(" ");
                }
            }
            sb.append("    |");
            for (j = 0; j < BYTES_PER_LINE; ++j) {
                if ((i + j) < end) {
                    sb.append(HEX_DUMP_CHARS[UnsignedUtil.uint8ToShort(buffer[i + j])]);
                } else {
                    sb.append(' ');
                }
            }
            sb.append("|").append(LINE_SEPARATOR);
        }
    }

    private static boolean appendUnsignedStringValue(
            StringBuilder sb, int value, int width, char padding) {
        for (int i = 0; i < width; ++i) {
            sb.append(padding);
        }

        if (value > Limits.MAX_UNSIGNED_SHORT || value < 0) {
            return false;
        }

        for (int idx = sb.length() - 1; width > 0; --width, --idx) {
            sb.setCharAt(idx, Character.forDigit(value % 10, 10));
            value /= 10;

            if (value == 0) break;
        }

        return true;
    }

    private static boolean appendHexByteValue(StringBuilder sb, byte byteVal) {
        int width = 2;
        for (int i = 0; i < width; ++i) {
            sb.append('0');
        }

        short unsignedShortVal = UnsignedUtil.uint8ToShort(byteVal);
        String hexByteStr = Integer.toHexString(unsignedShortVal);
        for (int srcPos = (hexByteStr.length() - 1), dstPos = sb.length() - 1;
                srcPos >= 0;
                --srcPos, --dstPos) {
            sb.setCharAt(dstPos, hexByteStr.charAt(srcPos));
        }

        return true;
    }

    private PrintUtil() {
        throw new IllegalStateException("Utility class");
    }
}
