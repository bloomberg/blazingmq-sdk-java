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
package com.bloomberg.bmq.impl.infr.proto;

import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.util.Argument;
import java.io.IOException;

public class ProtocolUtil {

    private static final byte[][] PADDING =
            new byte[][] {
                {0, 0, 0, 0},
                {1, 1, 1, 1},
                {2, 2, 2, 2},
                {3, 3, 3, 3},
                {4, 4, 4, 4}
            };

    public static int calculateNumWords(int payloadLen) {
        return (payloadLen + Protocol.WORD_SIZE) / Protocol.WORD_SIZE;
    }

    public static int calculatePadding(int payloadLen) {
        int numWords = calculateNumWords(payloadLen);
        return numWords * Protocol.WORD_SIZE - payloadLen;
    }

    public static byte[] getPaddingBytes(int padding) {
        Argument.expectCondition(padding >= 1 && padding <= 4, "Wrong padding: ", padding);

        // Note that the length of returned array is always 4.  The caller
        // needs to ensure reading appropriate number of bytes from the array.
        return PADDING[padding];
    }

    public static byte getPadding(ByteBufferInputStream bbis, int size) throws IOException {
        Argument.expectNonNull(bbis, "input stream");
        Argument.expectPositive(size, "size");
        Argument.expectCondition(
                size <= bbis.available(), "Input stream has invalid size: ", bbis.available());

        final int pos = bbis.position();

        final int numToSkip = size - 1;
        if (bbis.skip(numToSkip) != numToSkip) {
            throw new IOException("Failed to skip " + numToSkip + " bytes");
        }

        final byte numPaddingBytes = bbis.readByte();

        // Get padding buffer just to check that we have a valid
        // number of the padding bytes
        ProtocolUtil.getPaddingBytes(numPaddingBytes);

        if (size < numPaddingBytes) {
            throw new IOException(
                    "Wrong binary data and padding sizes: " + size + " " + numPaddingBytes);
        }

        // Reset initial position
        bbis.reset();

        if (bbis.skip(pos) != pos) {
            throw new IOException("Failed to skip " + pos + " bytes");
        }

        if (pos != bbis.position()) {
            throw new IOException("Failed to reset the stream to initial position");
        }

        return numPaddingBytes;
    }

    public static int calculateUnpaddedLength(ByteBufferInputStream bbis, int size)
            throws IOException {
        // Get padding bytes
        final byte numPaddingBytes = getPadding(bbis, size);
        // Cut off padding bytes from the end
        final int unpaddedLength = size - numPaddingBytes;

        if (unpaddedLength < 0) {
            throw new IOException("Invalid unpadded length: " + unpaddedLength);
        }

        // Verify padding bytes
        final int numPaddingBytesExp = calculatePadding(unpaddedLength);

        if (numPaddingBytes != numPaddingBytesExp) {
            throw new IOException(
                    "Unexpected padding: " + numPaddingBytes + ", should be " + numPaddingBytesExp);
        }

        return unpaddedLength;
    }

    private ProtocolUtil() {
        throw new IllegalStateException("Utility class");
    }
}
