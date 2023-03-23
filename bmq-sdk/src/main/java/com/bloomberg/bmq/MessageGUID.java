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
package com.bloomberg.bmq;

import com.bloomberg.bmq.impl.infr.util.Argument;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import javax.annotation.concurrent.Immutable;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a value-semantic global unique identifier for BMQ messages.
 *
 * <p>{@code MessageGUID} provides a value-semantic global unique identifier for BMQ messages. Each
 * message delivered to BMQ client from BMQ broker contains a unique {@code MessageGUID}.
 *
 * <H2>Externalization</H2>
 *
 * For convenience, this class provides {@code toHex} method that can be used to externalize a
 * {@code MessageGUID} instance. Applications can persist the resultant buffer (on filesystem, in
 * database) to keep track of last processed message ID across task instantiations. {@code fromHex}
 * method can be used to convert a valid externalized buffer back to a message ID.
 *
 * <H2>Example 1: Externalizing</H2>
 *
 * Below, {@code msg} is a valid instance of {@link com.bloomberg.bmq.AckMessage} obtained from an
 * instance of {@link com.bloomberg.bmq.AbstractSession} via {@link
 * com.bloomberg.bmq.AckMessageHandler}:
 *
 * <pre>
 *
 *  MessageGUID g1 = msg.messageGUID();
 *
 *  String hex1 = g1.toHex();
 *
 *  assert MessageGUID.isValidHexRepresentation(hex1.getBytes());
 *
 *  MessageGUID g2 = MessageGUID.fromHex(hex1);
 *
 *  assert g1.equals(g2);
 * </pre>
 */
@Immutable
public class MessageGUID {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final byte[] buffer; // mutable, but not exposed field

    /** The size of a buffer needed to represent a GUID. */
    public static final int SIZE_BINARY = 16; // Binary format of the GUID

    /** The size of a hexadecimal string representation of the GUID. */
    public static final int SIZE_HEX = 2 * SIZE_BINARY;

    private MessageGUID() {
        buffer = new byte[SIZE_BINARY];
    }

    /**
     * Returns true if the specified {@code buffer} is a valid hex representation of a {@code
     * MessageGUID}. The the {@code buffer} should be equal to {@link #SIZE_HEX}.
     *
     * @param buffer Array of bytes that contrains hex representation the {@code MessageGUID}.
     * @return boolean true if the specified {@code buffer} is a valid hex representation, false
     *     otherwise.
     * @throws NullPointerException if the specified {@code buffer} is null
     */
    public static boolean isValidHexRepresentation(byte[] buffer) {
        if (buffer.length != SIZE_HEX) {
            logger.warn("Wrong buffer size: {}, expected {}", buffer.length, SIZE_HEX);
            return false;
        }

        for (int i = 0; i < SIZE_HEX; ++i) {
            if (!(buffer[i] >= '0' && buffer[i] <= '9')
                    && !(buffer[i] >= 'A' && buffer[i] <= 'F')) {
                logger.warn("Wrong character: {}", buffer[i]);
                return false; // RETURN
            }
        }
        return true;
    }

    /**
     * Writes a binary representation of this object to the specified {@code destination} which
     * length should be {@link #SIZE_BINARY}. Note that {@code destination} can later be used to
     * populate a {@code MessageGUID} object using {@link #fromBinary} method.
     *
     * @param destination byte array to store a binary representation of this GUID
     * @throws java.lang.IllegalArgumentException in case of the {@code destination} length is less
     *     than {@link #SIZE_BINARY}
     * @throws NullPointerException if the specified {@code destination} is null
     */
    public void toBinary(byte[] destination) {
        Argument.expectCondition(buffer.length <= destination.length, "Invalid destination buffer");
        System.arraycopy(buffer, 0, destination, 0, buffer.length);
    }

    /**
     * Returns a hex representation of this object as a {@code String}. Note that the returned
     * string can later be used to create a MessageGUID object using {@link fromHex} method.
     *
     * @return String hex representation of this object
     */
    public String toHex() {
        return Hex.encodeHexString(buffer, false);
    }

    /**
     * Returns a new {@code MessageGUID} created from the specified {@code buffer}, in binary
     * format. The {@code buffer} length should be {@link #SIZE_BINARY}.
     *
     * @param buffer binary representation of the GUID
     * @return MessageGUID newly created {@code MessageGUID} object
     * @throws java.lang.IllegalArgumentException in case of the {@code buffer} length is less than
     *     {@link #SIZE_BINARY}
     * @throws NullPointerException if the specified {@code buffer} is null
     */
    public static MessageGUID fromBinary(byte[] buffer) {
        MessageGUID messageGUID = new MessageGUID();
        Argument.expectCondition(
                messageGUID.buffer.length <= buffer.length, "Invalid source buffer");
        System.arraycopy(buffer, 0, messageGUID.buffer, 0, messageGUID.buffer.length);
        return messageGUID;
    }

    /**
     * Returns a new {@code MessageGUID} created from the specified {@code String} contained
     * hexadecimal representation of the GUID.
     *
     * @param val string with a hexadecimal representation of the GUID
     * @return MessageGUID newly created {@code MessageGUID} object
     * @throws java.lang.IllegalArgumentException in case of the specified string is not a valid
     *     hexadecimal representation of the GUID, or if error happens during string decoding
     * @throws NullPointerException if the specified {@code val} is null
     */
    public static MessageGUID fromHex(String val) {
        Argument.expectCondition(
                isValidHexRepresentation(val.getBytes(StandardCharsets.US_ASCII)),
                "Invalid hex value");
        MessageGUID messageGUID = new MessageGUID();
        try {
            System.arraycopy(
                    Hex.decodeHex(val), 0, messageGUID.buffer, 0, messageGUID.buffer.length);
        } catch (DecoderException e) {
            throw new IllegalArgumentException("HEX decoding error: " + e);
        }
        return messageGUID;
    }

    /**
     * Returns a zero filled {@code MessageGUID} object.
     *
     * @return MessageGUID zero message GUID
     */
    public static MessageGUID createEmptyGUID() {
        return new MessageGUID();
    }

    /**
     * Returns a hex representation of this object as a {@code String}.
     *
     * @return String hex representation of this object
     */
    @Override
    public String toString() {
        return toHex();
    }

    /**
     * Returns true if this GUID is equal to the specified {@code other}, false otherwise. Two GUIDs
     * are equal if they have equal binary representations.
     *
     * @param other GUID object to compare with
     * @return boolean true if GUIDs are equal, false otherwise
     */
    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (!(other instanceof MessageGUID)) {
            return false;
        }
        MessageGUID otherGuid = (MessageGUID) other;
        return Arrays.equals(otherGuid.buffer, buffer);
    }

    /**
     * Returns a hash code value for this {@code MessageGUID} object.
     *
     * @return int a hash code value for this object.
     */
    @Override
    public int hashCode() {
        return Arrays.hashCode(buffer);
    }
}
