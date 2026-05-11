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

import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.infr.util.BitUtil;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public final class MessagePropertiesHeader {
    // This class represents the header for message properties area in a PUT
    // or PUSH message.  This header will be followed by one or more message
    // properties.

    // MessagePropertiesHeader structure datagram [6 bytes]:
    // ..
    //   +---------------+---------------+---------------+---------------+
    //   |0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
    //   +---------------+---------------+---------------+---------------+
    //   | R |PHS2X| HS2X|   MPAW Upper  |          MPAW Lower           |
    //   +---------------+---------------+---------------+---------------+
    //   |   Reserved    |    NumProps   |
    //   +---------------+---------------+
    //
    //       R......: Reserved ~ must be 0
    //       PHS2X..: 'MessagePropertyHeader' size as 2byte multiple.
    //       HS2X...: Header size as 2byte multiple.
    //       MPAW...: Message properties area words
    //
    //  MessagePropertyHeader size..: Size (as a multiple of 2) of a
    //                                'MessagePropertyHeader'.  This size is
    //                                captured in this struct instead of
    //                                'MessagePropertyHeader' itself so as
    //                                to keep later's size to a minimum.  Also
    //                                note that this size is captured as a
    //                                multiple of 2, not 4, because the size of
    //                                'MessagePropertyHeader' may not be a
    //                                multiple of 4.
    //  Header Size.................: Total size (as a multiple of 2) of this
    //                                header.  Note that this size is captured
    //                                as a multiple of 2, not 4, because its
    //                                size may not be a multiple of 4.
    //  MessagePropertiesAreaWords..: Total size (words) of the message
    //                                properties area, including this
    //                                'MessagePropertiesHeader', including any
    //                                other sub-header, and including all
    //                                message properties with any necessary
    //                                padding.
    //  Num Properties (NumProps)...: Total number of properties in the message
    // ..
    //
    // Alignment requirement: 2 bytes.
    //
    // Following this header, one or more message properties will be present.
    // The layout of the entire message properties area is described below:
    // ..
    //  [MessagePropertiesH][MessagePropertyH #1][MessagePropertyH #2]...
    //  [MessagePropertyH #n][PropName #1][PropValue #1][PropName #2]
    //  [PropValue #2]...[PropName #n][PropValue #n][word alignment padding]
    // ..
    //
    // To describe the layout in words, MessagePropertiesHeader is followed by
    // the MessagePropertyHeader's of *all* properties, which is then followed
    // by a sequence of pairs of (PropertyName, PropertyValue).
    //
    // Note that the alignment requirement for MessagePropertyHeader is 2-byte
    // boundary, and there is no alignment requirement on the property name or
    // value fields.  In order to avoid unnecessary padding after each pair of
    // property name and value (so as to fulfil the alignment requirement of
    // the succeeding MessagePropertyHeader), all MessagePropertyHeader's are
    // put first, a layout which doesn't any padding b/w the headers.
    //
    // Also note that the entire message properties area needs to be word
    // aligned, because of 'MessagePropertiesHeader.MessagePropertiesAreaWords'
    // field.  The payload following message properties area does not have any
    // alignment requirement though.

    // PRIVATE CONSTANTS
    private static final int MSG_PROPS_AREA_WORDS_LOWER_NUM_BITS = 16;
    private static final int MSG_PROPS_AREA_WORDS_UPPER_NUM_BITS = 8;
    private static final int MSG_PROPS_AREA_WORDS_NUM_BITS =
            MSG_PROPS_AREA_WORDS_LOWER_NUM_BITS + MSG_PROPS_AREA_WORDS_UPPER_NUM_BITS;
    private static final int NUM_PROPERTIES_NUM_BITS = 8;

    private static final int HEADER_SIZE_2X_NUM_BITS = 3;
    private static final int MPH_SIZE_2X_NUM_BITS = 3;
    private static final int RESERVED_NUM_BITS = 2;

    private static final int HEADER_SIZE_2X_START_IDX = 0;
    private static final int MPH_SIZE_2X_START_IDX = 3;

    private static final int HEADER_SIZE_2X_MASK =
            BitUtil.oneMask(HEADER_SIZE_2X_START_IDX, HEADER_SIZE_2X_NUM_BITS);

    private static final int MPH_SIZE_2X_MASK =
            BitUtil.oneMask(MPH_SIZE_2X_START_IDX, MPH_SIZE_2X_NUM_BITS);

    // DATA
    private byte mphSize2xAndHeaderSize2x;
    private byte msgPropsAreaWordsUpper;
    private short msgPropsAreaWordsLower;
    private byte reserved;
    private byte numProperties;

    // PUBLIC CLASS DATA
    public static final int MAX_MESSAGE_PROPERTIES_SIZE =
            ((1 << MSG_PROPS_AREA_WORDS_NUM_BITS) - 1) * Protocol.WORD_SIZE;
    // Maximum size (bytes)

    public static final int MAX_HEADER_SIZE = ((1 << HEADER_SIZE_2X_NUM_BITS) - 1) * 2;
    // Maximum size (bytes) of a 'MessagePropertiesHeader'.

    public static final int MIN_HEADER_SIZE = 1;
    // Minimum size (bytes) of a 'MessagePropertiesHeader' that is
    // sufficient to capture header words.  This value should *never*
    // change.

    public static final int MAX_MPH_SIZE = ((1 << MPH_SIZE_2X_NUM_BITS) - 1) * 2;
    // Maximum size (bytes) of a 'MessagePropertyHeader'.

    public static final int MAX_NUM_PROPERTIES = (1 << NUM_PROPERTIES_NUM_BITS) - 1;

    public static final int HEADER_SIZE = 6;

    public MessagePropertiesHeader() {
        setHeaderSize(HEADER_SIZE);
        int roundedSize = HEADER_SIZE + (HEADER_SIZE % Protocol.WORD_SIZE);
        setMessagePropertiesAreaWords(roundedSize / Protocol.WORD_SIZE);
        setMessagePropertyHeaderSize(MessagePropertyHeader.HEADER_SIZE);
    }

    public void setHeaderSize(int value) {
        value /= 2;

        Argument.expectNonNegative(value, "value");
        Argument.expectNotGreater(value, (1 << HEADER_SIZE_2X_NUM_BITS) - 1, "value/2");

        mphSize2xAndHeaderSize2x =
                (byte)
                        ((mphSize2xAndHeaderSize2x & MPH_SIZE_2X_MASK)
                                | (value & HEADER_SIZE_2X_MASK));
    }

    public void setMessagePropertiesAreaWords(int value) {
        Argument.expectNonNegative(value, "value");
        Argument.expectNotGreater(value, (1 << MSG_PROPS_AREA_WORDS_NUM_BITS) - 1, "value");

        msgPropsAreaWordsLower = (short) (value & 0xFFFF);
        msgPropsAreaWordsUpper = (byte) ((value >>> MSG_PROPS_AREA_WORDS_LOWER_NUM_BITS) & 0xFF);
    }

    public void setNumProperties(int value) {
        Argument.expectNonNegative(value, "value");
        Argument.expectNotGreater(value, MAX_NUM_PROPERTIES, "value");

        numProperties = (byte) value;
    }

    public void setMessagePropertyHeaderSize(int value) {
        value /= 2;

        Argument.expectNonNegative(value, "value");
        Argument.expectNotGreater(value, (1 << MPH_SIZE_2X_NUM_BITS) - 1, "value/2");

        mphSize2xAndHeaderSize2x =
                (byte)
                        ((mphSize2xAndHeaderSize2x & HEADER_SIZE_2X_MASK)
                                | (value << MPH_SIZE_2X_START_IDX));
    }

    public int headerSize() {
        return 2 * (mphSize2xAndHeaderSize2x & HEADER_SIZE_2X_MASK);
    }

    public int messagePropertyHeaderSize() {
        return 2 * ((mphSize2xAndHeaderSize2x & MPH_SIZE_2X_MASK) >>> MPH_SIZE_2X_START_IDX);
    }

    public int messagePropertiesAreaWords() {
        int result = (int) msgPropsAreaWordsLower & 0xFFFF;

        int upper = (int) msgPropsAreaWordsUpper & 0xFF;

        result |= (upper << MSG_PROPS_AREA_WORDS_LOWER_NUM_BITS);

        return result;
    }

    public int numProperties() {
        return numProperties;
    }

    public void streamIn(DataInput input) throws IOException {
        mphSize2xAndHeaderSize2x = input.readByte();
        msgPropsAreaWordsUpper = input.readByte();
        msgPropsAreaWordsLower = input.readShort();
        reserved = input.readByte();
        numProperties = input.readByte();

        final int mphSize = messagePropertyHeaderSize();
        final int numProps = numProperties();

        if (0 >= mphSize) {
            throw new IOException("Invalid MPH size");
        }

        if (0 >= numProps || MAX_NUM_PROPERTIES < numProps) {
            throw new IOException("Invalid number of properties");
        }

        // Skip unknown bytes
        if (headerSize() > HEADER_SIZE) {
            final int numToSkip = headerSize() - HEADER_SIZE;
            if (input.skipBytes(numToSkip) != numToSkip) {
                throw new IOException("Failed to skip " + numToSkip + " bytes.");
            }
        }
    }

    public void streamOut(DataOutput output) throws IOException {
        output.writeByte(mphSize2xAndHeaderSize2x);
        output.writeByte(msgPropsAreaWordsUpper);
        output.writeShort(msgPropsAreaWordsLower);
        output.writeByte(reserved);
        output.writeByte(numProperties);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[MessagePropertiesHeader]")
                .append("\n\t")
                .append("HeaderSize                 : ")
                .append(headerSize())
                .append("\n\t")
                .append("MessagePropertyHeaderSize  : ")
                .append(messagePropertyHeaderSize())
                .append("\n\t")
                .append("MessagePropertiesAreaWords : ")
                .append(messagePropertiesAreaWords())
                .append("\n\t")
                .append("NumProperties              : ")
                .append(numProperties())
                .append("\n");
        return sb.toString();
    }
}
