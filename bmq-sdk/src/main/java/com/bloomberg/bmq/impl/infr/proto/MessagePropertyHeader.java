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

public final class MessagePropertyHeader {
    // This class represents the header for a message property in a PUT or
    // PUSH message.

    // MessagePropertyHeader datagram [6 bytes]:
    // ..
    //   +---------------+---------------+---------------+---------------+
    //   |0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
    //   +---------------+---------------+---------------+---------------+
    //   |R|PropType | PropValueLenUpper |       PropValueLenLower       |
    //   +---------------+---------------+---------------+---------------+
    //   |   R2  |     PropNameLen       |
    //   +---------------+---------------+
    //       R...: Reserved
    //       R2..: Reserved (2nd set of bits)
    //
    //  PropType...........: Data type of the message property
    //  PropValueLenUpper..: Upper 10 bits of the field capturing length of the
    //                       property value.
    //  PropValueLenLower..: Lower 16 bits of the field capturing length of the
    //                       property value.
    //  PropNameLen........: Length of the property name.
    //  Reserved...........: For alignment and extension ~ must be 0
    // ..
    //
    // This datagram must be 2-byte aligned.  See comments in
    // 'MessagePropertiesHeader' for details.

    // PRIVATE CONSTANTS
    private static final int RESERVED1_NUM_BITS = 1;
    private static final int PROP_TYPE_NUM_BITS = 5;
    private static final int PROP_VALUE_LEN_UPPER_NUM_BITS = 10;
    private static final int PROP_VALUE_LEN_LOWER_NUM_BITS = 16;
    private static final int PROP_VALUE_LEN_NUM_BITS =
            PROP_VALUE_LEN_UPPER_NUM_BITS + PROP_VALUE_LEN_LOWER_NUM_BITS;
    private static final int RESERVED2_NUM_BITS = 4;
    private static final int PROP_NAME_LEN_NUM_BITS = 12;

    private static final int RESERVED1_START_IDX = 15;
    private static final int PROP_TYPE_START_IDX = 10;
    private static final int PROP_VALUE_LEN_UPPER_START_IDX = 0;
    private static final int RESERVED2_START_IDX = 12;
    private static final int PROP_NAME_LEN_START_IDX = 0;

    private static final int PROP_TYPE_MASK =
            BitUtil.oneMask(PROP_TYPE_START_IDX, PROP_TYPE_NUM_BITS);

    private static final int PROP_VALUE_LEN_UPPER_MASK =
            BitUtil.oneMask(PROP_VALUE_LEN_UPPER_START_IDX, PROP_VALUE_LEN_UPPER_NUM_BITS);

    private static final int PROP_NAME_LEN_MASK =
            BitUtil.oneMask(PROP_NAME_LEN_START_IDX, PROP_NAME_LEN_NUM_BITS);

    // DATA
    private short propTypeAndPropValueLenUpper;
    private short propValueLenLower;
    private short reservedAndPropNameLen;

    // PUBLIC CONSTANTS
    public static final int MAX_PROPERTY_VALUE_LENGTH = (1 << PROP_VALUE_LEN_NUM_BITS) - 1;

    public static final int MAX_PROPERTY_NAME_LENGTH = (1 << PROP_NAME_LEN_NUM_BITS) - 1;

    public static final int HEADER_SIZE = 6;

    public MessagePropertyHeader() {}

    public void setPropertyType(int value) {
        Argument.expectNonNegative(value, "value");
        Argument.expectNotGreater(value, (1 << PROP_TYPE_NUM_BITS) - 1, "value");

        propTypeAndPropValueLenUpper =
                (short)
                        ((propTypeAndPropValueLenUpper & PROP_VALUE_LEN_UPPER_MASK)
                                | (value << PROP_TYPE_START_IDX));
    }

    // TODO: rename to offset after 2nd rollout of "new style" brokers
    public void setPropertyValueLength(int value) {
        Argument.expectNonNegative(value, "value");
        Argument.expectNotGreater(value, MAX_PROPERTY_VALUE_LENGTH, "value");

        propValueLenLower = (short) (value & 0xFFFF);

        propTypeAndPropValueLenUpper =
                (short)
                        ((propTypeAndPropValueLenUpper & PROP_TYPE_MASK)
                                | ((value >>> PROP_VALUE_LEN_LOWER_NUM_BITS)
                                        & PROP_VALUE_LEN_UPPER_MASK));
    }

    public void setPropertyNameLength(int value) {
        Argument.expectNonNegative(value, "value");
        Argument.expectNotGreater(value, MAX_PROPERTY_NAME_LENGTH, "value");

        reservedAndPropNameLen = (short) (value & PROP_NAME_LEN_MASK);
    }

    // ACCESSORS
    public int propertyType() {
        int result = (int) propTypeAndPropValueLenUpper & 0xFFFF;
        result &= PROP_TYPE_MASK;
        return result >>> PROP_TYPE_START_IDX;
    }

    // TODO: rename to offset after 2nd rollout of "new style" brokers
    public int propertyValueLength() {
        int result =
                (propTypeAndPropValueLenUpper & PROP_VALUE_LEN_UPPER_MASK)
                        << PROP_VALUE_LEN_LOWER_NUM_BITS;
        result |= ((int) propValueLenLower & 0xFFFF);
        return result;
    }

    public int propertyNameLength() {
        int result = (int) reservedAndPropNameLen & 0xFFFF;
        return result & PROP_NAME_LEN_MASK;
    }

    public void streamIn(DataInput input, int size) throws IOException {
        if (size < HEADER_SIZE) {
            throw new IOException("Invalid size: " + size);
        }

        propTypeAndPropValueLenUpper = input.readShort();
        propValueLenLower = input.readShort();
        reservedAndPropNameLen = input.readShort();

        final int ptype = propertyType();

        if (!PropertyType.isValid(ptype)) {
            throw new IOException("Invalid property type: [" + ptype + "]");
        }

        final int propNameLen = propertyNameLength();
        final int propValueLen = propertyValueLength();

        if (MAX_PROPERTY_NAME_LENGTH < propNameLen) {
            throw new IOException("Invalid property name length: [" + propNameLen + "]");
        }

        if (MAX_PROPERTY_VALUE_LENGTH < propValueLen) {
            throw new IOException("Invalid property value length: [" + propValueLen + "]");
        }

        // Skip unknown bytes
        if (size > HEADER_SIZE) {
            final int numToSkip = size - HEADER_SIZE;
            if (input.skipBytes(numToSkip) != numToSkip) {
                throw new IOException("Failed to skip " + numToSkip + " bytes.");
            }
        }
    }

    public void streamOut(DataOutput output) throws IOException {
        output.writeShort(propTypeAndPropValueLenUpper);
        output.writeShort(propValueLenLower);
        output.writeShort(reservedAndPropNameLen);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ MessagePropertyHeader [")
                .append(" PropertyType=")
                .append(propertyType())
                .append(" PropertyValueLength=")
                .append(propertyValueLength())
                .append(" PropertyNameLength=")
                .append(propertyNameLength())
                .append(" ] ]");
        return sb.toString();
    }
}
