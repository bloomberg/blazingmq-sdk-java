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
import com.bloomberg.bmq.impl.infr.util.PrintUtil;
import java.util.Arrays;

public abstract class MessageProperty {
    private String propertyName;
    private PropertyType propertyType;
    private byte[] propertyValue;
    private int maxValueSize; // in bytes

    protected MessageProperty(PropertyType type, int maxValueSize) {
        propertyType = type;
        this.maxValueSize = maxValueSize / Byte.SIZE;
    }

    public void setPropertyName(String name) {
        propertyName = name;
    }

    public final void setPropertyValue(byte[] val) {
        Argument.expectNonNull(val, "val");
        Argument.expectNotGreater(val.length, maxValueSize, "property length");
        propertyValue = Arrays.copyOf(val, val.length);
    }

    public String name() {
        return propertyName;
    }

    public PropertyType type() {
        return propertyType;
    }

    public int valueLength() {
        return propertyValue.length;
    }

    public abstract Object getValue();

    public abstract void dispatch(MessagePropertyHandler handler);

    public boolean getValueAsBool() {
        throw new IllegalStateException("Invalid property type 'bool'");
    }

    public byte getValueAsByte() {
        throw new IllegalStateException("Invalid property type 'byte'");
    }

    public short getValueAsShort() {
        throw new IllegalStateException("Invalid property type 'short'");
    }

    public int getValueAsInt32() {
        throw new IllegalStateException("Invalid property type 'int32'");
    }

    public long getValueAsInt64() {
        throw new IllegalStateException("Invalid property type 'int64'");
    }

    public String getValueAsString() {
        throw new IllegalStateException("Invalid property type 'string'");
    }

    public byte[] getValueAsBinary() {
        throw new IllegalStateException("Invalid property type 'binary'");
    }

    byte[] value() {
        if (propertyType == null) {
            throw new IllegalStateException("Empty value");
        }
        return propertyValue;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ MessageProperty [")
                .append(" Type=")
                .append(propertyType)
                .append(" Name=")
                .append(propertyName)
                .append(" Value=");

        if (propertyValue != null) {
            switch (propertyType) {
                case STRING:
                case BINARY:
                    sb.append("\"");
                    PrintUtil.hexAppend(sb, propertyValue);
                    sb.append("\"");
                    break;
                default:
                    // expect that getValue() returns non-null value since 'propertyValue' is not
                    // null
                    sb.append(getValue().toString());
            }
        } else {
            sb.append("EMPTY");
        }
        sb.append(" ] ]");
        return sb.toString();
    }
}
