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

/**
 * Represents a message property, which is a name-value pair. The value can be of different sizes
 * depending of the property type.
 */
public interface MessageProperty {
    /**
     * Enum for supported data types for a message property.
     *
     * <p>This table describes the size of each data type:
     *
     * <table border="1">
     *  <caption>Data type sizes</caption>
     *  <tr>
     *   <td> Data Type </td> <td> Size (in bytes) </td>
     *  </tr>
     *  <tr>
     *   <td> BOOL </td> <td style="test-align: center;"> 1 </td>
     *  </tr>
     *  <tr>
     *   <td> BYTE </td> <td style="test-align: center;"> 1 </td>
     *  </tr>
     *  <tr>
     *   <td> SHORT </td> <td style="test-align: center;"> 2 </td>
     *  </tr>
     *  <tr>
     *   <td> INT </td> <td style="test-align: center;"> 4 </td>
     *  </tr>
     *  <tr>
     *   <td> INT64 </td> <td style="test-align: center;"> 8 </td>
     *  </tr>
     *  <tr>
     *   <td> STRING </td> <td style="test-align: center;"> variable </td>
     *  </tr>
     *  <tr>
     *   <td> BINARY </td> <td style="test-align: center;"> variable </td>
     *  </tr>
     * </table>
     *
     * <p>Note that the difference between 'BINARY' and 'STRING' data types is that the former
     * allows null ('\0') character while the later does not.
     */
    enum Type {
        UNDEFINED,
        BOOL,
        BYTE,
        SHORT,
        INT32,
        INT64,
        STRING,
        BINARY
    }

    /**
     * Returns this property type.
     *
     * @return Type property type
     */
    Type type();

    /**
     * Returns this property name.
     *
     * @return String property name
     */
    String name();

    /**
     * Returns this property value.
     *
     * @return Object property value
     */
    Object value();

    /** Represents a boolean message property. The value of this property has type BOOL. */
    interface BoolProperty extends MessageProperty {
        /**
         * Returns this property type, {@link Type#BOOL} by default.
         *
         * @return Type property type {@link Type#BOOL}
         */
        @Override
        default Type type() {
            return Type.BOOL;
        }

        /**
         * Returns this property value as {@code Boolean}.
         *
         * @return Boolean property value
         */
        @Override
        Boolean value();
    }

    /** Represents one byte message property. The value of this property has type BYTE. */
    interface ByteProperty extends MessageProperty {
        /**
         * Returns this property type, {@link Type#BYTE} by default.
         *
         * @return Type property type {@link Type#BYTE}
         */
        @Override
        default Type type() {
            return Type.BYTE;
        }

        /**
         * Returns this property value as {@code Byte}.
         *
         * @return Byte property value
         */
        @Override
        Byte value();
    }

    /** Represents message property which value has type SHORT. */
    interface ShortProperty extends MessageProperty {
        /**
         * Returns this property type, {@link Type#SHORT} by default.
         *
         * @return Type property type {@link Type#SHORT}
         */
        @Override
        default Type type() {
            return Type.SHORT;
        }

        /**
         * Returns this property value as {@code Short}.
         *
         * @return Short property value
         */
        @Override
        Short value();
    }

    /** Represents message property which value has type INT32. */
    interface Int32Property extends MessageProperty {
        /**
         * Returns this property type, {@link Type#INT32} by default.
         *
         * @return Type property type {@link Type#INT32}
         */
        @Override
        default Type type() {
            return Type.INT32;
        }

        /**
         * Returns this property value as {@code Integer}.
         *
         * @return Integer property value
         */
        @Override
        Integer value();
    }

    /** Represents message property which value has type INT64. */
    interface Int64Property extends MessageProperty {
        /**
         * Returns this property type, {@link Type#INT64} by default.
         *
         * @return Type property type {@link Type#INT64}
         */
        @Override
        default Type type() {
            return Type.INT64;
        }

        /**
         * Returns this property value as {@code Long}.
         *
         * @return Long property value
         */
        @Override
        Long value();
    }

    /** Represents message property which value has type STRING. */
    interface StringProperty extends MessageProperty {
        /**
         * Returns this property type, {@link Type#STRING} by default.
         *
         * @return Type property type {@link Type#STRING}
         */
        @Override
        default Type type() {
            return Type.STRING;
        }

        /**
         * Returns this property value as {@code String}.
         *
         * @return String property value
         */
        @Override
        String value();
    }

    /** Represents message property which value has type BINARY. */
    interface BinaryProperty extends MessageProperty {
        /**
         * Returns this property type, {@link Type#BINARY} by default.
         *
         * @return Type property type {@link Type#BINARY}
         */
        @Override
        default Type type() {
            return Type.BINARY;
        }

        /**
         * Returns this property value as byte array.
         *
         * @return byte[] property value
         */
        @Override
        byte[] value();
    }
}
