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
 * Interface to manipulate with message properties.
 *
 * <p>Allows to set a property with the specified {@code name} having the specified {@code value}
 * with the corresponding data type.
 *
 * <p>Note that if a property with the {@code name} exists, it will be updated with {@code value}.
 */
public interface MessageProperties {

    /**
     * Sets a property with the specified {@code name} having the specified {@code value} of the
     * type {@code boolean}.
     *
     * @param name property name
     * @param value property value
     * @return boolean {@code true} on success, and {@code false} in case of failure.
     */
    boolean setPropertyAsBool(String name, boolean value);

    /**
     * Sets a property with the specified {@code name} having the specified {@code value} of the
     * type {@code byte}.
     *
     * @param name property name
     * @param value property value
     * @return boolean {@code true} on success, and {@code false} in case of failure.
     */
    boolean setPropertyAsByte(String name, byte value);

    /**
     * Sets a property with the specified {@code name} having the specified {@code value} of the
     * type {@code short}.
     *
     * @param name property name
     * @param value property value
     * @return boolean {@code true} on success, and {@code false} in case of failure.
     */
    boolean setPropertyAsShort(String name, short value);

    /**
     * Sets a property with the specified {@code name} having the specified {@code value} of the
     * type {@code int}.
     *
     * @param name property name
     * @param value property value
     * @return boolean {@code true} on success, and {@code false} in case of failure.
     */
    boolean setPropertyAsInt32(String name, int value);

    /**
     * Sets a property with the specified {@code name} having the specified {@code value} of the
     * type {@code long}.
     *
     * @param name property name
     * @param value property value
     * @return boolean {@code true} on success, and {@code false} in case of failure.
     */
    boolean setPropertyAsInt64(String name, long value);

    /**
     * Sets a property with the specified {@code name} having the specified {@code value} of the
     * type {@code String}.
     *
     * <p>The value can be at most 64 MiB - 1 characters long.
     *
     * @param name property name
     * @param value property value
     * @return boolean {@code true} on success, and {@code false} in case of failure.
     * @throws IllegalArgumentException if the value is too long
     */
    boolean setPropertyAsString(String name, String value);

    /**
     * Sets a property with the specified {@code name} having the specified {@code value} as an
     * array of bytes.
     *
     * <p>The value can be at most 64 MiB - 1 bytes long.
     *
     * @param name property name
     * @param value property value
     * @return boolean {@code true} on success, and {@code false} in case of failure.
     * @throws IllegalArgumentException if the value is too long
     */
    boolean setPropertyAsBinary(String name, byte[] value);
}
