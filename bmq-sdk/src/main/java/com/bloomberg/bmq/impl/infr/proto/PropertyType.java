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

import javax.annotation.concurrent.Immutable;

@Immutable
public enum PropertyType {
    UNDEFINED(0),
    BOOL(1),
    BYTE(2),
    SHORT(3),
    INT32(4),
    INT64(5),
    STRING(6),
    BINARY(7);

    private final int id;

    PropertyType(int id) {
        this.id = id;
    }

    public int toInt() {
        return id;
    }

    public static boolean isValid(int value) {
        return ((value >= BOOL.toInt()) && (value <= BINARY.toInt()));
    }

    public static PropertyType fromInt(int i) {
        for (PropertyType t : PropertyType.values()) {
            if (t.toInt() == i) return t;
        }
        return UNDEFINED;
    }
}
