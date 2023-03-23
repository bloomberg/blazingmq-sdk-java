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

public enum PutHeaderFlags {
    ACK_REQUESTED(1 << 0), // Ack for PUT msg is requested
    MESSAGE_PROPERTIES(1 << 1), // Contains message properties
    UNUSED3(1 << 2),
    UNUSED4(1 << 3);

    private int id;

    PutHeaderFlags(int id) {
        this.id = id;
    }

    public int toInt() {
        return id;
    }

    public static PutHeaderFlags fromInt(int i) {
        for (PutHeaderFlags f : PutHeaderFlags.values()) {
            if (f.toInt() == i) return f;
        }
        return null;
    }

    public static int setFlag(int i, PutHeaderFlags flag) {
        return i | flag.toInt();
    }

    public static int unsetFlag(int i, PutHeaderFlags flag) {
        return i & ~flag.toInt();
    }

    public static boolean isSet(int i, PutHeaderFlags flag) {
        return ((flag.toInt() & i) != 0);
    }
}
