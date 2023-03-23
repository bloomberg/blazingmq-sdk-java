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

import javax.annotation.concurrent.Immutable;

/**
 * Represents all the flags that can be used at {@link com.bloomberg.bmq.AbstractSession#getQueue}.
 * Each value of the enum correspond to a bit of a bit-mask integer. It also exposes a set of
 * utilities to manipulate such bit-mask value.
 */
@Immutable
public enum QueueFlags {

    /** The queue is opened in admin mode (valid only for BlazingMQ admin tasks). */
    ADMIN(1 << 0),

    /** The queue is opened for consuming messages. */
    READ(1 << 1),

    /** The queue is opened for posting messages. */
    WRITE(1 << 2),

    /** Set to indicate interested in receiving {@code ACK} events for all message posted */
    ACK(1 << 3),
    EMPTY(0);

    private final int id;

    QueueFlags(int id) {
        this.id = id;
    }

    /**
     * Returns integer representation of this enum value.
     *
     * @return int this enum value as an integer
     */
    public int toInt() {
        return id;
    }

    /**
     * Sets the specified {@code flags} representation for reader.
     *
     * @param flags initial flags value to modify
     * @return long resulting flags value
     */
    public static long setReader(long flags) {
        return flags | READ.toInt();
    }

    /**
     * Sets the specified {@code flags} representation for admin.
     *
     * @param flags initial flags value to modify
     * @return long resulting flags value
     */
    public static long setAdmin(long flags) {
        return flags | ADMIN.toInt();
    }

    /**
     * Sets the specified {@code flags} representation for writer.
     *
     * @param flags initial flags value to modify
     * @return long resulting flags value
     */
    public static long setWriter(long flags) {
        return flags | WRITE.toInt();
    }

    /**
     * Sets the specified {@code flags} representation for ack.
     *
     * @param flags initial flags value to modify
     * @return long resulting flags value
     */
    public static long setAck(long flags) {
        return flags | ACK.toInt();
    }

    /**
     * Returns {@code true} if the specified {@code flags} represent a reader.
     *
     * @param flags flags value to check
     * @return boolean {@code true} if the reader bit is set, {@code false} otherwise
     */
    public static boolean isReader(long flags) {
        return ((READ.toInt() & flags) != 0);
    }

    /**
     * Returns {@code true} if the specified {@code flags} represent an admin.
     *
     * @param flags flags value to check
     * @return boolean {@code true} if the admin bit is set, {@code false} otherwise
     */
    public static boolean isAdmin(long flags) {
        return ((ADMIN.toInt() & flags) != 0);
    }

    /**
     * Returns {@code true} if the specified {@code flags} represent a writer.
     *
     * @param flags flags value to check
     * @return boolean {@code true} if the writer bit is set, {@code false} otherwise
     */
    public static boolean isWriter(long flags) {
        return ((WRITE.toInt() & flags) != 0);
    }

    /**
     * Returns {@code true} if the specified {@code flags} represent ack required.
     *
     * @param flags flags value to check
     * @return boolean {@code true} if the ack required bit is set, {@code false} otherwise
     */
    public static boolean isAck(long flags) {
        return ((ACK.toInt() & flags) != 0);
    }
}
