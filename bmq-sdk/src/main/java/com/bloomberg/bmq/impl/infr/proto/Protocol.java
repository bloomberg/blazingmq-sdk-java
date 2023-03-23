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

public class Protocol {
    // Version of the protocol
    public static final int VERSION = 1;

    // Number of bytes per word
    public static final int WORD_SIZE = 4;

    // Number of bytes per dword
    public static final int DWORD_SIZE = 8;

    // Minimum size (bytes) of a packet, needed to know what
    // its real size is.  Must NEVER be changed; used when
    // receiving a packet on the socket.
    public static final int PACKET_MIN_SIZE = 4;

    // Threshold below which PUT's message application data
    // will not be compressed regardless of the compression
    // algorithm type set to the PutMessageImpl
    public static final int COMPRESSION_MIN_APPDATA_SIZE = 1024; // 1KiB

    private Protocol() {
        throw new IllegalStateException("Utility class");
    }
}
