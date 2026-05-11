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

import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.proto.intf.Streamable;
import com.bloomberg.bmq.impl.infr.util.BitUtil;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AckHeader implements Streamable {
    // This class represents header for an 'ACK' event.  An 'ACK' event is the
    // event sent by the broker to a client in response to a post message on
    // queue.  Such event is optional, depending on flags used at queue open.

    // AckHeader structure datagram [4 bytes (followed by one or multiple
    //                                            AckMessageImpl)]:
    // ..
    //   +---------------+---------------+---------------+---------------+
    //   |0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
    //   +---------------+---------------+---------------+---------------+
    //   |  HW   |  PMW  |     Flags     |           Reserved            |
    //   +---------------+---------------+---------------+---------------+
    //       HW..: HeaderWords
    //       PMW.: PerMessageWords
    //       R...: Reserved
    //
    //  HeaderWords (HW)......: Total size (words) of this AckHeader
    //  PerMessageWords (PMW).: Size (words) used for each AckMessageImpl in the
    //                          payload following this AckHeader
    //  Flags.................: bitmask of flags specifying this header
    //                          see AckHeaderFlags struct
    //  Reserved..............: For alignment and extension ~ must be 0
    // ..

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private byte headerWordsAndPerMsgWords;
    // Total size (words) of this header and number of words of
    // each ConfirmMessage in the payload that follows.

    private byte flags;
    // Bitmask of flags.

    private byte[] reserved;
    // Reserved

    // PRIVATE CONSTANTS
    private static final int HEADER_RESERVED_BYTES = 2;
    private static final int HEADER_WORDS_NUM_BITS = 4;
    private static final int PER_MSG_WORDS_NUM_BITS = 4;

    private static final int HEADER_WORDS_START_IDX = 4;
    private static final int PER_MSG_WORDS_START_IDX = 0;

    private static final int HEADER_WORDS_MASK =
            BitUtil.oneMask(HEADER_WORDS_START_IDX, HEADER_WORDS_NUM_BITS);

    private static final int PER_MSG_WORDS_MASK =
            BitUtil.oneMask(PER_MSG_WORDS_START_IDX, PER_MSG_WORDS_NUM_BITS);

    public static final int HEADER_SIZE = 4;
    // Current size (bytes) of the header.

    public static final int MAX_HEADER_SIZE =
            ((1 << HEADER_WORDS_NUM_BITS) - 1) * Protocol.WORD_SIZE;
    // Maximum size (bytes) of an 'AckHeader'.

    public static final int MAX_PERMESSAGE_SIZE =
            ((1 << PER_MSG_WORDS_NUM_BITS) - 1) * Protocol.WORD_SIZE;
    // Maximum size (bytes) of an 'AckMessageImpl'.

    public static final int MIN_HEADER_SIZE = 1;
    // Minimum size (bytes) of a 'AckHeader' (which is sufficient to
    // capture header words).  This value should *never* change.

    public AckHeader() {
        reserved = new byte[HEADER_RESERVED_BYTES];
        setHeaderWords((byte) (HEADER_SIZE / Protocol.WORD_SIZE));
        setPerMessageWords((byte) (AckMessageImpl.MESSAGE_SIZE / Protocol.WORD_SIZE));
    }

    public void setPerMessageWords(byte value) {
        headerWordsAndPerMsgWords =
                (byte)
                        ((headerWordsAndPerMsgWords & HEADER_WORDS_MASK)
                                | (value & PER_MSG_WORDS_MASK));
    }

    public void setHeaderWords(byte value) {
        headerWordsAndPerMsgWords =
                (byte)
                        ((headerWordsAndPerMsgWords & PER_MSG_WORDS_MASK)
                                | (value << HEADER_WORDS_START_IDX));
    }

    public void setFlags(byte value) {
        flags = value;
    }

    public int perMessageWords() {
        return headerWordsAndPerMsgWords & PER_MSG_WORDS_MASK;
    }

    public int headerWords() {
        return (headerWordsAndPerMsgWords & HEADER_WORDS_MASK) >>> HEADER_WORDS_START_IDX;
    }

    public byte flags() {
        return flags;
    }

    @Override
    public void streamIn(ByteBufferInputStream bbis) throws IOException {
        headerWordsAndPerMsgWords = bbis.readByte();
        flags = bbis.readByte();
        for (int i = 0; i < HEADER_RESERVED_BYTES; i++) {
            reserved[i] = bbis.readByte();
        }

        if (headerWords() * Protocol.WORD_SIZE > HEADER_SIZE) {
            // Read and ignore bytes that we don't know in the header.

            int numExtraBytes = headerWords() * Protocol.WORD_SIZE - HEADER_SIZE;
            byte[] headerExtra = new byte[numExtraBytes];
            int readBytes = bbis.read(headerExtra);
            assert readBytes == numExtraBytes;
        }
    }

    public void streamOut(ByteBufferOutputStream bbos) throws IOException {
        bbos.writeByte(headerWordsAndPerMsgWords);
        bbos.writeByte(flags);
        for (int i = 0; i < HEADER_RESERVED_BYTES; i++) {
            bbos.writeByte(reserved[i]);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ AckHeader [")
                .append(" PerMessageWords=")
                .append(perMessageWords())
                .append(" HeaderWords=")
                .append(headerWords())
                .append(" Flags=")
                .append(flags())
                .append(" ] ]");
        return sb.toString();
    }
}
