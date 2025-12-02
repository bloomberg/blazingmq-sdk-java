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
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.infr.util.BitUtil;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OptionHeader implements Streamable {
    // This class represents the header for an option.  In a typical
    // implementation usage, every Option class will start by an
    // 'OptionHeader' member.

    // OptionHeader structure datagram [4 bytes]:
    // ..
    //   +---------------+---------------+---------------+---------------+
    //   |0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
    //   +---------------+---------------+---------------+---------------+
    //   |    Type   |P|   TS  |              Words                      |
    //   +---------------+---------------+---------------+---------------+
    //
    //  Type...............: Type of this option
    //  Packed (P).........: Flag to indicate *packed* options.  If set,
    //                       'words' field will be reinterpreted as extra
    //                       type-specific content of this option in addition
    //                       to the 'TS' field, and there will be no option
    //                       content following this header
    //  Type-Specific (TS).: Content specific to this option's type, see below
    //  Words..............: Length (words) of this option, including this
    //                       header.  If *packed*, this field will be
    //                       reinterpreted as additional type-specific content
    // ..
    //
    // TypeSpecific content:
    // o e_SUB_QUEUE_INFOS: If *packed*, 'words' field will be reinterpreted as
    //                      the RDA counter for the default SubQueueId.  Note
    //                      that the options is only *packed* for non-fanout
    //                      mode.  Else, 'TS' field will represent the size of
    //                      each item in the SubQueueInfosArray encoded in the
    //                      option content.
    //
    // NOTE:
    //  o In order to preserve alignment, this structure can *NOT* be changed
    //  o Every 'Option' must start by this header, and be 4 bytes aligned with
    //    optional padding at the end.
    //  o For efficiency, since option lookup will be a linear search, options
    //    should be added by decreasing order of usage (or could be sorted by
    //    option id and use optimize the linear search to cut off earlier
    //    eventually).
    //  o Options that are followed by one or multiple 'variable length'
    //    fields may include a byte representing the size (words) of the
    //    optionHeader.
    //
    /// Example of a 'real' option class:
    /// ---------------------------------
    // ..
    //  class MyClass {
    //     OptionHeader optionHeader;
    //     int          timestamp;
    //     int          msgId;
    //  }
    // ..

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // PRIVATE CONSTANTS
    private static final int TYPE_NUM_BITS = 6;
    private static final int PACKED_NUM_BITS = 1;
    private static final int TYPE_SPECIFIC_NUM_BITS = 4;
    private static final int WORDS_NUM_BITS = 21;

    private static final int TYPE_START_IDX = 26;
    private static final int PACKED_START_IDX = 25;
    private static final int TYPE_SPECIFIC_START_IDX = 21;
    private static final int WORDS_START_IDX = 0;

    private static final int TYPE_MASK = BitUtil.oneMask(TYPE_START_IDX, TYPE_NUM_BITS);
    private static final int PACKED_MASK = BitUtil.oneMask(PACKED_START_IDX, PACKED_NUM_BITS);
    private static final int TYPE_SPECIFIC_MASK =
            BitUtil.oneMask(TYPE_SPECIFIC_START_IDX, TYPE_SPECIFIC_NUM_BITS);
    private static final int WORDS_MASK = BitUtil.oneMask(WORDS_START_IDX, WORDS_NUM_BITS);

    // PUBLIC CLASS DATA
    public static final int MAX_TYPE = (1 << TYPE_NUM_BITS) - 1;
    // Highest possible value for the type of an option.

    public static final int MAX_TYPE_SPECIFIC = (1 << TYPE_SPECIFIC_NUM_BITS) - 1;
    // Highest possible value for the type-specific field of an option.

    public static final int MAX_WORDS = (1 << WORDS_NUM_BITS) - 1;
    // Maximum size (words) of an option, including the header.

    public static final int MAX_SIZE = MAX_WORDS * Protocol.WORD_SIZE;
    // Maximum size (bytes) of an option, including the header.

    public static final int MIN_HEADER_SIZE = 4;
    // Minimum size (bytes) of an 'OptionHeader' (that is sufficient to
    // capture header words).  This value should *never* change.

    private int content;
    // Option type, packed, type-specific info and number of words in this option
    // (including this OptionHeader).

    public static final int HEADER_SIZE = 4;
    // Current size (bytes) of the header.

    public OptionHeader() {
        setType(OptionType.UNDEFINED);
        setWords(HEADER_SIZE / Protocol.WORD_SIZE);
        setPacked(false);
        setTypeSpecific((byte) 0);
    }

    public OptionHeader setType(OptionType type) {
        int value = type.toInt();
        content = (content & ~TYPE_MASK) | (value << TYPE_START_IDX);
        return this;
    }

    public OptionHeader setPacked(boolean packed) {
        int value = packed ? 1 : 0;
        content = (content & ~PACKED_MASK) | (value << PACKED_START_IDX);

        return this;
    }

    public OptionHeader setTypeSpecific(byte value) {
        Argument.expectNonNegative(value, "value");
        Argument.expectNotGreater(value, MAX_TYPE_SPECIFIC, "value");

        content = (content & ~TYPE_SPECIFIC_MASK) | (value << TYPE_SPECIFIC_START_IDX);
        return this;
    }

    public OptionHeader setWords(int value) {
        Argument.expectNonNegative(value, "value");
        Argument.expectNotGreater(value, MAX_WORDS, "value");

        content = (content & ~WORDS_MASK) | value;
        return this;
    }

    public OptionType type() {
        return OptionType.fromInt((content & TYPE_MASK) >>> TYPE_START_IDX);
    }

    public boolean packed() {
        return (content & PACKED_MASK) >>> PACKED_START_IDX == 1;
    }

    public byte typeSpecific() {
        return (byte) (((content & TYPE_SPECIFIC_MASK) >>> TYPE_SPECIFIC_START_IDX) & 0xff);
    }

    public int words() {
        return content & WORDS_MASK;
    }

    @Override
    public void streamIn(ByteBufferInputStream bbis) throws IOException {
        content = bbis.readInt();
    }

    public void streamOut(ByteBufferOutputStream bbos) throws IOException {
        bbos.writeInt(content);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[OptionHeader]")
                .append("\n\t")
                .append("Type  : ")
                .append(type())
                .append("\n\t")
                .append("Words : ")
                .append(words());
        return sb.toString();
    }
}
