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

import com.bloomberg.bmq.MessageGUID;
import com.bloomberg.bmq.impl.CorrelationIdImpl;
import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.util.BitUtil;
import java.io.IOException;

public final class PutHeader {
    // This class represents the header for a 'PUT' event.  A 'PUT' event is
    // the event sent by a client to the broker to post message on queue(s).

    // PutHeader structure datagram [36 bytes (followed by zero or more options
    //                                         then zero or more message
    //                                         properties, then data payload)]:
    // ..
    //   +---------------+---------------+---------------+---------------+
    //   |0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
    //   +---------------+---------------+---------------+---------------+
    //   |   F   |                     MessageWords                      |
    //   +---------------+---------------+---------------+---------------+
    //   |                 OptionsWords                  | CAT |   HW    |
    //   +---------------+---------------+---------------+---------------+
    //   |                            QueueId                            |
    //   +---------------+---------------+---------------+---------------+
    //   |                  MessageGUID or CorrelationId                 |
    //   +---------------+---------------+---------------+---------------+
    //   |                  MessageGUID (cont.) or Null                  |
    //   +---------------+---------------+---------------+---------------+
    //   |                  MessageGUID (cont.) or Null                  |
    //   +---------------+---------------+---------------+---------------+
    //   |                  MessageGUID (cont.) or Null                  |
    //   +---------------+---------------+---------------+---------------+
    //   |                            CRC32-C                            |
    //   +---------------+---------------+---------------+---------------+
    //   |           SchemaId            |           Reserved            |
    //   +---------------+---------------+---------------+---------------+
    //       F...: Flags
    //       CAT.: Compression Algorithm Type
    //       HW..: HeaderWords
    //
    //  Flags (F)..................: Flags. See PutHeaderFlags struct.
    //  MessageWords...............: Total size (words) of the message,
    //                               including this header, the options (header
    //                               + content) and application data
    //  OptionsWords...............: Total size (words) of the options area
    //  CAT........................: Compression Algorithm Type. See CompressionAlgorithmType enum
    //  HeaderWords (HW)...........: Total size (words) of this MessageHeader
    //  QueueId....................: Id of the queue (as announced during open
    //                               queue by the producer of this Put event)
    //  MessageGUID/CorrelationId..: Depending upon context, this field is
    //                               either MessageGUID, or CorrelationId
    //                               specified by the source of 'PUT' message
    //                               Note that correlationId is always limited
    //                               to 24 bits.
    //  CRC32-C....................: CRC32-C calculated over the application
    //                               data.
    //  SchemaId...................: Binary protocol representation of the
    //                               Schema Id associated with subsequent
    //                               MessageProperties.
    //  Reserved...................: Reserved bytes.
    // ..
    //
    // Following this header, zero or more options may be present.  They are
    // aligned consecutively in memory, and all start with an OptionHeader,
    // like so:
    // ..
    //   [OptionHeader..(OptionContent)][OptionHeader..(OptionContent)]
    // ..
    //
    // Options are followed by message properties, which are also optional.
    // One of the 'PutHeaderFlags' indicates the presence of message
    // properties.  Message properties area must be word aligned (each message
    // property need not be word aligned).
    //
    // Message properties are followed by the data payload.  Data must be word
    // aligned.
    //
    // The combination of message properties and data payload may be referred
    // to as 'application data'.
    //
    // From the first byte of the PutHeader, application data can be found by:
    // ..
    //     ptr += 4 * (headerWords + optionsWords)
    // ..

    private int flagsAndMessageWords;
    private int optionsWordsAndHeaderWords;
    private int queueId;
    private byte[] guidOrCorrId;
    private int crc32c;
    private short schemaWireId;
    private short reserved = 0;

    private CorrelationIdImpl correlationId = CorrelationIdImpl.NULL_CORRELATION_ID;

    // PRIVATE CONSTANTS
    private static final int FLAGS_NUM_BITS = 4;
    private static final int MSG_WORDS_NUM_BITS = 28;
    private static final int OPTIONS_WORDS_NUM_BITS = 24;
    private static final int COMPRESSION_TYPE_NUM_BITS = 3;
    private static final int HEADER_WORDS_NUM_BITS = 5;
    private static final int CORRELATION_ID_NUM_BITS = 24;
    private static final int CORRELATION_ID_LEN = 3; // In bytes

    private static final int FLAGS_START_IDX = 28;
    private static final int MSG_WORDS_START_IDX = 0;
    private static final int OPTIONS_WORDS_START_IDX = 8;
    private static final int COMPRESSION_TYPE_START_IDX = 5;
    private static final int HEADER_WORDS_START_IDX = 0;

    private static final int FLAGS_MASK = BitUtil.oneMask(FLAGS_START_IDX, FLAGS_NUM_BITS);

    private static final int MSG_WORDS_MASK =
            BitUtil.oneMask(MSG_WORDS_START_IDX, MSG_WORDS_NUM_BITS);

    private static final int OPTIONS_WORDS_MASK =
            BitUtil.oneMask(OPTIONS_WORDS_START_IDX, OPTIONS_WORDS_NUM_BITS);

    private static final int COMPRESSION_TYPE_MASK =
            BitUtil.oneMask(COMPRESSION_TYPE_START_IDX, COMPRESSION_TYPE_NUM_BITS);

    private static final int HEADER_WORDS_MASK =
            BitUtil.oneMask(HEADER_WORDS_START_IDX, HEADER_WORDS_NUM_BITS);

    public static final int MAX_SIZE = ((1 << MSG_WORDS_NUM_BITS) - 1) * Protocol.WORD_SIZE;
    // Maximum size (bytes) of a full PutMessage with header and payload.

    public static final int MAX_PAYLOAD_SIZE_SOFT = 64 * 1024 * 1024;
    // Maximum size (bytes) of a PutMessage payload (without header or
    // options, ...).
    //
    // NOTE: the protocol supports up to MAX_SIZE for the entire
    // PutMessage with payload, but the PutEventBuilder only validates the
    // payload size against this size; so this MAX_PAYLOAD_SIZE_SOFT can
    // be increased but not up to 'MAX_SIZE'.

    public static final int MAX_OPTIONS_SIZE =
            ((1 << OPTIONS_WORDS_NUM_BITS) - 1) * Protocol.WORD_SIZE;
    // Maximum size (bytes) of the options area.

    public static final int MAX_HEADER_SIZE =
            ((1 << HEADER_WORDS_NUM_BITS) - 1) * Protocol.WORD_SIZE;
    // Maximum size (bytes) of a 'PutHeader'.

    public static final int MIN_HEADER_SIZE = 8;
    // Minimum size (bytes) of a 'PutHeader' (that is sufficient to
    // capture header words).  This value should *never* change.

    public static final int MAX_CORRELATION_ID = (1 << CORRELATION_ID_NUM_BITS) - 1;

    public static final int HEADER_SIZE = 36;
    // Current size (bytes) of the header.

    public PutHeader() {
        guidOrCorrId = new byte[MessageGUID.SIZE_BINARY];
        setMessageWords((byte) (HEADER_SIZE / Protocol.WORD_SIZE));
        setHeaderWords((byte) (HEADER_SIZE / Protocol.WORD_SIZE));
    }

    public void setMessageWords(int value) {
        flagsAndMessageWords = (flagsAndMessageWords & FLAGS_MASK) | (value & MSG_WORDS_MASK);
    }

    public void setFlags(int value) {
        flagsAndMessageWords = (flagsAndMessageWords & MSG_WORDS_MASK) | (value << FLAGS_START_IDX);
    }

    public void setOptionsWords(int value) {
        optionsWordsAndHeaderWords =
                (optionsWordsAndHeaderWords & HEADER_WORDS_MASK)
                        | (optionsWordsAndHeaderWords & COMPRESSION_TYPE_MASK)
                        | ((value << OPTIONS_WORDS_START_IDX) & OPTIONS_WORDS_MASK);
    }

    public void setCompressionType(int value) {
        optionsWordsAndHeaderWords =
                (optionsWordsAndHeaderWords & HEADER_WORDS_MASK)
                        | ((value << COMPRESSION_TYPE_START_IDX) & COMPRESSION_TYPE_MASK)
                        | (optionsWordsAndHeaderWords & OPTIONS_WORDS_MASK);
    }

    public void setHeaderWords(int value) {
        optionsWordsAndHeaderWords =
                (value & HEADER_WORDS_MASK)
                        | (optionsWordsAndHeaderWords & COMPRESSION_TYPE_MASK)
                        | (optionsWordsAndHeaderWords & OPTIONS_WORDS_MASK);
    }

    public void setQueueId(int value) {
        queueId = value;
    }

    public void setCorrelationId(CorrelationIdImpl id) {
        int value = id.toInt();
        // Copy 3 lower bytes of 'value' in guidOrCorrId[1-3]
        // skip MSB
        guidOrCorrId[1] = (byte) (value >> 16);
        guidOrCorrId[2] = (byte) (value >> 8);
        guidOrCorrId[3] = (byte) (value);
        correlationId = id;
    }

    public void setMessageGUID(final MessageGUID value) {
        value.toBinary(guidOrCorrId);
    }

    public void setCrc32c(Long value) {
        crc32c = value.intValue();
    }

    public void setSchemaWireId(short value) {
        schemaWireId = value;
    }

    public int flags() {
        return (flagsAndMessageWords & FLAGS_MASK) >>> FLAGS_START_IDX;
    }

    public int messageWords() {
        return flagsAndMessageWords & MSG_WORDS_MASK;
    }

    public int optionsWords() {
        return (optionsWordsAndHeaderWords & OPTIONS_WORDS_MASK) >>> OPTIONS_WORDS_START_IDX;
    }

    public int compressionType() {
        return (optionsWordsAndHeaderWords & COMPRESSION_TYPE_MASK) >>> COMPRESSION_TYPE_START_IDX;
    }

    public int headerWords() {
        return optionsWordsAndHeaderWords & HEADER_WORDS_MASK;
    }

    public int queueId() {
        return queueId;
    }

    public CorrelationIdImpl correlationId() {
        return correlationId;
    }

    public long crc32c() {
        return Integer.toUnsignedLong(crc32c);
    }

    public MessageGUID messageGUID() {
        return MessageGUID.fromBinary(guidOrCorrId);
    }

    public short schemaWireId() {
        return schemaWireId;
    }

    public void streamIn(ByteBufferInputStream bbis) throws IOException {
        flagsAndMessageWords = bbis.readInt();
        optionsWordsAndHeaderWords = bbis.readInt();

        final int headerSize = headerWords() * Protocol.WORD_SIZE;
        if (headerSize < HEADER_SIZE) {
            throw new IOException("Invalid size: " + headerSize);
        }

        queueId = bbis.readInt();
        for (int i = 0; i < MessageGUID.SIZE_BINARY; i++) {
            guidOrCorrId[i] = bbis.readByte();
        }
        int id =
                (guidOrCorrId[1] & 0xFF) << 16
                        | (guidOrCorrId[2] & 0xFF) << 8
                        | (guidOrCorrId[3] & 0xFF);
        correlationId = CorrelationIdImpl.restoreId(id);

        crc32c = bbis.readInt();

        schemaWireId = bbis.readShort();
        reserved = bbis.readShort();

        if (headerSize > HEADER_SIZE) {
            // Read and ignore bytes that we don't know in the header.
            int numExtraBytes = headerSize - HEADER_SIZE;

            if (bbis.skip(numExtraBytes) != numExtraBytes) {
                throw new IOException("Failed to skip " + numExtraBytes + " bytes.");
            }
        }
    }

    public void streamOut(ByteBufferOutputStream bbos) throws IOException {
        final int headerSize = headerWords() * Protocol.WORD_SIZE;
        if (headerSize != HEADER_SIZE) {
            throw new IOException("Invalid size: " + headerSize);
        }

        bbos.writeInt(flagsAndMessageWords);
        bbos.writeInt(optionsWordsAndHeaderWords);
        bbos.writeInt(queueId);
        for (int i = 0; i < MessageGUID.SIZE_BINARY; i++) {
            bbos.writeByte(guidOrCorrId[i]);
        }
        bbos.writeInt(crc32c);
        bbos.writeShort(schemaWireId);
        bbos.writeShort(reserved);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ PutHeader [")
                .append(" Flags=")
                .append(flags())
                .append(" MessageWords=")
                .append(messageWords())
                .append(" OptionsWords=")
                .append(optionsWords())
                .append(" CompressionType=")
                .append(compressionType())
                .append(" HeaderWords=")
                .append(headerWords())
                .append(" QueueId=")
                .append(queueId())
                .append(" CorrelationId=")
                .append(correlationId())
                .append(" Crc32c=")
                .append(crc32c())
                .append(" SchemaWireId=")
                .append(schemaWireId())
                .append(" ] ]");
        return sb.toString();
    }
}
