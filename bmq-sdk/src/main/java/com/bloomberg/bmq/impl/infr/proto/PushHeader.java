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
import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.util.BitUtil;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PushHeader {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // This struct represents the header for a 'PUSH' event.  A 'PUSH' event is
    // the event sent by the broker to a client to deliver a message from
    // queue(s).

    // PushHeader structure datagram [32 bytes (followed by options, if any,
    //                                          then data payload)]:
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
    //   |                          MessageGUID                          |
    //   +---------------+---------------+---------------+---------------+
    //   |                      MessageGUID (cont.)                      |
    //   +---------------+---------------+---------------+---------------+
    //   |                      MessageGUID (cont.)                      |
    //   +---------------+---------------+---------------+---------------+
    //   |                      MessageGUID (cont.)                      |
    //   +---------------+---------------+---------------+---------------+
    //   |           SchemaId            |           Reserved            |
    //   +---------------+---------------+---------------+---------------+
    //       F...: Flags
    //       CAT.: Compression Algorithm Type
    //       HW..: HeaderWords
    //
    //  Flags (F)........: Flags associated to this message.  See
    //                     PushHeaderFlags struct
    //  MessageWords.....: Total size (words) of the message, including this
    //                     header, the options (header + content) and the
    //                     message payload
    //  OptionsWords.....: Total size (words) of the options area
    //  CAT..............: Compression Algorithm Type. See CompressionAlgorithmType enum
    //  HeaderWords (HW).: Total size (words) of this PushHeader
    //  QueueId..........: Id of the queue (as anounced during open queue by
    //                     the consumer of this Push event)
    //  MessageGUID......: MessageGUID associated to this message by the broker
    //  SchemaId.........: Binary protocol representation of the
    //                     Schema Id associated with subsequent
    //                     MessageProperties.
    //  Reserved.........: Reserved bytes.
    // ..
    //
    // NOTE: From the first byte of the PushHeader the payload can be found by:
    //       ptr +=  4 * (headerWords + optionsWords).  Note that if
    //       'bmqp::PushHeaderFlags::e_IMPLICIT_PAYLOAD' flag is set, then
    //       payload is not present.
    //
    // Following this header, each options are aligned consecutively in memory,
    // and all start with an OptionHeader.
    //   [OptionHeader..(OptionContent)][OptionHeader..(OptionContent)]
    //
    // The (optional) data payload then follows this header.  If present, data
    // must be padded.

    private int flagsAndMessageWords;
    private int optionsWordsAndHeaderWords;
    private int queueId;
    private byte[] messageGUID;
    private short schemaWireId;
    private short reserved;

    // PRIVATE CONSTANTS
    private static final int FLAGS_NUM_BITS = 4;
    private static final int MSG_WORDS_NUM_BITS = 28;
    private static final int OPTIONS_WORDS_NUM_BITS = 24;
    private static final int COMPRESSION_TYPE_NUM_BITS = 3;
    private static final int HEADER_WORDS_NUM_BITS = 5;

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
    // Maximum size (bytes) of a full PushMessageImpl with header and payload.

    public static final int MAX_PAYLOAD_SIZE_SOFT = 64 * 1024 * 1024;
    // Maximum size (bytes) of a PushMessageImpl payload (without header or
    // options, ...).
    //
    // NOTE: the protocol supports up to MAX_SIZE for the entire
    // PushMessageImpl with payload, but the PushEventBuilder only validates the
    // payload size against this size; so this MAX_PAYLOAD_SIZE_SOFT can
    // be increased but not up to 'MAX_SIZE'.

    public static final int MAX_OPTIONS_SIZE =
            ((1 << OPTIONS_WORDS_NUM_BITS) - 1) * Protocol.WORD_SIZE;
    // Maximum size (bytes) of the options area.

    public static final int MAX_HEADER_SIZE =
            ((1 << HEADER_WORDS_NUM_BITS) - 1) * Protocol.WORD_SIZE;
    // Maximum size (bytes) of a 'PushHeader'.

    public static final int MIN_HEADER_SIZE = 8;
    // Minimum size (bytes) of a 'PushHeader' (that is sufficient to
    // capture header words).  This value should *never* change.

    public static final int HEADER_SIZE = 28;
    // Current size (bytes) of the header.
    // TODO: set to 32 after 2nd release of "new style" brokers

    public static final int HEADER_SIZE_FOR_SCHEMA_ID = 32;
    // Current size (bytes) of the header with schema id
    // TODO: remove after 2nd release of "new style" brokers

    public PushHeader() {
        messageGUID = new byte[MessageGUID.SIZE_BINARY];
        setMessageWords((byte) (HEADER_SIZE_FOR_SCHEMA_ID / Protocol.WORD_SIZE));
        setHeaderWords((byte) (HEADER_SIZE_FOR_SCHEMA_ID / Protocol.WORD_SIZE));
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

    public void setMessageGUID(final MessageGUID value) {
        value.toBinary(messageGUID);
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

    public MessageGUID messageGUID() {
        return MessageGUID.fromBinary(messageGUID);
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
            messageGUID[i] = bbis.readByte();
        }

        int numRead = HEADER_SIZE;

        // Check if it's new header with schema id
        schemaWireId = 0;
        // TODO: update after 2nd release of "new style" brokers
        if (headerSize >= HEADER_SIZE_FOR_SCHEMA_ID) {
            schemaWireId = bbis.readShort();
            reserved = bbis.readShort();
            numRead += 4;
        }

        if (numRead < headerSize) {
            // Skip bytes that we don't know or ignore in the header.
            final int numExtraBytes = headerSize - numRead;

            if (bbis.skip(numExtraBytes) != numExtraBytes) {
                throw new IOException("Failed to skip " + numExtraBytes + " bytes.");
            }
        }
    }

    public void streamOut(ByteBufferOutputStream bbos) throws IOException {
        final int headerSize = headerWords() * Protocol.WORD_SIZE;
        if (headerSize != HEADER_SIZE_FOR_SCHEMA_ID) {
            throw new IOException("Invalid size: " + headerSize);
        }

        bbos.writeInt(flagsAndMessageWords);
        bbos.writeInt(optionsWordsAndHeaderWords);
        bbos.writeInt(queueId);
        for (int i = 0; i < MessageGUID.SIZE_BINARY; i++) {
            bbos.writeByte(messageGUID[i]);
        }

        bbos.writeShort(schemaWireId);
        bbos.writeShort(reserved);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ PushHeader [")
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
                .append(" MessageGUID=")
                .append(messageGUID())
                .append(" SchemaWireId=")
                .append(schemaWireId())
                .append(" ] ]");
        return sb.toString();
    }
}
