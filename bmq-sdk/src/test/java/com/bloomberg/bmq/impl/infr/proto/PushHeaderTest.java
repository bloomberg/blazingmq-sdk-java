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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bloomberg.bmq.MessageGUID;
import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.msg.MessagesTestSamples;
import com.bloomberg.bmq.impl.infr.util.BitUtil;
import com.bloomberg.bmq.util.TestHelpers;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PushHeaderTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // PRIVATE CONSTANTS
    private static final int FLAGS_NUM_BITS = 4;
    private static final int MSG_WORDS_NUM_BITS = 28;
    private static final int OPTIONS_WORDS_NUM_BITS = 24;
    private static final int HEADER_WORDS_NUM_BITS = 5;

    private static final int FLAGS_START_IDX = 28;
    private static final int MSG_WORDS_START_IDX = 0;
    private static final int OPTIONS_WORDS_START_IDX = 8;
    private static final int HEADER_WORDS_START_IDX = 0;

    private static final int FLAGS_MASK = BitUtil.oneMask(FLAGS_START_IDX, FLAGS_NUM_BITS);

    private static final int MSG_WORDS_MASK =
            BitUtil.oneMask(MSG_WORDS_START_IDX, MSG_WORDS_NUM_BITS);

    private static final int OPTIONS_WORDS_MASK =
            BitUtil.oneMask(OPTIONS_WORDS_START_IDX, OPTIONS_WORDS_NUM_BITS);

    private static final int HEADER_WORDS_MASK =
            BitUtil.oneMask(HEADER_WORDS_START_IDX, HEADER_WORDS_NUM_BITS);

    @Test
    void testStreamIn() throws IOException {
        ByteBuffer buf = TestHelpers.readFile(MessagesTestSamples.PUSH_MULTI_MSG.filePath());

        ByteBufferInputStream bbis = new ByteBufferInputStream(buf);
        EventHeader header = new EventHeader();

        final int length = bbis.available();
        header.streamIn(bbis);

        assertEquals(0, header.fragmentBit());
        assertEquals(256, header.length());
        assertEquals(length, header.length());
        assertEquals(1, header.protocolVersion());
        assertEquals(2, header.headerWords());
        assertEquals(EventType.PUSH, header.type());

        for (int i = 0; i < 2; ++i) {
            PushHeader pushHeader = new PushHeader();

            pushHeader.streamIn(bbis);
            logger.info("PUSH header {}: {}", i + 1, pushHeader);

            assertEquals(2, pushHeader.flags());
            assertEquals(31, pushHeader.messageWords());
            assertEquals(0, pushHeader.optionsWords());
            assertEquals(0, pushHeader.compressionType());
            assertEquals(8, pushHeader.headerWords());
            assertEquals(9876, pushHeader.queueId());
            assertEquals(i, pushHeader.schemaWireId());

            assertEquals("ABCDEF0123456789ABCDEF0123456789", pushHeader.messageGUID().toString());

            final int toSkip =
                    (pushHeader.messageWords() - pushHeader.headerWords()) * Protocol.WORD_SIZE;
            assertEquals(toSkip, bbis.skip(toSkip));
        }

        assertEquals(0, bbis.available());
    }

    @Test
    void testStreamInZlib() throws IOException {
        ByteBuffer buf = TestHelpers.readFile(MessagesTestSamples.PUSH_MSG_ZLIB.filePath());

        ByteBufferInputStream bbis = new ByteBufferInputStream(buf);
        EventHeader header = new EventHeader();

        header.streamIn(bbis);

        assertEquals(0, header.fragmentBit());
        assertEquals(64, header.length());
        assertEquals(1, header.protocolVersion());
        assertEquals(2, header.headerWords());
        assertEquals(EventType.PUSH, header.type());

        PushHeader pushHeader = new PushHeader();

        pushHeader.streamIn(bbis);

        assertEquals(0, pushHeader.flags());
        assertEquals(14, pushHeader.messageWords());
        assertEquals(0, pushHeader.optionsWords());
        assertEquals(1, pushHeader.compressionType());
        assertEquals(7, pushHeader.headerWords());
        assertEquals(9876, pushHeader.queueId());

        assertEquals("ABCDEF0123456789ABCDEF0123456789", pushHeader.messageGUID().toString());
    }

    @Test
    void testGetterSetterFlags() {
        PushHeader pushHeader = new PushHeader();
        int value = Integer.MAX_VALUE;
        int flagVal = ((value << FLAGS_START_IDX) & FLAGS_MASK) >>> FLAGS_START_IDX;
        pushHeader.setFlags(value);
        assertEquals(flagVal, pushHeader.flags());

        value = Integer.MIN_VALUE;
        flagVal = ((value << FLAGS_START_IDX) & FLAGS_MASK) >>> FLAGS_START_IDX;
        pushHeader.setFlags(value);
        assertEquals(flagVal, pushHeader.flags());
    }

    @Test
    void testGetterSetterHeaderWords() {
        PushHeader pushHeader = new PushHeader();
        int value = Integer.MAX_VALUE;
        int headerWords = value & HEADER_WORDS_MASK;
        pushHeader.setHeaderWords(value);
        assertEquals(headerWords, pushHeader.headerWords());

        value = Integer.MIN_VALUE;
        headerWords = value & HEADER_WORDS_MASK;
        pushHeader.setHeaderWords(value);
        assertEquals(headerWords, pushHeader.headerWords());
    }

    @Test
    void testGetterSetterMessageWords() {
        PushHeader pushHeader = new PushHeader();
        int value = Integer.MAX_VALUE;
        int messageWords = value & MSG_WORDS_MASK;
        pushHeader.setMessageWords(value);
        assertEquals(messageWords, pushHeader.messageWords());

        value = Integer.MIN_VALUE;
        messageWords = value & MSG_WORDS_MASK;
        pushHeader.setMessageWords(value);
        assertEquals(messageWords, pushHeader.messageWords());
    }

    @Test
    void testGetterSetterCompressionType() {
        PushHeader pushHeader = new PushHeader();
        assertEquals(0, pushHeader.compressionType());

        // min value greater than zero
        pushHeader.setCompressionType(1);
        assertEquals(1, pushHeader.compressionType());

        // max valid value (2^3 - 1)
        pushHeader.setCompressionType(7);
        assertEquals(7, pushHeader.compressionType());

        // overflow (possibly later need to refactor the code to throw exception)
        pushHeader.setCompressionType(8);
        assertEquals(0, pushHeader.compressionType());
    }

    @Test
    void testGetterSetterOptionWords() {
        PushHeader pushHeader = new PushHeader();
        int value = Integer.MAX_VALUE;
        int optionsWords =
                ((HEADER_WORDS_MASK | (value << OPTIONS_WORDS_START_IDX)) & OPTIONS_WORDS_MASK)
                        >>> OPTIONS_WORDS_START_IDX;
        pushHeader.setOptionsWords(value);
        assertEquals(optionsWords, pushHeader.optionsWords());

        value = Integer.MIN_VALUE;
        optionsWords =
                ((HEADER_WORDS_MASK | (value << OPTIONS_WORDS_START_IDX)) & OPTIONS_WORDS_MASK)
                        >>> OPTIONS_WORDS_START_IDX;
        pushHeader.setOptionsWords(value);
        assertEquals(optionsWords, pushHeader.optionsWords());
    }

    @Test
    void testStreamOut() throws IOException {
        PushHeader pushHeader = new PushHeader();

        pushHeader.setMessageWords(14);
        pushHeader.setHeaderWords(8);
        pushHeader.setQueueId(9876);
        pushHeader.setMessageGUID(MessageGUID.fromHex("ABCDEF0123456789ABCDEF0123456789"));
        pushHeader.setSchemaWireId((short) 0xFFFF); // max value

        ByteBufferOutputStream bbos = new ByteBufferOutputStream(1024);
        // Specifying a large enough size for the output stream so that
        // the streamed out EventHeader instance is captured in the 1st
        // ByteBuffer of 'bbos'.

        pushHeader.streamOut(bbos);

        ByteBuffer[] buffers = bbos.reset();
        assertEquals(1, buffers.length);

        // Stream in

        ByteBufferInputStream bbis = new ByteBufferInputStream(buffers);

        pushHeader = new PushHeader();
        pushHeader.streamIn(bbis);

        assertEquals(0, pushHeader.flags());
        assertEquals(14, pushHeader.messageWords());
        assertEquals(0, pushHeader.optionsWords());
        assertEquals(0, pushHeader.compressionType());
        assertEquals(8, pushHeader.headerWords());
        assertEquals(9876, pushHeader.queueId());
        assertEquals((short) 0xffff, pushHeader.schemaWireId());

        assertEquals("ABCDEF0123456789ABCDEF0123456789", pushHeader.messageGUID().toString());
    }

    @Test
    void testStreamOutZlib() throws IOException {
        PushHeader pushHeader = new PushHeader();

        pushHeader.setMessageWords(14);
        pushHeader.setHeaderWords(8);
        pushHeader.setCompressionType(1);
        pushHeader.setQueueId(9876);
        pushHeader.setMessageGUID(MessageGUID.fromHex("ABCDEF0123456789ABCDEF0123456789"));
        pushHeader.setSchemaWireId((short) 1);

        ByteBufferOutputStream bbos = new ByteBufferOutputStream(1024);
        // Specifying a large enough size for the output stream so that
        // the streamed out EventHeader instance is captured in the 1st
        // ByteBuffer of 'bbos'.

        pushHeader.streamOut(bbos);

        ByteBuffer[] buffers = bbos.reset();
        assertEquals(1, buffers.length);

        // Stream in

        ByteBufferInputStream bbis = new ByteBufferInputStream(buffers);

        pushHeader = new PushHeader();
        pushHeader.streamIn(bbis);

        assertEquals(0, pushHeader.flags());
        assertEquals(14, pushHeader.messageWords());
        assertEquals(0, pushHeader.optionsWords());
        assertEquals(1, pushHeader.compressionType());
        assertEquals(8, pushHeader.headerWords());
        assertEquals(9876, pushHeader.queueId());
        assertEquals(1, pushHeader.schemaWireId());

        assertEquals("ABCDEF0123456789ABCDEF0123456789", pushHeader.messageGUID().toString());
    }
}
