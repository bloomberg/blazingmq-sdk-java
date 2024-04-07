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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.bloomberg.bmq.CorrelationId;
import com.bloomberg.bmq.impl.CorrelationIdImpl;
import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.msg.MessagesTestSamples;
import com.bloomberg.bmq.util.TestHelpers;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PutHeaderTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    void testStreamIn() throws IOException {
        ByteBuffer buf = TestHelpers.readFile(MessagesTestSamples.PUT_MULTI_MSG.filePath());

        ByteBufferInputStream bbis = new ByteBufferInputStream(buf);
        EventHeader header = new EventHeader();

        final int length = bbis.available();
        header.streamIn(bbis);

        assertEquals(0, header.fragmentBit());
        assertEquals(264, header.length());
        assertEquals(length, header.length());
        assertEquals(1, header.protocolVersion());
        assertEquals(2, header.headerWords());
        assertNotNull(header.type());
        assertEquals(EventType.PUT, header.type());

        final long[] crc32s = new long[] {3469549003L, 340340870L};
        final int[] schemaIds = new int[] {0, 1};

        for (int i = 0; i < 2; ++i) {
            PutHeader putHeader = new PutHeader();

            putHeader.streamIn(bbis);
            logger.info("PUT header {}: {}", i + 1, putHeader);

            assertEquals(3, putHeader.flags());
            assertEquals(32, putHeader.messageWords());
            assertEquals(0, putHeader.optionsWords());
            assertEquals(0, putHeader.compressionType());
            assertEquals(9, putHeader.headerWords());
            assertEquals(9876, putHeader.queueId());

            CorrelationId corId = CorrelationIdImpl.restoreId(1234);
            assertEquals(corId, putHeader.correlationId());
            assertEquals(crc32s[i], putHeader.crc32c());
            assertEquals(schemaIds[i], putHeader.schemaWireId());

            final int toSkip =
                    (putHeader.messageWords() - putHeader.headerWords()) * Protocol.WORD_SIZE;
            assertEquals(toSkip, bbis.skip(toSkip));
        }

        assertEquals(0, bbis.available());
    }

    @Test
    void testStreamInZlib() throws IOException {
        ByteBuffer buf = TestHelpers.readFile(MessagesTestSamples.PUT_MSG_ZLIB.filePath());

        ByteBufferInputStream bbis = new ByteBufferInputStream(buf);
        EventHeader header = new EventHeader();

        header.streamIn(bbis);

        assertEquals(0, header.fragmentBit());
        assertEquals(72, header.length());
        assertEquals(1, header.protocolVersion());
        assertEquals(2, header.headerWords());
        assertNotNull(header.type());
        assertEquals(EventType.PUT, header.type());

        PutHeader putHeader = new PutHeader();

        putHeader.streamIn(bbis);

        assertEquals(0, putHeader.flags());
        assertEquals(16, putHeader.messageWords());
        assertEquals(0, putHeader.optionsWords());
        assertEquals(1, putHeader.compressionType());
        assertEquals(9, putHeader.headerWords());
        assertEquals(9876, putHeader.queueId());

        CorrelationId corId = CorrelationIdImpl.restoreId(1234);
        assertEquals(putHeader.correlationId(), corId);
        assertEquals(0, putHeader.crc32c());
        assertEquals(0, putHeader.schemaWireId());
    }

    @Test
    void testStreamOut() throws IOException {
        // Stream out

        PutHeader putHeader = new PutHeader();

        putHeader.setMessageWords(15);
        putHeader.setHeaderWords(9);
        putHeader.setQueueId(9876);
        putHeader.setCorrelationId(CorrelationIdImpl.restoreId(1234));
        putHeader.setSchemaWireId((short) 0xFFFF); // max value

        ByteBufferOutputStream bbos = new ByteBufferOutputStream(1024);
        // Specifying a large enough size for the output stream so that
        // the streamed out EventHeader instance is captured in the 1st
        // ByteBuffer of 'bbos'.

        putHeader.streamOut(bbos);

        ByteBuffer[] buffers = bbos.reset();
        assertEquals(1, buffers.length);

        // Stream in

        ByteBufferInputStream bbis = new ByteBufferInputStream(buffers);

        putHeader = new PutHeader();
        putHeader.streamIn(bbis);

        assertEquals(0, putHeader.flags());
        assertEquals(15, putHeader.messageWords());
        assertEquals(0, putHeader.optionsWords());
        assertEquals(0, putHeader.compressionType());
        assertEquals(9, putHeader.headerWords());
        assertEquals(9876, putHeader.queueId());

        CorrelationId corId = CorrelationIdImpl.restoreId(1234);
        assertEquals(corId, putHeader.correlationId());
        assertEquals(0, putHeader.crc32c());
        assertEquals((short) 0xffff, putHeader.schemaWireId());
    }

    @Test
    void testStreamOutZlib() throws IOException {
        // Stream out

        PutHeader putHeader = new PutHeader();

        putHeader.setMessageWords(15);
        putHeader.setHeaderWords(9);
        putHeader.setCompressionType(1);
        putHeader.setQueueId(9876);
        putHeader.setCorrelationId(CorrelationIdImpl.restoreId(1234));
        putHeader.setSchemaWireId((short) 1);

        ByteBufferOutputStream bbos = new ByteBufferOutputStream(1024);
        // Specifying a large enough size for the output stream so that
        // the streamed out EventHeader instance is captured in the 1st
        // ByteBuffer of 'bbos'.

        putHeader.streamOut(bbos);

        ByteBuffer[] buffers = bbos.reset();
        assertEquals(1, buffers.length);

        // Stream in

        ByteBufferInputStream bbis = new ByteBufferInputStream(buffers);

        putHeader = new PutHeader();
        putHeader.streamIn(bbis);

        assertEquals(0, putHeader.flags());
        assertEquals(15, putHeader.messageWords());
        assertEquals(0, putHeader.optionsWords());
        assertEquals(1, putHeader.compressionType());
        assertEquals(9, putHeader.headerWords());
        assertEquals(9876, putHeader.queueId());

        CorrelationId corId = CorrelationIdImpl.restoreId(1234);
        assertEquals(corId, putHeader.correlationId());
        assertEquals(0, putHeader.crc32c());
        assertEquals(1, putHeader.schemaWireId());
    }
}
