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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.msg.MessagesTestSamples;
import com.bloomberg.bmq.util.TestHelpers;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventImplHeaderTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testStreamIn() throws IOException {
        try (InputStream is =
                this.getClass()
                        .getResourceAsStream(MessagesTestSamples.STATUS_MSG_27032018.filePath())) {
            // Allocate a direct (memory-mapped) byte buffer
            ByteBuffer buf =
                    ByteBuffer.allocateDirect(MessagesTestSamples.STATUS_MSG_27032018.length());

            int b;
            while ((b = is.read()) != -1) {
                buf.put((byte) b);
            }

            // Flips this buffer. The limit is set to the current position and then
            // the position is set to zero. If the mark is defined then it is discarded.
            buf.flip();

            ByteBufferInputStream bbis = new ByteBufferInputStream(buf);
            EventHeader header = new EventHeader();

            header.streamIn(bbis);

            assertEquals(0, header.fragmentBit());
            assertEquals(36, header.length());
            assertEquals(1, header.protocolVersion());
            assertEquals(2, header.headerWords());

            EventType type = header.type();
            assertNotNull(type);
            assertEquals(EventType.CONTROL, type);
        }
    }

    @Test
    public void testStreamInFragmented() throws IOException {
        try (InputStream is =
                this.getClass()
                        .getResourceAsStream(MessagesTestSamples.STATUS_MSG_FRAGMENT.filePath())) {
            // Allocate a direct (memory-mapped) byte buffer
            ByteBuffer buf =
                    ByteBuffer.allocateDirect(MessagesTestSamples.STATUS_MSG_FRAGMENT.length());

            int b;
            while ((b = is.read()) != -1) {
                buf.put((byte) b);
            }

            // Flips this buffer. The limit is set to the current position and then
            // the position is set to zero. If the mark is defined then it is discarded.
            buf.flip();

            ByteBufferInputStream bbis = new ByteBufferInputStream(buf);
            EventHeader header = new EventHeader();

            header.streamIn(bbis);

            assertEquals(1, header.fragmentBit());
            assertEquals(36, header.length());
            assertEquals(1, header.protocolVersion());
            assertEquals(2, header.headerWords());

            EventType type = header.type();
            assertNotNull(type);
            assertEquals(EventType.CONTROL, type);
        }
    }

    @Test
    public void testStreamOut() {
        // Plan:
        // - Create an EventHeader instance, populate it and stream it out.
        // - Compare the streamed out representation with the one stored in the
        //   binary file.

        try {
            EventHeader header = new EventHeader();

            header.setLength(72);
            header.setType(EventType.CONTROL);
            header.setControlEventEncodingType(EncodingType.JSON);

            ByteBufferOutputStream bbos = new ByteBufferOutputStream(1024);
            // Specifying a large enough size for the output stream so that
            // the streamed out EventHeader instance is captured in the 1st
            // ByteBuffer of 'bbos'.

            header.streamOut(bbos);

            ByteBuffer[] message = bbos.reset();
            assertEquals(1, message.length);
            TestHelpers.compareWithFileContent(
                    message, MessagesTestSamples.STATUS_MSG.setLength(EventHeader.HEADER_SIZE));
        } catch (IOException e) {
            logger.info("IOException while encoding header: ", e);
            fail();
        }
    }

    @Test
    public void testGetters() {
        // Ensure that getters on a default constructed EventHeader return
        // correct values.

        EventHeader header = new EventHeader();

        assertEquals(EventHeader.HEADER_SIZE, header.length());
        assertEquals(0, header.fragmentBit());
        assertEquals(Protocol.VERSION, header.protocolVersion());
        assertEquals(EventHeader.HEADER_SIZE / Protocol.WORD_SIZE, header.headerWords());
        assertEquals(EventType.UNDEFINED, header.type());
    }

    @Test
    public void testSetters() {
        // Set the maximum values for various fields as allowed by the
        // EventHeader struct, and ensure that getters return the same values.

        final int MAX_LENGTH = Integer.MAX_VALUE;
        final byte MAX_PV = (byte) 3; // protocol version
        final EventType MAX_TYPE = EventType.ACK;
        final byte MAX_HEADER_WORDS = (byte) ((1 << 8) - 1);

        EventHeader header = new EventHeader();

        header.setLength(MAX_LENGTH);
        header.setProtocolVersion(MAX_PV);
        header.setType(MAX_TYPE);
        header.setHeaderWords(MAX_HEADER_WORDS);

        assertEquals(MAX_LENGTH, header.length());
        assertEquals(0, header.fragmentBit());
        assertEquals(MAX_PV, header.protocolVersion());
        assertEquals(MAX_HEADER_WORDS, header.headerWords());
        assertEquals(EventType.ACK, header.type());
    }
}
