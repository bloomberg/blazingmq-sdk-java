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
package com.bloomberg.bmq.impl.infr.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ByteBufferInputStreamTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    void testEmpty() {
        ByteBuffer buf = ByteBuffer.allocate(0);
        ByteBufferInputStream bbis = new ByteBufferInputStream(buf);
        assertEquals(0, bbis.available());
        try {
            byte b = bbis.readByte();
            fail(); // Shouldn't be here
        } catch (IOException e) {
            logger.debug("IOException: ", e);
        }
        try {
            bbis.skip(1);
            fail(); // Shouldn't be here
        } catch (IOException e) {
            logger.debug("IOException: ", e);
        }
        assertEquals(0, bbis.available());
    }

    @Test
    void testReadByte() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(1);
        ByteBufferInputStream bbis = new ByteBufferInputStream(buf);
        assertEquals(1, bbis.available());

        byte b = bbis.readByte();

        assertEquals(0, bbis.available());

        try {
            b = bbis.readByte();
            fail(); // Shouldn't be here
        } catch (IOException e) {
            logger.debug("IOException: ", e);
        }
    }

    @Test
    void testReadBytes() throws IOException {
        ByteBuffer buf1 = ByteBuffer.allocate(1);
        ByteBuffer buf2 = ByteBuffer.allocate(1);
        buf1.put((byte) 'A').rewind();
        buf2.put((byte) 'B').rewind();

        ByteBufferInputStream bbis = new ByteBufferInputStream(buf1, buf2);
        assertEquals(2, bbis.available());
        assertEquals(0, bbis.position());

        assertEquals('A', (char) bbis.readByte());
        assertEquals(1, bbis.available());
        assertEquals(1, bbis.position());
        assertEquals('B', (char) bbis.readByte());
        assertEquals(0, bbis.available());
        assertEquals(2, bbis.position());
        bbis.reset();
        assertEquals(2, bbis.available());
        assertEquals(0, bbis.position());
        assertEquals('A', (char) bbis.readByte());
        assertEquals('B', (char) bbis.readByte());
        assertEquals(0, bbis.available());
        assertEquals(2, bbis.position());
    }

    @Test
    void testSkipBytes() throws IOException {
        ByteBuffer buf1 = ByteBuffer.allocate(1);
        ByteBuffer buf2 = ByteBuffer.allocate(1);
        buf1.put((byte) 'A').rewind();
        buf2.put((byte) 'B').rewind();

        ByteBufferInputStream bbis = new ByteBufferInputStream(buf1, buf2);
        assertEquals(2, bbis.available(), 2);

        assertEquals(1, bbis.skip(1));
        assertEquals(1, bbis.available());
        assertEquals('B', (char) bbis.readByte());
        assertEquals(0, bbis.available());
        bbis.reset();
        assertEquals(2, bbis.available());
        assertEquals(2, bbis.skip(2));
        assertEquals(0, bbis.available());
        bbis.reset();
        assertEquals('A', (char) bbis.readByte());
        assertEquals('B', (char) bbis.readByte());
        assertEquals(0, bbis.available());

        bbis.reset();
        assertEquals(2, bbis.available());
        try {
            assertEquals(3, bbis.skip(3));
            fail(); // Shouldn't be here
        } catch (IOException e) {
            logger.debug("IOException: ", e);
        }
    }

    @Test
    void testReadType() throws IOException {
        ByteBuffer buf1 = ByteBuffer.allocate(1);
        ByteBuffer buf2 = ByteBuffer.allocate(1);
        buf1.put((byte) 1).rewind();
        buf2.put((byte) 1).rewind();

        ByteBufferInputStream bbis = new ByteBufferInputStream(buf1, buf2);
        assertEquals(2, bbis.available());

        assertEquals(257, bbis.readShort());
        assertEquals(0, bbis.available());
        bbis.reset();
        assertEquals(2, bbis.available());
        assertEquals(1, bbis.skip(1));

        try {
            assertEquals(257, bbis.readShort());
            fail();
        } catch (IOException e) {
            logger.debug("IOException: ", e);
        }
        assertEquals(0, bbis.available());
    }

    @Test
    void testRead() throws IOException {
        ByteBuffer buf1 = ByteBuffer.allocate(1);
        ByteBuffer buf2 = ByteBuffer.allocate(1);
        buf1.put((byte) 1).rewind();
        buf2.put((byte) 1).rewind();

        ByteBufferInputStream bbis = new ByteBufferInputStream(buf1, buf2);

        assertEquals(2, bbis.available());

        final int bytesAvailable = bbis.available();
        byte[] bytes = new byte[bytesAvailable];

        assertEquals(1, bbis.read(bytes, 0, bytesAvailable));
        assertEquals(1, bbis.available());
        bbis.reset();
        assertEquals(bytesAvailable, bbis.read(bytes));
        assertEquals(0, bbis.available());
    }
}
