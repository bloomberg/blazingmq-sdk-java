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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LimitedInputStreamTest {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testConstructor() throws IOException {
        InputStream source = null;

        try {
            new LimitedInputStream(source, -1);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'source stream' must be non-null", e.getMessage());
            logger.debug("Caught expected exception: ", e);
        }

        try {
            source = new ByteBufferInputStream(ByteBuffer.allocate(0));

            new LimitedInputStream(source, -1);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'limit' must be non-negative", e.getMessage());
            logger.debug("Caught expected exception: ", e);
        }

        try {
            source = new ByteBufferInputStream(ByteBuffer.allocate(0));

            new LimitedInputStream(source, 1);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'limit' must not exceed 0", e.getMessage());
            logger.debug("Caught expected exception: ", e);
        }

        source = new ByteBufferInputStream(ByteBuffer.allocate(0));
        new LimitedInputStream(source, 0);
        // no exceptions
    }

    @Test
    public void testReadEmptyStream() throws IOException {
        InputStream source = new ByteBufferInputStream(ByteBuffer.allocate(0));
        LimitedInputStream stream = new LimitedInputStream(source, 0);

        assertEquals(0, stream.available());
        assertEquals(-1, stream.read());

        byte[] bytes = new byte[0];

        assertEquals(-1, stream.read(bytes));
        assertEquals(-1, stream.read(bytes, 0, bytes.length));
    }

    @Test
    public void testReadLimitedStream() throws IOException {
        byte[] content = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

        InputStream source = new ByteBufferInputStream(ByteBuffer.wrap(content));
        LimitedInputStream stream = new LimitedInputStream(source, 9);

        assertEquals(9, stream.available());

        // Read one byte per call
        assertEquals(0, stream.read());
        assertEquals(8, stream.available());

        assertEquals(1, stream.read());
        assertEquals(7, stream.available());

        // Read whole array
        byte[] bytes = new byte[4];
        int read = stream.read(bytes);

        assertEquals(3, stream.available());
        assertEquals(bytes.length, read);
        assertEquals(2, bytes[0]);
        assertEquals(3, bytes[1]);
        assertEquals(4, bytes[2]);
        assertEquals(5, bytes[3]);

        // Read one byte per call
        assertEquals(6, stream.read());
        assertEquals(2, stream.available());

        // Read part of array
        read = stream.read(bytes, 0, 3);

        assertEquals(0, stream.available());
        assertEquals(2, read);
        assertEquals(7, bytes[0]);
        assertEquals(8, bytes[1]);

        // Stream is empty
        assertEquals(-1, stream.read());
        assertEquals(-1, stream.read(bytes));
        assertEquals(-1, stream.read(bytes, 0, bytes.length));

        // Source stream has one byte
        assertEquals(1, source.available());
        assertEquals(9, source.read());
    }
}
