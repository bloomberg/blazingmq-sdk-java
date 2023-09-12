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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteBufferOutputStreamTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private byte[] readAsBytes(ByteBufferOutputStream bbos) throws IOException {
        int total = bbos.size();
        byte[] actual = new byte[total];
        int pos = 0;
        ByteBufferInputStream bbis = new ByteBufferInputStream(bbos.peek());
        while (pos < total) {
            int qty = bbis.read(actual, pos, total - pos);
            if (qty == 0) {
                break;
            }
            pos += qty;
        }
        return actual;
    }

    @Test
    public void testFresh() {
        ByteBufferOutputStream bbos = new ByteBufferOutputStream();
        assertEquals(0, bbos.size());
        ByteBuffer[] buffers = bbos.peek();
        assertEquals(0, buffers.length);
    }

    @Test
    public void testWriteEmptyBuffer() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(0);
        ByteBufferOutputStream bbos = new ByteBufferOutputStream();
        bbos.writeBuffer(buf);
        assertEquals(0, bbos.size());

        byte[] actual = readAsBytes(bbos);
        assertEquals(0, actual.length);
    }

    @Test
    public void testWriteBuffers() throws IOException {
        String payload1 = "The Quick Brown Fox jumps over the Lazy Dog";
        String payload2 = "All work and no play makes Jack a dull boy";
        String payload3 = "Lorem ipsum dolor sit amet, consectetur adipiscing elit";
        String payload4 = "Once upon a midnight dreary, while I pondered weak and weary";

        String expected = payload1 + payload2 + payload3 + payload4;
        ByteBuffer[] bufs =
                new ByteBuffer[] {
                    ByteBuffer.allocate(100),
                    ByteBuffer.allocate(200),
                    ByteBuffer.allocate(100),
                    ByteBuffer.allocate(100)
                };
        bufs[0].put(payload1.getBytes());
        bufs[1].put(payload2.getBytes()).put(payload3.getBytes());
        bufs[3].put(payload4.getBytes());

        ByteBufferOutputStream bbos = new ByteBufferOutputStream();
        bbos.writeBuffers(bufs);
        assertEquals("bytes written", expected.getBytes().length, bbos.size());

        String actual = new String(readAsBytes(bbos));
        assertEquals("contents", expected, actual);
    }

    @Test
    public void testWritePrimitives() throws IOException {
        ByteBufferOutputStream bbos = new ByteBufferOutputStream();
        bbos.write(42);
        bbos.writeShort(1000);
        bbos.writeInt(31337);
        bbos.writeLong(1234567890L);
        bbos.writeChar('Z');
        bbos.writeFloat(0.0001F);
        bbos.writeDouble(1234.5678);

        assertEquals("bytes written", 1 + 2 + 4 + 8 + 2 + 4 + 8, bbos.size());
        ByteBufferInputStream bbis = new ByteBufferInputStream(bbos.peek());

        assertEquals("byte value", 42, bbis.readByte());
        assertEquals("short value", 1000, bbis.readShort());
        assertEquals("int value", 31337, bbis.readInt());
        assertEquals("long value", 1234567890L, bbis.readLong());
        assertEquals("char value", 'Z', bbis.readChar());
        assertEquals("float value", 0.0001F, bbis.readFloat(), 0.000001);
        assertEquals("double value", 1234.5678, bbis.readDouble(), 0.000001);
        assertEquals("bytes remaining", 0, bbis.available());
    }

    @Test
    public void testWritePrimitivesAndBuffers() throws IOException {
        String payload1 = "The Quick Brown Fox jumps over the Lazy Dog";
        String payload2 = "All work and no play makes Jack a dull boy";
        String payload3 = "Lorem ipsum dolor sit amet, consectetur adipiscing elit";
        ByteBuffer[] bufs = new ByteBuffer[] {ByteBuffer.allocate(100), ByteBuffer.allocate(200)};
        bufs[0].put(payload1.getBytes());
        bufs[1].put(payload2.getBytes()).put(payload3.getBytes());

        ByteBufferOutputStream bbos = new ByteBufferOutputStream();
        bbos.write(42);
        bbos.writeShort(1000);
        bbos.writeInt(31337);
        bbos.writeLong(1234567890L);
        bbos.writeBuffers(bufs);
        bbos.writeChar('Z');
        bbos.writeFloat(0.0001F);
        bbos.writeDouble(1234.5678);

        assertEquals(
                "bytes written",
                1
                        + 2
                        + 4
                        + 8
                        + payload1.length()
                        + payload2.length()
                        + payload3.length()
                        + 2
                        + 4
                        + 8,
                bbos.size());

        ByteBufferInputStream bbis = new ByteBufferInputStream(bbos.peek());

        assertEquals("byte value", 42, bbis.readByte());
        assertEquals("short value", 1000, bbis.readShort());
        assertEquals("int value", 31337, bbis.readInt());
        assertEquals("long value", 1234567890L, bbis.readLong());

        byte[] actual1 = new byte[payload1.length()];
        byte[] actual2 = new byte[payload2.length()];
        byte[] actual3 = new byte[payload3.length()];
        assertEquals("payload 1 length", payload1.length(), bbis.read(actual1));
        assertEquals("payload 1 contents", payload1, new String(actual1));
        assertEquals("payload 2 length", payload2.length(), bbis.read(actual2));
        assertEquals("payload 2 contents", payload2, new String(actual2));
        assertEquals("payload 3 length", payload3.length(), bbis.read(actual3));
        assertEquals("payload 3 contents", payload3, new String(actual3));

        assertEquals("char value", 'Z', bbis.readChar());
        assertEquals("float value", 0.0001F, bbis.readFloat(), 0.000001);
        assertEquals("double value", 1234.5678, bbis.readDouble(), 0.000001);
        assertEquals("bytes remaining", 0, bbis.available());
    }

    @Test
    public void testWriteTooMuchForOneBuffer() throws IOException {
        String payload = "Lorem ipsum dolor sit amet, consectetur adipiscing elit.";

        // make small buffers
        ByteBufferOutputStream bbos = new ByteBufferOutputStream(32);
        bbos.write(42);
        bbos.writeShort(1000);
        bbos.writeInt(31337);
        bbos.writeLong(1234567890L);
        bbos.write(payload.getBytes());
        bbos.writeChar('Z');
        bbos.writeFloat(0.0001F);
        bbos.writeDouble(1234.5678);

        assertEquals("bytes written", 1 + 2 + 4 + 8 + payload.length() + 2 + 4 + 8, bbos.size());

        ByteBufferInputStream bbis = new ByteBufferInputStream(bbos.peek());

        assertEquals("byte value", 42, bbis.readByte());
        assertEquals("short value", 1000, bbis.readShort());
        assertEquals("int value", 31337, bbis.readInt());
        assertEquals("long value", 1234567890L, bbis.readLong());

        byte[] actual = new byte[payload.length()];
        assertEquals("payload length", payload.length(), bbis.read(actual));
        assertEquals("payload contents", payload, new String(actual));

        assertEquals("char value", 'Z', bbis.readChar());
        assertEquals("float value", 0.0001F, bbis.readFloat(), 0.000001);
        assertEquals("double value", 1234.5678, bbis.readDouble(), 0.000001);
        assertEquals("bytes remaining", 0, bbis.available());
    }
}
