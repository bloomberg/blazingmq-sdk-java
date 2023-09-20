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

import com.bloomberg.bmq.impl.infr.util.Argument;
import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteBufferInputStream extends InputStream implements DataInput {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final ByteBuffer[] byteBuffers;
    private int currentBuffer = 0;

    public ByteBufferInputStream(ByteBuffer bb) {
        Argument.expectNonNull(bb, "buffer");

        ByteBuffer b = ByteBuffer.allocate(bb.limit());
        // Spotbugs requires to make a copy
        b.put(bb);
        b.rewind();
        byteBuffers = new ByteBuffer[] {b};
    }

    public ByteBufferInputStream(ByteBuffer... bb) {
        Argument.expectNonNull(bb, "buffer array");
        for (ByteBuffer b : bb) {
            Argument.expectNonNull(b, "buffer");
        }
        byteBuffers = Arrays.copyOf(bb, bb.length); // Spotbugs requires to make a copy
    }

    private ByteBuffer getBuffer() throws IOException {
        while (currentBuffer < byteBuffers.length) {
            ByteBuffer buffer = byteBuffers[currentBuffer];
            if (buffer.hasRemaining()) {
                return buffer;
            }
            currentBuffer++;
        }
        throw new IOException("Stream is empty");
    }

    @Override
    public int read(byte[] buf) throws IOException {
        // To be refactored later.
        // In standard InputStream implementations `read(byte[])` method either read all bytes,
        // read part of bytes or return -1;

        int start = 0;
        while (start < buf.length) {
            start += read(buf, start, buf.length - start);
        }
        return start;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        ByteBuffer buffer = getBuffer();
        int remaining = buffer.remaining();
        if (len > remaining) {
            buffer.get(b, off, remaining);
            return remaining;
        } else {
            buffer.get(b, off, len);
            return len;
        }
    }

    public int read(ByteBufferOutputStream bbos, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        int needed = len;
        while (needed > 0 && currentBuffer < byteBuffers.length) {
            ByteBuffer buffer = getBuffer();
            ByteBuffer readable = buffer.slice();
            int remaining = readable.remaining();
            if (needed > remaining) {
                readable.position(remaining);
                bbos.writeBuffer(false, readable);
                buffer.position(buffer.limit());
                needed -= remaining;
            } else {
                readable.limit(needed);
                readable.position(needed);
                bbos.writeBuffer(false, readable);
                buffer.position(buffer.position() + needed);
                needed = 0;
            }
        }
        return len - needed;
    }

    private ByteBuffer getBuffer(int length) throws IOException {
        if (length == 0) {
            return ByteBuffer.allocate(0);
        }
        ByteBuffer buffer = getBuffer();
        if (buffer.remaining() >= length) { // can return current as-is?
            return buffer; // return w/o copying
        }
        logger.trace("Buffer allocation {}", length);
        ByteBuffer result = ByteBuffer.allocate(length);
        int start = 0;
        while (start < length) {
            start += read(result.array(), start, length - start);
        }
        return result;
    }

    @Override
    public byte readByte() throws IOException {
        try {
            return getBuffer(Byte.BYTES).get();
        } catch (BufferUnderflowException bue) {
            throw new IOException(bue.getMessage());
        }
    }

    @Override
    public short readShort() throws IOException {
        try {
            return getBuffer(Short.BYTES).getShort();
        } catch (BufferUnderflowException bue) {
            throw new IOException(bue.getMessage());
        }
    }

    @Override
    public int readInt() throws IOException {
        try {
            return getBuffer(Integer.BYTES).getInt();
        } catch (BufferUnderflowException bue) {
            throw new IOException(bue.getMessage());
        }
    }

    @Override
    public long readLong() throws IOException {
        try {
            return getBuffer(Long.BYTES).getLong();
        } catch (BufferUnderflowException bue) {
            throw new IOException(bue.getMessage());
        }
    }

    @Override
    public float readFloat() throws IOException {
        try {
            return getBuffer(Float.BYTES).getFloat();
        } catch (BufferUnderflowException bue) {
            throw new IOException(bue.getMessage());
        }
    }

    @Override
    public double readDouble() throws IOException {
        try {
            return getBuffer(Double.BYTES).getDouble();
        } catch (BufferUnderflowException bue) {
            throw new IOException(bue.getMessage());
        }
    }

    @Override
    public boolean readBoolean() {
        throw new UnsupportedOperationException();
    }

    @Override
    public char readChar() throws IOException {
        try {
            return getBuffer(Character.BYTES).getChar();
        } catch (BufferUnderflowException bue) {
            throw new IOException(bue.getMessage());
        }
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        // To be refactored later.
        // Currently 'read(byte[])' either reads all bytes or throws an exception.
        // After refactoring, it will either read all bytes, read part of bytes or return -1;
        int n = read(b);

        if (n != b.length) {
            throw new IOException("Failed to read " + n + " bytes into array");
        }
    }

    @Override
    public void readFully(byte[] b, int off, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readLine() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int readUnsignedByte() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int readUnsignedShort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int skipBytes(int n) throws IOException {
        return skip(n);
    }

    @Override
    public int read() throws IOException {
        try {
            return readByte() & 0xFF;
        } catch (BufferUnderflowException bue) {
            throw new IOException(bue.getMessage());
        }
    }

    @Override
    public int available() {
        int res = 0;
        for (int i = currentBuffer; i < byteBuffers.length; i++) {
            res += byteBuffers[i].remaining();
        }
        return res;
    }

    @Override
    public synchronized void reset() {
        for (ByteBuffer b : byteBuffers) {
            if (b.position() != 0) b.rewind();
        }
        currentBuffer = 0;
    }

    public int skip(int n) throws IOException {
        int numSkip = n;
        if (numSkip <= 0) {
            return 0;
        }
        while (numSkip > 0) {
            ByteBuffer b = getBuffer();
            int newPosition = b.position() + numSkip;
            numSkip = newPosition - b.limit();
            if (numSkip > 0) {
                b.position(b.limit());
            } else {
                b.position(newPosition);
            }
        }
        return n;
    }

    public int position() {
        int res = 0;
        for (int i = 0; i <= currentBuffer; i++) {
            res += byteBuffers[i].position();
        }
        return res;
    }
}
