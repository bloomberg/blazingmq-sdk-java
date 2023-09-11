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
import com.bloomberg.bmq.impl.infr.util.Limits;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteBufferOutputStream extends OutputStream implements DataOutput {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private ArrayList<ByteBuffer> bbArray;
    private ByteBuffer currentBuffer;

    private int currentBufferIndex = 0;
    private int bufSize;
    private int prevBuffersNumBytes;

    private boolean isOpen;

    private static final int KB = 1024;
    private static final int DEFAULT_BUF_SIZE = 4 * KB;

    public ByteBufferOutputStream() {
        init(DEFAULT_BUF_SIZE);
    }

    public ByteBufferOutputStream(int bufSize) {
        init(Argument.expectPositive(bufSize, "bufSize"));
    }

    private void init(int bufSize) {
        bbArray = new ArrayList<>();
        this.bufSize = bufSize;
        currentBuffer = ByteBuffer.allocate(bufSize);
        bbArray.add(currentBuffer);
        currentBufferIndex = 0;
        isOpen = true;
        prevBuffersNumBytes = 0;
    }

    @Override
    public void write(int b) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        if (currentBuffer.remaining() < Limits.BYTE_SIZE) getNewBuffer();

        currentBuffer.put((byte) b);
    }

    @Override
    public void write(byte[] ba) throws IOException {
        write(ba, 0, ba.length);
    }

    @Override
    public void write(byte[] ba, int offset, int length) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        if (length <= 0 || (length > ba.length - offset)) return;

        int numBytesLeft = length;
        while (true) {
            int numToWrite = Math.min(numBytesLeft, currentBuffer.remaining());
            currentBuffer.put(ba, offset, numToWrite);
            numBytesLeft -= numToWrite;
            offset += numToWrite;

            if (numBytesLeft > 0) getNewBuffer();
            else break;
        }
    }

    /**
     * Make a readable view of the underlying data without copying it.
     *
     * <p>The bbos can continue to be written to.
     */
    public ByteBuffer[] peek() {
        return bbArray.stream()
                .map(bb -> (ByteBuffer) (bb.duplicate().flip()))
                .toArray(ByteBuffer[]::new);
    }

    private ArrayList<ByteBuffer> buffers() {
        return bbArray;
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeByte(int v) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        if (currentBuffer.remaining() < Limits.BYTE_SIZE) getNewBuffer();

        currentBuffer.put((byte) v);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        throw new UnsupportedOperationException();
    }

    private boolean bufferIsFresh(ByteBuffer b) {
        // a buffer that has never been put() to nor flipped
        return b.position() == 0 && b.limit() == b.capacity();
    }

    public void writeBuffer(ByteBuffer b) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");
        if (bufferIsFresh(b)) return;

        boolean currentIsFresh = bufferIsFresh(currentBuffer);
        // remove the currentBuffer if it is fresh
        // we'll put it back at the end after adding other's buffers
        // to avoid allocating a new one in that case
        if (currentIsFresh) {
            bbArray.remove(currentBufferIndex);
        }
        ByteBuffer buf = b.duplicate();
        if (buf.limit() != buf.capacity()) {
            // it has already been flipped - unflip it
            int newPosition = buf.limit();
            buf.limit(buf.capacity());
            buf.position(newPosition);
        }
        prevBuffersNumBytes += buf.position();
        bbArray.add(buf);
        if (currentIsFresh) {
            bbArray.add(currentBuffer);
        } else {
            addBuffer();
        }
        currentBufferIndex = bbArray.size() - 1;
    }

    /**
     * Appends the bytes in the specified ByteBuffer stream bbobs into this Stream. This stream and
     * the bbos should be open for writing.
     *
     * @param bbos stream to write bytes to
     * @throws IOException if the stream is not open
     */
    public void writeBuffers(ByteBufferOutputStream other) throws IOException {
        if (!isOpen || !other.isOpen) throw new IOException("Stream closed");
        for (ByteBuffer b : other.buffers()) {
            writeBuffer(b);
        }
    }

    @Override
    public void writeChar(int v) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        if (currentBuffer.remaining() < Limits.CHAR_SIZE) getNewBuffer();

        currentBuffer.putChar((char) v);
    }

    @Override
    public void writeChars(String s) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeDouble(double v) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        if (currentBuffer.remaining() < Limits.DOUBLE_SIZE) getNewBuffer();

        currentBuffer.putDouble(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        if (currentBuffer.remaining() < Limits.FLOAT_SIZE) getNewBuffer();

        currentBuffer.putFloat(v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        if (currentBuffer.remaining() < Limits.INT_SIZE) getNewBuffer();

        currentBuffer.putInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        if (currentBuffer.remaining() < Limits.LONG_SIZE) getNewBuffer();

        currentBuffer.putLong(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        if (currentBuffer.remaining() < Limits.SHORT_SIZE) getNewBuffer();

        currentBuffer.putShort((short) v);
    }

    @Override
    public void writeUTF(String str) throws IOException {
        write(str.getBytes(StandardCharsets.UTF_8));
    }

    public void writeAscii(String str) throws IOException {
        write(str.getBytes(StandardCharsets.US_ASCII));
    }

    /**
     * Resets the Stream. Previously allocated Buffers are not reused for this Stream.
     *
     * @return ByteBuffer[] previously allocated buffers flipped to the read mode.
     */
    public ByteBuffer[] reset() {
        ByteBuffer[] bbArrayCopy = peek();

        bbArray.clear();
        prevBuffersNumBytes = 0;
        currentBuffer = null;
        currentBufferIndex = 0;

        isOpen = false;

        return bbArrayCopy;
    }

    public int numByteBuffers() {
        return bbArray.size();
    }

    public int size() {
        return (isOpen ? (prevBuffersNumBytes + currentBuffer.position()) : (0));
    }

    private void getNewBuffer() {
        addBuffer();
    }

    private void addBuffer() {
        addBuffer(bufSize);
    }

    // allocate a buffer which is large enough to store data
    // of specified size
    private void addBuffer(int size) {
        prevBuffersNumBytes += currentBuffer.position();
        int allocationSize = Math.max(size, bufSize);
        currentBuffer = ByteBuffer.allocate(allocationSize);
        bbArray.add(currentBuffer);
        currentBufferIndex = bbArray.size() - 1;
    }
}
