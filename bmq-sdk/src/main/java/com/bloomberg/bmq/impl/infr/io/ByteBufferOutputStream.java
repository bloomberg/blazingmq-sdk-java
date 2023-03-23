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

    public void writeBoolean(boolean v) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void writeByte(int v) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        if (currentBuffer.remaining() < Limits.BYTE_SIZE) getNewBuffer();

        currentBuffer.put((byte) v);
    }

    public void writeBytes(String s) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void writeBytes(ByteBuffer b) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");
        b.rewind();
        while (b.hasRemaining()) {
            if (currentBuffer.hasRemaining()) {
                if (b.remaining() > currentBuffer.remaining()) {
                    // Read one byte
                    currentBuffer.put(b.get());
                } else {
                    // Read the whole buffer
                    currentBuffer.put(b);
                }
                continue;
            }
            getNewBuffer();
        }
    }

    /**
     * Appends the bytes in the specified ByteBuffer stream bbobs into this Stream. This stream and
     * the bbos should be open for writing.
     *
     * @param bbos stream to write bytes to
     * @throws IOException if the stream is not open
     */
    public void writeBytes(ByteBufferOutputStream bbos) throws IOException {

        if (!isOpen || !bbos.isOpen) throw new IOException("Stream closed");

        for (int i = 0; i < bbos.bbArray.size(); i++) {
            ByteBuffer data = bbos.bbArray.get(i);
            data.flip();
            writeBytes(data);
        }
    }

    public void writeChar(int v) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        if (currentBuffer.remaining() < Limits.CHAR_SIZE) getNewBuffer();

        currentBuffer.putChar((char) v);
    }

    public void writeChars(String s) throws IOException {
        throw new UnsupportedOperationException();
    }

    public void writeDouble(double v) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        if (currentBuffer.remaining() < Limits.DOUBLE_SIZE) getNewBuffer();

        currentBuffer.putDouble(v);
    }

    public void writeFloat(float v) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        if (currentBuffer.remaining() < Limits.FLOAT_SIZE) getNewBuffer();

        currentBuffer.putFloat(v);
    }

    public void writeInt(int v) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        if (currentBuffer.remaining() < Limits.INT_SIZE) getNewBuffer();

        currentBuffer.putInt(v);
    }

    public void writeLong(long v) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        if (currentBuffer.remaining() < Limits.LONG_SIZE) getNewBuffer();

        currentBuffer.putLong(v);
    }

    public void writeShort(int v) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        if (currentBuffer.remaining() < Limits.SHORT_SIZE) getNewBuffer();

        currentBuffer.putShort((short) v);
    }

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
        ByteBuffer[] bbArrayCopy = new ByteBuffer[currentBufferIndex + 1];

        for (int i = 0; i <= currentBufferIndex; ++i) {
            bbArrayCopy[i] = bbArray.get(i);
            bbArrayCopy[i].flip();
        }

        bbArray.clear();
        prevBuffersNumBytes = 0;
        currentBuffer = null;
        currentBufferIndex = 0;

        isOpen = false;

        return bbArrayCopy;
    }

    /**
     * Clears the Stream for reuse. Clears the previously stored data but leaves the stream in its
     * previous state. If the stream is still open, then it can be reused to insert new data. Reuses
     * previously allocated Buffers if possible.
     */
    public void clear() {
        if (!isOpen) return;

        for (int i = 0; i <= currentBufferIndex; i++) {
            bbArray.get(i).clear();
        }
        prevBuffersNumBytes = 0;
        currentBuffer = bbArray.get(0);
        currentBufferIndex = 0;
    }

    public int numByteBuffers() {
        return bbArray.size();
    }

    public int size() {
        return (isOpen ? (prevBuffersNumBytes + currentBuffer.position()) : (0));
    }

    private void getNewBuffer() {
        // Try to reuse a previously allocated buffer before allocating new.
        if (currentBufferIndex < bbArray.size() - 1) {
            prevBuffersNumBytes += currentBuffer.position();
            currentBuffer = bbArray.get(++currentBufferIndex);
        } else {
            addBuffer();
        }
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
        currentBufferIndex++;
    }
}
