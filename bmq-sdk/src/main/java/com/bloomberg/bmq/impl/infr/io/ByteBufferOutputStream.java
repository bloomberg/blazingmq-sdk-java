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

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An output stream of ByteBuffers
 *
 * <p>invariants of this structure are listed below.
 *
 * <p>1. a stream that has never been written to will have no buffers yet.
 *
 * <p>2. buffers are added on demand.
 *
 * <p>3. writeBuffers() or big array write(), the current buffer is sliced and the remainder is
 * added after as the current buffer.
 *
 * <p>4. writeBuffers() appends duplicated buffers wholesale instead of copying.
 *
 * <p>5. writeBuffers() ByteBuffers from outside should always be either unflipped or a wrapped
 * array.
 *
 * <p>7. write() byte arrays larger than the buffer size get wrapped, smaller ones get copied in one
 * piece.
 *
 * <p>8. as a result of 7, byte arrays can always be read fully in one read().
 *
 * <p>9. totalBytes is kept up to date as fields / buffers are written in.
 *
 * <p>10. the current append buffer is always the last buffer in bbArray.
 */
public class ByteBufferOutputStream extends OutputStream implements DataOutput {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final ArrayList<ByteBuffer> bbArray;
    private final int bufSize;
    private final int minSliceSize;
    private int totalBytes;
    private int currentBufferIndex;
    private boolean isOpen;

    private static final int KB = 1024;
    private static final int DEFAULT_BUF_SIZE = 4 * KB;
    private static final int BIG_BUF_SIZE = 4 * KB;

    // small buffer size should be adequete for many event types.
    private static final int SMALL_BUF_SIZE = 512;

    private static final int DEFAULT_MIN_SLICE_SIZE = 128;

    public static ByteBufferOutputStream smallBlocks() {
        return new ByteBufferOutputStream(SMALL_BUF_SIZE);
    }

    public static ByteBufferOutputStream bigBlocks() {
        return new ByteBufferOutputStream(BIG_BUF_SIZE);
    }

    public ByteBufferOutputStream() {
        this(DEFAULT_BUF_SIZE, DEFAULT_MIN_SLICE_SIZE);
    }

    public ByteBufferOutputStream(int bufSize) {
        this(bufSize, DEFAULT_MIN_SLICE_SIZE);
    }

    public ByteBufferOutputStream(int bufSize, int minSliceSize) {
        bbArray = new ArrayList<>();
        this.bufSize = bufSize;
        this.minSliceSize = minSliceSize;
        isOpen = true;
        totalBytes = 0;
        currentBufferIndex = -1;
    }

    private int availableCapacity() {
        if (bbArray.isEmpty() || currentBufferIndex >= bbArray.size()) {
            return 0;
        }
        return getCurrent().remaining();
    }

    private void swapBuffers(int aIndex, int bIndex) {
        if (aIndex == bIndex) return;
        if (aIndex >= bbArray.size() || bIndex >= bbArray.size()) {
            logger.error(
                    "tried to swap indexes "
                            + aIndex
                            + " and "
                            + bIndex
                            + " in array of size "
                            + bbArray.size());
            return;
        }
        ByteBuffer a = bbArray.get(aIndex);
        ByteBuffer b = bbArray.get(bIndex);
        bbArray.set(aIndex, b);
        bbArray.set(bIndex, a);
    }

    private void ensureCapacity(int size) {
        int currentBufferCapacity = availableCapacity();
        if (size > currentBufferCapacity - 1) {
            // why -1 ? if we write all the way to to end of a
            // ByteBuffer, then we cannot tell if is is cleared
            // or flipped.
            // need more space, either find a slice with enough or allocate a new one
            if (currentBufferCapacity >= minSliceSize) {
                ByteBuffer remainder = maybeSliceCurrent();
                if (remainder != null) {
                    bbArray.add(remainder);
                }
            }
            currentBufferIndex++;
            int candidateIndex = currentBufferIndex;
            while (bbArray.size() > candidateIndex
                    && bbArray.get(candidateIndex).remaining() < size) {
                candidateIndex++;
            }

            if (candidateIndex == bbArray.size()) {
                // couldn't find a big enough existing slice - have to allocate a new buffer
                addBuffer(Math.max(bufSize, size));
            }
            // put it in place
            swapBuffers(currentBufferIndex, candidateIndex);
        }
    }

    private void addRemainderOrNew(ByteBuffer remainder) {
        if (remainder != null) {
            bbArray.add(remainder);
        } else {
            if (currentBufferIndex >= bbArray.size()) {
                addBuffer();
            }
        }
    }

    @Override
    public void write(int b) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        ensureCapacity(1);
        getCurrent().put((byte) b);
        totalBytes += 1;
    }

    @Override
    public void write(byte[] ba) throws IOException {
        write(ba, 0, ba.length);
    }

    @Override
    public void write(byte[] ba, int offset, int length) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        if (length <= 0 || (length > ba.length - offset)) return;

        ensureCapacity(length);
        getCurrent().put(ba, offset, length);
        totalBytes += length;
    }

    public ByteBuffer[] peekUnflipped() {
        return bbArray.stream()
                .limit(currentBufferIndex + 1)
                .map(ByteBuffer::duplicate)
                .toArray(ByteBuffer[]::new);
    }

    /**
     * Make a readable view of the underlying data without copying it.
     *
     * <p>The bbos can continue to be written to.
     */
    public ByteBuffer[] peek() {
        ByteBuffer[] duplicates = peekUnflipped();
        for (ByteBuffer b : duplicates) {
            b.flip();
        }
        return duplicates;
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

        ensureCapacity(1);
        getCurrent().put((byte) v);
        totalBytes += 1;
    }

    @Override
    public void writeBytes(String s) throws IOException {
        throw new UnsupportedOperationException();
    }

    // a buffer that has never been put() to nor flipped
    private boolean bufferIsClear(ByteBuffer b) {
        return b.position() == 0 && b.limit() == b.capacity();
    }

    private ByteBuffer getCurrent() {
        if (currentBufferIndex < 0 || currentBufferIndex >= bbArray.size()) return null;
        return bbArray.get(currentBufferIndex);
    }

    private ByteBuffer maybeSliceCurrent() {
        ByteBuffer current = getCurrent();
        if (current != null) {
            // a remainder slice should be meaningfully sized - at least as big as the ByteBuffer
            // overhead
            if (current.remaining() >= minSliceSize) {
                ByteBuffer remainder = current.slice();
                return remainder;
            }
        }
        return null;
    }

    public void writeBuffer(ByteBuffer buffer) throws IOException {
        writeBuffer(true, buffer);
    }

    public void writeBuffer(boolean skipCleared, ByteBuffer buffer) throws IOException {
        writeBuffers(skipCleared, Collections.singletonList(buffer));
    }

    public void writeBuffers(ByteBuffer... buffers) throws IOException {
        writeBuffers(true, Arrays.asList(buffers));
    }

    public void writeBuffers(boolean skipCleared, ByteBuffer... buffers) throws IOException {
        writeBuffers(skipCleared, Arrays.asList(buffers));
    }

    public void writeBuffers(Collection<ByteBuffer> buffers) throws IOException {
        writeBuffers(true, buffers);
    }

    public void writeBuffers(boolean skipCleared, Collection<ByteBuffer> buffers)
            throws IOException {
        if (!isOpen) throw new IOException("Stream closed");
        ByteBuffer remainder = maybeSliceCurrent();
        bbArray.ensureCapacity(bbArray.size() + buffers.size() + 1 /* remainder or new buffer */);
        for (ByteBuffer b : buffers) {
            if (skipCleared && bufferIsClear(b)) continue;
            ByteBuffer dup = b.duplicate();
            if (dup.position() == 0) {
                dup.position(dup.limit());
            }
            currentBufferIndex++;
            bbArray.add(dup);
            swapBuffers(currentBufferIndex, bbArray.size() - 1);
            totalBytes += dup.position();
        }
        addRemainderOrNew(remainder);
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
        writeBuffers(other.buffers());
    }

    @Override
    public void writeChar(int v) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        ensureCapacity(2);
        getCurrent().putChar((char) v);
        totalBytes += 2;
    }

    @Override
    public void writeChars(String s) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeDouble(double v) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        ensureCapacity(8);
        getCurrent().putDouble(v);
        totalBytes += 8;
    }

    @Override
    public void writeFloat(float v) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        ensureCapacity(4);
        getCurrent().putFloat(v);
        totalBytes += 4;
    }

    @Override
    public void writeInt(int v) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        ensureCapacity(4);
        getCurrent().putInt(v);
        totalBytes += 4;
    }

    @Override
    public void writeLong(long v) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        ensureCapacity(8);
        getCurrent().putLong(v);
        totalBytes += 8;
    }

    @Override
    public void writeShort(int v) throws IOException {
        if (!isOpen) throw new IOException("Stream closed");

        ensureCapacity(2);
        getCurrent().putShort((short) v);
        totalBytes += 2;
    }

    @Override
    public void writeUTF(String str) throws IOException {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        write(bytes);
    }

    public void writeAscii(String str) throws IOException {
        byte[] bytes = str.getBytes(StandardCharsets.US_ASCII);
        write(bytes);
    }

    /**
     * Resets the Stream. Previously allocated Buffers are not reused for this Stream.
     *
     * @return ByteBuffer[] previously allocated buffers flipped to the read mode.
     */
    public ByteBuffer[] reset() {
        ByteBuffer[] bbArrayCopy = peek();

        bbArray.clear();
        totalBytes = 0;
        isOpen = false;

        return bbArrayCopy;
    }

    public int numByteBuffers() {
        return currentBufferIndex + 1;
    }

    public int size() {
        return (isOpen ? (totalBytes) : (0));
    }

    private void addBuffer() {
        addBuffer(bufSize);
    }

    // allocate a buffer which is large enough to store data
    // of specified size
    private void addBuffer(int size) {
        ByteBuffer buf = ByteBuffer.allocate(size);
        bbArray.add(buf);
    }
}
