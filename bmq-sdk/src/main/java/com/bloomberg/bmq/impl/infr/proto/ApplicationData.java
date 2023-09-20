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

import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.util.Compression;
import com.bloomberg.bmq.impl.infr.util.PrintUtil;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationData {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final ThreadLocal<byte[]> decompressionBuffer =
            ThreadLocal.withInitial(() -> new byte[1024]);

    private ByteBufferOutputStream payload;
    private MessagePropertiesImpl properties;

    private CompressionAlgorithmType compressionType = CompressionAlgorithmType.E_NONE;
    private ByteBufferOutputStream compressedData;

    // TODO: remove after 2nd release of "new style" brokers.
    private boolean isOldStyleProperties = false;
    private boolean arePropertiesCompressed;

    private ByteBufferOutputStream outputBuffer;
    private long crc;

    private void resetCompressedData() {
        compressionType = CompressionAlgorithmType.E_NONE;
        compressedData = null;
        arePropertiesCompressed = false;
    }

    public final void setPayload(ByteBuffer... data) throws IOException {
        setPayload(true, data);
    }

    public final void setPayload(boolean skipCleared, ByteBuffer... data) throws IOException {
        if (data == null) {
            throw new IllegalArgumentException("'buffer array' must be non-null");
        }
        payload = new ByteBufferOutputStream();
        payload.writeBuffers(skipCleared, data);
        resetCompressedData();
    }

    public final void setProperties(MessagePropertiesImpl props) {
        properties = props;
        resetCompressedData();
    }

    // TODO: remove after 2nd release of "new style" brokers.
    public void setIsOldStyleProperties(boolean value) {
        isOldStyleProperties = value;
    }

    public boolean isOldStyleProperties() {
        return isOldStyleProperties;
    }

    public ByteBuffer[] applicationData() throws IOException {
        // used only in tests
        serializeToBuffer();
        return outputBuffer.peek();
    }

    public ByteBuffer[] payload() throws IOException {
        decompressData();
        if (payload == null) {
            return new ByteBuffer[0];
        }
        return payload.peek();
    }

    public MessagePropertiesImpl properties() {
        if (arePropertiesCompressed) {
            try {
                decompressData();
            } catch (IOException e) {
                throw new RuntimeException("Failed to decompress payload", e);
            }
        }

        return properties;
    }

    public boolean isCompressed() {
        return compressionType != CompressionAlgorithmType.E_NONE;
    }

    public int payloadSize() {
        return payload == null ? 0 : payload.size();
    }

    boolean hasProperties() {
        return properties != null && properties.numProperties() > 0;
    }

    public int propertiesSize() {
        return hasProperties() ? properties.totalSize() : 0;
    }

    public int unpackedSize() {
        int size = 0;

        if (!arePropertiesCompressed) {
            size += propertiesSize();
        }

        if (compressionType == CompressionAlgorithmType.E_NONE) {
            size += payloadSize();
        } else {
            size += compressedData.size();
        }

        return size;
    }

    public double compressionRatio() {
        final int unpackedSize = unpackedSize();

        if (unpackedSize == 0) {
            throw new IllegalStateException("Unpacked size must be greater than zero.");
        }

        final int uncompressedSize = propertiesSize() + payloadSize();
        return uncompressedSize / (double) unpackedSize;
    }

    public int numPaddingBytes() {
        return ProtocolUtil.calculatePadding(unpackedSize());
    }

    // TODO: remove "isOldStyleProperties" after 2nd release of "new style" brokers.
    public void streamIn(
            int size,
            boolean hasProperties,
            boolean isOldStyleProperties,
            CompressionAlgorithmType compressionType,
            ByteBufferInputStream bbis)
            throws IOException {

        // 1. Find number of padding bytes from the last payload byte
        // 2. Check compression type, stream in uncompressed data and/or
        //    buffer compressed data
        // 3. Skip padding byte

        // 1. Find number of padding bytes from the last payload byte
        final byte numPaddingBytes = ProtocolUtil.getPadding(bbis, size);

        // Cut off padding bytes from the end
        size -= numPaddingBytes;

        // 2. Check compression type, stream in uncompressed data and/or
        //    buffer compressed data

        resetCompressedData();

        // Stream in properties if they are not compressed
        this.isOldStyleProperties = isOldStyleProperties;
        if (hasProperties) {
            if (!isOldStyleProperties) {
                // New properties
                size -= streamInProperties(bbis);
            } else if (compressionType == CompressionAlgorithmType.E_NONE) {
                // Old properties
                size -= streamInPropertiesOld(bbis);
            }
        }

        // Stream uncompressed payload
        if (compressionType == CompressionAlgorithmType.E_NONE) {
            if (size > 0) {
                size -= streamInPayload(size, bbis);
            }

            if (size != 0) {
                throw new IOException("Failed to read uncompressed payload from input stream");
            }
        } else { // or buffer compressed data
            try (ByteBufferOutputStream bbos = new ByteBufferOutputStream(size)) {
                if (bbis.read(bbos, size) != size) {
                    throw new IOException("failed to read compressed payload into buffer");
                }

                compressedData = bbos;
                this.compressionType = compressionType;
                arePropertiesCompressed = hasProperties && isOldStyleProperties;
            }
        }

        // 3. Skip padding byte
        if (bbis.skip(numPaddingBytes) != numPaddingBytes) {
            throw new IOException("Failed to skip " + numPaddingBytes + " bytes.");
        }
    }

    private void decompressData() throws IOException {
        if (compressionType == CompressionAlgorithmType.E_NONE) {
            return;
        }

        if (compressedData == null) {
            throw new IllegalStateException("There is no data to decompress");
        }

        logger.debug("Decompressing application data with algorithm={}", compressionType);
        ByteBuffer[] data = compressedData.peek();
        ByteBufferInputStream bbis = new ByteBufferInputStream(data);

        InputStream decompressedStream = compressionType.getCompression().decompress(bbis);
        DataInputStream inputStream = new DataInputStream(decompressedStream);

        // Stream in properties
        if (arePropertiesCompressed) {
            // If properties are compressed then they are encoded in old format
            streamInPropertiesOld(inputStream);
        }

        // Stream in payload
        streamInCompressedPayload(inputStream);

        // Check if all data has been read
        if (bbis.available() > 0) {
            throw new IOException("Not all compressed bytes have been read");
        }

        resetCompressedData();
    }

    private int streamInProperties(ByteBufferInputStream input) throws IOException {
        int read = 0;

        properties = new MessagePropertiesImpl();
        read += properties.streamIn(input);

        return read;
    }

    private <T extends InputStream & DataInput> int streamInPropertiesOld(T input)
            throws IOException {
        int read = 0;

        properties = new MessagePropertiesImpl();
        read += properties.streamInOld(input);

        return read;
    }

    private int streamInPayload(int size, ByteBufferInputStream bbis) throws IOException {
        payload = new ByteBufferOutputStream(size);
        int read = bbis.read(payload, size);
        if (payload.size() != size) {
            throw new IOException("payload size doesn't match: " + payload.size() + " != " + size);
        }
        if (read != size) {
            throw new IOException("Failed to read payload from input stream");
        }

        return read;
    }

    private void streamInCompressedPayload(InputStream input) throws IOException {
        payload = new ByteBufferOutputStream();

        byte[] buf = decompressionBuffer.get();
        int read;

        while ((read = input.read(buf)) > 0) {
            payload.write(buf, 0, read);
        }
    }

    public void compressData(CompressionAlgorithmType compressionType) throws IOException {
        resetCompressedData();

        if (compressionType == CompressionAlgorithmType.E_NONE) {
            return;
        }

        logger.debug("Compressing application data with algorithm={}", compressionType);

        ByteBufferOutputStream bbos = ByteBufferOutputStream.smallBlocks();
        Compression compression = compressionType.getCompression();

        // We need to close compressed stream in order to flush all compressed bytes
        // from compressor to compressed stream to underlying stream.
        //
        // The problem is that underlying stream`s close() method is called automatically.
        //
        // That's ok for now because ByteBufferOutputStream is not closeable (close() method is
        // empty).
        //
        // Later we will need to refactor the code in order to close compressed stream without
        // closing underlying stream
        try (OutputStream compressedStream = compression.compress(bbos);
                DataOutputStream compressedOutput = new DataOutputStream(compressedStream)) {

            // TODO: remove after 2nd rollout of "new style" brokers.
            if (hasProperties() && isOldStyleProperties) {
                properties.streamOutOld(compressedOutput);
            }

            if (payload != null) {
                for (ByteBuffer b : payload.peek()) {
                    compressedOutput.write(b.array(), b.arrayOffset(), b.remaining());
                }
            }
        }

        compressedData = bbos;
        this.compressionType = compressionType;
        arePropertiesCompressed = hasProperties() && isOldStyleProperties;
    }

    public long calculateCrc32c() throws IOException {
        serializeToBuffer();
        return crc;
    }

    public void streamOut(ByteBufferOutputStream bbos) throws IOException {
        streamOut(bbos, true /* addPadding */);
    }

    private void serializeToBuffer() throws IOException {
        if (outputBuffer != null) {
            return;
        }
        outputBuffer = ByteBufferOutputStream.smallBlocks();

        // Stream out properties if they are not compressed (no compression or
        // new style properties).
        if (hasProperties() && !arePropertiesCompressed) {
            properties.streamOut(outputBuffer, isOldStyleProperties);
        }

        if (compressionType == CompressionAlgorithmType.E_NONE) {
            if (payload != null) {
                outputBuffer.writeBuffers(payload.peekUnflipped());
            }
        } else {
            outputBuffer.writeBuffers(compressedData);
        }
        // calculate crc32c without padding
        crc = Crc32c.calculate(outputBuffer.peek());
    }

    private void streamOut(ByteBufferOutputStream bbos, boolean addPadding) throws IOException {
        serializeToBuffer();
        bbos.writeBuffers(outputBuffer);
        if (addPadding) {
            final int paddingSize = ProtocolUtil.calculatePadding(outputBuffer.size());
            bbos.write(ProtocolUtil.getPaddingBytes(paddingSize), 0, paddingSize);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (compressionType != CompressionAlgorithmType.E_NONE) {
            sb.append("[ COMPRESSED ]");
        } else {
            sb.append("[ Payload [");
            if (payload != null) {
                sb.append("\"");
                PrintUtil.hexAppend(sb, payload.peek());
                sb.append("\" ]");
            } else {
                sb.append(" EMPTY ]");
            }
            if (hasProperties()) {
                sb.append(properties.toString());
            }

            sb.append(" ]");
        }
        return sb.toString();
    }
}
