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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.util.TestHelpers;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.ZipException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationDataTest {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static final double THRESHOLD = 0.0001;

    @Test
    public void testEmpty() throws IOException {
        ApplicationData appData = new ApplicationData();

        assertEquals(0, appData.payloadSize());
        assertEquals(0, appData.propertiesSize());
        assertEquals(0, appData.unpackedSize());
        assertEquals(4, appData.numPaddingBytes());
        assertEquals(0, getSize(appData.payload()));

        assertNull(appData.properties());

        assertFalse(appData.hasProperties());
        assertFalse(appData.isCompressed());
        assertFalse(appData.isOldStyleProperties());
    }

    @Test
    public void testStreamOut() throws IOException {
        for (ByteBuffer[] payload :
                new ByteBuffer[][] {null, generatePayload(0), generatePayload(1024)})
            for (MessagePropertiesImpl props :
                    new MessagePropertiesImpl[] {
                        null, new MessagePropertiesImpl(), generateProps()
                    })
                for (boolean isOldStyleProperties : new boolean[] {false, true})
                    for (CompressionAlgorithmType compressionType :
                            CompressionAlgorithmType.values()) {
                        logger.info(
                                "Stream out with payload:{}, props:{}, oldStyleProperties:{}, compression: {}",
                                payload,
                                props,
                                isOldStyleProperties,
                                compressionType);
                        verifyStreamOut(payload, props, isOldStyleProperties, compressionType);
                    }
    }

    @Test
    public void testStreamIn() throws IOException {
        for (ByteBuffer[] payload :
                new ByteBuffer[][] {null, generatePayload(0), generatePayload(1024)})
            for (MessagePropertiesImpl props :
                    new MessagePropertiesImpl[] {
                        null, new MessagePropertiesImpl(), generateProps()
                    })
                for (boolean isOldStyleProperties : new boolean[] {false, true})
                    for (CompressionAlgorithmType compressionType :
                            CompressionAlgorithmType.values()) {
                        logger.info(
                                "Stream in with payload:{}, props:{}, oldStyleProperties:{}, compression: {}",
                                payload,
                                props,
                                isOldStyleProperties,
                                compressionType);
                        verifyStreamIn(payload, props, isOldStyleProperties, compressionType);
                    }
    }

    @Test
    public void testStreamInInvalidCompression() throws IOException {
        ByteBufferOutputStream bbos = new ByteBufferOutputStream();

        ByteBuffer[] payload = generatePayload(1024);
        for (ByteBuffer b : payload) {
            bbos.writeBuffer(b);
        }

        int numPaddingBytes = ProtocolUtil.calculatePadding(bbos.size());
        bbos.write(ProtocolUtil.getPaddingBytes(numPaddingBytes), 0, numPaddingBytes);

        ByteBufferInputStream bbis = new ByteBufferInputStream(bbos.peek());

        ApplicationData data = new ApplicationData();

        // Stream in uncompressed data as compressed
        data.streamIn(bbis.available(), false, false, CompressionAlgorithmType.E_ZLIB, bbis);

        // "Compressed" data should be buffered
        assertTrue(data.isCompressed());

        // Try to decompress payload - fail
        try {
            assertNotEquals(payload, data.payload());
            fail();
        } catch (ZipException e) {
            assertEquals("incorrect header check", e.getMessage());
        }
    }

    public ByteBuffer[] generatePayload(int size) throws IOException {
        return generatePayload(size, true);
    }

    public ByteBuffer[] generatePayload(int size, boolean flipped) throws IOException {
        ByteBufferOutputStream bbos = new ByteBufferOutputStream();

        for (int i = 0; i < size; i++) {
            bbos.writeByte(i % 10);
        }

        return flipped ? bbos.peek() : bbos.peekUnflipped();
    }

    public MessagePropertiesImpl generateProps() {
        MessagePropertiesImpl props = new MessagePropertiesImpl();

        props.setPropertyAsInt32("Int32Prop", 32);
        props.setPropertyAsBinary("BinaryProp", new byte[] {1, 2, 3});

        return props;
    }

    private ByteBuffer[] generateOutput(
            ByteBuffer[] payload,
            MessagePropertiesImpl props,
            boolean isOldStyleProperties,
            CompressionAlgorithmType compressionType)
            throws IOException {
        ByteBufferOutputStream bbos = new ByteBufferOutputStream();

        boolean hasProperties = props != null && props.numProperties() > 0;

        if (compressionType == CompressionAlgorithmType.E_NONE) {
            if (hasProperties) {
                if (isOldStyleProperties) {
                    props.streamOutOld(bbos);
                } else {
                    props.streamOut(bbos);
                }
            }

            if (payload != null) {
                for (ByteBuffer b : payload) {
                    ByteBuffer dup = b.duplicate();
                    dup.position(dup.limit());
                    bbos.writeBuffer(dup);
                }
            }
        } else {
            // Stream out if new style properties
            if (hasProperties && !isOldStyleProperties) {
                props.streamOut(bbos);
            }
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
            try (OutputStream compressedStream = compressionType.getCompression().compress(bbos);
                    DataOutputStream compressedOutput = new DataOutputStream(compressedStream)) {

                // Stream out if old style properties
                if (hasProperties && isOldStyleProperties) {
                    props.streamOutOld(compressedOutput);
                }

                if (payload != null) {
                    for (ByteBuffer b : payload) {
                        byte[] bytes = new byte[b.remaining()];
                        b.get(bytes, 0, bytes.length);
                        compressedOutput.write(bytes);
                    }
                }
            }
        }

        int numPaddingBytes = ProtocolUtil.calculatePadding(bbos.size());
        bbos.write(ProtocolUtil.getPaddingBytes(numPaddingBytes), 0, numPaddingBytes);

        return bbos.reset();
    }

    public void verifyStreamOut(
            ByteBuffer[] payload,
            MessagePropertiesImpl props,
            boolean isOldStyleProperties,
            CompressionAlgorithmType compressionType)
            throws IOException {

        ApplicationData appData = new ApplicationData();
        ByteBufferOutputStream bbos = new ByteBufferOutputStream();

        // Set properties
        if (props != null) {
            appData.setProperties(props);
            verifyProperties(props, appData.properties());
        }
        final boolean hasProperties = props != null && props.numProperties() > 0;
        final int propsSize = hasProperties ? props.totalSize() : 0;

        if (hasProperties) {
            assertTrue(appData.hasProperties());
            assertEquals(propsSize, appData.propertiesSize());
        } else {
            assertFalse(appData.hasProperties());
            assertEquals(0, appData.propertiesSize());
        }

        // Set payload
        if (payload != null) {
            appData.setPayload(duplicate(payload));
            verifyPayload(payload, appData.payload());
        } else {
            try {
                appData.setPayload(payload);
                fail();
            } catch (IllegalArgumentException e) {
                assertEquals("'buffer array' must be non-null", e.getMessage());
            }
            assertEquals(0, appData.payloadSize());
        }

        // Verify unpackedSize
        assertEquals(getSize(payload) + propsSize, appData.unpackedSize());

        ByteBuffer[] expected =
                generateOutput(duplicate(payload), props, isOldStyleProperties, compressionType);

        appData.setIsOldStyleProperties(isOldStyleProperties);
        appData.compressData(compressionType);

        int numPaddingBytes = ProtocolUtil.calculatePadding(appData.unpackedSize());
        assertEquals(numPaddingBytes, appData.numPaddingBytes());

        int appDataSize = getSize(expected) - numPaddingBytes;
        assertEquals(appDataSize, appData.unpackedSize());

        // Check compression ratio
        verifyCompressionRatio(appData);

        // Check streamOut output
        appData.streamOut(bbos);
        verifyPayload(expected, bbos.peek());

        // Skip padding bytes
        int lastBufferSize = expected[expected.length - 1].limit();
        int lastBufferNoPaddingSize = lastBufferSize - numPaddingBytes;
        assertTrue(lastBufferNoPaddingSize >= 0);

        expected[expected.length - 1].limit(lastBufferNoPaddingSize);

        // Check applicationData() output
        verifyPayload(expected, appData.applicationData());

        // Revert limit
        expected[expected.length - 1].limit(lastBufferSize);

        // Double check. Stream in generated data and compare with original payload and props
        appData = new ApplicationData();
        assertEquals(0, appData.unpackedSize());

        ByteBufferInputStream bbis = new ByteBufferInputStream(expected);
        final int unpackedInputSize = bbis.available() - numPaddingBytes;

        appData.streamIn(
                getSize(expected), hasProperties, isOldStyleProperties, compressionType, bbis);
        assertEquals(unpackedInputSize, appData.unpackedSize());

        verifyPayload(payload, appData.payload());
        verifyProperties(hasProperties ? props : null, appData.properties());
    }

    public void verifyStreamIn(
            ByteBuffer[] payload,
            MessagePropertiesImpl props,
            boolean isOldStyleProperties,
            CompressionAlgorithmType compressionType)
            throws IOException {

        // Prepare data to stream in
        ByteBuffer[] data =
                generateOutput(duplicate(payload), props, isOldStyleProperties, compressionType);
        ByteBufferInputStream bbis = new ByteBufferInputStream(duplicate(data));

        // Get num of padding bytes
        byte numPaddingBytes = ProtocolUtil.getPadding(bbis, bbis.available());

        // Stream in
        final int size = bbis.available();

        ApplicationData appData = new ApplicationData();

        final boolean hasProperties = props != null && props.numProperties() > 0;

        appData.streamIn(size, hasProperties, isOldStyleProperties, compressionType, bbis);

        assertEquals(isOldStyleProperties, appData.isOldStyleProperties());

        // Check unpacked input unpackedSize has been set
        int unpackedSize = size - numPaddingBytes;
        assertEquals(unpackedSize, appData.unpackedSize());

        // Check all bytes have been read
        assertEquals(0, bbis.available());

        // When app data is compressed, `isCompressed()` should be set
        if (compressionType != CompressionAlgorithmType.E_NONE) {
            assertTrue(appData.isCompressed());
        }

        // Check properties
        verifyProperties(hasProperties ? props : null, appData.properties());

        // If data is compressed and properties are new style encoded, data
        // should stay compressed
        if (compressionType != CompressionAlgorithmType.E_NONE && !isOldStyleProperties) {
            assertTrue(appData.isCompressed());
        }

        // Check payload has been populated
        verifyPayload(payload, appData.payload());

        // `isCompressed()` should be reset
        assertFalse(appData.isCompressed());

        // Double check. Stream out data and compare to original input
        appData = new ApplicationData();
        if (payload != null) {
            appData.setPayload(payload);
        }
        if (props != null) {
            appData.setProperties(props);
        }

        ByteBufferOutputStream bbos = new ByteBufferOutputStream();

        appData.setIsOldStyleProperties(isOldStyleProperties);
        appData.compressData(compressionType);

        appData.streamOut(bbos);

        // Check compression ratio
        verifyCompressionRatio(appData);

        // Check the data
        assertArrayEquals(
                TestHelpers.buffersContents(data), TestHelpers.buffersContents(bbos.reset()));
    }

    private void verifyPayload(ByteBuffer[] expected, ByteBuffer[] actual) throws IOException {
        if (expected == null) {
            expected = new ByteBufferOutputStream().reset();
        }
        if (actual == null) {
            actual = new ByteBufferOutputStream().reset();
        }

        assertArrayEquals(
                TestHelpers.buffersContents(expected), TestHelpers.buffersContents(actual));
    }

    private void verifyProperties(MessagePropertiesImpl expected, MessagePropertiesImpl actual) {
        if (expected != null) {
            assertNotNull(actual);

            assertEquals(expected.numProperties(), actual.numProperties());
            if (expected.numProperties() > 0) {
                Iterator<Map.Entry<String, MessageProperty>> propsIt = actual.iterator();
                assertTrue(propsIt.hasNext());

                MessageProperty prop = propsIt.next().getValue();
                assertNotNull(prop);
                assertEquals(PropertyType.INT32, prop.type());
                assertEquals("Int32Prop", prop.name());
                assertEquals(32, prop.getValueAsInt32());

                assertTrue(propsIt.hasNext());
                prop = propsIt.next().getValue();
                assertNotNull(prop);
                assertEquals(PropertyType.BINARY, prop.type());
                assertEquals("BinaryProp", prop.name());
                assertArrayEquals(new byte[] {1, 2, 3}, prop.getValueAsBinary());

                assertFalse(propsIt.hasNext());
            }
        } else {
            assertNull(actual);
        }
    }

    private static void verifyCompressionRatio(ApplicationData appData) {
        if (appData.unpackedSize() == 0) {
            try {
                appData.compressionRatio();
                fail();
            } catch (IllegalStateException e) {
                assertEquals("Unpacked size must be greater than zero.", e.getMessage());
            }

            return;
        }

        final double ratio = appData.compressionRatio();
        final double expectedRatio =
                (appData.propertiesSize() + appData.payloadSize())
                        / (double) appData.unpackedSize();

        assertEquals(expectedRatio, ratio, THRESHOLD);
    }

    private ByteBuffer[] duplicate(ByteBuffer[] data) {
        if (data == null) {
            return null;
        }

        ByteBuffer[] copy = new ByteBuffer[data.length];

        for (int i = 0; i < copy.length; i++) {
            copy[i] = data[i].duplicate();
        }

        return copy;
    }

    private int getSize(ByteBuffer[] data) throws IOException {
        int size = 0;

        if (data != null) {
            try (ByteBufferInputStream bbis = new ByteBufferInputStream(data)) {
                size = bbis.available();
            }
        }

        return size;
    }
}
