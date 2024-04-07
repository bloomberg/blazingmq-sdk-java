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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.bloomberg.bmq.BMQException;
import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PutMessageImplTest {
    static final int HEADER_WORDS = PutHeader.HEADER_SIZE / Protocol.WORD_SIZE;
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    void testStreamOut() throws Exception {
        final MessagePropertiesImpl validProps = generateProps();

        final ByteBuffer[][] payloads =
                new ByteBuffer[][] {
                    null,
                    generatePayload(0),
                    generatePayload(
                            Protocol.COMPRESSION_MIN_APPDATA_SIZE - 1 - validProps.totalSize()),
                    generatePayload(Protocol.COMPRESSION_MIN_APPDATA_SIZE)
                };

        final MessagePropertiesImpl[] propsArray =
                new MessagePropertiesImpl[] {null, new MessagePropertiesImpl(), validProps};

        final CompressionAlgorithmType[] compressionTypes =
                new CompressionAlgorithmType[] {
                    null, // indicates using of default compression
                    CompressionAlgorithmType.E_NONE,
                    CompressionAlgorithmType.E_ZLIB
                };

        for (ByteBuffer[] payload : payloads)
            for (MessagePropertiesImpl props : propsArray)
                for (boolean isOldStyleProperties : new boolean[] {false, true})
                    for (CompressionAlgorithmType compressionType : compressionTypes) {
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
    void testStreamIn() throws Exception {
        final MessagePropertiesImpl validProps = generateProps();

        final ByteBuffer[][] payloads =
                new ByteBuffer[][] {
                    null,
                    generatePayload(0),
                    generatePayload(
                            Protocol.COMPRESSION_MIN_APPDATA_SIZE - 1 - validProps.totalSize()),
                    generatePayload(Protocol.COMPRESSION_MIN_APPDATA_SIZE)
                };

        final MessagePropertiesImpl[] propsArray =
                new MessagePropertiesImpl[] {null, new MessagePropertiesImpl(), validProps};

        final CompressionAlgorithmType[] compressionTypes =
                new CompressionAlgorithmType[] {
                    CompressionAlgorithmType.E_NONE, CompressionAlgorithmType.E_ZLIB
                };

        for (ByteBuffer[] payload : payloads)
            for (MessagePropertiesImpl props : propsArray)
                for (boolean isOldStyleProperties : new boolean[] {false, true})
                    for (CompressionAlgorithmType compressionType : compressionTypes) {
                        logger.info(
                                "Stream in with payload:{}, props:{}, oldStyleProperties:{}, compression: {}",
                                payload,
                                props,
                                isOldStyleProperties,
                                compressionType);
                        verifyStreamIn(payload, props, isOldStyleProperties, compressionType);
                    }
    }

    private ByteBuffer[] generatePayload(int size) throws IOException {
        ByteBufferOutputStream bbos = new ByteBufferOutputStream();

        for (int i = 0; i < size; i++) {
            bbos.writeByte(i % 10);
        }

        return bbos.reset();
    }

    private MessagePropertiesImpl generateProps() {
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
        try (ByteBufferOutputStream bbos = new ByteBufferOutputStream()) {

            ApplicationData appData = new ApplicationData();
            appData.setIsOldStyleProperties(isOldStyleProperties);

            if (payload != null) {
                appData.setPayload(payload);
            }

            PutHeader header = new PutHeader();

            appData.setProperties(props);
            if (appData.hasProperties()) {
                header.setFlags(PutHeaderFlags.setFlag(0, PutHeaderFlags.MESSAGE_PROPERTIES));

                if (!isOldStyleProperties) {
                    header.setSchemaWireId(PutMessageImpl.INVALID_SCHEMA_WIRE_ID);
                }
            }

            final int sizeToCompress =
                    isOldStyleProperties ? appData.unpackedSize() : appData.payloadSize();
            final boolean canCompress = sizeToCompress >= Protocol.COMPRESSION_MIN_APPDATA_SIZE;

            // Compress data if compression is set and size is not below threshold
            if (compressionType != null && canCompress) {
                appData.compressData(compressionType);
                header.setCompressionType(compressionType.toInt());
            } else {
                appData.compressData(CompressionAlgorithmType.E_NONE);
                header.setCompressionType(CompressionAlgorithmType.E_NONE.toInt());
            }

            final int numWords = ProtocolUtil.calculateNumWords(appData.unpackedSize());

            header.setHeaderWords(HEADER_WORDS);
            header.setMessageWords(HEADER_WORDS + numWords);

            header.streamOut(bbos);
            appData.streamOut(bbos);

            return bbos.reset();
        }
    }

    private void verifyStreamOut(
            ByteBuffer[] payload,
            MessagePropertiesImpl props,
            boolean isOldStyleProperties,
            CompressionAlgorithmType compressionType)
            throws IOException {

        final boolean hasProperties = props != null && props.numProperties() > 0;

        ByteBufferOutputStream bbos = new ByteBufferOutputStream();

        PutMessageImpl msg = new PutMessageImpl();

        assertEquals(CompressionAlgorithmType.E_NONE.toInt(), msg.header().compressionType());
        assertEquals(0, msg.appData().unpackedSize());
        assertNull(msg.appData().properties());

        // Set compression type if provided.
        // Otherwise default value will be used.
        if (compressionType != null) {
            msg.setCompressionType(compressionType);
        }

        if (props != null) {
            msg.appData().setProperties(props);
        }

        if (payload != null) {
            msg.appData().setPayload(duplicate(payload));
        }

        if (msg.appData().payloadSize() == 0) {
            // if payload is null or empty, cannot compress data
            try {
                msg.compressData();
                fail();
            } catch (IllegalStateException e) {
                assertEquals("Empty payload.", e.getMessage());
            }

            // Nothing more to do
            return;
        }

        msg.appData().setIsOldStyleProperties(isOldStyleProperties);

        final int dataToCompress =
                isOldStyleProperties ? msg.appData().unpackedSize() : msg.appData().payloadSize();
        CompressionAlgorithmType actualCompressionType;
        if (dataToCompress < Protocol.COMPRESSION_MIN_APPDATA_SIZE) {
            // Data is not compressed if the size below the threshold.
            actualCompressionType = CompressionAlgorithmType.E_NONE;
        } else {
            actualCompressionType = compressionType;

            // if compression is not provided, set to default value.
            if (compressionType == null) {
                actualCompressionType = CompressionAlgorithmType.E_NONE;
            }
        }

        msg.compressData();

        assertNotEquals(0, msg.appData().unpackedSize());

        assertEquals(actualCompressionType.toInt(), msg.header().compressionType());

        assertEquals(0, bbos.size());
        msg.streamOut(bbos);

        ByteBuffer[] expected =
                generateOutput(duplicate(payload), props, isOldStyleProperties, compressionType);
        ByteBuffer[] streamedData = bbos.reset();

        assertArrayEquals(expected, streamedData);

        // Do double check. Use generated data as input and then compare payload and props
        ByteBufferInputStream bbis = new ByteBufferInputStream(streamedData);

        msg = new PutMessageImpl();

        final boolean isPayloadEmpty = getSize(payload) == 0;
        try {
            msg.streamIn(bbis);
            assertFalse(isPayloadEmpty);
        } catch (BMQException e) {
            assertEquals("Application data is empty.", e.getMessage());
            assertTrue(isPayloadEmpty);

            // Nothing more to do.
            return;
        }

        // check payload
        verifyPayload(payload, msg.appData().payload());

        // check properties
        verifyProperties(hasProperties ? props : null, msg.appData().properties());
    }

    private void verifyStreamIn(
            ByteBuffer[] payload,
            MessagePropertiesImpl props,
            boolean isOldStyleProperties,
            CompressionAlgorithmType compressionType)
            throws IOException {

        final boolean hasProperties = props != null && props.numProperties() > 0;
        final boolean hasPayload = getSize(payload) > 0;

        ByteBuffer[] input =
                generateOutput(duplicate(payload), props, isOldStyleProperties, compressionType);
        ByteBufferInputStream bbis = new ByteBufferInputStream(duplicate(input));

        // Get num of padding bytes
        final byte numPaddingBytes = ProtocolUtil.getPadding(bbis, bbis.available());

        PutMessageImpl msg = new PutMessageImpl();

        // Stream in
        final int size = bbis.available();
        final int unpackedSize = size - numPaddingBytes - PutHeader.HEADER_SIZE;
        final boolean isDataEmpty = unpackedSize == 0;
        try {
            msg.streamIn(bbis);
            assertFalse(isDataEmpty);
        } catch (BMQException e) {
            // Fail to stream in if data is empty
            assertTrue(isDataEmpty);
            assertEquals("Application data is empty.", e.getMessage());

            // Nothing more to do.
            return;
        }

        // Check unpacked input size
        assertEquals(unpackedSize, msg.appData().unpackedSize());

        // check payload
        verifyPayload(payload, msg.appData().payload());

        // check properties
        verifyProperties(hasProperties ? props : null, msg.appData().properties());
        assertEquals(!hasProperties || isOldStyleProperties, msg.appData().isOldStyleProperties());

        // Do double check. Stream out data and compare with original input
        msg = new PutMessageImpl();
        msg.setCompressionType(compressionType);

        if (payload != null) {
            msg.appData().setPayload(duplicate(payload));
        }
        if (props != null) {
            msg.appData().setProperties(props);
        }

        msg.appData().setIsOldStyleProperties(isOldStyleProperties);

        try {
            msg.compressData();
            assertTrue(hasPayload);
        } catch (IllegalStateException e) {
            assertEquals("Empty payload.", e.getMessage());
            assertFalse(hasPayload);

            // Nothing more to do.
            return;
        }

        ByteBufferOutputStream bbos = new ByteBufferOutputStream();

        // Stream out
        msg.streamOut(bbos);
        assertArrayEquals(input, bbos.reset());
    }

    private void verifyPayload(ByteBuffer[] expected, ByteBuffer[] actual) {
        if (expected == null) {
            expected = new ByteBufferOutputStream().reset();
        }

        assertArrayEquals(expected, actual);
    }

    private void verifyProperties(MessagePropertiesImpl expected, MessagePropertiesImpl actual) {
        if (expected != null) {
            assertNotNull(actual);

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
        } else {
            assertNull(actual);
        }
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
