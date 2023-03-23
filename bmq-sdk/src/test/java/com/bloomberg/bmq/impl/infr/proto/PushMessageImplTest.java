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

import com.bloomberg.bmq.BMQException;
import com.bloomberg.bmq.impl.QueueId;
import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.util.Argument;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushMessageImplTest {
    static final int HEADER_WORDS = PushHeader.HEADER_SIZE_FOR_SCHEMA_ID / Protocol.WORD_SIZE;
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testStreamIn() throws Exception {
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

    @Test
    public void testStreamOut() throws Exception {
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
    public void testSubQueueIds() throws IOException {
        byte[] options =
                new byte[] {
                    0b00001100, 0b01000000, 0b00000000, 0b00000101, // header
                    0b00000000, 0b00000000, 0b00000000, 0b00000001, // subqueue id 1
                    0b00000101, 0b00000000, 0b00000000, 0b00000000, // rda counter + reserved
                    0b00000000, 0b00000000, 0b00000000, 0b00000010, // subqueue id 2
                    0b00000101, 0b00000000, 0b00000000, 0b00000000, // rda counter + reserved
                };

        ByteBuffer[] input = generateOptionsOutput(options);
        ByteBufferInputStream bbis = new ByteBufferInputStream(input);

        PushMessageImpl msg = new PushMessageImpl();
        assertArrayEquals(new Integer[] {QueueId.k_DEFAULT_SUBQUEUE_ID}, msg.subQueueIds());

        msg.streamIn(bbis);
        assertArrayEquals(new Integer[] {1, 2}, msg.subQueueIds());
    }

    @Test
    public void testSubQueueIdsOld() throws IOException {
        byte[] options =
                new byte[] {
                    0b00000100, 0b00000000, 0b00000000, 0b00000011, // header
                    0b00000000, 0b00000000, 0b00000000, 0b00000001, // subqueue id 1
                    0b00000000, 0b00000000, 0b00000000, 0b00000010, // subqueue id 2
                };

        ByteBuffer[] input = generateOptionsOutput(options);
        ByteBufferInputStream bbis = new ByteBufferInputStream(input);

        PushMessageImpl msg = new PushMessageImpl();
        assertArrayEquals(new Integer[] {QueueId.k_DEFAULT_SUBQUEUE_ID}, msg.subQueueIds());

        msg.streamIn(bbis);
        assertArrayEquals(new Integer[] {1, 2}, msg.subQueueIds());
    }

    @Test
    public void testSubQueueIdsBoth() throws IOException {
        byte[] options =
                new byte[] {
                    // New option
                    0b00001100, 0b01000000, 0b00000000, 0b00000101, // header
                    0b00000000, 0b00000000, 0b00000000, 0b00000001, // subqueue id 1
                    0b00000101, 0b00000000, 0b00000000, 0b00000000, // rda counter + reserved
                    0b00000000, 0b00000000, 0b00000000, 0b00000010, // subqueue id 2
                    0b00000101, 0b00000000, 0b00000000, 0b00000000, // rda counter + reserved

                    // Old option
                    0b00000100, 0b00000000, 0b00000000, 0b00000010, // header
                    0b00000000, 0b00000000, 0b00000000, 0b00000011, // subqueue id 3
                };

        ByteBuffer[] input = generateOptionsOutput(options);
        ByteBufferInputStream bbis = new ByteBufferInputStream(input);

        PushMessageImpl msg = new PushMessageImpl();
        assertArrayEquals(new Integer[] {QueueId.k_DEFAULT_SUBQUEUE_ID}, msg.subQueueIds());

        msg.streamIn(bbis);
        assertArrayEquals(new Integer[] {1, 2}, msg.subQueueIds());
    }

    @Test
    public void testStreamInUnknownCompression() throws IOException {
        ByteBufferOutputStream bbos = new ByteBufferOutputStream();
        ByteBuffer[] payload = generatePayload(1024);

        ApplicationData appData = new ApplicationData();
        appData.setPayload(payload);

        // Compress with ZLIB
        appData.compressData(CompressionAlgorithmType.E_ZLIB);

        // Set header compression type to unknown value
        PushHeader header = new PushHeader();
        final int UNKNOWN_COMPRESSION_TYPE = 7;
        header.setCompressionType(UNKNOWN_COMPRESSION_TYPE);

        final int numWords = ProtocolUtil.calculateNumWords(appData.unpackedSize());

        header.setHeaderWords(HEADER_WORDS);
        header.setMessageWords(HEADER_WORDS + numWords);

        header.streamOut(bbos);
        appData.streamOut(bbos);

        ByteBufferInputStream bbis = new ByteBufferInputStream(bbos.reset());

        // Try to stream in PUSH message with unknown compression type
        PushMessageImpl msg = new PushMessageImpl();

        try {
            msg.streamIn(bbis);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(
                    "'" + UNKNOWN_COMPRESSION_TYPE + "' - unknown compression algorithm type",
                    e.getMessage());
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

            PushHeader header = new PushHeader();

            appData.setProperties(props);
            if (appData.hasProperties()) {
                header.setFlags(PushHeaderFlags.setFlag(0, PushHeaderFlags.MESSAGE_PROPERTIES));

                if (!isOldStyleProperties) {
                    header.setSchemaWireId(PushMessageImpl.INVALID_SCHEMA_WIRE_ID);
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

            if (appData.payloadSize() == 0) {
                header.setFlags(
                        PushHeaderFlags.setFlag(header.flags(), PushHeaderFlags.IMPLICIT_PAYLOAD));
            }

            final int numWords = ProtocolUtil.calculateNumWords(appData.unpackedSize());

            header.setHeaderWords(HEADER_WORDS);
            header.setMessageWords(HEADER_WORDS + numWords);

            header.streamOut(bbos);
            appData.streamOut(bbos);

            return bbos.reset();
        }
    }

    private ByteBuffer[] generateOptionsOutput(byte[] options) throws IOException {
        Argument.expectNonNull(options, "options");

        try (ByteBufferOutputStream bbos = new ByteBufferOutputStream()) {

            PushHeader header = new PushHeader();

            final int payloadSize = 16;
            ApplicationData appData = new ApplicationData();
            appData.setPayload(generatePayload(payloadSize));
            appData.compressData(CompressionAlgorithmType.E_NONE);
            header.setCompressionType(CompressionAlgorithmType.E_NONE.toInt());

            int numWords = ProtocolUtil.calculateNumWords(options.length + appData.unpackedSize());

            header.setHeaderWords(HEADER_WORDS);
            header.setOptionsWords(options.length / Protocol.WORD_SIZE);
            header.setMessageWords(HEADER_WORDS + numWords);

            header.streamOut(bbos);
            bbos.write(options);
            appData.streamOut(bbos);

            return bbos.reset();
        }
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

        PushMessageImpl msg = new PushMessageImpl();

        // Stream in and ensure that IMPLICIT_PAYLOAD flag is not set and data is not empty
        final int size = bbis.available();
        final int unpackedSize = size - numPaddingBytes - PushHeader.HEADER_SIZE_FOR_SCHEMA_ID;

        try {
            msg.streamIn(bbis);
            assertTrue(hasPayload);
            assertFalse(
                    PushHeaderFlags.isSet(msg.header().flags(), PushHeaderFlags.IMPLICIT_PAYLOAD));
        } catch (BMQException e) {
            assertEquals("IMPLICIT_PAYLOAD flag is set", e.getMessage());
            assertFalse(hasPayload);
            assertTrue(
                    PushHeaderFlags.isSet(msg.header().flags(), PushHeaderFlags.IMPLICIT_PAYLOAD));

            // Nothing more to do
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
        msg = new PushMessageImpl();
        msg.setCompressionType(compressionType);

        if (payload != null) {
            msg.appData().setPayload(duplicate(payload));
        }
        if (props != null) {
            msg.appData().setProperties(props);
        }

        msg.appData().setIsOldStyleProperties(isOldStyleProperties);
        msg.compressData();

        ByteBufferOutputStream bbos = new ByteBufferOutputStream();
        msg.streamOut(bbos);
        assertArrayEquals(input, bbos.reset());

        // Override compression type to unknown value
        int unknownValue =
                EnumSet.allOf(CompressionAlgorithmType.class).stream()
                                .mapToInt(CompressionAlgorithmType::toInt)
                                .max()
                                .getAsInt()
                        + 1;
        msg.header().setCompressionType(unknownValue);

        // Stream out
        bbos = new ByteBufferOutputStream();
        msg.streamOut(bbos);

        // Stream in and get exception
        msg = new PushMessageImpl();
        bbis = new ByteBufferInputStream(bbos.reset());

        try {
            msg.streamIn(bbis);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(
                    "'" + unknownValue + "' - unknown compression algorithm type", e.getMessage());
        }
    }

    private void verifyStreamOut(
            ByteBuffer[] payload,
            MessagePropertiesImpl props,
            boolean isOldStyleProperties,
            CompressionAlgorithmType compressionType)
            throws IOException {

        final boolean hasProperties = props != null && props.numProperties() > 0;
        final boolean hasPayload = getSize(payload) > 0;

        ByteBufferOutputStream bbos = new ByteBufferOutputStream();

        PushMessageImpl msg = new PushMessageImpl();
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
        if (!hasPayload && !hasProperties) {
            assertEquals(0, msg.appData().unpackedSize());
        } else {
            assertNotEquals(0, msg.appData().unpackedSize());
        }

        assertEquals(actualCompressionType.toInt(), msg.header().compressionType());

        assertEquals(0, bbos.size());
        msg.streamOut(bbos);

        // when payload is not set, IMPLICIT_PAYLOAD flag should be set
        if (!hasPayload) {
            assertTrue(PushHeaderFlags.isSet(msg.flags(), PushHeaderFlags.IMPLICIT_PAYLOAD));
        } else {
            assertFalse(PushHeaderFlags.isSet(msg.flags(), PushHeaderFlags.IMPLICIT_PAYLOAD));
        }

        ByteBuffer[] expected =
                generateOutput(duplicate(payload), props, isOldStyleProperties, compressionType);
        ByteBuffer[] streamedData = bbos.reset();

        assertArrayEquals(expected, streamedData);

        // Do double check. Use generated data as input and then compare payload and props
        ByteBufferInputStream bbis = new ByteBufferInputStream(streamedData);

        msg = new PushMessageImpl();

        try {
            msg.streamIn(bbis);
            assertTrue(hasPayload);
            assertFalse(
                    PushHeaderFlags.isSet(msg.header().flags(), PushHeaderFlags.IMPLICIT_PAYLOAD));
        } catch (BMQException e) {
            assertEquals("IMPLICIT_PAYLOAD flag is set", e.getMessage());
            assertFalse(hasPayload);
            assertTrue(
                    PushHeaderFlags.isSet(msg.header().flags(), PushHeaderFlags.IMPLICIT_PAYLOAD));

            // Nothing more to do
            return;
        }

        // check payload
        verifyPayload(payload, msg.appData().payload());

        // check properties
        verifyProperties(hasProperties ? props : null, msg.appData().properties());
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
