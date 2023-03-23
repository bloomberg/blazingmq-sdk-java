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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.msg.MessagesTestSamples;
import com.bloomberg.bmq.util.TestHelpers;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessagePropertiesTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testWithPattern() throws IOException {
        class TestData {
            final MessagesTestSamples.SampleFileMetadata sampleFile;
            final boolean streamInOld;
            final boolean streamOutOld;
            final MessagesTestSamples.SampleFileMetadata sampleFileCompare;

            TestData(
                    MessagesTestSamples.SampleFileMetadata sampleFile,
                    boolean streamInOld,
                    boolean streamOutOld,
                    MessagesTestSamples.SampleFileMetadata sampleFileCompare) {
                this.sampleFile = sampleFile;
                this.streamInOld = streamInOld;
                this.streamOutOld = streamOutOld;
                this.sampleFileCompare = sampleFileCompare;
            }
        }

        final TestData[] data =
                new TestData[] {
                    new TestData(
                            MessagesTestSamples.MSG_PROPS_OLD,
                            true,
                            true,
                            MessagesTestSamples.MSG_PROPS_OLD),
                    new TestData(
                            MessagesTestSamples.MSG_PROPS_OLD,
                            true,
                            false,
                            MessagesTestSamples.MSG_PROPS),
                    new TestData(
                            MessagesTestSamples.MSG_PROPS,
                            false,
                            false,
                            MessagesTestSamples.MSG_PROPS),
                    new TestData(
                            MessagesTestSamples.MSG_PROPS,
                            false,
                            true,
                            MessagesTestSamples.MSG_PROPS_OLD),
                    new TestData(
                            MessagesTestSamples.MSG_PROPS_LONG_HEADERS,
                            false,
                            false,
                            MessagesTestSamples.MSG_PROPS),
                    new TestData(
                            MessagesTestSamples.MSG_PROPS_LONG_HEADERS,
                            false,
                            true,
                            MessagesTestSamples.MSG_PROPS_OLD)
                };

        for (TestData testData : data) {
            logger.info(
                    "Sample: {}, stream in old: {}, stream out old: {}, compare: {}",
                    testData.sampleFile.filePath(),
                    testData.streamInOld,
                    testData.streamOutOld,
                    testData.sampleFileCompare.filePath());

            ByteBuffer buf = TestHelpers.readFile(testData.sampleFile.filePath());

            // The binary file contains the following properties:
            // Type       Name          Value
            // INT32     "encoding"      3
            // INT64     "timestamp"     1234567890LL
            // STRING    "id"           "myCoolId"

            ByteBufferInputStream bbis = new ByteBufferInputStream(buf);
            MessagePropertiesImpl props = new MessagePropertiesImpl();

            int toRead = bbis.available();
            logger.info("Stream in {} bytes", toRead);

            if (testData.streamInOld) {
                toRead -= props.streamInOld(bbis);
            } else {
                toRead -= props.streamIn(bbis);
            }

            assertEquals(0, toRead);
            assertEquals(0, bbis.available());

            assertEquals(3, props.numProperties());

            boolean encodingFound = false, timestampFound = false, idFound = false;
            Iterator<Map.Entry<String, MessageProperty>> pit = props.iterator();

            while (pit.hasNext()) {
                MessageProperty p = pit.next().getValue();
                String name = p.name();

                assertEquals(p, props.get(name));

                switch (name) {
                    case "encoding":
                        assertEquals(PropertyType.INT32, p.type());
                        assertEquals(3, p.getValueAsInt32());
                        encodingFound = true;
                        break;
                    case "timestamp":
                        assertEquals(PropertyType.INT64, p.type());
                        assertEquals(1234567890L, p.getValueAsInt64());
                        timestampFound = true;
                        break;
                    case "id":
                        assertEquals(PropertyType.STRING, p.type());
                        assertEquals("myCoolId", p.getValueAsString());
                        idFound = true;
                        break;
                    default:
                        fail();
                        break;
                }
            }
            assertTrue(idFound);
            assertTrue(timestampFound);
            assertTrue(encodingFound);

            // Stream out to another format and compare
            ByteBufferOutputStream bbos = new ByteBufferOutputStream();

            if (testData.streamOutOld) {
                props.streamOutOld(bbos);
            } else {
                props.streamOut(bbos);
            }

            TestHelpers.compareWithFileContent(bbos.reset(), testData.sampleFileCompare);
        }
    }

    @Test
    public void testStreamOut() throws IOException {
        for (boolean isOldStyleProperties : new boolean[] {false, true}) {
            final boolean BOOL_VAL = true;
            final byte BYTE_VAL = 2;
            final short SHORT_VAL = 12;
            final int INT32_VAL = 12345;
            final long INT64_VAL = 987654321L;
            final String STRING_VAL = "myValue";
            final byte[] BINARY_VAL = "abcdefgh".getBytes();

            final int NUM_PROPERTIES = 7;

            ByteBufferOutputStream bbos = new ByteBufferOutputStream();
            MessagePropertiesImpl props = new MessagePropertiesImpl();
            assertEquals(0, props.numProperties());
            assertEquals(0, props.totalSize());

            for (PropertyType t : PropertyType.values()) {
                switch (t) {
                    case UNDEFINED: // Skip
                        break;
                    case BOOL:
                        props.setPropertyAsBool(PropertyType.BOOL.toString(), BOOL_VAL);
                        break;
                    case BYTE:
                        props.setPropertyAsByte(PropertyType.BYTE.toString(), BYTE_VAL);
                        break;
                    case SHORT:
                        props.setPropertyAsShort(PropertyType.SHORT.toString(), SHORT_VAL);
                        break;
                    case INT32:
                        props.setPropertyAsInt32(PropertyType.INT32.toString(), INT32_VAL);
                        break;
                    case INT64:
                        props.setPropertyAsInt64(PropertyType.INT64.toString(), INT64_VAL);
                        break;
                    case STRING:
                        props.setPropertyAsString(PropertyType.STRING.toString(), STRING_VAL);
                        break;
                    case BINARY:
                        props.setPropertyAsBinary(PropertyType.BINARY.toString(), BINARY_VAL);
                        break;
                    default: // Unknown type
                        fail();
                        break;
                }
            }

            assertEquals(NUM_PROPERTIES, props.numProperties());

            if (isOldStyleProperties) {
                props.streamOutOld(bbos);
            } else {
                props.streamOut(bbos);
            }

            assertTrue(bbos.size() > 0);

            ByteBufferInputStream bbis = new ByteBufferInputStream(bbos.reset());
            props = new MessagePropertiesImpl();
            assertEquals(0, props.numProperties());

            int toRead = bbis.available();

            if (isOldStyleProperties) {
                toRead -= props.streamInOld(bbis);
            } else {
                toRead -= props.streamIn(bbis);
            }

            assertEquals(0, toRead);
            assertEquals(0, bbis.available());

            assertEquals(NUM_PROPERTIES, props.numProperties());

            Iterator<Map.Entry<String, MessageProperty>> pit = props.iterator();

            for (PropertyType t : PropertyType.values()) {
                if (!PropertyType.isValid(t.toInt())) {
                    continue;
                }
                assertTrue(pit.hasNext());
                MessageProperty p = pit.next().getValue();
                String name = null;
                switch (t) {
                    case BOOL:
                        name = PropertyType.BOOL.toString();
                        assertEquals(BOOL_VAL, p.getValueAsBool());
                        break;
                    case BYTE:
                        name = PropertyType.BYTE.toString();
                        assertEquals(BYTE_VAL, p.getValueAsByte());
                        break;
                    case SHORT:
                        name = PropertyType.SHORT.toString();
                        assertEquals(SHORT_VAL, p.getValueAsShort());
                        break;
                    case INT32:
                        name = PropertyType.INT32.toString();
                        assertEquals(INT32_VAL, p.getValueAsInt32());
                        break;
                    case INT64:
                        name = PropertyType.INT64.toString();
                        assertEquals(INT64_VAL, p.getValueAsInt64());
                        break;
                    case STRING:
                        name = PropertyType.STRING.toString();
                        assertEquals(STRING_VAL, p.getValueAsString());
                        break;
                    case BINARY:
                        name = PropertyType.BINARY.toString();
                        assertArrayEquals(BINARY_VAL, p.getValueAsBinary());
                        break;
                    default: // Unknown type
                        fail();
                        break;
                }
                assertEquals(name, p.name());
            }
        }
    }

    @Test
    public void testRemove() {
        MessagePropertiesImpl props = new MessagePropertiesImpl();
        props.setPropertyAsInt32("myProp", 123);

        Iterator<Map.Entry<String, MessageProperty>> pit = props.iterator();
        assertTrue(pit.hasNext());
        pit.next();
        try {
            pit.remove();
            // Remove is not supported, so should't be here
            fail();
        } catch (UnsupportedOperationException e) {
            // OK
        } catch (Exception e) {
            logger.info("Unexpected exception: ", e);
            fail();
        }
    }

    @Test
    public void testUpdate() {
        final boolean BOOL_VAL = true;
        final byte BYTE_VAL = 2;
        final short SHORT_VAL = 12;
        final int INT32_VAL = 12345;
        final long INT64_VAL = 987654321L;
        final String STRING_VAL = "myValue";
        final byte[] BINARY_VAL = "abcdefgh".getBytes();

        final boolean NEW_BOOL_VAL = false;
        final byte NEW_BYTE_VAL = 7;
        final short NEW_SHORT_VAL = 68;
        final int NEW_INT32_VAL = 789;
        final long NEW_INT64_VAL = 12345678L;
        final String NEW_STRING_VAL = "myNewValue";
        final byte[] NEW_BINARY_VAL = "newBinaryVal".getBytes();

        final int NUM_PROPERTIES = 7;

        MessagePropertiesImpl props = new MessagePropertiesImpl();
        // Add properties
        for (PropertyType t : PropertyType.values()) {
            switch (t) {
                case UNDEFINED: // Skip
                    break;
                case BOOL:
                    props.setPropertyAsBool(PropertyType.BOOL.toString(), BOOL_VAL);
                    break;
                case BYTE:
                    props.setPropertyAsByte(PropertyType.BYTE.toString(), BYTE_VAL);
                    break;
                case SHORT:
                    props.setPropertyAsShort(PropertyType.SHORT.toString(), SHORT_VAL);
                    break;
                case INT32:
                    props.setPropertyAsInt32(PropertyType.INT32.toString(), INT32_VAL);
                    break;
                case INT64:
                    props.setPropertyAsInt64(PropertyType.INT64.toString(), INT64_VAL);
                    break;
                case STRING:
                    props.setPropertyAsString(PropertyType.STRING.toString(), STRING_VAL);
                    break;
                case BINARY:
                    props.setPropertyAsBinary(PropertyType.BINARY.toString(), BINARY_VAL);
                    break;
                default: // Unknown type
                    fail();
                    break;
            }
        }
        assertEquals(NUM_PROPERTIES, props.numProperties());

        // Update values
        for (PropertyType t : PropertyType.values()) {
            switch (t) {
                case UNDEFINED: // Skip
                    break;
                case BOOL:
                    props.setPropertyAsBool(PropertyType.BOOL.toString(), NEW_BOOL_VAL);
                    break;
                case BYTE:
                    props.setPropertyAsByte(PropertyType.BYTE.toString(), NEW_BYTE_VAL);
                    break;
                case SHORT:
                    props.setPropertyAsShort(PropertyType.SHORT.toString(), NEW_SHORT_VAL);
                    break;
                case INT32:
                    props.setPropertyAsInt32(PropertyType.INT32.toString(), NEW_INT32_VAL);
                    break;
                case INT64:
                    props.setPropertyAsInt64(PropertyType.INT64.toString(), NEW_INT64_VAL);
                    break;
                case STRING:
                    props.setPropertyAsString(PropertyType.STRING.toString(), NEW_STRING_VAL);
                    break;
                case BINARY:
                    props.setPropertyAsBinary(PropertyType.BINARY.toString(), NEW_BINARY_VAL);
                    break;
                default: // Unknown type
                    fail();
                    break;
            }
        }

        assertEquals(NUM_PROPERTIES, props.numProperties());

        // Check updated values
        Iterator<Map.Entry<String, MessageProperty>> pit = props.iterator();

        for (PropertyType t : PropertyType.values()) {
            if (!PropertyType.isValid(t.toInt())) {
                continue;
            }
            assertTrue(pit.hasNext());
            MessageProperty p = pit.next().getValue();
            switch (t) {
                case BOOL:
                    assertEquals(NEW_BOOL_VAL, p.getValueAsBool());
                    break;
                case BYTE:
                    assertEquals(NEW_BYTE_VAL, p.getValueAsByte());
                    break;
                case SHORT:
                    assertEquals(NEW_SHORT_VAL, p.getValueAsShort());
                    break;
                case INT32:
                    assertEquals(NEW_INT32_VAL, p.getValueAsInt32());
                    break;
                case INT64:
                    assertEquals(NEW_INT64_VAL, p.getValueAsInt64());
                    break;
                case STRING:
                    assertEquals(NEW_STRING_VAL, p.getValueAsString());
                    break;
                case BINARY:
                    assertArrayEquals(NEW_BINARY_VAL, p.getValueAsBinary());
                    break;
                default: // Unknown type
                    fail();
                    break;
            }
        }
    }

    private String constructOutput(PropertyType t, Object v) {
        return String.format("[ MessageProperty [ Type=%s Name=%s Value=%s ] ]", t, t, v);
    }

    @Test
    public void testToString() throws IOException {
        final boolean BOOL_VAL = true;
        final byte BYTE_VAL = 2;
        final short SHORT_VAL = 12;
        final int INT32_VAL = 12345;
        final long INT64_VAL = 987654321L;
        final String STRING_VAL = "myValue";
        final byte[] BINARY_VAL = "abcdefgh".getBytes();

        final String BOOL_STR = constructOutput(PropertyType.BOOL, BOOL_VAL);
        final String BYTE_STR = constructOutput(PropertyType.BYTE, BYTE_VAL);
        final String SHORT_STR = constructOutput(PropertyType.SHORT, SHORT_VAL);
        final String INT32_STR = constructOutput(PropertyType.INT32, INT32_VAL);
        final String INT64_STR = constructOutput(PropertyType.INT64, INT64_VAL);
        final String STRING_STR =
                constructOutput(PropertyType.STRING, String.format("\"%s\"", STRING_VAL));
        final String BINARY_STR =
                constructOutput(
                        PropertyType.BINARY, String.format("\"%s\"", new String(BINARY_VAL)));

        final String BOOL_STR_EMPTY = constructOutput(PropertyType.BOOL, "EMPTY");
        final String BYTE_STR_EMPTY = constructOutput(PropertyType.BYTE, "EMPTY");
        final String SHORT_STR_EMPTY = constructOutput(PropertyType.SHORT, "EMPTY");
        final String INT32_STR_EMPTY = constructOutput(PropertyType.INT32, "EMPTY");
        final String INT64_STR_EMPTY = constructOutput(PropertyType.INT64, "EMPTY");
        final String STRING_STR_EMPTY = constructOutput(PropertyType.STRING, "EMPTY");
        final String BINARY_STR_EMPTY = constructOutput(PropertyType.BINARY, "EMPTY");

        final int NUM_PROPERTIES = 7;

        ByteBufferOutputStream bbos = new ByteBufferOutputStream();
        MessagePropertiesImpl props = new MessagePropertiesImpl();
        assertEquals(0, props.numProperties());

        for (PropertyType t : PropertyType.values()) {
            switch (t) {
                case UNDEFINED: // Skip
                    break;
                case BOOL:
                    props.setPropertyAsBool(PropertyType.BOOL.toString(), BOOL_VAL);
                    break;
                case BYTE:
                    props.setPropertyAsByte(PropertyType.BYTE.toString(), BYTE_VAL);
                    break;
                case SHORT:
                    props.setPropertyAsShort(PropertyType.SHORT.toString(), SHORT_VAL);
                    break;
                case INT32:
                    props.setPropertyAsInt32(PropertyType.INT32.toString(), INT32_VAL);
                    break;
                case INT64:
                    props.setPropertyAsInt64(PropertyType.INT64.toString(), INT64_VAL);
                    break;
                case STRING:
                    props.setPropertyAsString(PropertyType.STRING.toString(), STRING_VAL);
                    break;
                case BINARY:
                    props.setPropertyAsBinary(PropertyType.BINARY.toString(), BINARY_VAL);
                    break;
                default: // Unknown type
                    fail();
                    break;
            }
        }

        assertEquals(NUM_PROPERTIES, props.numProperties());

        props.streamOut(bbos);

        assertTrue(bbos.size() > 0);

        ByteBufferInputStream bbis = new ByteBufferInputStream(bbos.reset());
        props = new MessagePropertiesImpl();
        assertEquals(0, props.numProperties());

        props.streamIn(bbis);

        assertEquals(NUM_PROPERTIES, props.numProperties());

        Iterator<Map.Entry<String, MessageProperty>> pit = props.iterator();

        for (PropertyType t : PropertyType.values()) {
            if (!PropertyType.isValid(t.toInt())) {
                continue;
            }
            assertTrue(pit.hasNext());
            MessageProperty p = pit.next().getValue();
            String str = p.toString();
            switch (t) {
                case BOOL:
                    assertEquals(BOOL_STR, str);
                    break;
                case BYTE:
                    assertEquals(BYTE_STR, str);
                    break;
                case SHORT:
                    assertEquals(SHORT_STR, str);
                    break;
                case INT32:
                    assertEquals(INT32_STR, str);
                    break;
                case INT64:
                    assertEquals(INT64_STR, str);
                    break;
                case STRING:
                    assertEquals(STRING_STR, str);
                    break;
                case BINARY:
                    assertEquals(BINARY_STR, str);
                    break;
                default: // Unknown type
                    fail();
                    break;
            }
        }

        // Special case with empty value

        props = new MessagePropertiesImpl();
        for (PropertyType t : PropertyType.values()) {
            switch (t) {
                case UNDEFINED: // Skip
                    break;
                case BOOL:
                    props.addProperty(new BoolMessageProperty(), t.toString());
                    break;
                case BYTE:
                    props.addProperty(new ByteMessageProperty(), t.toString());
                    break;
                case SHORT:
                    props.addProperty(new ShortMessageProperty(), t.toString());
                    break;
                case INT32:
                    props.addProperty(new Int32MessageProperty(), t.toString());
                    break;
                case INT64:
                    props.addProperty(new Int64MessageProperty(), t.toString());
                    break;
                case STRING:
                    props.addProperty(new StringMessageProperty(), t.toString());
                    break;
                case BINARY:
                    props.addProperty(new BinaryMessageProperty(), t.toString());
                    break;
                default: // Unknown type
                    fail();
                    break;
            }
        }

        assertEquals(NUM_PROPERTIES, props.numProperties());

        pit = props.iterator();

        for (PropertyType t : PropertyType.values()) {
            if (!PropertyType.isValid(t.toInt())) {
                continue;
            }
            assertTrue(pit.hasNext());
            MessageProperty p = pit.next().getValue();
            String str = p.toString();
            switch (t) {
                case BOOL:
                    assertEquals(BOOL_STR_EMPTY, str);
                    break;
                case BYTE:
                    assertEquals(BYTE_STR_EMPTY, str);
                    break;
                case SHORT:
                    assertEquals(SHORT_STR_EMPTY, str);
                    break;
                case INT32:
                    assertEquals(INT32_STR_EMPTY, str);
                    break;
                case INT64:
                    assertEquals(INT64_STR_EMPTY, str);
                    break;
                case STRING:
                    assertEquals(STRING_STR_EMPTY, str);
                    break;
                case BINARY:
                    assertEquals(BINARY_STR_EMPTY, str);
                    break;
                default: // Unknown type
                    fail();
                    break;
            }
        }
    }
}
