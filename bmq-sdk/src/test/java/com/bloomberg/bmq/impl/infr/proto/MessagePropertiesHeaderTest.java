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

import static org.junit.Assert.assertEquals;

import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.msg.MessagesTestSamples;
import com.bloomberg.bmq.util.TestHelpers;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessagePropertiesHeaderTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testWithPattern() throws IOException {
        class TestData {
            final String fileName;
            final int headerSize;
            final int propertyHeaderSize;
            final int areaWords;

            TestData(String fileName, int headerSize, int propertyHeaderSize, int areaWords) {
                this.fileName = fileName;
                this.headerSize = headerSize;
                this.propertyHeaderSize = propertyHeaderSize;
                this.areaWords = areaWords;
            }
        }

        final TestData[] data =
                new TestData[] {
                    new TestData(MessagesTestSamples.MSG_PROPS.filePath(), 6, 6, 16),
                    new TestData(MessagesTestSamples.MSG_PROPS_LONG_HEADERS.filePath(), 8, 8, 18)
                };

        for (TestData testData : data) {
            logger.info(
                    "Sample: {}, header size: {}, property header size: {}, area words: {}",
                    testData.fileName,
                    testData.headerSize,
                    testData.propertyHeaderSize,
                    testData.areaWords);

            ByteBuffer buf = TestHelpers.readFile(testData.fileName);

            // The file contains the following properties:
            // Type       Name          Value
            // INT32     "encoding"      3
            // INT64     "timestamp"     1234567890LL
            // STRING    "id"           "myCoolId"

            ByteBufferInputStream bbis = new ByteBufferInputStream(buf);
            MessagePropertiesHeader mpsh = new MessagePropertiesHeader();

            mpsh.streamIn(bbis);

            assertEquals(testData.headerSize, mpsh.headerSize());
            assertEquals(testData.propertyHeaderSize, mpsh.messagePropertyHeaderSize());
            assertEquals(testData.areaWords, mpsh.messagePropertiesAreaWords());
            assertEquals(3, mpsh.numProperties());

            final int numProps = mpsh.numProperties();

            MessagePropertyHeader[] msgPropHeaders = new MessagePropertyHeader[numProps];
            for (int j = 0; j < numProps; j++) {
                MessagePropertyHeader ph = new MessagePropertyHeader();
                ph.streamIn(bbis, mpsh.messagePropertyHeaderSize());
                msgPropHeaders[j] = ph;
            }

            MessagePropertyHeader ph = msgPropHeaders[0];
            assertEquals(PropertyType.INT32.toInt(), ph.propertyType());
            assertEquals(0, ph.propertyValueLength()); // offset
            assertEquals(8, ph.propertyNameLength());

            ph = msgPropHeaders[1];
            assertEquals(PropertyType.INT64.toInt(), ph.propertyType());
            assertEquals(12, ph.propertyValueLength()); // offset
            assertEquals(9, ph.propertyNameLength());

            ph = msgPropHeaders[2];
            assertEquals(PropertyType.STRING.toInt(), ph.propertyType());
            assertEquals(29, ph.propertyValueLength()); // offset
            assertEquals(2, ph.propertyNameLength());

            final int available =
                    testData.areaWords * Protocol.WORD_SIZE
                            - testData.headerSize
                            - testData.propertyHeaderSize * 3;
            assertEquals(available, bbis.available());
        }
    }

    @Test
    public void testGettersSetters() {
        MessagePropertiesHeader mph = new MessagePropertiesHeader();
        assertEquals(MessagePropertiesHeader.HEADER_SIZE, mph.headerSize());
        assertEquals(MessagePropertyHeader.HEADER_SIZE, mph.messagePropertyHeaderSize());
        assertEquals(2, mph.messagePropertiesAreaWords());
        // Entire msg property area is word aligned, so even though
        // sizeof(MessagePropertiesHeader) == 6 bytes, total area is
        // recorded as 8 bytes (2 words), because it is assumed that
        // padding will be added.

        MessagePropertiesHeader mph2 = new MessagePropertiesHeader();
        mph2.setHeaderSize(14); // max per protocol
        mph2.setMessagePropertyHeaderSize(14); // max per protocol
        mph2.setMessagePropertiesAreaWords(
                MessagePropertiesHeader.MAX_MESSAGE_PROPERTIES_SIZE
                        / Protocol.WORD_SIZE); // max per protocol

        assertEquals(14, mph2.headerSize());
        assertEquals(14, mph2.messagePropertyHeaderSize());
        assertEquals(
                MessagePropertiesHeader.MAX_MESSAGE_PROPERTIES_SIZE / Protocol.WORD_SIZE,
                mph2.messagePropertiesAreaWords());

        MessagePropertiesHeader mph3 = new MessagePropertiesHeader();
        mph3.setHeaderSize(10);
        mph3.setMessagePropertyHeaderSize(12);
        mph3.setMessagePropertiesAreaWords(1024 * 123);
        mph3.setNumProperties(8);

        assertEquals(10, mph3.headerSize());
        assertEquals(12, mph3.messagePropertyHeaderSize());
        assertEquals(1024 * 123, mph3.messagePropertiesAreaWords());
        assertEquals(8, mph3.numProperties());
    }
}
