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
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessagePropertyHeaderTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testGettersSetters() {
        MessagePropertyHeader mph = new MessagePropertyHeader();
        assertEquals(0, mph.propertyType());
        assertEquals(0, mph.propertyValueLength());
        assertEquals(0, mph.propertyNameLength());

        MessagePropertyHeader mph2 = new MessagePropertyHeader();
        mph2.setPropertyType(31); // max per protocol
        mph2.setPropertyValueLength((1 << 26) - 1); // max per protocol
        mph2.setPropertyNameLength((1 << 12) - 1); // max per protocol

        assertEquals(31, mph2.propertyType());
        assertEquals(((1 << 26) - 1), mph2.propertyValueLength());
        assertEquals(((1 << 12) - 1), mph2.propertyNameLength());

        MessagePropertyHeader mph3 = new MessagePropertyHeader();
        mph3.setPropertyType(17);
        mph3.setPropertyValueLength((1 << 19) - 1);
        mph3.setPropertyNameLength((1 << 8) - 1);

        assertEquals(17, mph3.propertyType());
        assertEquals(((1 << 19) - 1), mph3.propertyValueLength());
        assertEquals(((1 << 8) - 1), mph3.propertyNameLength());
    }

    @Test
    public void testStreamInStreamOut() throws IOException {
        final int valueLength = (1 << 26) - 1; // max per protocol
        final int nameLength = (1 << 12) - 1; // max per protocol

        MessagePropertyHeader mph = new MessagePropertyHeader();
        mph.setPropertyType(PropertyType.STRING.toInt());
        mph.setPropertyValueLength(valueLength);
        mph.setPropertyNameLength(nameLength);

        final int[] sizes =
                new int[] {
                    MessagePropertyHeader.HEADER_SIZE - 1, // short
                    MessagePropertyHeader.HEADER_SIZE, // normal
                    MessagePropertyHeader.HEADER_SIZE + 2 // long
                };

        for (int size : sizes) {
            logger.info("Header size: {}", size);

            ByteBufferOutputStream bbos = new ByteBufferOutputStream();
            mph.streamOut(bbos);

            assertEquals(MessagePropertyHeader.HEADER_SIZE, bbos.size());

            // add extra bytes
            if (size > MessagePropertyHeader.HEADER_SIZE) {
                final int num = size - MessagePropertyHeader.HEADER_SIZE;
                final byte[] extra = new byte[num];
                bbos.write(extra);
            }

            ByteBufferInputStream bbis = new ByteBufferInputStream(bbos.reset());

            // skip bytes
            if (size < MessagePropertyHeader.HEADER_SIZE) {
                final int numSkip = MessagePropertyHeader.HEADER_SIZE - size;
                assertEquals(numSkip, bbis.skip(numSkip));
            }

            MessagePropertyHeader header = new MessagePropertyHeader();
            logger.info("Stream in {} bytes", bbis.available());
            try {
                header.streamIn(bbis, size);
                assert (size >= MessagePropertyHeader.HEADER_SIZE);
            } catch (IOException e) {
                assertEquals("Invalid size: " + size, e.getMessage());
                assert (size < MessagePropertyHeader.HEADER_SIZE);

                continue;
            }

            assertEquals(PropertyType.STRING.toInt(), header.propertyType());
            assertEquals(valueLength, header.propertyValueLength());
            assertEquals(nameLength, header.propertyNameLength());

            assertEquals(0, bbis.available());
        }
    }
}
