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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptionHeaderTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testPreconditions() {
        assertTrue(OptionHeader.HEADER_SIZE > 0);
        assertTrue(Protocol.WORD_SIZE > 0);
        assertEquals(0, OptionHeader.HEADER_SIZE % Protocol.WORD_SIZE);
    }

    @Test
    public void testConstructor() {
        OptionHeader optionHeader = new OptionHeader();

        assertEquals(OptionType.UNDEFINED, optionHeader.type());
        assertFalse(optionHeader.packed());
        assertEquals(0, optionHeader.typeSpecific());
        assertEquals(1, optionHeader.words());
    }

    @Test
    public void testGetterSetterType() {
        OptionHeader optionHeader = new OptionHeader();

        for (OptionType t : OptionType.values()) {
            optionHeader.setType(t);
            assertEquals(t, optionHeader.type());
            // Other fields are unchanged
            assertFalse(optionHeader.packed());
            assertEquals(0, optionHeader.typeSpecific());
            assertEquals(1, optionHeader.words());
        }
    }

    @Test
    public void testGetterSetterPacked() {
        OptionHeader optionHeader = new OptionHeader();

        optionHeader.setPacked(true);
        assertTrue(optionHeader.packed());
        // Other fields are unchanged
        assertEquals(OptionType.UNDEFINED, optionHeader.type());
        assertEquals(0, optionHeader.typeSpecific());
        assertEquals(1, optionHeader.words());

        optionHeader.setPacked(false);
        assertFalse(optionHeader.packed());
        // Other fields are unchanged
        assertEquals(OptionType.UNDEFINED, optionHeader.type());
        assertEquals(0, optionHeader.typeSpecific());
        assertEquals(1, optionHeader.words());
    }

    @Test
    public void testGetterSetterTypeSpecific() {
        OptionHeader optionHeader = new OptionHeader();

        // ts < 0
        try {
            optionHeader.setTypeSpecific((byte) -1);
            fail(); // shouldn't get here
        } catch (IllegalArgumentException e) {
            // OK
        }

        // ts > MAX_TYPE_SPECIFIC
        try {
            optionHeader.setTypeSpecific((byte) (OptionHeader.MAX_TYPE_SPECIFIC + 1));
            fail(); // shouldn't get here
        } catch (IllegalArgumentException e) {
            // OK
        }

        // ts = 0
        optionHeader.setTypeSpecific((byte) 0);
        assertEquals(0, optionHeader.typeSpecific());
        // Other fields are unchanged
        assertEquals(OptionType.UNDEFINED, optionHeader.type());
        assertFalse(optionHeader.packed());
        assertEquals(1, optionHeader.words());

        // ts = MAX_TYPE_SPECIFIC
        optionHeader.setTypeSpecific((byte) OptionHeader.MAX_TYPE_SPECIFIC);
        assertEquals(OptionHeader.MAX_TYPE_SPECIFIC, optionHeader.typeSpecific());
        // Other fields are unchanged
        assertEquals(OptionType.UNDEFINED, optionHeader.type());
        assertFalse(optionHeader.packed());
        assertEquals(1, optionHeader.words());
    }

    @Test
    public void testGetterSetterWords() {
        OptionHeader optionHeader = new OptionHeader();

        // words < 0
        try {
            optionHeader.setWords(-1);
            fail(); // shouldn't get here
        } catch (IllegalArgumentException e) {
            // OK
        }

        // words > MAX_WORDS
        try {
            optionHeader.setWords(OptionHeader.MAX_WORDS + 1);
            fail(); // shouldn't get here
        } catch (IllegalArgumentException e) {
            // OK
        }

        // words = 0
        optionHeader.setWords(0);
        assertEquals(0, optionHeader.words());
        // Other fields are unchanged
        assertEquals(OptionType.UNDEFINED, optionHeader.type());
        assertFalse(optionHeader.packed());
        assertEquals(0, optionHeader.typeSpecific());

        // words = MAX_WORDS
        optionHeader.setWords(OptionHeader.MAX_WORDS);
        assertEquals(OptionHeader.MAX_WORDS, optionHeader.words());
        // Other fields are unchanged
        assertEquals(OptionType.UNDEFINED, optionHeader.type());
        assertFalse(optionHeader.packed());
        assertEquals(0, optionHeader.typeSpecific());
    }

    @Test
    public void testStreamIn() throws IOException {
        // packed option
        ByteBuffer bb =
                ByteBuffer.wrap(new byte[] {0b00001110, 0b00000000, 0b00000000, 0b00000101});
        ByteBufferInputStream bbis = new ByteBufferInputStream(bb);

        OptionHeader optionHeader = new OptionHeader();
        optionHeader.streamIn(bbis);

        assertEquals(OptionType.SUB_QUEUE_INFOS, optionHeader.type());
        assertTrue(optionHeader.packed());
        assertEquals(0, optionHeader.typeSpecific());
        assertEquals(5, optionHeader.words()); // rda counter

        // non-packed option
        bb = ByteBuffer.wrap(new byte[] {0b00001100, 0b01000000, 0b00000000, 0b00000011});
        bbis = new ByteBufferInputStream(bb);

        optionHeader = new OptionHeader();
        optionHeader.streamIn(bbis);

        assertEquals(OptionType.SUB_QUEUE_INFOS, optionHeader.type());
        assertFalse(optionHeader.packed());
        assertEquals(2, optionHeader.typeSpecific()); // size of SubQueueInfo
        assertEquals(3, optionHeader.words()); // header + SubQueueInfo
    }
}
