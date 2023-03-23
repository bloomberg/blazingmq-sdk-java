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
import static org.junit.Assert.fail;

import com.bloomberg.bmq.impl.QueueId;
import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubQueueInfosOptionTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testConstructor() {
        // Null header
        try {
            new SubQueueInfosOption(null);
            fail(); // shouldn't get here
        } catch (IllegalArgumentException e) {
            // OK
        }

        // Wrong option type
        try {
            OptionHeader optionHeader = new OptionHeader();
            optionHeader.setType(OptionType.SUB_QUEUE_IDS_OLD);

            new SubQueueInfosOption(optionHeader);
            fail(); // shouldn't get here
        } catch (IllegalArgumentException e) {
            // OK
        }

        // Wrong type-specific value
        try {
            OptionHeader optionHeader = new OptionHeader();
            optionHeader.setType(OptionType.SUB_QUEUE_INFOS);
            optionHeader.setTypeSpecific((byte) 0);

            new SubQueueInfosOption(optionHeader);
            fail(); // shouldn't get here
        } catch (IllegalArgumentException e) {
            assertEquals(
                    "Type-specific value must be positive for non-packed option", e.getMessage());
        }

        // Proper header
        OptionHeader optionHeader = new OptionHeader();
        optionHeader.setType(OptionType.SUB_QUEUE_INFOS);
        optionHeader.setTypeSpecific((byte) 2);

        SubQueueInfosOption option = new SubQueueInfosOption(optionHeader);
        assertArrayEquals(new Integer[] {}, option.subQueueIds());
    }

    @Test
    public void testStreamIn() throws IOException {
        ByteBuffer bb =
                ByteBuffer.wrap(
                        new byte[] {
                            0b00001110, 0b00000000, 0b00000000, 0b00000101, // header (packed)
                            0b00001100, 0b01000000, 0b00000000, 0b00000101, // header
                            0b00000000, 0b00000000, 0b00000000, 0b00000001, // subqueue id 1
                            0b00000101, 0b00000000, 0b00000000,
                                    0b00000000, // rda counter + reserved
                            0b00000000, 0b00000000, 0b00000000, 0b00000010, // subqueue id 2
                            0b00000101, 0b00000000, 0b00000000,
                                    0b00000000, // rda counter + reserved
                            0b00001100, 0b01000000, 0b00000000, 0b00000011, // header
                            0b00000000, 0b00000000, 0b00000000, 0b00000011, // subqueue id 3
                            0b00000101, 0b00000000, 0b00000000,
                                    0b00000000, // rda counter + reserved
                        });
        ByteBufferInputStream bbis = new ByteBufferInputStream(bb);

        // Packed option
        OptionHeader optionHeader = new OptionHeader();
        optionHeader.streamIn(bbis);

        SubQueueInfosOption option = new SubQueueInfosOption(optionHeader);
        option.streamIn(bbis);

        assertArrayEquals(new Integer[] {QueueId.k_DEFAULT_SUBQUEUE_ID}, option.subQueueIds());

        // Non-packed
        optionHeader = new OptionHeader();
        optionHeader.streamIn(bbis);

        option = new SubQueueInfosOption(optionHeader);
        option.streamIn(bbis);

        assertArrayEquals(new Integer[] {1, 2}, option.subQueueIds());

        // Stream in one more time
        optionHeader.streamIn(bbis);
        option.streamIn(bbis);

        assertArrayEquals(new Integer[] {3}, option.subQueueIds());

        // Check that input stream is empty
        assertEquals(0, bbis.available());
    }
}
