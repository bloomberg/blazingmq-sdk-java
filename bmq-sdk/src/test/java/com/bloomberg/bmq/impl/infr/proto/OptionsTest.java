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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptionsTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testStreamInIds() throws IOException {
        ByteBuffer bb =
                ByteBuffer.wrap(
                        new byte[] {
                            0b00000100, 0b00000000, 0b00000000, 0b00000011, // header
                            0b00000000, 0b00000000, 0b00000000, 0b00000001, // subqueue id 1
                            0b00000000, 0b00000000, 0b00000000, 0b00000010, // subqueue id 2

                            // Second option will be ignored
                            0b00000100, 0b00000000, 0b00000000, 0b00000010, // header
                            0b00000000, 0b00000000, 0b00000000, 0b00000011, // subqueue id 3
                        });
        ByteBufferInputStream bbis = new ByteBufferInputStream(bb);

        Options options = new Options();
        options.streamIn(20, bbis);

        logger.info("Options: {}", options);

        assertNotNull(options.subQueueIdsOption());
        assertArrayEquals(new Integer[] {1, 2}, options.subQueueIdsOption().subQueueIds());

        assertNull(options.subQueueInfosOption());

        // Check that input stream is empty
        assertEquals(0, bbis.available());
    }

    @Test
    public void testStreamInInfos() throws IOException {
        ByteBuffer bb =
                ByteBuffer.wrap(
                        new byte[] {
                            0b00001100, 0b01000000, 0b00000000, 0b00000101, // header
                            0b00000000, 0b00000000, 0b00000000, 0b00000001, // subqueue id 1
                            0b00000101, 0b00000000, 0b00000000,
                                    0b00000000, // rda counter + reserved
                            0b00000000, 0b00000000, 0b00000000, 0b00000010, // subqueue id 2
                            0b00000101, 0b00000000, 0b00000000,
                                    0b00000000, // rda counter + reserved

                            // Second option will be ignored
                            0b00001100, 0b01000000, 0b00000000, 0b00000011, // header
                            0b00000000, 0b00000000, 0b00000000, 0b00000011, // subqueue id 3
                            0b00000101, 0b00000000, 0b00000000,
                                    0b00000000, // rda counter + reserved
                        });
        ByteBufferInputStream bbis = new ByteBufferInputStream(bb);

        Options options = new Options();
        options.streamIn(32, bbis);

        logger.info("Options: {}", options);

        assertNotNull(options.subQueueInfosOption());
        assertArrayEquals(new Integer[] {1, 2}, options.subQueueInfosOption().subQueueIds());

        assertNull(options.subQueueIdsOption());

        // Check that input stream is empty
        assertEquals(0, bbis.available());
    }

    @Test
    public void testStreamInInfosIds() throws IOException {
        ByteBuffer bb =
                ByteBuffer.wrap(
                        new byte[] {
                            // Infos
                            0b00001100, 0b01000000, 0b00000000, 0b00000101, // header
                            0b00000000, 0b00000000, 0b00000000, 0b00000001, // subqueue id 1
                            0b00000101, 0b00000000, 0b00000000,
                                    0b00000000, // rda counter + reserved
                            0b00000000, 0b00000000, 0b00000000, 0b00000010, // subqueue id 2
                            0b00000101, 0b00000000, 0b00000000,
                                    0b00000000, // rda counter + reserved

                            // Ids
                            0b00000100, 0b00000000, 0b00000000, 0b00000010, // header
                            0b00000000, 0b00000000, 0b00000000, 0b00000011, // subqueue id 3
                        });
        ByteBufferInputStream bbis = new ByteBufferInputStream(bb);

        Options options = new Options();
        options.streamIn(28, bbis);

        logger.info("Options: {}", options);

        assertNotNull(options.subQueueInfosOption());
        assertArrayEquals(new Integer[] {1, 2}, options.subQueueInfosOption().subQueueIds());
        assertNotNull(options.subQueueIdsOption());
        assertArrayEquals(new Integer[] {3}, options.subQueueIdsOption().subQueueIds());

        // Check that input stream is empty
        assertEquals(0, bbis.available());
    }
}
