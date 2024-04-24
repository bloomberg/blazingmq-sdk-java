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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.msg.MessagesTestSamples;
import com.bloomberg.bmq.util.TestHelpers;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AckHeaderTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    void testStreamIn() {
        try {
            ByteBuffer buf = TestHelpers.readFile(MessagesTestSamples.ACK_MSG.filePath());

            ByteBufferInputStream bbis = new ByteBufferInputStream(buf);
            EventHeader header = new EventHeader();

            header.streamIn(bbis);

            assertEquals(0, header.fragmentBit());

            assertEquals(252, header.length());

            assertEquals(1, header.protocolVersion());

            assertEquals(2, header.headerWords());

            assertNotNull(header.type());

            assertEquals(EventType.ACK, header.type());

            AckHeader ackHeader = new AckHeader();

            ackHeader.streamIn(bbis);

            assertEquals(0, ackHeader.flags());
            assertEquals(1, ackHeader.headerWords());
            assertEquals(6, ackHeader.perMessageWords());

        } catch (IOException e) {
            logger.info("Failed to stream in AckHeader: ", e);
            fail();
        }
    }
}
