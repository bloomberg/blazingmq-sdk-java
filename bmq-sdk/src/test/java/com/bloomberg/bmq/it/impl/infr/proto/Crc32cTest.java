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
package com.bloomberg.bmq.it.impl.infr.proto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.proto.Crc32c;
import com.bloomberg.bmq.impl.infr.scm.VersionUtil;
import com.bloomberg.bmq.impl.infr.util.SystemUtil;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Crc32cTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static HashMap<String, Long> DATA = new HashMap<>();

    static {
        DATA.put("", 0x0L);
        DATA.put("DYB|O", 0x0L);
        DATA.put("0", 0x629E1AE0L);
        DATA.put("1", 0x90F599E3L);
        DATA.put("2", 0x83A56A17L);
        DATA.put("~", 0x8F9DB87BL);
        DATA.put("22", 0x47B26CF9L);
        DATA.put("fa", 0x8B9F1387L);
        DATA.put("t0-", 0x77E2D1A9L);
        DATA.put("34v}", 0x031AD8A7L);
        DATA.put("shaii", 0xB0638FB5L);
        DATA.put("3jf-_3", 0xE186B745L);
        DATA.put("bonjour", 0x156088D2L);
        DATA.put("vbPHbvtB", 0x12AAFAA6L);
        DATA.put("aoDicgezd", 0xBF5E01C8L);
        DATA.put("123456789", 0xe3069283L);
        DATA.put("gaaXsSP1al", 0xC4E61D23L);
        DATA.put("2Wm9bbNDehd", 0x54A11873L);
        DATA.put("GamS0NJhAl8y", 0x0044AC66L);
    }

    @Test
    void testImplementation() {
        // Ensure we are running with supported version of JVM
        boolean isPartOfJar = VersionUtil.getJarVersion() != null;

        if (!isPartOfJar) {
            // We execute unit test, 3rd party implementation of Crc32c should be used.
            logger.info("Running Crc32cTest as a separate class (not packed into Jar file)");
            assertFalse(Crc32c.isJdkImplementation());
        } else {
            // We are running integration test.
            // if JDK8 is used to build and verify, then the generated JAR file contains only Java8
            // version of Crc32c class (3rd party implementation).
            // If JDK11 or above is used, then the generated JAR file also contains Java9 version of
            // Crc32c (native JDK implementation) and it should be used by JVM.
            logger.info("Running Crc32cTest as a part of Jar file");
            assertEquals(SystemUtil.isJava8(), !Crc32c.isJdkImplementation());
        }
    }

    @Test
    void testCrc32c() throws IOException {
        Set<Map.Entry<String, Long>> set = DATA.entrySet();
        for (Map.Entry<String, Long> me : set) {
            ByteBufferOutputStream bbos = new ByteBufferOutputStream();
            bbos.writeAscii(me.getKey());
            assertEquals(me.getValue().longValue(), Crc32c.calculate(bbos.reset()));
        }
    }

    @Test
    void testCrc32cUnsigned() throws IOException {
        final String PAYLOAD = "3jf-_3";
        final long CRC32C_VALUE = 0xE186B745L;
        ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
        ByteBufferOutputStream bbos = new ByteBufferOutputStream();

        bbos.writeAscii(PAYLOAD);
        long crc32c = Crc32c.calculate(bbos.reset());
        bb.putInt((int) crc32c);
        bb.flip();
        long res = Integer.toUnsignedLong(bb.getInt());

        assertEquals(CRC32C_VALUE, res);
    }

    @Test
    void testCrc32cMultithreaded() throws InterruptedException {
        class TestThread implements Runnable {
            final Thread thread;
            boolean hasError;

            TestThread() {
                thread = new Thread(this, "Crc32c test thread");
                thread.start();
            }

            public void run() {
                try {
                    Set<Map.Entry<String, Long>> set = DATA.entrySet();
                    for (Map.Entry<String, Long> me : set) {
                        ByteBufferOutputStream bbos = new ByteBufferOutputStream();
                        bbos.writeAscii(me.getKey());
                        assertEquals(me.getValue().longValue(), Crc32c.calculate(bbos.reset()));
                    }
                } catch (Exception e) {
                    logger.info("Failed to calculate CRC32: ", e);
                    hasError = true;
                }
            }
        }

        final int NUM_TREADS = 5;
        ArrayList<TestThread> threads = new ArrayList<>();
        long startTime = System.nanoTime();

        for (int i = 0; i < NUM_TREADS; i++) {
            threads.add(new TestThread());
        }

        for (TestThread t : threads) {
            t.thread.join();
            assertFalse(t.hasError);
        }

        long endTime = System.nanoTime();
        long duration = (endTime - startTime);

        logger.info("[testCrc32cMultithreaded] Time spent: {} ms", duration / 1000000);
    }
}
