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

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.zip.CRC32C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Crc32c {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static {
        logger.info("*** Using java.util.zip.CRC32C ***");
    }

    public static boolean isJdkImplementation() {
        return true;
    }

    public static long calculate(ByteBuffer[] buffer) {
        CRC32C crc32c = new CRC32C();

        for (ByteBuffer b : buffer) {
            b.mark();
            crc32c.update(b);
            b.reset();
        }

        return crc32c.getValue();
    }
}
