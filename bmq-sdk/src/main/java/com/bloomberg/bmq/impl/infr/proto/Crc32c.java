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

import com.bloomberg.bmq.impl.infr.util.SystemUtil;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import org.apache.commons.codec.digest.PureJavaCrc32C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Crc32c {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static {
        logger.info("*** Using org.apache.commons.codec.digest.PureJavaCrc32C ***");

        // Warn if Java version > 1.8 is used.
        // The warning record is logged only in situation if SDK was built with
        // JDK8 but is used with newer one e.g. JDK11
        if (!SystemUtil.isJava8()) {
            logger.warn("*** [NOTE] Starting from JDK9 switch to java.util.zip.CRC32C ***");
        }
    }

    public static boolean isJdkImplementation() {
        return false;
    }

    public static long calculate(ByteBuffer[] buffer) {

        PureJavaCrc32C crc32c = new PureJavaCrc32C();

        for (ByteBuffer b : buffer) {
            byte[] buf;
            if (b.hasArray()) {
                buf = b.array();
            } else {
                ByteBuffer t = ByteBuffer.allocate(b.limit());
                t.put(b);
                buf = t.array();
            }
            crc32c.update(buf, 0, b.limit());
        }
        return crc32c.getValue();
    }
}
