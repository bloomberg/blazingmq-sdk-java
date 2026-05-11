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
package com.bloomberg.bmq.impl.infr.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.invoke.MethodHandles;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SystemUtilTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    void testPid() {
        int p = SystemUtil.getProcessId();

        logger.info("PID: {}", p);

        assertTrue(p > 0);
    }

    @Test
    void testProcName() {
        assertTrue(SystemUtil.getProcessName().length() > 0);
    }

    @Test
    void testOsName() {
        logger.info("OS: [{}]", SystemUtil.getOsName());
        assertTrue(SystemUtil.getOsName().length() > 0);
    }
}
