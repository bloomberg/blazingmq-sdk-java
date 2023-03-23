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
package com.bloomberg.bmq.impl.infr.scm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.lang.invoke.MethodHandles;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersionUtilTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testJarVersion() {
        String ver = VersionUtil.getJarVersion();
        assertNull(ver);
    }

    @Test
    public void testSdkVersion() {
        int ver = VersionUtil.getSdkVersion();
        assertEquals(999999, ver); // the default version
    }

    @Test
    public void testVersionStringToInt() {
        assertEquals(999999, VersionUtil.versionStringToInt("0.0"));
        assertEquals(999999, VersionUtil.versionStringToInt("0"));
        assertEquals(999999, VersionUtil.versionStringToInt("0"));
        assertEquals(999999, VersionUtil.versionStringToInt("x.y.z"));
        assertEquals(999999, VersionUtil.versionStringToInt("ab.cd.ef"));

        assertEquals(0, VersionUtil.versionStringToInt("0.0.0"));
        assertEquals(1, VersionUtil.versionStringToInt("0.0.1"));
        assertEquals(112233, VersionUtil.versionStringToInt("11.22.33"));
        assertEquals(10300, VersionUtil.versionStringToInt("1.3.0"));
        assertEquals(41902, VersionUtil.versionStringToInt("4.19.2"));
    }
}
