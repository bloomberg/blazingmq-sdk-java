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

import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class VersionUtil {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static final int DEFAULT_VERSION = 999999;
    static final int SNAPSHOT_INDICATOR = 99000000;

    private VersionUtil() {}

    public static int versionStringToInt(String version) {
        // 'version' is of the form 'x.y.z', and returned integer is calculated
        // like so:
        // (10000 * x) + (100 * y) + z
        // In case of any error, 'DEFAULT_VERSION' is returned.

        logger.info("Version string: [{}].", version);

        String[] tokens = version.split("\\.");

        if (tokens.length != 3) {
            logger.warn("Invalid version string, num tokens: {}", tokens.length);
            return DEFAULT_VERSION;
        }

        int result = DEFAULT_VERSION;

        try {
            int x = Integer.parseInt(tokens[0]);
            int y = Integer.parseInt(tokens[1]);
            int z = Integer.parseInt(tokens[2]);
            result = (10000 * x) + (100 * y) + z;
        } catch (Exception e) {
            logger.warn("Exception while parsing version string: ", e);
        }
        return result;
    }

    public static String getJarVersion() {
        // This implementation returns 'Implementation-Version' field from the
        // JAR's manifest to which this class belongs.

        // Note that returned value will be null in the test driver of this
        // class because JAR is not loaded and there is no 'package' or
        // manifest file.

        return VersionUtil.class.getPackage().getImplementationVersion();
    }

    public static int getSdkVersion() {
        final String implVersion = getJarVersion();
        logger.info("BMQ SDK Jar ImplementationVersion: [{}]", implVersion);

        if (implVersion == null) {
            // When loaded outside of a JAR, just return a default value of
            // 999999 , which is what we return in C++ when program is built
            // from the master instead of using 'libbmq'.
            return DEFAULT_VERSION;
        }

        // 'implVersion' is not null. It can be of 2 forms:
        // - x.y.z
        // - x.y.z-SNAPSHOT

        // If its 'x.y.z', we return an integer calculated from this formula:
        // (10000 * x) + (100 * y) + z
        // This formula is same as what we use in C++.

        // If its 'x.y.z-SNAPSHOT', we use this formula:
        // 99000000 + (10000 * x) + (100 * y) + z
        // i.e., we just add 99000000 to the result of 'x.y.z'.

        String ver = implVersion;
        boolean isSnapshot = false;

        if (implVersion.endsWith("-SNAPSHOT")) {
            isSnapshot = true;
            ver = implVersion.replace("-SNAPSHOT", "");
        }

        int result = versionStringToInt(ver);

        if (isSnapshot) {
            return result + SNAPSHOT_INDICATOR;
        }

        return result;
    }
}
