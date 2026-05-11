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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemUtil {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static String getJavaVersionString() {
        return System.getProperty("java.version");
    }

    public static boolean isJava8() {
        return getJavaVersionString().startsWith("1.");
    }

    public static int getProcessId() {
        int pid = 0;
        String vmname = null;
        try {
            vmname = ManagementFactory.getRuntimeMXBean().getName();
            int pos = vmname.indexOf('@');
            if (pos != -1) {
                String processId = vmname.substring(0, pos);
                pid = Integer.parseInt(processId);
            }
        } catch (NumberFormatException e) {
            logger.info("Cannot get pid from {}: {}", vmname, e);
        } catch (Exception e) {
            logger.info("Cannot get pid: ", e);
        }
        return pid;
    }

    public static String getProcessName() {
        // So far return 'sun.java.command' property.
        return System.getProperty("sun.java.command");
    }

    public static String getHostName() {
        // So far use unix env variable HOSTNAME.
        // For windows it should be COMPUTERNAME.
        // Another way is to call
        // InetAddress.getLocalHost().getHostName()
        // but there could be issues with multiple interfaces, etc.
        String res = System.getenv("HOSTNAME");
        if (res == null) {
            logger.warn("HOSTNAME env variable is not set, using 'localhost'");
            res = "localhost";
        }
        return res;
    }

    public static String getOsName() {
        return System.getProperty("os.name");
    }

    public static int getEphemeralPort() {
        // Warning: Any code using the port number returned by this method
        // is subject to a race condition - a different process / thread may
        // bind to the same port immediately after we close the ServerSocket instance.
        int port = 30114;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        } catch (IOException e) {
            logger.error("Failed to find a free random port: ", e);
        }
        return port;
    }

    private SystemUtil() {
        throw new IllegalStateException("Utility class");
    }
}
