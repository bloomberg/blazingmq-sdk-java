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
package com.bloomberg.bmq.it.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.infr.util.SystemUtil;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BmqBrokerTestServer implements BmqBroker {
    // A wrapper over native broker.
    // 'start' forks a process and executes `bmqbrk.tsk`.
    // 'stop' kills the process.

    // From bmqbrkr.cfg: error|warn|info|debug|trace
    enum LogLevel {
        error,
        warn,
        info,
        debug,
        trace;

        static LogLevel fromString(String lvl) {
            for (LogLevel ll : LogLevel.values()) {
                if (ll.toString().equals(lvl)) return ll;
            }
            return info;
        }
    }

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final String PATH_TO_BMQ_BROKER;
    private static final String INTEGRATION_TEST_PATH_TO_BMQ_BROKER;
    private static final int DEFAULT_WAITING_TIME = 20; // sec
    private static final int DEFAULT_INVALID_PID = -111; // sec
    private static final LogLevel BMQ_BROKER_LOG_LEVEL;

    static {
        PATH_TO_BMQ_BROKER = System.getenv("BMQ_BROKER_PATH");
        INTEGRATION_TEST_PATH_TO_BMQ_BROKER = System.getenv("BMQ_BROKER_INTEGRATION_TEST_PATH");
        BMQ_BROKER_LOG_LEVEL = LogLevel.fromString(System.getenv("BMQ_BROKER_LOG_LEVEL"));
    }

    Process process;
    Path tmpFolder;
    File outputFile;
    int pid = DEFAULT_INVALID_PID;
    int waitingTime = DEFAULT_WAITING_TIME;
    SessionOptions sessionOptions;
    String defaultTier;
    boolean dropTmpFolder = false;
    boolean dumpBrokerOutput = false;

    private static long getPidOfProcess(Process p) {
        try {
            if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
                Field f = p.getClass().getDeclaredField("pid");
                f.setAccessible(true);
                long pid = f.getLong(p);
                f.setAccessible(false);
                return pid;
            }
        } catch (Exception e) {
            logger.error("Failed to get process PID");
        }
        return DEFAULT_INVALID_PID;
    }

    private static Path makeTempDir(String basePath, String prefix) {
        try {
            return Files.createTempDirectory(
                    Paths.get(basePath), // base path
                    prefix, // directory name prefix
                    PosixFilePermissions.asFileAttribute(
                            PosixFilePermissions.fromString("rwxr-xr-x")));
        } catch (Exception e) {
            logger.error("Error while creating temporary directory: ", e);
            throw new IllegalStateException("Error while creating temporary directory", e);
        }
    }

    private BmqBrokerTestServer(SessionOptions so) {
        sessionOptions = Argument.expectNonNull(so, "session options");
        process = null;
    }

    public static BmqBrokerTestServer createStoppedBroker(int port) {
        Argument.expectPositive(port, "port");

        final URI uri = URI.create("tcp://localhost:" + port);
        SessionOptions so = SessionOptions.builder().setBrokerUri(uri).build();

        return new BmqBrokerTestServer(so);
    }

    private void init(int waitTime) throws IOException, InterruptedException {
        final String envPath = BmqBroker.brokerDir();

        // Cleanup from previous run. Delete named pipe if it's there
        File np = new File(envPath + "/bmqbrkr.ctl");
        if (np.exists()) {
            logger.info("Deleting orphan bmqbrkr.ctl");
            np.delete();
        }

        // Since we run multiple tests at the same time starting instances
        // of BlazingMQ broker in the same location (BMQ_PREFIX),
        // we need to create a tmp folder for storage, log and stat files
        tmpFolder = makeTempDir(envPath, "localBMQ_");

        ProcessBuilder pb = new ProcessBuilder("./run");

        final Path storagePath = tmpFolder.resolve("storage");

        Map<String, String> env = pb.environment();
        env.put("BMQ_PREFIX", envPath);
        env.put("BMQ_STORAGE", storagePath.toString());
        logger.info("BlazingMQ Broker storage directory: {}", storagePath);

        outputFile = new File(tmpFolder.resolve("output").toString());

        // Common environment vars
        env.put("BMQ_PORT", String.valueOf(sessionOptions.brokerUri().getPort()));
        env.put("BMQ_HOSTTAGS_FILE", "/bb/bin/bbcpu.lst");
        env.put("PYTHONPATH", envPath + "/python");

        pb.directory(new File(envPath));

        logger.info("BlazingMQ Broker BMQ_PREFIX: [{}]", env.get("BMQ_PREFIX"));
        logger.info("BlazingMQ Broker tmp directory: [{}]", tmpFolder);
        logger.info("BlazingMQ Broker output file: [{}]", outputFile.getCanonicalPath());

        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(outputFile));

        logger.info("BlazingMQ Broker run command:\n{}", String.join(" ", pb.command()));
        process = pb.start();
        if (waitTime > 0) {
            process.waitFor(waitTime, TimeUnit.SECONDS);
        }
        if (!process.isAlive()) {
            logger.error(
                    "Failed to start broker process after waiting for [{}] seconds.", waitTime);
            throw new IllegalStateException("Failed to start broker process");
        }

        pid = (int) getPidOfProcess(process);
    }

    @Override
    public void start() {
        Argument.expectNonNull(BmqBroker.brokerDir(), "it.brokerDir");

        if (process != null) {
            logger.info("Broker already started.");
            assertTrue(process.isAlive());
            return;
        }

        try {
            logger.info(
                    "Starting broker on {}.  Env path: [{}].  Waiting time: [{}].",
                    sessionOptions.brokerUri(),
                    BmqBroker.brokerDir(),
                    waitingTime);

            init(waitingTime);

            logger.info("Broker started on {} with pid [{}].", sessionOptions.brokerUri(), pid);
        } catch (Exception e) {
            logger.error(
                    "Failed to start BlazingMQ broker on port '{}'",
                    sessionOptions.brokerUri().getPort());
            throw new RuntimeException(e);
        }

        // For standalone BlazingMQ broker default tier should be the 'lcl-{hostname}'
        defaultTier = "lcl-" + SystemUtil.getHostName();
    }

    @Override
    public void stop() {
        try {
            if (process != null) {
                logger.info("Stopping broker, pid [{}].", pid);
                if (pid > 0) {
                    Runtime.getRuntime().exec("kill -SIGINT " + pid);
                } else {
                    process.destroyForcibly();
                }
                process.waitFor();
                assertFalse(process.isAlive());
                logger.info("Broker stopped");
            }
        } catch (Exception e) {
            logger.error("Failed to stop BlazingMQ broker", e);
            throw new RuntimeException(e);
        } finally {
            process = null;
        }
    }

    @Override
    public void enableRead() {
        // no-op for bmqbroker server.
    }

    @Override
    public void disableRead() {
        throw new UnsupportedOperationException(
                "'disableRead' not supported for bmqbrkr-based server.");
    }

    @Override
    public String defaultTier() {
        return defaultTier;
    }

    @Override
    public void close() throws IOException {
        stop();

        if (dumpBrokerOutput) {
            // dump broker output
            logger.info(
                    "Dump BlazingMQ Broker output:\n{}",
                    new String(Files.readAllBytes(outputFile.toPath())));
        }

        if (dropTmpFolder) {
            // remove tmp folder
            logger.info("Drop '{}' tmp folder for {}", tmpFolder, this);
            FileUtils.deleteDirectory(tmpFolder.toFile());
        }
    }

    @Override
    public void setDropTmpFolder() {
        logger.info("Set to drop tmp folder for {}", this);
        dropTmpFolder = true;
    }

    @Override
    public void setDumpBrokerOutput() {
        logger.info("Set to dump broker output for {}", this);
        dumpBrokerOutput = true;
    }

    @Override
    public SessionOptions sessionOptions() {
        return sessionOptions;
    }

    @Override
    public String toString() {
        return "BmqBrokerTestServer [" + pid + " on " + sessionOptions.brokerUri() + "]";
    }
}
