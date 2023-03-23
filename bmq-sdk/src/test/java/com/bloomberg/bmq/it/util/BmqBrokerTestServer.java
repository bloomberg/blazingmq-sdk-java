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

    private static boolean hasValidEnvPath() {
        // At least one of the two env vars must be non-null and non-empty.

        final boolean hasPath = PATH_TO_BMQ_BROKER != null && !PATH_TO_BMQ_BROKER.isEmpty();
        final boolean hasIntegrationPath =
                INTEGRATION_TEST_PATH_TO_BMQ_BROKER != null
                        && !INTEGRATION_TEST_PATH_TO_BMQ_BROKER.isEmpty();

        return hasPath || hasIntegrationPath;
    }

    private static String getEnvPath() {
        if (!hasValidEnvPath()) {
            throw new IllegalStateException(
                    "Neither BMQ_BROKER_PATH nor "
                            + "BMQ_BROKER_INTEGRATION_TEST_PATH "
                            + "env variables are specified.");
        }

        // Always give preference to 'BMQ_BROKER_PATH', which user may have
        // specified.  'BMQ_BROKER_INTEGRATION_TEST_PATH' is typically used in
        // the Jenkins run.
        if (PATH_TO_BMQ_BROKER != null) {
            return PATH_TO_BMQ_BROKER;
        } else {
            if (INTEGRATION_TEST_PATH_TO_BMQ_BROKER == null) {
                throw new IllegalStateException(
                        "Both paths to bmq broker cannot be null at the same time");
            }
            return INTEGRATION_TEST_PATH_TO_BMQ_BROKER;
        }
    }

    private static boolean isLocalRun() {
        if (!hasValidEnvPath()) {
            throw new IllegalStateException(
                    "Neither BMQ_BROKER_PATH nor "
                            + "BMQ_BROKER_INTEGRATION_TEST_PATH "
                            + "env variables are specified.");
        }

        return PATH_TO_BMQ_BROKER != null && !PATH_TO_BMQ_BROKER.isEmpty();
    }

    private static boolean isJenkinsRun() {
        if (!hasValidEnvPath()) {
            throw new IllegalStateException(
                    "Neither BMQ_BROKER_PATH nor "
                            + "BMQ_BROKER_INTEGRATION_TEST_PATH "
                            + "env variables are specified.");
        }

        return INTEGRATION_TEST_PATH_TO_BMQ_BROKER != null
                && !INTEGRATION_TEST_PATH_TO_BMQ_BROKER.isEmpty();
    }

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
        final String envPath = getEnvPath();

        // Cleanup from previous run. Delete named pipe if it's there
        File np = new File(envPath + "/bmqbrkr.ctl");
        if (np.exists()) {
            logger.info("Deleting orphan bmqbrkr.ctl");
            np.delete();
        }

        ProcessBuilder pb = null;
        if (isLocalRun()) {
            // Since we run multiple tests at the same time starting instances
            // of BMQ broker in the same location (BMQ_PREFIX),
            // we need to create a tmp folder for storage, log and stat files
            tmpFolder = makeTempDir(envPath, "localBMQ_");

            pb =
                    new ProcessBuilder(
                            "./bmqbrkr.tsk",
                            "./bmqbrkr.cfg",
                            "development",
                            "set:appConfig/binDir=" + envPath + "/python/bin",
                            "set:appConfig/etcDir=" + envPath + "/etc",
                            "set:taskConfig/logController/fileName=" + tmpFolder + "/logs.%T.%p",
                            "set:appConfig/stats/printer/file=" + tmpFolder + "/stat.%T.%p",
                            "set:appConfig/plugins/libraries=" + envPath + "/../../plugins",
                            BMQ_BROKER_LOG_LEVEL.toString());

            final Path storagePath = tmpFolder.resolve("storage");

            Map<String, String> env = pb.environment();
            env.put("BMQ_PREFIX", envPath);
            env.put("BMQ_STORAGE", storagePath.toString());
            logger.info("BMQ Broker storage directory: {}", storagePath);

            outputFile = new File(tmpFolder.resolve("output").toString());
        } else if (isJenkinsRun()) {
            // Domains path location will be the one used on dev.
            // Since we use temp folder here, there is no need to override
            // log settings like we do for local run
            pb =
                    new ProcessBuilder(
                            "./bmqbrkr.tsk",
                            "./bmqbrkr.cfg",
                            "development",
                            "set:appConfig/binDir=" + envPath + "/python/bin",
                            "set:appConfig/etcDir=" + envPath + "/etc",
                            BMQ_BROKER_LOG_LEVEL.toString());

            // For Jenkins run, we need to create a temporary directory which
            // be used for BMQ_PREFIX env variable.  This unique temp directory
            // is required mainly to ensure that 'bmqbrkr.ctl' named pipe is
            // created in a unique location, and subsequent runs of bmqbrkr
            // succeed without complaining about 'named pipe already exists'.

            // Note that setting the BMQ_PREFIX env variable will also mean
            // that a 'storage' sub-directory could also be created in that
            // folder, but since in Jenkins run, bmqbrkr acts as just a proxy
            // to dev cluster, it won't create storage folder.

            tmpFolder = makeTempDir("/bb/data/tmp", "bmq");

            Map<String, String> env = pb.environment();
            env.put("BMQ_PREFIX", tmpFolder.toString());

            outputFile = new File(tmpFolder.toFile(), "output");
        }

        // Common environment vars
        Map<String, String> env = pb.environment();
        env.put("BMQ_PORT", String.valueOf(sessionOptions.brokerUri().getPort()));
        env.put("BMQ_HOSTTAGS_FILE", "/bb/bin/bbcpu.lst");
        env.put("PYTHONPATH", envPath + "/python");

        pb.directory(new File(envPath));

        logger.info("BMQ Broker BMQ_PREFIX: [{}]", env.get("BMQ_PREFIX"));
        logger.info("BMQ Broker tmp directory: [{}]", tmpFolder);
        logger.info("BMQ Broker output file: [{}]", outputFile.getCanonicalPath());

        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(outputFile));

        logger.info("BMQ Broker run command:\n{}", String.join(" ", pb.command()));
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

        if (process != null) {
            logger.info("Broker already started.");
            assertTrue(process.isAlive());
            return;
        }

        if (!hasValidEnvPath()) {
            throw new RuntimeException("Doesn't have valid path for broker");
        }

        try {
            logger.info(
                    "Starting broker on {}.  Env path: [{}].  Waiting time: [{}].",
                    sessionOptions.brokerUri(),
                    getEnvPath(),
                    waitingTime);

            init(waitingTime);

            logger.info("Broker started on {} with pid [{}].", sessionOptions.brokerUri(), pid);
        } catch (Exception e) {
            logger.error(
                    "Failed to start BMQ broker on port '{}'",
                    sessionOptions.brokerUri().getPort());
            throw new RuntimeException(e);
        }

        // For standalone BMQ broker default tier should be the 'lcl-{hostname}'
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
            logger.error("Failed to stop BMQ broker", e);
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
                    "Dump BMQ Broker output:\n{}",
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
