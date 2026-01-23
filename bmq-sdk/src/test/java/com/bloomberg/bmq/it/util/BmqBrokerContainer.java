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

import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BmqBrokerContainer implements BmqBroker {

    private static final Logger logger =
            LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String CONTAINER_TMP_LOGS = "/tmp/logs";
    private static final String IMAGE_NAME = "bmq-broker-java-it";
    private static final String OUTPUT_FILENAME = "output.log";
    private static final long MAX_CONTAINER_WAIT_TIME_MS = 5000;
    private static final long CONTAINER_HEALTH_CHECK_DT_MS = 100;

    private final SessionOptions sessionOptions;
    private final DockerClient client;
    private final String containerName;
    private final String containerId;
    private final Path tmpFolder;
    private final File output;

    private boolean isStarted = false;
    private boolean dropTmpFolder = false;
    private boolean dumpBrokerOutput = false;
    private String defaultTier;

    public static BmqBrokerContainer createContainer(int port) throws IOException {
        Argument.expectPositive(port, "port");

        DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder().build();
        DockerHttpClient httpClient =
                new ApacheDockerHttpClient.Builder()
                        .dockerHost(config.getDockerHost())
                        .sslConfig(config.getSSLConfig())
                        .maxConnections(100)
                        .connectionTimeout(Duration.ofSeconds(30))
                        .responseTimeout(Duration.ofSeconds(45))
                        .build();

        final URI uri = URI.create("tcp://localhost:" + port);
        final SessionOptions opts = SessionOptions.builder().setBrokerUri(uri).build();

        // hashCode() may be not unique across different jvm forks
        // so use port number here
        final String name = IMAGE_NAME + opts.brokerUri().getPort();

        logger.info("Create '{}' container", name);

        final Path tmpDir = makeTempDir(name);
        final Path logsPath = tmpDir.resolve("logs");

        logger.info("Use '{}' directory for broker logs", logsPath);

        final PortBinding portBinding =
                PortBinding.parse(opts.brokerUri().getPort() + ":" + BROKER_DEFAULT_PORT);
        final DockerClient client = DockerClientImpl.getInstance(config, httpClient);

        final HostConfig hostConfig =
                new HostConfig()
                        .withPortBindings(portBinding)
                        .withBinds(Bind.parse(logsPath + ":" + CONTAINER_TMP_LOGS));

        final String id =
                client.createContainerCmd(IMAGE_NAME)
                        .withName(name)
                        .withHostConfig(hostConfig)
                        .exec()
                        .getId();

        return new BmqBrokerContainer(opts, client, name, id, tmpDir);
    }

    private static Path makeTempDir(String containerName) throws IOException {
        // BlazingMQ brokers running in the docker containers create storage files in the
        // /tmp directory in the container, which is bit of a black box.  This can
        // be undesirable since it does not simulate the "real world" scenario as
        // far as filesystem is concerned. Therefore, we try to map '/tmp'
        // inside the container to a directory on the host, which is created
        // based on these rules:
        //  1. If `BMQ_DOCKER_TMPDIR` env var is set and that directory exists,
        //     create temp directory there
        //  2. Else, if `/bb/data/tmp` exists, create temp directory there
        //  3. Else, create temp directory in tmp location (/tmp/bmq-broker)

        Path tempDir;
        // Check `BMQ_DOCKER_TMPDIR` first
        Path basePath = getBmqDockerTmp();

        // If null, then check `/bb/data/tmp` location
        if (basePath == null) {
            basePath = getBbDataTmp();
        }

        // If null, then use tmp directory in tmp location (/tmp/bmq-broker)
        if (basePath == null) {
            basePath = Paths.get("/tmp/bmq-broker");
            logger.info("Use '/tmp/bmq-broker' location");
        }

        Files.createDirectories(
                basePath,
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x")));

        tempDir =
                Files.createTempDirectory(
                        basePath, // base path
                        containerName + "_", // directory name prefix
                        PosixFilePermissions.asFileAttribute(
                                PosixFilePermissions.fromString("rwxr-xr-x")));

        return tempDir;
    }

    private static Path getBmqDockerTmp() {
        String bmqDockerTmp = System.getenv("BMQ_DOCKER_TMPDIR");
        if (bmqDockerTmp == null || bmqDockerTmp.isEmpty()) {
            return null;
        }

        logger.info("Found BMQ_DOCKER_TMPDIR ('{}') env variable", bmqDockerTmp);
        return Paths.get(bmqDockerTmp);
    }

    private static Path getBbDataTmp() {
        Path bbDataTmpPath = Paths.get("/bb/data/tmp");
        if (Files.isWritable(bbDataTmpPath)) {
            logger.info("Have write access to '/bb/data/tmp' location");
            return bbDataTmpPath;
        }

        return null;
    }

    private BmqBrokerContainer(
            SessionOptions so, DockerClient dockerClient, String name, String id, Path tmp) {
        sessionOptions = so;
        client = dockerClient;
        containerName = name;
        containerId = id;
        tmpFolder = tmp;
        output = tmpFolder.resolve(OUTPUT_FILENAME).toFile();
    }

    @Override
    public void start() {
        logger.info("Starting container '{}'...", containerName);
        client.startContainerCmd(containerId).exec();

        try {
            for (long totalTimeMs = 0;
                    totalTimeMs < MAX_CONTAINER_WAIT_TIME_MS;
                    totalTimeMs += CONTAINER_HEALTH_CHECK_DT_MS) {
                Thread.sleep(CONTAINER_HEALTH_CHECK_DT_MS);

                InspectContainerResponse resp = client.inspectContainerCmd(containerId).exec();
                if (!resp.getState().getRunning()) {
                    logger.error(
                            "Container '{}' is not running, status = '{}'",
                            containerId,
                            resp.getState().getStatus());
                    throw new RuntimeException(
                            String.format("Failed to start container '{}'", containerId));
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        logger.info("Container '{}' is running", containerId);

        // For BlazingMQ broker running in container default tier should be the 'lcl-{guest
        // hostname}'
        defaultTier = "lcl-" + getHostname();
        isStarted = true;
    }

    @Override
    public void stop() {
        if (!isStarted) {
            return;
        }

        // Files and directories created by containers are owned by root user.
        // In order to be able to read and delete them later, we need to
        // add read and write permissions.
        // Since we map container logs directory, we need to change its permissions
        logger.info(
                "Change '{}' directory permissions (recursively) of '{}' container",
                CONTAINER_TMP_LOGS,
                containerName);

        final String execId =
                client.execCreateCmd(containerId)
                        .withCmd("/bin/bash", "-c", "chmod a+rw -R " + CONTAINER_TMP_LOGS)
                        .withAttachStdout(true)
                        .withAttachStderr(true)
                        .exec()
                        .getId();

        ResultCallback.Adapter<Frame> resulCallback =
                new ResultCallback.Adapter<Frame>() {
                    @Override
                    public void onNext(Frame item) {
                        logger.info(item.toString());
                    }
                };

        try {
            client.execStartCmd(execId).exec(resulCallback).awaitCompletion();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        // Stop the broker
        logger.info("Gracefully stop the broker in '{}' container", containerName);
        client.killContainerCmd(containerId).withSignal("SIGINT").exec();

        // Stop the container
        logger.info("Stop '{}' container", containerName);
        client.stopContainerCmd(containerId).withTimeout(20).exec();

        isStarted = false;
    }

    @Override
    public void enableRead() {
        throw new UnsupportedOperationException(
                "'enableRead' not supported for bmqbrkr-based server.");
    }

    @Override
    public void disableRead() {
        throw new UnsupportedOperationException(
                "'disableRead' not supported for bmqbrkr-based server.");
    }

    @Override
    public boolean isOldStyleMessageProperties() {
        return false;
    }

    @Override
    public SessionOptions sessionOptions() {
        return sessionOptions;
    }

    @Override
    public String defaultTier() {
        return defaultTier;
    }

    @Override
    public void setDropTmpFolder() {
        logger.info("Set to drop tmp folder for '{}' container", containerName);
        dropTmpFolder = true;
    }

    @Override
    public void setDumpBrokerOutput() {
        logger.info("Set to dump broker output for '{}' container", containerName);
        dumpBrokerOutput = true;
    }

    @Override
    public String toString() {
        return "BmqBrokerContainer [" + containerName + " on " + sessionOptions.brokerUri() + "]";
    }

    @Override
    public void close() throws IOException {
        stop();
        saveContainerOutput();
        if (dumpBrokerOutput) {
            logger.info(
                    "Dump BlazingMQ Broker output for '{}' container:\n{}",
                    containerName,
                    new String(Files.readAllBytes(output.toPath())));
        }

        if (dropTmpFolder) {
            logger.info("Drop '{}' tmp folder for '{}' container", tmpFolder, containerName);
            FileUtils.deleteDirectory(tmpFolder.toFile());
        }

        logger.info("Remove '{}' container", containerName);
        client.removeContainerCmd(containerId).withForce(true).exec();
    }

    private void saveContainerOutput() throws IOException {
        logger.info("Save '{}' container output to '{}' file", containerName, output);

        try (BufferedWriter out = new BufferedWriter(new FileWriter(output, true))) {
            ResultCallback.Adapter<Frame> resultCallback =
                    new ResultCallback.Adapter<Frame>() {
                        @Override
                        public void onNext(Frame item) {
                            try {
                                out.append(item.toString());
                                out.newLine();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    };

            client.logContainerCmd(containerId)
                    .withStdOut(true)
                    .withStdErr(true)
                    .exec(resultCallback)
                    .awaitCompletion();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String getHostname() {
        // Get container hostname
        logger.info("Get '{}' container hostname", containerName);
        final String hostname =
                client.inspectContainerCmd(containerId).exec().getConfig().getHostName();
        logger.info("'{}' container has '{}' hostname", containerName, hostname);

        return hostname;
    }
}
