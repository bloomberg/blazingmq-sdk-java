/*
 * Copyright 2026 Bloomberg Finance L.P.
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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Loads and modifies BlazingMQ broker configuration for CI testing.
 *
 * <p>Supports modifying:
 *
 * <ul>
 *   <li>bmqbrkrcfg.json: log file path, stats file path, TCP port
 *   <li>clusters.json: node endpoint port, storage locations
 * </ul>
 */
public class BmqBrokerConfiguration {

    private static final String BROKER_CONFIG_FILE = "bmqbrkrcfg.json";
    private static final String CLUSTERS_CONFIG_FILE = "clusters.json";
    private static final String DOMAINS_DIR = "domains";

    private final Gson gson = new Gson();
    private final Gson prettyGson = new GsonBuilder().setPrettyPrinting().create();

    private final Path configPath;
    private JsonObject brokerConfigJson;
    private JsonObject clustersConfigJson;

    public static BmqBrokerConfiguration createFromDefaultPath() throws IOException {
        // Find config directory relative to this class's location
        Path classPath =
                Paths.get(
                        BmqBrokerConfiguration.class
                                .getProtectionDomain()
                                .getCodeSource()
                                .getLocation()
                                .getPath());
        // Navigate from target/test-classes to src/test/docker/config
        Path configPath = classPath.getParent().getParent().resolve("src/test/docker/config");
        return new BmqBrokerConfiguration(configPath);
    }

    public static BmqBrokerConfiguration createFromPath(Path configPath) throws IOException {
        return new BmqBrokerConfiguration(configPath);
    }

    private BmqBrokerConfiguration(Path configPath) throws IOException {
        this.configPath = configPath;

        Path brokerConfigPath = configPath.resolve(BROKER_CONFIG_FILE);
        brokerConfigJson =
                gson.fromJson(
                        new String(Files.readAllBytes(brokerConfigPath), StandardCharsets.UTF_8),
                        JsonObject.class);

        Path clustersConfigPath = configPath.resolve(CLUSTERS_CONFIG_FILE);
        clustersConfigJson =
                gson.fromJson(
                        new String(Files.readAllBytes(clustersConfigPath), StandardCharsets.UTF_8),
                        JsonObject.class);
    }

    // ==================== bmqbrkrcfg.json accessors ====================

    /** taskConfig/logController/fileName */
    public String getLogFileName() {
        return brokerConfigJson
                .getAsJsonObject("taskConfig")
                .getAsJsonObject("logController")
                .get("fileName")
                .getAsString();
    }

    public void setLogFileName(String fileName) {
        brokerConfigJson
                .getAsJsonObject("taskConfig")
                .getAsJsonObject("logController")
                .addProperty("fileName", fileName);
    }

    /** appConfig/stats/printer/file */
    public String getStatsFile() {
        return brokerConfigJson
                .getAsJsonObject("appConfig")
                .getAsJsonObject("stats")
                .getAsJsonObject("printer")
                .get("file")
                .getAsString();
    }

    public void setStatsFile(String file) {
        brokerConfigJson
                .getAsJsonObject("appConfig")
                .getAsJsonObject("stats")
                .getAsJsonObject("printer")
                .addProperty("file", file);
    }

    /** appConfig/networkInterfaces/tcpInterface/port */
    public int getTcpPort() {
        return brokerConfigJson
                .getAsJsonObject("appConfig")
                .getAsJsonObject("networkInterfaces")
                .getAsJsonObject("tcpInterface")
                .get("port")
                .getAsInt();
    }

    public void setTcpPort(int port) {
        brokerConfigJson
                .getAsJsonObject("appConfig")
                .getAsJsonObject("networkInterfaces")
                .getAsJsonObject("tcpInterface")
                .addProperty("port", port);
    }

    // ==================== clusters.json accessors ====================

    private JsonObject getFirstCluster() {
        return clustersConfigJson.getAsJsonArray("myClusters").get(0).getAsJsonObject();
    }

    private JsonObject getFirstNode() {
        return getFirstCluster().getAsJsonArray("nodes").get(0).getAsJsonObject();
    }

    /** myClusters[0]/nodes[0]/transport/tcp/endpoint */
    public String getNodeEndpoint() {
        return getFirstNode()
                .getAsJsonObject("transport")
                .getAsJsonObject("tcp")
                .get("endpoint")
                .getAsString();
    }

    public void setNodeEndpoint(String endpoint) {
        getFirstNode()
                .getAsJsonObject("transport")
                .getAsJsonObject("tcp")
                .addProperty("endpoint", endpoint);
    }

    /** Updates the port in the node endpoint (tcp://localhost:PORT) */
    public void setNodeEndpointPort(int port) {
        String endpoint = getNodeEndpoint();
        String newEndpoint = endpoint.replaceFirst(":\\d+$", ":" + port);
        setNodeEndpoint(newEndpoint);
    }

    /** myClusters[0]/partitionConfig/location */
    public String getPartitionLocation() {
        return getFirstCluster().getAsJsonObject("partitionConfig").get("location").getAsString();
    }

    public void setPartitionLocation(String location) {
        getFirstCluster().getAsJsonObject("partitionConfig").addProperty("location", location);
    }

    /** myClusters[0]/partitionConfig/archiveLocation */
    public String getArchiveLocation() {
        return getFirstCluster()
                .getAsJsonObject("partitionConfig")
                .get("archiveLocation")
                .getAsString();
    }

    public void setArchiveLocation(String location) {
        getFirstCluster()
                .getAsJsonObject("partitionConfig")
                .addProperty("archiveLocation", location);
    }

    // ==================== Save ====================

    /**
     * Saves configuration files to the specified directory, including domains.
     *
     * @param outputPath directory to save bmqbrkrcfg.json, clusters.json, and domains/
     */
    public void saveTo(Path outputPath) throws IOException {
        Files.createDirectories(outputPath);

        Files.write(
                outputPath.resolve(BROKER_CONFIG_FILE),
                prettyGson.toJson(brokerConfigJson).getBytes(StandardCharsets.UTF_8));

        Files.write(
                outputPath.resolve(CLUSTERS_CONFIG_FILE),
                prettyGson.toJson(clustersConfigJson).getBytes(StandardCharsets.UTF_8));

        // Copy domains directory
        Path srcDomains = configPath.resolve(DOMAINS_DIR);
        if (Files.isDirectory(srcDomains)) {
            Path destDomains = outputPath.resolve(DOMAINS_DIR);
            Files.createDirectories(destDomains);
            Files.list(srcDomains)
                    .filter(p -> p.toString().endsWith(".json"))
                    .forEach(
                            src -> {
                                try {
                                    Files.copy(src, destDomains.resolve(src.getFileName()));
                                } catch (IOException e) {
                                    throw new RuntimeException("Failed to copy domain: " + src, e);
                                }
                            });
        }
    }
}
