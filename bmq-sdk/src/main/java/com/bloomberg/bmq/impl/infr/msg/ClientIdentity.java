/*
 * Copyright 2022-2025 Bloomberg Finance L.P.
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
package com.bloomberg.bmq.impl.infr.msg;

public class ClientIdentity {
    private Integer protocolVersion;
    private Integer sdkVersion;
    private ClientType clientType;
    private String processName;
    private Integer pid;
    private Integer sessionId;
    private String hostName;
    private String features;
    private String clusterName;
    private Integer clusterNodeId;
    private ClientLanguage sdkLanguage;
    private String userAgent;

    public ClientIdentity() {
        init();
    }

    public void reset() {
        init();
    }

    private void init() {
        protocolVersion = 0;
        sdkVersion = 0;
        clientType = ClientType.E_UNKNOWN;
        processName = "";
        pid = 0;
        sessionId = 0;
        hostName = "";
        features = "";
        clusterName = "";
        clusterNodeId = -1;
        sdkLanguage = ClientLanguage.E_UNKNOWN;
        userAgent = "";
    }

    public Integer protocolVersion() {
        return protocolVersion;
    }

    public void setProtocolversion(Integer value) {
        protocolVersion = value;
    }

    public Integer sdkVersion() {
        return sdkVersion;
    }

    public void setSdkVersion(Integer value) {
        sdkVersion = value;
    }

    public ClientType clientType() {
        return clientType;
    }

    public void setClientType(ClientType value) {
        clientType = value;
    }

    public String processName() {
        return processName;
    }

    public void setProcessName(String value) {
        processName = value;
    }

    public Integer pid() {
        return pid;
    }

    public void setPid(Integer value) {
        pid = value;
    }

    public Integer sessionId() {
        return sessionId;
    }

    public void setSessionId(Integer value) {
        sessionId = value;
    }

    public String hostName() {
        return hostName;
    }

    public void setHostName(String value) {
        hostName = value;
    }

    public String features() {
        return features;
    }

    public void setFeatures(String value) {
        features = value;
    }

    public String clusterName() {
        return clusterName;
    }

    public void setClusterName(String value) {
        clusterName = value;
    }

    public Integer clusterNodeId() {
        return clusterNodeId;
    }

    public void setClusterNodeId(Integer value) {
        clusterNodeId = value;
    }

    public ClientLanguage sdkLanguage() {
        return sdkLanguage;
    }

    public void setSdkLanguage(ClientLanguage value) {
        sdkLanguage = value;
    }

    public String userAgent() {
        return userAgent;
    }

    public void setUserAgent(String value) {
        userAgent = value;
    }

    public Object createNewInstance() {
        return new ClientIdentity();
    }
}
