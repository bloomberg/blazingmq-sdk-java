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
package com.bloomberg.bmq.impl.infr.msg;

public class BrokerResponse implements ProtocolResponse<ClientIdentity> {

    private Status result;
    private Integer protocolVersion;
    private Integer brokerVersion;
    private boolean isDeprecatedSdk;
    private ClientIdentity brokerIdentity;

    public BrokerResponse() {
        init();
    }

    public Object createNewInstance() {
        return new BrokerResponse();
    }

    public final void reset() {
        init();
    }

    public final Status result() {
        return result;
    }

    public void setResult(Status value) {
        result = value;
    }

    public Integer protocolVersion() {
        return protocolVersion;
    }

    public void setProtocolversion(Integer value) {
        protocolVersion = value;
    }

    public Integer brokerVersion() {
        return brokerVersion;
    }

    public void setBrokerVersion(Integer value) {
        brokerVersion = value;
    }

    public boolean isDeprecatedSdk() {
        return isDeprecatedSdk;
    }

    public void setIsDeprecatedSdk(boolean value) {
        isDeprecatedSdk = value;
    }

    @Override
    public ClientIdentity getOriginalRequest() {
        return brokerIdentity;
    }

    @Override
    public void setOriginalRequest(ClientIdentity clientIdentity) {
        brokerIdentity = clientIdentity;
    }

    public final void init() {
        result = new Status();
        isDeprecatedSdk = true;
        brokerIdentity = new ClientIdentity();
    }
}
