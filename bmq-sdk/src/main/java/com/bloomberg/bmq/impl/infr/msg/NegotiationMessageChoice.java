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

public class NegotiationMessageChoice {

    private ClientIdentity clientIdentity;
    private BrokerResponse brokerResponse;

    public NegotiationMessageChoice() {
        init();
    }

    public Object createNewInstance() {
        return new NegotiationMessageChoice();
    }

    public final void reset() {
        init();
    }

    public final void makeClientIdentity() {
        reset();
        clientIdentity = new ClientIdentity();
    }

    public final void makeBrokerResponse() {
        reset();
        brokerResponse = new BrokerResponse();
    }

    public final boolean isClientIdentityValue() {
        return clientIdentity != null;
    }

    public final boolean isBrokerResponseValue() {
        return brokerResponse != null;
    }

    public final ClientIdentity clientIdentity() {
        return clientIdentity;
    }

    public final BrokerResponse brokerResponse() {
        return brokerResponse;
    }

    public final void init() {
        clientIdentity = null;
        brokerResponse = null;
    }

    public final void reset(NegotiationMessageChoice copied) {
        clientIdentity = copied.clientIdentity;
        brokerResponse = copied.brokerResponse;
    }

    @Override
    public String toString() {
        return new com.google.gson.Gson().toJson(this);
    }

    public Boolean isEmpty() {
        return clientIdentity == null && brokerResponse == null;
    }
}
