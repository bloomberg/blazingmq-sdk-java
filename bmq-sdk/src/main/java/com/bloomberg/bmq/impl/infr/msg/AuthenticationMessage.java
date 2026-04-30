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
package com.bloomberg.bmq.impl.infr.msg;

public class AuthenticationMessage {

    private AuthenticationRequest authenticationRequest;
    private AuthenticationResponse authenticationResponse;

    public AuthenticationMessage() {
        init();
    }

    public Object createNewInstance() {
        return new AuthenticationMessage();
    }

    public final void reset() {
        init();
    }

    public final void makeAuthenticationRequest(String mechanism, String data) {
        reset();
        authenticationRequest = new AuthenticationRequest(mechanism, data);
    }

    public final boolean isAuthenticationRequestValue() {
        return authenticationRequest != null;
    }

    public final boolean isAuthenticationResponseValue() {
        return authenticationResponse != null;
    }

    public final AuthenticationRequest authenticationRequest() {
        return authenticationRequest;
    }

    public final AuthenticationResponse authenticationResponse() {
        return authenticationResponse;
    }

    public void init() {
        authenticationRequest = null;
        authenticationResponse = null;
    }

    public final void reset(AuthenticationMessage copied) {
        authenticationRequest = copied.authenticationRequest;
        authenticationResponse = copied.authenticationResponse;
    }

    @Override
    public String toString() {
        return new com.google.gson.Gson().toJson(this);
    }

    public Boolean isEmpty() {
        return authenticationRequest == null && authenticationResponse == null;
    }
}
