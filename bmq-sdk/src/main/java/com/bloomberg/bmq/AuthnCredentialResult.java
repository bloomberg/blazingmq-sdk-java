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
package com.bloomberg.bmq;

import com.bloomberg.bmq.impl.infr.util.Argument;

/**
 * Represents the result of an authentication credential callback, containing either a credential on
 * success or an error message on failure.
 */
public final class AuthnCredentialResult {

    private final AuthnCredential credential;
    private final String error;

    private AuthnCredentialResult(AuthnCredential credential, String error) {
        this.credential = credential;
        this.error = error;
    }

    /**
     * Creates a successful result with the specified mechanism and data.
     *
     * @param mechanism the authentication mechanism
     * @param data the authentication data
     * @return a successful result
     * @throws NullPointerException if mechanism is null
     */
    public static AuthnCredentialResult success(String mechanism, byte[] data) {
        return new AuthnCredentialResult(
                AuthnCredential.builder().setMechanism(mechanism).setData(data).build(), null);
    }

    /**
     * Creates a failed result containing the specified error message.
     *
     * @param message the error message describing why credentials could not be obtained
     * @return a failed result
     * @throws NullPointerException if message is null
     */
    public static AuthnCredentialResult error(String message) {
        Argument.expectNonNull(message, "message");
        return new AuthnCredentialResult(null, message);
    }

    /**
     * Returns true if this result contains a credential.
     *
     * @return true on success, false on error
     */
    public boolean isSuccess() {
        return credential != null;
    }

    /**
     * Returns the credential, or null if this is an error result.
     *
     * @return the credential, or null
     */
    public AuthnCredential credential() {
        return credential;
    }

    /**
     * Returns the error message, or null if this is a success result.
     *
     * @return the error message, or null
     */
    public String error() {
        return error;
    }
}
