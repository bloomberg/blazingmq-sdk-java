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
import java.util.Arrays;
import javax.annotation.concurrent.Immutable;

/**
 * A value-semantic type representing authentication credentials for a broker session.
 *
 * <p>An {@code AuthnCredential} consists of an authentication mechanism name and optional
 * authentication data. It is used to configure credentials on a {@link SessionOptions} for
 * authenticating with the broker during session initiation.
 *
 * <H2>Thread Safety</H2>
 *
 * Once created, an {@code AuthnCredential} object is immutable and thread safe. A {@code Builder}
 * is provided for construction.
 *
 * <H2>Usage Example</H2>
 *
 * <pre>
 *
 * AuthnCredential credential = AuthnCredential.builder()
 *                                             .setMechanism("OAUTH2")
 *                                             .setData(tokenBytes)
 *                                             .build();
 * </pre>
 */
@Immutable
public final class AuthnCredential {

    private final String mechanism;
    private final byte[] data;

    private AuthnCredential() {
        mechanism = "";
        data = null;
    }

    private AuthnCredential(Builder builder) {
        mechanism = builder.mechanism;
        data = builder.data != null ? Arrays.copyOf(builder.data, builder.data.length) : null;
    }

    /**
     * Creates an {@code AuthnCredential} with default values (empty mechanism, no data).
     *
     * @return AuthnCredential credential with default values
     */
    public static AuthnCredential createDefault() {
        return new AuthnCredential();
    }

    /**
     * Returns a helper class object to create an immutable {@code AuthnCredential} with custom
     * settings.
     *
     * @return Builder a helper class object to create an immutable {@code AuthnCredential}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns a helper class object initialized with values from the specified {@code credential}.
     *
     * @param credential specifies credential which initializes the builder
     * @return Builder a helper class object to create an immutable {@code AuthnCredential}
     */
    public static Builder builder(AuthnCredential credential) {
        Argument.expectNonNull(credential, "credential");
        return new Builder(credential);
    }

    /**
     * Returns the authentication mechanism.
     *
     * @return String authentication mechanism
     */
    public String mechanism() {
        return mechanism;
    }

    /**
     * Returns the authentication data, or null if not set.
     *
     * @return byte array of authentication data, or null
     */
    public byte[] data() {
        return data != null ? Arrays.copyOf(data, data.length) : null;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (!(obj instanceof AuthnCredential)) return false;
        AuthnCredential other = (AuthnCredential) obj;
        return mechanism.equals(other.mechanism) && Arrays.equals(data, other.data);
    }

    @Override
    public int hashCode() {
        int result = mechanism.hashCode();
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ mechanism: ")
                .append(mechanism)
                .append(", data: ")
                .append(data != null ? "<" + data.length + " bytes>" : "null")
                .append(" ]");
        return sb.toString();
    }

    /** A helper class to create an immutable {@code AuthnCredential} with custom settings. */
    public static class Builder {

        private String mechanism;
        private byte[] data;

        private Builder() {
            mechanism = "";
            data = null;
        }

        private Builder(AuthnCredential credential) {
            mechanism = credential.mechanism;
            data =
                    credential.data != null
                            ? Arrays.copyOf(credential.data, credential.data.length)
                            : null;
        }

        /**
         * Creates an {@code AuthnCredential} object based on this builder's properties.
         *
         * @return AuthnCredential immutable credential object
         */
        public AuthnCredential build() {
            return new AuthnCredential(this);
        }

        /**
         * Sets the authentication mechanism.
         *
         * @param value authentication mechanism
         * @return Builder this object
         * @throws NullPointerException if the specified value is null
         */
        public Builder setMechanism(String value) {
            mechanism = Argument.expectNonNull(value, "mechanism");
            return this;
        }

        /**
         * Sets the authentication data.
         *
         * @param value authentication data, may be null
         * @return Builder this object
         */
        public Builder setData(byte[] value) {
            data = value != null ? Arrays.copyOf(value, value.length) : null;
            return this;
        }
    }
}
