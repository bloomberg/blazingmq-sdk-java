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
package com.bloomberg.bmq.impl.infr.net;

import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.SessionOptions.WriteBufferWaterMark;
import com.bloomberg.bmq.impl.infr.util.Argument;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConnectionOptions {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final URI k_DEFAULT_URI = URI.create("tcp://localhost:30114");

    private static final Duration DEFAULT_START_ATTEMPT_TIMEOUT = Duration.ofSeconds(15);
    private static final int DEFAULT_START_NUM_RETRIES = Integer.MAX_VALUE;
    private static final Duration DEFAULT_START_RETRY_INTERVAL = Duration.ofSeconds(5);

    private URI brokerUri = k_DEFAULT_URI;
    private Duration startAttemptTimeout = DEFAULT_START_ATTEMPT_TIMEOUT;
    private int startNumRetries = DEFAULT_START_NUM_RETRIES;
    private Duration startRetryInterval = DEFAULT_START_RETRY_INTERVAL;
    private WriteBufferWaterMark writeWaterMark = new WriteBufferWaterMark();
    private String userAgentPrefix = "";

    public ConnectionOptions() {}

    public ConnectionOptions(SessionOptions sesOpts) {
        brokerUri = sesOpts.brokerUri();
        writeWaterMark = sesOpts.writeBufferWaterMark();
        userAgentPrefix = sesOpts.userAgentPrefix();
    }

    public ConnectionOptions setBrokerUri(URI value) {
        Argument.expectNonNull(value.getHost(), "host");
        Argument.expectPositive(value.getPort(), "port");
        brokerUri = value;
        return this;
    }

    public ConnectionOptions setStartAttemptTimeout(Duration value) {
        Argument.expectCondition(
                !(value.isNegative() || value.isZero()), "'startAttemptTimeout' must be positive");
        startAttemptTimeout = value;
        return this;
    }

    public ConnectionOptions setStartNumRetries(int value) {
        startNumRetries = Argument.expectNonNegative(value, "startNumRetries");
        return this;
    }

    public ConnectionOptions setStartRetryInterval(Duration value) {
        Argument.expectCondition(
                !(value.isNegative() || value.isZero()), "'startRetryInterval' must be positive");
        startRetryInterval = value;
        return this;
    }

    public ConnectionOptions setWriteBufferWaterMark(WriteBufferWaterMark value) {
        Argument.expectPositive(value.lowWaterMark(), "low water mark");
        Argument.expectPositive(value.highWaterMark(), "high water mark");
        writeWaterMark = value;
        return this;
    }

    public ConnectionOptions setUserAgentPrefix(String value) {
        Argument.expectNonNull(value, "user agent prefix");
        Argument.expectCondition(
                value.codePoints().allMatch(c -> c < 128 && !Character.isISOControl(c)),
                "user agent prefix must be printable ASCII");
        Argument.expectCondition(
                value.length() < 128, "user agent prefix must be shorter than 128 characters");
        userAgentPrefix = value;
        return this;
    }

    public URI brokerUri() {
        return brokerUri;
    }

    public Duration startAttemptTimeout() {
        return startAttemptTimeout;
    }

    public int startNumRetries() {
        return startNumRetries;
    }

    public Duration startRetryInterval() {
        return startRetryInterval;
    }

    public WriteBufferWaterMark writeBufferWaterMark() {
        return writeWaterMark;
    }

    public String userAgentPrefix() {
        return userAgentPrefix;
    }
}
