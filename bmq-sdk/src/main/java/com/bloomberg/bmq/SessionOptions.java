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
package com.bloomberg.bmq;

import com.bloomberg.bmq.impl.infr.util.Argument;
import java.net.URI;
import java.time.Duration;
import javax.annotation.concurrent.Immutable;

/**
 * A value-semantic type to configure session with the broker.
 *
 * <p>Provides a value-semantic type which is used to specify session-level configuration
 * parameters.
 *
 * <p>Most applications should not need to change the parameters for creating a {@link
 * com.bloomberg.bmq.AbstractSession}; the default parameters are fine.
 *
 * <p>The following parameters are supported:
 *
 * <ul>
 *   <li>{@code brokerUri}: Address of the broker to connect to. Default is to connect to the broker
 *       on the local host on the default port (30114). The format is {@code tcp://<host>:port}.
 *       Host can be:
 *       <ul>
 *         <li>an explicit hostname, example {@code foobar} (or {@code localhost})
 *         <li>an ip, example {@code 10.11.12.13}
 *         <li>a DNS entry. In this case, the client will resolve the list of addresses from that
 *             entry, and try to connect to one of them. When the connection with the host goes
 *             down, it will automatically immediately failover and reconnects to another entry from
 *             the address list.
 *       </ul>
 *   <li>{@code WriteBufferWaterMark}: Sizes (in bytes) to use for write cache high and low
 *       watermarks on the channel. Default value for high watermark is 64MB. Note that BlazingMQ
 *       reserves 4MB of this value for control message, so the actual watermark for data published
 *       is 'HighWatermark - 4MB'. Default value for low watermark is 1MB.
 *   <li>{@code statsDumpInterval}: Interval (in seconds) at which to dump stats in the logs. Set to
 *       0 to disable recurring dump of stats (final stats are always dumped at end of session).
 *       Default is 5min.
 *   <li>Default timeouts to use for various operations:
 *       <ul>
 *         <li>{@code startTimeout},
 *         <li>{@code stopTimeout},
 *         <li>{@code openQueueTimeout},
 *         <li>{@code configureQueueTimeout},
 *         <li>{@code closeQueueTimeout}.
 *       </ul>
 *   <li>{@code hostHealthMonitor}: Optional instance of a class implementing {@code
 *       HostHealthMonitor} interface responsible for notifying the session when the health of the
 *       host machine has changed. A {@code hostHealthMonitor} must be specified in oder for queues
 *       opened through the session to suspend on unhealthy hosts.
 *   <li>{@code userAgentPrefix}: String to include in the user agent for broker telemetry. This
 *       string must only contain printable characters and must be less than 128 characters long.
 *       This is provided for libraries that are wrapping this SDK. Applications directly using the
 *       SDK are encouraged <strong>NOT</strong> to set this value.
 * </ul>
 *
 * <H2>Thread Safety</H2>
 *
 * Once created {@code SessionOptions} object is immutable and thread safe. Helper class {@code
 * Builder} is provided for {@code SessionOptions} creation.
 *
 * <H2>Usage Example 1</H2>
 *
 * Create {@code SessionOptions} object with default values:
 *
 * <pre>
 *
 * SessionOptions opts = SessionOptions.createDefault();
 * </pre>
 *
 * <H2>Usage Example 2</H2>
 *
 * Create {@code SessionOptions} object with custom values:
 *
 * <pre>
 *
 * SessionOptions opts = SessionOptions.builder()
 *                                     .setBrokerUri(URI.create("tcp://localhost:" + 12345))
 *                                     .build();
 * </pre>
 */
@Immutable
public final class SessionOptions {

    /**
     * Immutable helper class to set sizes (in bytes) for write cache high and low watermarks on the
     * channel. Default value for high watermark is 64MB. Default value for low watermark is 1MB.
     */
    @Immutable
    public static class WriteBufferWaterMark {

        private static final int DEFAULT_LWM = 1024 * 1024; //  1MB
        private static final int DEFAULT_HWM = 64 * 1024 * 1024; // 64MB

        private final int lwm;
        private final int hwm;

        /** Creates this class object with default high and low watermark values. */
        public WriteBufferWaterMark() {
            this(DEFAULT_LWM, DEFAULT_HWM);
        }

        /**
         * Creates this class object with specified high and low watermark values.
         *
         * @param lowWaterMark zero or positive integer value for low watermark
         * @param highWaterMark positive integer value for high watermark
         * @throws IllegalArgumentException in case of either LWM is negative or HWM isn't greater
         *     than zero
         */
        public WriteBufferWaterMark(int lowWaterMark, int highWaterMark) {
            lwm = Argument.expectNonNegative(lowWaterMark, "lowWaterMark");
            hwm = Argument.expectPositive(highWaterMark, "highWaterMark");
        }

        /**
         * Returns LWM value.
         *
         * @return int LWM
         */
        public int lowWaterMark() {
            return lwm;
        }

        /**
         * Returns HWM value.
         *
         * @return int HWM
         */
        public int highWaterMark() {
            return hwm;
        }
    }

    /**
     * Immutable helper class to set sizes (in number of events) for the inbound event buffer high
     * and low watermarks. Default value for high watermark is 1000 events. Default value for low
     * watermark is 500 events.
     */
    @Immutable
    public static class InboundEventBufferWaterMark {

        private static final int DEFAULT_BUFFER_LWM = 500; // num of events
        private static final int DEFAULT_BUFFER_HWM = 1000; // num of events

        private final int bufferLwm;
        private final int bufferHwm;

        /** Creates this class object with default high and low watermark values. */
        public InboundEventBufferWaterMark() {
            this(DEFAULT_BUFFER_LWM, DEFAULT_BUFFER_HWM);
        }

        /**
         * Creates this class object with specified high and low watermark values.
         *
         * @param lowWaterMark zero or positive integer value for low watermark
         * @param highWaterMark positive integer value for high watermark
         * @throws IllegalArgumentException in case of either LWM is negative or HWM isn't greater
         *     than zero
         */
        public InboundEventBufferWaterMark(int lowWaterMark, int highWaterMark) {
            bufferLwm = Argument.expectNonNegative(lowWaterMark, "lowWaterMark");
            bufferHwm = Argument.expectPositive(highWaterMark, "highWaterMark");
            Argument.expectCondition(
                    lowWaterMark < highWaterMark,
                    "'highWaterMark' must be greater than 'lowWaterMark'");
        }

        /**
         * Returns LWM value.
         *
         * @return int LWM
         */
        public int lowWaterMark() {
            return bufferLwm;
        }

        /**
         * Returns HWM value.
         *
         * @return int HWM
         */
        public int highWaterMark() {
            return bufferHwm;
        }
    }

    private static final URI DEFAULT_URI = URI.create("tcp://localhost:30114");
    private static final Duration DEFAULT_START_TIMEOUT = Duration.ofSeconds(65);
    private static final Duration DEFAULT_STOP_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration DEFAULT_STATS_DUMP_INTERVAL = Duration.ofMinutes(5);
    private static final Duration QUEUE_OPERATION_TIMEOUT = Duration.ofMinutes(3);

    private final URI brokerUri;
    private final Duration startTimeout;
    private final Duration stopTimeout;
    private final WriteBufferWaterMark writeWaterMark;
    private final InboundEventBufferWaterMark inboundWaterMark;

    private final Duration statsDumpInterval;
    private final Duration openQueueTimeout;
    private final Duration configureQueueTimeout;
    private final Duration closeQueueTimeout;

    private final HostHealthMonitor hostHealthMonitor;

    private final String userAgentPrefix;

    private SessionOptions() {
        brokerUri = DEFAULT_URI;
        startTimeout = DEFAULT_START_TIMEOUT;
        stopTimeout = DEFAULT_STOP_TIMEOUT;
        writeWaterMark = new WriteBufferWaterMark();
        inboundWaterMark = new InboundEventBufferWaterMark();
        statsDumpInterval = DEFAULT_STATS_DUMP_INTERVAL;
        openQueueTimeout = QUEUE_OPERATION_TIMEOUT;
        configureQueueTimeout = QUEUE_OPERATION_TIMEOUT;
        closeQueueTimeout = QUEUE_OPERATION_TIMEOUT;
        hostHealthMonitor = null;
        userAgentPrefix = "";
    }

    private SessionOptions(Builder builder) {
        brokerUri = builder.brokerUri;
        startTimeout = builder.startTimeout;
        stopTimeout = builder.stopTimeout;
        writeWaterMark = builder.writeWaterMark;
        inboundWaterMark = builder.inboundWaterMark;
        statsDumpInterval = builder.statsDumpInterval;
        openQueueTimeout = builder.openQueueTimeout;
        configureQueueTimeout = builder.configureQueueTimeout;
        closeQueueTimeout = builder.closeQueueTimeout;
        hostHealthMonitor = builder.hostHealthMonitor;
        userAgentPrefix = builder.userAgentPrefix;
    }

    /**
     * Creates this class object with default settings.
     *
     * @return SessionOptions immutable session settings object with default values
     */
    public static SessionOptions createDefault() {
        return new SessionOptions();
    }

    /**
     * Returns a helper class object to set different session level options.
     *
     * @return Builder session options setter
     */
    public static Builder builder() {
        return builder(createDefault());
    }

    /**
     * Returns a helper class object to set different session level options.
     *
     * @param options specifies options which initializes the builder
     * @return Builder session options setter
     */
    public static Builder builder(SessionOptions options) {
        Argument.expectNonNull(options, "options");
        return new Builder(options);
    }

    /**
     * Returns URI which will be used by the {@link com.bloomberg.bmq.Session} to connect with the
     * BlazingMQ broker.
     *
     * @return URI broker URI
     */
    public URI brokerUri() {
        return brokerUri;
    }

    /**
     * Returns timeout value that defines how much time the user gives to the {@link
     * com.bloomberg.bmq.Session} for successful start.
     *
     * <p>Note: this value is used if explicit timeout calling the {@link
     * com.bloomberg.bmq.Session#start} or {@link com.bloomberg.bmq.Session#startAsync} is not
     * provided.
     *
     * @return Duration session start timeout
     */
    public Duration startTimeout() {
        return startTimeout;
    }

    /**
     * Returns timeout value that defines how much time the user gives to the {@link
     * com.bloomberg.bmq.Session} for successful stop.
     *
     * <p>Note: this value is used if explicit timeout calling the {@link
     * com.bloomberg.bmq.Session#stop} or {@link com.bloomberg.bmq.Session#stopAsync} is not
     * provided.
     *
     * @return Duration session stop timeout
     */
    public Duration stopTimeout() {
        return stopTimeout;
    }

    /**
     * Returns a helper class object that holds high and low channel watermark values.
     *
     * @return WriteBufferWaterMark LWM and HWM holder
     */
    public WriteBufferWaterMark writeBufferWaterMark() {
        return writeWaterMark;
    }

    /**
     * Returns a helper class object that holds high and low inbound event buffer watermark values.
     *
     * @return InboundEventBufferWaterMark LWM and HWM holder
     */
    public InboundEventBufferWaterMark inboundBufferWaterMark() {
        return inboundWaterMark;
    }

    /**
     * Returns interval (in seconds) at which to dump stats in the logs. Default is 5min. "0" means
     * disabled recurring dump of stats (final stats are always dumped at end of session).
     *
     * @return Duration stats dump interval
     */
    public Duration statsDumpInterval() {
        return statsDumpInterval;
    }

    /**
     * Returns timeout value that defines how much time the user gives to the {@link
     * com.bloomberg.bmq.Queue} for successful queue opening. Note: this value is used if explicit
     * timeout calling the {@link com.bloomberg.bmq.Queue#open} or {@link
     * com.bloomberg.bmq.Queue#openAsync} is not provided.
     *
     * @return Duration open queue timeout
     */
    public Duration openQueueTimeout() {
        return openQueueTimeout;
    }

    /**
     * Returns timeout value that defines how much time the user gives to the {@link
     * com.bloomberg.bmq.Queue} for successful configure call. Note: this value is used if explicit
     * timeout calling the {@link com.bloomberg.bmq.Queue#configure} or {@link
     * com.bloomberg.bmq.Queue#configureAsync} is not provided.
     *
     * @return Duration configure queue timeout
     */
    public Duration configureQueueTimeout() {
        return configureQueueTimeout;
    }

    /**
     * Returns timeout value that defines how much time the user gives to the {@link
     * com.bloomberg.bmq.Queue} for successful close operation. Note: this value is used if explicit
     * timeout calling the {@link com.bloomberg.bmq.Queue#close} or {@link
     * com.bloomberg.bmq.Queue#closeAsync} is not provided.
     *
     * @return Duration close queue timeout
     */
    public Duration closeQueueTimeout() {
        return closeQueueTimeout;
    }

    /**
     * Returns an object which monitors the health of the host. BlazingMQ sessions can use such
     * objects to conditionally suspend queue activity while the host is marked unhealthy. Default
     * value is null that means no monitor is set.
     *
     * @return host health monitor object or null if not set
     */
    public HostHealthMonitor hostHealthMonitor() {
        return hostHealthMonitor;
    }

    /**
     * Returns the string that will be prefixed to the user agent used by {@link
     * com.bloomberg.bmq.Session} to identify itself to the broker.
     *
     * @return String user agent string prefix
     */
    public String userAgentPrefix() {
        return userAgentPrefix;
    }

    /** Helper class to create a {@code SesssionOptions} object with custom settings. */
    public static class Builder {
        private URI brokerUri;
        private Duration startTimeout;
        private Duration stopTimeout;
        private WriteBufferWaterMark writeWaterMark;
        private InboundEventBufferWaterMark inboundWaterMark;

        private Duration statsDumpInterval;
        private Duration openQueueTimeout;
        private Duration configureQueueTimeout;
        private Duration closeQueueTimeout;

        private HostHealthMonitor hostHealthMonitor;

        private String userAgentPrefix;

        /**
         * Creates a {@code SesssionOptions} object based on this {@code Builder} properties.
         *
         * @return SessionOptions immutable session options with custom settings
         */
        public SessionOptions build() {
            return new SessionOptions(this);
        }

        private Builder(SessionOptions options) {
            brokerUri = options.brokerUri;
            startTimeout = options.startTimeout;
            stopTimeout = options.stopTimeout;
            writeWaterMark = options.writeWaterMark;
            inboundWaterMark = options.inboundWaterMark;
            statsDumpInterval = options.statsDumpInterval;
            openQueueTimeout = options.openQueueTimeout;
            configureQueueTimeout = options.configureQueueTimeout;
            closeQueueTimeout = options.closeQueueTimeout;
            hostHealthMonitor = options.hostHealthMonitor;
            userAgentPrefix = options.userAgentPrefix;
        }

        /**
         * Sets URI to connect with the broker.
         *
         * @param value URI to set
         * @return Builder this object
         * @throws IllegalArgumentException in case of invalid URI value
         */
        public Builder setBrokerUri(URI value) {
            Argument.expectNonNull(value.getHost(), "host");
            Argument.expectPositive(value.getPort(), "port");

            brokerUri = value;
            return this;
        }

        /**
         * Sets session start timeout
         *
         * @param value start timeout value
         * @return Builder this object
         * @throws IllegalArgumentException in case of timeout value is zero or negative
         */
        public Builder setStartTimeout(Duration value) {
            Argument.expectCondition(
                    !(value.isNegative() || value.isZero()), "'startTimeout' must be positive");

            startTimeout = value;
            return this;
        }

        /**
         * Sets session stop timeout
         *
         * @param value stop timeout value
         * @return Builder this object
         * @throws IllegalArgumentException in case of timeout value is zero or negative
         */
        public Builder setStopTimeout(Duration value) {
            Argument.expectCondition(
                    !(value.isNegative() || value.isZero()), "'stopTimeout' must be positive");
            stopTimeout = value;
            return this;
        }

        /**
         * Sets low and high channel watermark values
         *
         * @param value holder object for watermark values
         * @return Builder this object
         * @throws IllegalArgumentException in case of any of watermark values are zero or negative
         */
        public Builder setWriteBufferWaterMark(WriteBufferWaterMark value) {
            Argument.expectPositive(value.lowWaterMark(), "low water mark");
            Argument.expectPositive(value.highWaterMark(), "high water mark");
            writeWaterMark = value;
            return this;
        }

        /**
         * Sets low and high inbound buffer watermark values
         *
         * @param value holder object for watermark values
         * @return Builder this object
         */
        public Builder setInboundBufferWaterMark(InboundEventBufferWaterMark value) {
            inboundWaterMark = value;
            return this;
        }

        /**
         * Sets the stats dump interval to the specified 'value'. 0 - disable recurring dump of
         * stats (final stats are always dumped at end of session).
         *
         * @param value stats dump interval value
         * @return Builder this object
         * @throws IllegalArgumentException in case of interval value is negative
         */
        public Builder setStatsDumpInterval(Duration value) {
            Argument.expectCondition(
                    !value.isNegative(), "'statsDumpInterval' must be non-negative.");
            statsDumpInterval = value;
            return this;
        }

        /**
         * Sets timeout value for open queue operation
         *
         * @param value timeout value
         * @return Builder this object
         * @throws IllegalArgumentException in case of timeout value is zero or negative
         */
        public Builder setOpenQueueTimeout(Duration value) {
            Argument.expectCondition(
                    !(value.isNegative() || value.isZero()), "'openQueueTimeout' must be positive");
            openQueueTimeout = value;
            return this;
        }

        /**
         * Sets timeout value for configure queue operation
         *
         * @param value timeout value
         * @return Builder this object
         * @throws IllegalArgumentException in case of timeout value is zero or negative
         */
        public Builder setConfigureQueueTimeout(Duration value) {
            Argument.expectCondition(
                    !(value.isNegative() || value.isZero()),
                    "'configureQueueTimeout' must be positive");
            configureQueueTimeout = value;
            return this;
        }

        /**
         * Sets timeout value for close queue operation
         *
         * @param value timeout value
         * @return Builder this object
         * @throws IllegalArgumentException in case of timeout value is zero or negative
         */
        public Builder setCloseQueueTimeout(Duration value) {
            Argument.expectCondition(
                    !(value.isNegative() || value.isZero()),
                    "'closeQueueTimeout' must be positive");
            closeQueueTimeout = value;
            return this;
        }

        /**
         * Sets host health monitor object.
         *
         * @param value host health monitor object
         * @return Builder this object
         * @throws NullPointerException if the specified value is null
         */
        public Builder setHostHealthMonitor(HostHealthMonitor value) {
            hostHealthMonitor = Argument.expectNonNull(value, "host health monitor");
            return this;
        }

        /**
         * Sets the user agent prefix. This string is prefixed to a user agent constructed by the
         * SDK. This is intended ONLY for other libraries that wrap this SDK to identify themselves
         * for broker telemetry. Applications that are directly using this SDK are encouraged not to
         * set this.
         *
         * @param value String user agent prefix
         * @return Builder this object
         * @throws NullPointerException if the specified value is null
         * @throws IllegalArgumentException in case the specified value contains non-ASCII
         *     characters, contains non-printable characters (i.e., control characters), or is
         *     longer than 127 characters.
         */
        public Builder setUserAgentPrefix(String value) {
            Argument.expectNonNull(value, "user agent prefix");
            Argument.expectCondition(
                    value.codePoints().allMatch(c -> c < 128 && !Character.isISOControl(c)),
                    "user agent prefix must be printable ASCII");
            Argument.expectCondition(
                    value.length() < 128, "user agent prefix must be shorter than 128 characters");
            userAgentPrefix = value;
            return this;
        }
    }
}
