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
package com.bloomberg.bmq;

import com.bloomberg.bmq.impl.infr.util.Argument;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides value-semantic type and utilities for a BlazingMQ queue URI.
 *
 * <p>This class provides a value-semantic type, representing a URI for a BlazingMQ queue. A {@code
 * Uri} can be built by parsing from a string representation provided to the class constructor.
 *
 * <H2>URI format</H2>
 *
 * In a nutshell, a URI representing an application queue managed by a BlazingMQ broker on a given
 * machine looks like one of the following:
 *
 * <pre>
 *
 *  bmq://ts.trades.myapp/my.queue
 *  bmq://ts.trades.myapp.~bt/my.queue
 *  bmq://ts.trades.myapp/my.queue?id=foo
 * </pre>
 *
 * where:
 *
 * <ul>
 *   <li>The URI scheme is always "bmq".
 *   <li>The URI authority is the name of BlazingMQ domain (such as "ts.trades.myapp") as registered
 *       with the BlazingMQ infrastructure. The domain name may contain alphanumeric characters,
 *       dots and dashes (it has to match the following regular expression: {@code
 *       [-a-zA-Z0-9\\._]+'}. The domain may be followed by an optional tier, introduced by the ".~"
 *       sequence and consisting of alphanumeric characters and dashes. The ".~" sequence is not
 *       part of the tier.
 *   <li>The URI path is the name of the queue ("my.queue" above) and may contain alphanumeric
 *       characters, dashes, underscores and tild (it has to match the following regular expression:
 *       {@code [-a-zA-Z0-9_~\\.]+}).
 *   <li>The name of the queue ({@code my.queue} above) may contain alphanumeric characters and
 *       dots.
 *   <li>The URI may contain an optional query with a key-value pair, where key is always {@code
 *       id}, and corresponding value represents a name that will be used by BlazingMQ broker to
 *       uniquely identify the client.
 *   <li>The URI fragment part is currently unused.
 * </ul>
 *
 * <H2>Usage Example</H2>
 *
 * Instantiate a {@code Uri} object with a string representation of the URI:
 *
 * <pre>
 *
 *  Uri uri = new Uri("bmq://my.domain/queue");
 *  assert uri.scheme() == "bmq";
 *  assert uri.domain() == "my.domain";
 *  assert uri.queue()  == "queue";
 * </pre>
 *
 * <H2>Thread Safety</H2>
 *
 * Once created {@code Uri} object is immutable and thread safe.
 */
@Immutable
public class Uri {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Pattern pattern;

    private final URI uri;
    private final String domain;
    private final String tier;
    private final String path;
    private final String id;

    static {
        String patternRegex =
                "^bmq:\\/\\/"
                        + "(?<authority>"
                        + "(?<domain>[-a-zA-Z0-9\\._]+)"
                        + "(?<tier>\\.~[-a-zA-Z0-9]*)?"
                        + ")/"
                        + "(?<path>[-a-zA-Z0-9_\\.]+)"
                        + "(?<q1>\\?(id=)([-a-zA-Z0-9_\\.]+))?$";

        pattern = Pattern.compile(patternRegex);
    }

    /**
     * Constructs this object from the specified {@code uri} string. If the {@code uri} input string
     * doesn't represent a valid URI, this object won't be created.
     *
     * @param uriStr string representation of the URI
     * @throws RuntimeException if the specified string is not a valid URI
     */
    public Uri(String uriStr) {
        try {
            Matcher mat = pattern.matcher(uriStr);
            Argument.expectCondition(mat.matches(), "Wrong URI format");

            path = mat.group("path");
            domain = mat.group("domain");
            String tierStr = mat.group("tier");
            if (tierStr != null && !tierStr.isEmpty()) {
                String[] segments = tierStr.split("~");
                tierStr = "";
                if (segments.length == 2) {
                    tierStr = segments[1];
                }
                Argument.expectCondition(!tierStr.isEmpty(), "Empty tier");
            }
            String idStr = mat.group(7);
            if (idStr == null) {
                idStr = "";
            }
            id = idStr;
            tier = tierStr;
            uri = new URI(uriStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Failed to create URI", e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Wrong URI string format", e);
        }
    }

    /**
     * Returns {@code scheme} part of the URI in terms of the URI RFC terminology.
     *
     * @return String {@code scheme} part of the URI
     */
    public String scheme() {
        return uri.getScheme();
    }

    /**
     * Returns {@code authority} part of the URI in terms of the URI RFC terminology.
     *
     * @return String {@code authority} part of the URI
     */
    public String authority() {
        return uri.getAuthority();
    }

    /**
     * Returns {@code path} part of the URI in terms of the URI RFC terminology.
     *
     * @return String {@code path} part of the URI
     */
    public String path() {
        return path;
    }

    /**
     * Returns {@code qualifiedDomain} part of the URI in terms of the BlazingMQ terminology.
     *
     * @return String {@code qualifiedDomain} part of the URI
     */
    public String qualifiedDomain() {
        return authority();
    }

    /**
     * Returns {@code domain} part of the URI in terms of the BlazingMQ terminology.
     *
     * @return String {@code domain} part of the URI
     */
    public String domain() {
        return domain;
    }

    /**
     * Returns {@code tier} part of the URI in terms of the BlazingMQ terminology.
     *
     * @return String {@code tier} part of the URI
     */
    public String tier() {
        return tier;
    }

    /**
     * Returns {@code queue} part of the URI in terms of the BlazingMQ terminology.
     *
     * @return String {@code queue} part of the URI
     */
    public String queue() {
        return path();
    }

    /**
     * Returns {@code id} part of the URI in terms of the BlazingMQ terminology.
     *
     * @return String {@code id} part of the URI
     */
    public String id() {
        return id;
    }

    /**
     * Returns the canonical form of the URI.
     *
     * <p>Note that canonical form includes everything except the query part of the URI.
     *
     * @return String canonical form of the URI except query part
     */
    public String canonical() {
        StringBuilder sb = new StringBuilder();
        sb.append(scheme()).append("://").append(authority()).append("/").append(path());
        return sb.toString();
    }

    /**
     * Returns a string representation of the URI.
     *
     * @return String string representation of the URI
     */
    @Override
    public String toString() {
        return uri.toString();
    }

    /**
     * Returns true if this {@code Uri} is equal to the specified {@code obj}, false otherwise.
     *
     * @param obj {@code Uri} object to compare with
     * @return boolean true if {@code Uri} are equal, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;
        if (!(obj instanceof Uri)) return false;
        return uri.equals(((Uri) obj).uri);
    }

    /**
     * Returns a hash code value for this {@code Uri} object.
     *
     * @return int a hash code value for this object.
     */
    @Override
    public int hashCode() {
        return uri.hashCode();
    }
}
