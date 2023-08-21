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
package com.bloomberg.bmq.impl.infr.net;

import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NetResolver {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final LinkedList<InetAddress> resolvedHosts = new LinkedList<>();

    private URI uri;
    private boolean isUnknownHost;

    public void setUri(URI uri) {
        this.uri = uri;
        init();
    }

    public int numResolvedHosts() {
        return resolvedHosts.size();
    }

    private void init() {
        resolvedHosts.clear();
        isUnknownHost = false;

        if (uri != null) {
            String host = uri.getHost();
            if (host == null) {
                host = uri.getPath();
            }
            try {
                resolvedHosts.addAll(Arrays.asList(InetAddress.getAllByName(host)));
            } catch (UnknownHostException e) {
                isUnknownHost = true;
                logger.warn("Failed to resolve host {}: {}", host, e);
            }
        }
    }

    public URI getNextUri() {
        URI nextUri = null;
        if (resolvedHosts.isEmpty()) {
            init();
        }
        try {
            InetAddress host = resolvedHosts.pop();
            if (host != null) {
                int port = uri.getPort();
                if (port < 0) {
                    logger.info("Use default port");
                    port = ConnectionOptions.k_DEFAULT_URI.getPort();
                }
                nextUri = URI.create("tcp://" + host.getHostAddress() + ":" + port);
            }

        } catch (NoSuchElementException e) {
            logger.warn("Empty host list");
        }
        if (nextUri == null) {
            nextUri = uri;
        }
        return nextUri;
    }

    public boolean isUnknownHost() {
        return isUnknownHost;
    }
}
