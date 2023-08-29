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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetResolverTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Test
    public void testValidHosts() {

        NetResolver nr = new NetResolver();
        URI uri = nr.getNextUri();

        assertEquals(0, nr.numResolvedHosts());
        assertNull(uri);

        String[] uris = {"tcp://localhost:30114", "www.apache.org", "tcp://255.255.255.255:30114"};

        try {
            for (String u : uris) {
                uri = new URI(u);
                nr.setUri(uri);
                assertTrue(nr.numResolvedHosts() > 0);
                final int numRsolved = nr.numResolvedHosts();
                String firstHost = null;
                for (int i = 0; i < numRsolved; i++) {
                    uri = nr.getNextUri();
                    assertNotNull(uri);
                    logger.info("Resolved for {} => {}", u, uri.getHost());
                    if (i == 0) {
                        firstHost = uri.getHost();
                    }
                }
                // Cycle check
                uri = nr.getNextUri();
                assertEquals(firstHost, uri.getHost());
            }
        } catch (URISyntaxException e) {
            logger.error(e.toString());
            fail();
        }
    }

    @Test
    public void testInvalidHosts() {

        NetResolver nr = new NetResolver();

        String[] uris = {"cp://localho:", "zzzzzzz", "tcp://10000.122.165.92:30114"};

        for (String u : uris) {
            try {
                URI uri = new URI(u);
                nr.setUri(uri);

                assertTrue(nr.isUnknownHost());
                assertEquals(uri, nr.getNextUri());
            } catch (URISyntaxException e) {
                logger.error(e.toString());
                fail();
            } catch (IllegalArgumentException e) {
                logger.info(e.toString());
            }
        }
    }

    @Test
    public void testUnknownHost() throws URISyntaxException, InterruptedException {

        NetResolver nr = new NetResolver();
        assertFalse(nr.isUnknownHost());

        URI unknown = new URI("tcp://unknownhost:30114");
        nr.setUri(unknown);
        assertTrue(nr.isUnknownHost());
        assertEquals(unknown, nr.getNextUri());

        URI known = new URI("tcp://localhost:30114");
        nr.setUri(known);
        assertFalse(nr.isUnknownHost());
        assertEquals(new URI("tcp://127.0.0.1:30114"), nr.getNextUri());
    }
}
