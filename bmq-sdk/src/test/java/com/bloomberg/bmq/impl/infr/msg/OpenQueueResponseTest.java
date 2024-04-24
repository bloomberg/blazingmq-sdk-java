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

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class OpenQueueResponseTest {

    @Test
    void testReset() {
        OpenQueueResponse openQueueResponse = new OpenQueueResponse();
        assertNull(openQueueResponse.getOriginalRequest());
        assertNull(openQueueResponse.getRoutingConfiguration());
        openQueueResponse.setOriginalRequest(new OpenQueue());
        openQueueResponse.setRoutingConfiguration(new RoutingConfiguration());
        assertNotNull(openQueueResponse.getOriginalRequest());
        assertNotNull(openQueueResponse.getRoutingConfiguration());
        openQueueResponse.reset();
        assertNull(openQueueResponse.getOriginalRequest());
        assertNull(openQueueResponse.getRoutingConfiguration());
    }

    @Test
    void testCreateNewInstance() {
        OpenQueueResponse openQueueResponse = new OpenQueueResponse();
        OpenQueueResponse responseBefore = openQueueResponse;
        openQueueResponse = (OpenQueueResponse) openQueueResponse.createNewInstance();
        OpenQueueResponse responseAfter = openQueueResponse;
        assertNotEquals(responseBefore, responseAfter);
    }
}
