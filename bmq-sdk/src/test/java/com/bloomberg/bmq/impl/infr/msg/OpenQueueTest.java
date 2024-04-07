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

import org.junit.jupiter.api.Test;

class OpenQueueTest {

    @Test
    void testReset() {
        OpenQueue openQueue = new OpenQueue();
        QueueHandleParameters parametersBefore = openQueue.getHandleParameters();
        openQueue.reset();
        QueueHandleParameters parametersAfter = openQueue.getHandleParameters();
        assertNotEquals(parametersBefore, parametersAfter);
    }

    @Test
    void testCreateNewInstance() {
        OpenQueue openQueue = new OpenQueue();
        OpenQueue queueBefore = openQueue;
        openQueue = (OpenQueue) openQueue.createNewInstance();
        OpenQueue queueAfter = openQueue;
        assertNotEquals(queueBefore, queueAfter);
    }
}
