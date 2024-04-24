/*
 * Copyright 2024 Bloomberg Finance L.P.
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
package com.bloomberg.bmq.resultcodes;

import static com.bloomberg.bmq.resultcodes.GenericResultChecks.checkCanceled;
import static com.bloomberg.bmq.resultcodes.GenericResultChecks.checkIfNotGeneric;
import static com.bloomberg.bmq.resultcodes.GenericResultChecks.checkInvalidArgument;
import static com.bloomberg.bmq.resultcodes.GenericResultChecks.checkNotConnected;
import static com.bloomberg.bmq.resultcodes.GenericResultChecks.checkNotReady;
import static com.bloomberg.bmq.resultcodes.GenericResultChecks.checkNotSupported;
import static com.bloomberg.bmq.resultcodes.GenericResultChecks.checkRefused;
import static com.bloomberg.bmq.resultcodes.GenericResultChecks.checkSuccess;
import static com.bloomberg.bmq.resultcodes.GenericResultChecks.checkTimeout;
import static com.bloomberg.bmq.resultcodes.GenericResultChecks.checkUnknown;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bloomberg.bmq.ResultCodes;
import org.junit.jupiter.api.Test;

// ================================================================
// ConfigureQueueResult tests
// ================================================================
class ConfigureQueueResultTest {
    static class ConfigureQueueGenericTest {
        void isAlreadyInProgressTest(ResultCodes.ConfigureQueueCode obj) {
            assertFalse(obj.isAlreadyInProgress());
        }

        void isInvalidQueueTest(ResultCodes.ConfigureQueueCode obj) {
            assertFalse(obj.isInvalidQueue());
        }
    }

    static class AlreadyInProgressTest extends ConfigureQueueGenericTest {
        @Override
        public void isAlreadyInProgressTest(ResultCodes.ConfigureQueueCode obj) {
            assertTrue(obj.isAlreadyInProgress());
        }
    }

    static class InvalidQueueTest extends ConfigureQueueGenericTest {
        @Override
        public void isInvalidQueueTest(ResultCodes.ConfigureQueueCode obj) {
            assertTrue(obj.isInvalidQueue());
        }
    }

    static void checkPredicates(
            ConfigureQueueResultTest.ConfigureQueueGenericTest test,
            ResultCodes.ConfigureQueueCode obj) {
        test.isAlreadyInProgressTest(obj);
        test.isInvalidQueueTest(obj);
    }

    static void checkGeneric(ResultCodes.ConfigureQueueCode obj) {
        checkPredicates(new ConfigureQueueResultTest.ConfigureQueueGenericTest(), obj);
    }

    static void checkAlreadyInProgress(ResultCodes.ConfigureQueueCode obj) {
        checkPredicates(new ConfigureQueueResultTest.AlreadyInProgressTest(), obj);
    }

    static void checkAlreadyOpenedTest(ResultCodes.ConfigureQueueCode obj) {
        checkPredicates(new ConfigureQueueResultTest.InvalidQueueTest(), obj);
    }

    @Test
    void successTest() {
        checkIfNotGeneric(ResultCodes.ConfigureQueueResult.SUCCESS);
        checkSuccess(ResultCodes.ConfigureQueueResult.SUCCESS);
        checkGeneric(ResultCodes.ConfigureQueueResult.SUCCESS);
    }

    @Test
    void timeOutTest() {
        checkIfNotGeneric(ResultCodes.ConfigureQueueResult.TIMEOUT);
        checkTimeout(ResultCodes.ConfigureQueueResult.TIMEOUT);
        checkGeneric(ResultCodes.ConfigureQueueResult.TIMEOUT);
    }

    @Test
    void notConnectedTest() {
        checkIfNotGeneric(ResultCodes.ConfigureQueueResult.NOT_CONNECTED);
        checkNotConnected(ResultCodes.ConfigureQueueResult.NOT_CONNECTED);
        checkGeneric(ResultCodes.ConfigureQueueResult.NOT_CONNECTED);
    }

    @Test
    void canceledTest() {
        checkIfNotGeneric(ResultCodes.ConfigureQueueResult.CANCELED);
        checkCanceled(ResultCodes.ConfigureQueueResult.CANCELED);
        checkGeneric(ResultCodes.ConfigureQueueResult.CANCELED);
    }

    @Test
    void notSupportedTest() {
        checkIfNotGeneric(ResultCodes.ConfigureQueueResult.NOT_SUPPORTED);
        checkNotSupported(ResultCodes.ConfigureQueueResult.NOT_SUPPORTED);
        checkGeneric(ResultCodes.ConfigureQueueResult.NOT_SUPPORTED);
    }

    @Test
    void refusedTest() {
        checkIfNotGeneric(ResultCodes.ConfigureQueueResult.REFUSED);
        checkRefused(ResultCodes.ConfigureQueueResult.REFUSED);
        checkGeneric(ResultCodes.ConfigureQueueResult.REFUSED);
    }

    @Test
    void invalidArgumentTest() {
        checkIfNotGeneric(ResultCodes.ConfigureQueueResult.INVALID_ARGUMENT);
        checkInvalidArgument(ResultCodes.ConfigureQueueResult.INVALID_ARGUMENT);
        checkGeneric(ResultCodes.ConfigureQueueResult.INVALID_ARGUMENT);
    }

    @Test
    void notReadyTest() {
        checkIfNotGeneric(ResultCodes.ConfigureQueueResult.NOT_READY);
        checkNotReady(ResultCodes.ConfigureQueueResult.NOT_READY);
        checkGeneric(ResultCodes.ConfigureQueueResult.NOT_READY);
    }

    @Test
    void unknownTest() {
        checkIfNotGeneric(ResultCodes.ConfigureQueueResult.UNKNOWN);
        checkUnknown(ResultCodes.ConfigureQueueResult.UNKNOWN);
        checkGeneric(ResultCodes.ConfigureQueueResult.UNKNOWN);
    }

    @Test
    void alreadyInProgressTest() {
        checkIfNotGeneric(ResultCodes.ConfigureQueueResult.ALREADY_IN_PROGRESS);
        checkAlreadyInProgress(ResultCodes.ConfigureQueueResult.ALREADY_IN_PROGRESS);
    }

    @Test
    void alreadyOpenedTest() {
        checkIfNotGeneric(ResultCodes.ConfigureQueueResult.INVALID_QUEUE);
        checkAlreadyOpenedTest(ResultCodes.ConfigureQueueResult.INVALID_QUEUE);
    }
}
