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
// OpenQueueResult tests
// ================================================================
class OpenQueueResultTest {
    static class OpenQueueGenericTest {
        void isAlreadyOpenedTest(ResultCodes.OpenQueueCode obj) {
            assertFalse(obj.isAlreadyOpened());
        }

        void isAlreadyInProgressTest(ResultCodes.OpenQueueCode obj) {
            assertFalse(obj.isAlreadyInProgress());
        }

        void isInvalidUriTest(ResultCodes.OpenQueueCode obj) {
            assertFalse(obj.isInvalidUri());
        }

        void isInvalidFlagsTest(ResultCodes.OpenQueueCode obj) {
            assertFalse(obj.isInvalidFlags());
        }

        void isQueueIdNotUniqueTest(ResultCodes.OpenQueueCode obj) {
            assertFalse(obj.isQueueIdNotUnique());
        }
    }

    static class AlreadyOpenedTest extends OpenQueueGenericTest {
        @Override
        void isAlreadyOpenedTest(ResultCodes.OpenQueueCode obj) {
            assertTrue(obj.isAlreadyOpened());
        }
    }

    static class AlreadyInProgressTest extends OpenQueueGenericTest {
        void isAlreadyInProgressTest(ResultCodes.OpenQueueCode obj) {
            assertTrue(obj.isAlreadyInProgress());
        }
    }

    static class InvalidUriTest extends OpenQueueGenericTest {
        @Override
        void isInvalidUriTest(ResultCodes.OpenQueueCode obj) {
            assertTrue(obj.isInvalidUri());
        }
    }

    static class InvalidFlagsTest extends OpenQueueGenericTest {
        @Override
        void isInvalidFlagsTest(ResultCodes.OpenQueueCode obj) {
            assertTrue(obj.isInvalidFlags());
        }
    }

    static class QueueIdNotUniqueTest extends OpenQueueGenericTest {
        @Override
        void isQueueIdNotUniqueTest(ResultCodes.OpenQueueCode obj) {
            assertTrue(obj.isQueueIdNotUnique());
        }
    }

    static void checkExtraPredicates(
            OpenQueueResultTest.OpenQueueGenericTest test, ResultCodes.OpenQueueCode obj) {
        test.isAlreadyInProgressTest(obj);
        test.isAlreadyOpenedTest(obj);
        test.isInvalidFlagsTest(obj);
        test.isInvalidUriTest(obj);
        test.isQueueIdNotUniqueTest(obj);
    }

    static void checkGeneric(ResultCodes.OpenQueueCode obj) {
        checkExtraPredicates(new OpenQueueResultTest.OpenQueueGenericTest(), obj);
    }

    static void checkAlreadyInProgress(ResultCodes.OpenQueueCode obj) {
        checkExtraPredicates(new OpenQueueResultTest.AlreadyInProgressTest(), obj);
    }

    static void checkAlreadyOpened(ResultCodes.OpenQueueCode obj) {
        checkExtraPredicates(new OpenQueueResultTest.AlreadyOpenedTest(), obj);
    }

    static void checkInvalidFlags(ResultCodes.OpenQueueCode obj) {
        checkExtraPredicates(new OpenQueueResultTest.InvalidFlagsTest(), obj);
    }

    static void checkInvalidUri(ResultCodes.OpenQueueCode obj) {
        checkExtraPredicates(new OpenQueueResultTest.InvalidUriTest(), obj);
    }

    static void checkQueueIdNotUnique(ResultCodes.OpenQueueCode obj) {
        checkExtraPredicates(new OpenQueueResultTest.QueueIdNotUniqueTest(), obj);
    }

    @Test
    void successTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.SUCCESS);
        checkSuccess(ResultCodes.OpenQueueResult.SUCCESS);
        checkGeneric(ResultCodes.OpenQueueResult.SUCCESS);
    }

    @Test
    void timeOutTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.TIMEOUT);
        checkTimeout(ResultCodes.OpenQueueResult.TIMEOUT);
        checkGeneric(ResultCodes.OpenQueueResult.TIMEOUT);
    }

    @Test
    void notConnectedTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.NOT_CONNECTED);
        checkNotConnected(ResultCodes.OpenQueueResult.NOT_CONNECTED);
        checkGeneric(ResultCodes.OpenQueueResult.NOT_CONNECTED);
    }

    @Test
    void canceledTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.CANCELED);
        checkCanceled(ResultCodes.OpenQueueResult.CANCELED);
        checkGeneric(ResultCodes.OpenQueueResult.CANCELED);
    }

    @Test
    void notSupportedTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.NOT_SUPPORTED);
        checkNotSupported(ResultCodes.OpenQueueResult.NOT_SUPPORTED);
        checkGeneric(ResultCodes.OpenQueueResult.NOT_SUPPORTED);
    }

    @Test
    void refusedTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.REFUSED);
        checkRefused(ResultCodes.OpenQueueResult.REFUSED);
        checkGeneric(ResultCodes.OpenQueueResult.REFUSED);
    }

    @Test
    void invalidArgumentTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.INVALID_ARGUMENT);
        checkInvalidArgument(ResultCodes.OpenQueueResult.INVALID_ARGUMENT);
        checkGeneric(ResultCodes.OpenQueueResult.INVALID_ARGUMENT);
    }

    @Test
    void notReadyTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.NOT_READY);
        checkNotReady(ResultCodes.OpenQueueResult.NOT_READY);
        checkGeneric(ResultCodes.OpenQueueResult.NOT_READY);
    }

    @Test
    void unknownTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.UNKNOWN);
        checkUnknown(ResultCodes.OpenQueueResult.UNKNOWN);
        checkGeneric(ResultCodes.OpenQueueResult.UNKNOWN);
    }

    @Test
    void alreadyInProgressTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.ALREADY_IN_PROGRESS);
        checkAlreadyInProgress(ResultCodes.OpenQueueResult.ALREADY_IN_PROGRESS);
    }

    @Test
    void alreadyOpenedTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.ALREADY_OPENED);
        checkAlreadyOpened(ResultCodes.OpenQueueResult.ALREADY_OPENED);
    }

    @Test
    void invalidFlagsTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.INVALID_FLAGS);
        checkInvalidFlags(ResultCodes.OpenQueueResult.INVALID_FLAGS);
    }

    @Test
    void invalidUriTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.INVALID_URI);
        checkInvalidUri(ResultCodes.OpenQueueResult.INVALID_URI);
    }

    @Test
    void queueIdNotUniqueTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.QUEUE_ID_NOT_UNIQUE);
        checkQueueIdNotUnique(ResultCodes.OpenQueueResult.QUEUE_ID_NOT_UNIQUE);
    }
}
