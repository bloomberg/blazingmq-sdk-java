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
import static com.bloomberg.bmq.resultcodes.OpenQueueResultChecks.checkAlreadyInProgress;
import static com.bloomberg.bmq.resultcodes.OpenQueueResultChecks.checkAlreadyOpened;
import static com.bloomberg.bmq.resultcodes.OpenQueueResultChecks.checkGeneric;
import static com.bloomberg.bmq.resultcodes.OpenQueueResultChecks.checkInvalidFlags;
import static com.bloomberg.bmq.resultcodes.OpenQueueResultChecks.checkInvalidUri;
import static com.bloomberg.bmq.resultcodes.OpenQueueResultChecks.checkQueueIdNotUnique;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bloomberg.bmq.ResultCodes;
import org.junit.jupiter.api.Test;

// ================================================================
// OpenQueueResult tests
// ================================================================
public class OpenQueueResultTest {
    public static class OpenQueueGenericTest {
        public void isAlreadyOpenedTest(ResultCodes.OpenQueueCode obj) {
            assertFalse(obj.isAlreadyOpened());
        }

        public void isAlreadyInProgressTest(ResultCodes.OpenQueueCode obj) {
            assertFalse(obj.isAlreadyInProgress());
        }

        public void isInvalidUriTest(ResultCodes.OpenQueueCode obj) {
            assertFalse(obj.isInvalidUri());
        }

        public void isInvalidFlagsTest(ResultCodes.OpenQueueCode obj) {
            assertFalse(obj.isInvalidFlags());
        }

        public void isQueueIdNotUniqueTest(ResultCodes.OpenQueueCode obj) {
            assertFalse(obj.isQueueIdNotUnique());
        }
    }

    public static class AlreadyOpenedTest extends OpenQueueGenericTest {
        @Override
        public void isAlreadyOpenedTest(ResultCodes.OpenQueueCode obj) {
            assertTrue(obj.isAlreadyOpened());
        }
    }

    public static class AlreadyInProgressTest extends OpenQueueGenericTest {
        public void isAlreadyInProgressTest(ResultCodes.OpenQueueCode obj) {
            assertTrue(obj.isAlreadyInProgress());
        }
    }

    public static class InvalidUriTest extends OpenQueueGenericTest {
        @Override
        public void isInvalidUriTest(ResultCodes.OpenQueueCode obj) {
            assertTrue(obj.isInvalidUri());
        }
    }

    public static class InvalidFlagsTest extends OpenQueueGenericTest {
        @Override
        public void isInvalidFlagsTest(ResultCodes.OpenQueueCode obj) {
            assertTrue(obj.isInvalidFlags());
        }
    }

    public static class QueueIdNotUniqueTest extends OpenQueueGenericTest {
        @Override
        public void isQueueIdNotUniqueTest(ResultCodes.OpenQueueCode obj) {
            assertTrue(obj.isQueueIdNotUnique());
        }
    }

    @Test
    public void successTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.SUCCESS);
        checkSuccess(ResultCodes.OpenQueueResult.SUCCESS);
        checkGeneric(ResultCodes.OpenQueueResult.SUCCESS);
    }

    @Test
    public void timeOutTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.TIMEOUT);
        checkTimeout(ResultCodes.OpenQueueResult.TIMEOUT);
        checkGeneric(ResultCodes.OpenQueueResult.TIMEOUT);
    }

    @Test
    public void notConnectedTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.NOT_CONNECTED);
        checkNotConnected(ResultCodes.OpenQueueResult.NOT_CONNECTED);
        checkGeneric(ResultCodes.OpenQueueResult.NOT_CONNECTED);
    }

    @Test
    public void canceledTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.CANCELED);
        checkCanceled(ResultCodes.OpenQueueResult.CANCELED);
        checkGeneric(ResultCodes.OpenQueueResult.CANCELED);
    }

    @Test
    public void notSupportedTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.NOT_SUPPORTED);
        checkNotSupported(ResultCodes.OpenQueueResult.NOT_SUPPORTED);
        checkGeneric(ResultCodes.OpenQueueResult.NOT_SUPPORTED);
    }

    @Test
    public void refusedTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.REFUSED);
        checkRefused(ResultCodes.OpenQueueResult.REFUSED);
        checkGeneric(ResultCodes.OpenQueueResult.REFUSED);
    }

    @Test
    public void invalidArgumentTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.INVALID_ARGUMENT);
        checkInvalidArgument(ResultCodes.OpenQueueResult.INVALID_ARGUMENT);
        checkGeneric(ResultCodes.OpenQueueResult.INVALID_ARGUMENT);
    }

    @Test
    public void notReadyTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.NOT_READY);
        checkNotReady(ResultCodes.OpenQueueResult.NOT_READY);
        checkGeneric(ResultCodes.OpenQueueResult.NOT_READY);
    }

    @Test
    public void unknownTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.UNKNOWN);
        checkUnknown(ResultCodes.OpenQueueResult.UNKNOWN);
        checkGeneric(ResultCodes.OpenQueueResult.UNKNOWN);
    }

    @Test
    public void alreadyInProgressTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.ALREADY_IN_PROGRESS);
        checkAlreadyInProgress(ResultCodes.OpenQueueResult.ALREADY_IN_PROGRESS);
    }

    @Test
    public void alreadyOpenedTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.ALREADY_OPENED);
        checkAlreadyOpened(ResultCodes.OpenQueueResult.ALREADY_OPENED);
    }

    @Test
    public void invalidFlagsTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.INVALID_FLAGS);
        checkInvalidFlags(ResultCodes.OpenQueueResult.INVALID_FLAGS);
    }

    @Test
    public void invalidUriTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.INVALID_URI);
        checkInvalidUri(ResultCodes.OpenQueueResult.INVALID_URI);
    }

    @Test
    public void queueIdNotUniqueTest() {
        checkIfNotGeneric(ResultCodes.OpenQueueResult.QUEUE_ID_NOT_UNIQUE);
        checkQueueIdNotUnique(ResultCodes.OpenQueueResult.QUEUE_ID_NOT_UNIQUE);
    }
}
