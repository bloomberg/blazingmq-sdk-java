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

import static com.bloomberg.bmq.resultcodes.CloseQueueResultChecks.checkAlreadyClosed;
import static com.bloomberg.bmq.resultcodes.CloseQueueResultChecks.checkAlreadyInProgress;
import static com.bloomberg.bmq.resultcodes.CloseQueueResultChecks.checkGeneric;
import static com.bloomberg.bmq.resultcodes.CloseQueueResultChecks.checkInvalidQueue;
import static com.bloomberg.bmq.resultcodes.CloseQueueResultChecks.checkUnknownQueue;
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
// CloseQueueResult tests
// ================================================================
public class CloseQueueResultTest {
    public static class CloseQueueGenericTest {
        public void isAlreadyClosed(ResultCodes.CloseQueueCode obj) {
            assertFalse(obj.isAlreadyClosed());
        }

        public void isAlreadyInProgressTest(ResultCodes.CloseQueueCode obj) {
            assertFalse(obj.isAlreadyInProgress());
        }

        public void isUnknownQueueTest(ResultCodes.CloseQueueCode obj) {
            assertFalse(obj.isUnknownQueue());
        }

        public void isInvalidQueueTest(ResultCodes.CloseQueueCode obj) {
            assertFalse(obj.isInvalidQueue());
        }
    }

    public static class AlreadyClosedTest extends CloseQueueGenericTest {
        @Override
        public void isAlreadyClosed(ResultCodes.CloseQueueCode obj) {
            assertTrue(obj.isAlreadyClosed());
        }
    }

    public static class AlreadyInProgressTest extends CloseQueueGenericTest {
        @Override
        public void isAlreadyInProgressTest(ResultCodes.CloseQueueCode obj) {
            assertTrue(obj.isAlreadyInProgress());
        }
    }

    public static class UnknownQueueTest extends CloseQueueGenericTest {
        @Override
        public void isUnknownQueueTest(ResultCodes.CloseQueueCode obj) {
            assertTrue(obj.isUnknownQueue());
        }
    }

    public static class InvalidQueueTest extends CloseQueueGenericTest {
        @Override
        public void isInvalidQueueTest(ResultCodes.CloseQueueCode obj) {
            assertTrue(obj.isInvalidQueue());
        }
    }

    @Test
    public void successTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.SUCCESS);
        checkSuccess(ResultCodes.CloseQueueResult.SUCCESS);
        checkGeneric(ResultCodes.CloseQueueResult.SUCCESS);
    }

    @Test
    public void timeOutTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.TIMEOUT);
        checkTimeout(ResultCodes.CloseQueueResult.TIMEOUT);
        checkGeneric(ResultCodes.CloseQueueResult.TIMEOUT);
    }

    @Test
    public void notConnectedTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.NOT_CONNECTED);
        checkNotConnected(ResultCodes.CloseQueueResult.NOT_CONNECTED);
        checkGeneric(ResultCodes.CloseQueueResult.NOT_CONNECTED);
    }

    @Test
    public void canceledTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.CANCELED);
        checkCanceled(ResultCodes.CloseQueueResult.CANCELED);
        checkGeneric(ResultCodes.CloseQueueResult.CANCELED);
    }

    @Test
    public void notSupportedTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.NOT_SUPPORTED);
        checkNotSupported(ResultCodes.CloseQueueResult.NOT_SUPPORTED);
        checkGeneric(ResultCodes.CloseQueueResult.NOT_SUPPORTED);
    }

    @Test
    public void refusedTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.REFUSED);
        checkRefused(ResultCodes.CloseQueueResult.REFUSED);
        checkGeneric(ResultCodes.CloseQueueResult.REFUSED);
    }

    @Test
    public void invalidArgumentTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.INVALID_ARGUMENT);
        checkInvalidArgument(ResultCodes.CloseQueueResult.INVALID_ARGUMENT);
        checkGeneric(ResultCodes.CloseQueueResult.INVALID_ARGUMENT);
    }

    @Test
    public void notReadyTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.NOT_READY);
        checkNotReady(ResultCodes.CloseQueueResult.NOT_READY);
        checkGeneric(ResultCodes.CloseQueueResult.NOT_READY);
    }

    @Test
    public void unknownTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.UNKNOWN);
        checkUnknown(ResultCodes.CloseQueueResult.UNKNOWN);
        checkGeneric(ResultCodes.CloseQueueResult.UNKNOWN);
    }

    @Test
    public void alreadyClosedTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.ALREADY_CLOSED);
        checkAlreadyClosed(ResultCodes.CloseQueueResult.ALREADY_CLOSED);
    }

    @Test
    public void alreadyInProgressTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.ALREADY_IN_PROGRESS);
        checkAlreadyInProgress(ResultCodes.CloseQueueResult.ALREADY_IN_PROGRESS);
    }

    @Test
    public void unknownQueueTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.UNKNOWN_QUEUE);
        checkUnknownQueue(ResultCodes.CloseQueueResult.UNKNOWN_QUEUE);
    }

    @Test
    public void invalidQueueTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.INVALID_QUEUE);
        checkInvalidQueue(ResultCodes.CloseQueueResult.INVALID_QUEUE);
    }
}
