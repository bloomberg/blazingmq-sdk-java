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
// CloseQueueResult tests
// ================================================================
class CloseQueueResultTest {
    static class CloseQueueGenericTest {
        void isAlreadyClosed(ResultCodes.CloseQueueCode obj) {
            assertFalse(obj.isAlreadyClosed());
        }

        void isAlreadyInProgressTest(ResultCodes.CloseQueueCode obj) {
            assertFalse(obj.isAlreadyInProgress());
        }

        void isUnknownQueueTest(ResultCodes.CloseQueueCode obj) {
            assertFalse(obj.isUnknownQueue());
        }

        void isInvalidQueueTest(ResultCodes.CloseQueueCode obj) {
            assertFalse(obj.isInvalidQueue());
        }
    }

    static class AlreadyClosedTest extends CloseQueueGenericTest {
        @Override
        public void isAlreadyClosed(ResultCodes.CloseQueueCode obj) {
            assertTrue(obj.isAlreadyClosed());
        }
    }

    static class AlreadyInProgressTest extends CloseQueueGenericTest {
        @Override
        public void isAlreadyInProgressTest(ResultCodes.CloseQueueCode obj) {
            assertTrue(obj.isAlreadyInProgress());
        }
    }

    static class UnknownQueueTest extends CloseQueueGenericTest {
        @Override
        public void isUnknownQueueTest(ResultCodes.CloseQueueCode obj) {
            assertTrue(obj.isUnknownQueue());
        }
    }

    static class InvalidQueueTest extends CloseQueueGenericTest {
        @Override
        public void isInvalidQueueTest(ResultCodes.CloseQueueCode obj) {
            assertTrue(obj.isInvalidQueue());
        }
    }

    static void checkPredicates(
            CloseQueueResultTest.CloseQueueGenericTest test, ResultCodes.CloseQueueResult obj) {
        test.isAlreadyClosed(obj);
        test.isAlreadyInProgressTest(obj);
        test.isUnknownQueueTest(obj);
        test.isInvalidQueueTest(obj);
    }

    static void checkGeneric(ResultCodes.CloseQueueResult obj) {
        checkPredicates(new CloseQueueResultTest.CloseQueueGenericTest(), obj);
    }

    static void checkAlreadyClosed(ResultCodes.CloseQueueResult obj) {
        checkPredicates(new CloseQueueResultTest.AlreadyClosedTest(), obj);
    }

    static void checkAlreadyInProgress(ResultCodes.CloseQueueResult obj) {
        checkPredicates(new CloseQueueResultTest.AlreadyInProgressTest(), obj);
    }

    static void checkUnknownQueue(ResultCodes.CloseQueueResult obj) {
        checkPredicates(new CloseQueueResultTest.UnknownQueueTest(), obj);
    }

    static void checkInvalidQueue(ResultCodes.CloseQueueResult obj) {
        checkPredicates(new CloseQueueResultTest.InvalidQueueTest(), obj);
    }

    @Test
    void successTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.SUCCESS);
        checkSuccess(ResultCodes.CloseQueueResult.SUCCESS);
        checkGeneric(ResultCodes.CloseQueueResult.SUCCESS);
    }

    @Test
    void timeOutTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.TIMEOUT);
        checkTimeout(ResultCodes.CloseQueueResult.TIMEOUT);
        checkGeneric(ResultCodes.CloseQueueResult.TIMEOUT);
    }

    @Test
    void notConnectedTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.NOT_CONNECTED);
        checkNotConnected(ResultCodes.CloseQueueResult.NOT_CONNECTED);
        checkGeneric(ResultCodes.CloseQueueResult.NOT_CONNECTED);
    }

    @Test
    void canceledTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.CANCELED);
        checkCanceled(ResultCodes.CloseQueueResult.CANCELED);
        checkGeneric(ResultCodes.CloseQueueResult.CANCELED);
    }

    @Test
    void notSupportedTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.NOT_SUPPORTED);
        checkNotSupported(ResultCodes.CloseQueueResult.NOT_SUPPORTED);
        checkGeneric(ResultCodes.CloseQueueResult.NOT_SUPPORTED);
    }

    @Test
    void refusedTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.REFUSED);
        checkRefused(ResultCodes.CloseQueueResult.REFUSED);
        checkGeneric(ResultCodes.CloseQueueResult.REFUSED);
    }

    @Test
    void invalidArgumentTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.INVALID_ARGUMENT);
        checkInvalidArgument(ResultCodes.CloseQueueResult.INVALID_ARGUMENT);
        checkGeneric(ResultCodes.CloseQueueResult.INVALID_ARGUMENT);
    }

    @Test
    void notReadyTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.NOT_READY);
        checkNotReady(ResultCodes.CloseQueueResult.NOT_READY);
        checkGeneric(ResultCodes.CloseQueueResult.NOT_READY);
    }

    @Test
    void unknownTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.UNKNOWN);
        checkUnknown(ResultCodes.CloseQueueResult.UNKNOWN);
        checkGeneric(ResultCodes.CloseQueueResult.UNKNOWN);
    }

    @Test
    void alreadyClosedTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.ALREADY_CLOSED);
        checkAlreadyClosed(ResultCodes.CloseQueueResult.ALREADY_CLOSED);
    }

    @Test
    void alreadyInProgressTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.ALREADY_IN_PROGRESS);
        checkAlreadyInProgress(ResultCodes.CloseQueueResult.ALREADY_IN_PROGRESS);
    }

    @Test
    void unknownQueueTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.UNKNOWN_QUEUE);
        checkUnknownQueue(ResultCodes.CloseQueueResult.UNKNOWN_QUEUE);
    }

    @Test
    void invalidQueueTest() {
        checkIfNotGeneric(ResultCodes.CloseQueueResult.INVALID_QUEUE);
        checkInvalidQueue(ResultCodes.CloseQueueResult.INVALID_QUEUE);
    }
}
