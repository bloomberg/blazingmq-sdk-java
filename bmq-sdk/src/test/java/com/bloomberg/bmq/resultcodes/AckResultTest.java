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
// AckResult tests
// ================================================================
class AckResultTest {
    static class AckGenericTest {
        void isLimitMessages(ResultCodes.AckCode obj) {
            assertFalse(obj.isLimitMessages());
        }

        void isLimitBytes(ResultCodes.AckCode obj) {
            assertFalse(obj.isLimitBytes());
        }

        void isStorageFailure(ResultCodes.AckCode obj) {
            assertFalse(obj.isStorageFailure());
        }
    }

    static class LimitMessagesTest extends AckGenericTest {
        @Override
        public void isLimitMessages(ResultCodes.AckCode obj) {
            assertTrue(obj.isLimitMessages());
        }
    }

    static class LimitBytesTest extends AckGenericTest {
        @Override
        public void isLimitBytes(ResultCodes.AckCode obj) {
            assertTrue(obj.isLimitBytes());
        }
    }

    static class StorageFailureTest extends AckGenericTest {
        @Override
        public void isStorageFailure(ResultCodes.AckCode obj) {
            assertTrue(obj.isStorageFailure());
        }
    }

    static void checkExtraPredicates(AckResultTest.AckGenericTest test, ResultCodes.AckResult obj) {
        test.isLimitMessages(obj);
        test.isLimitBytes(obj);
        test.isStorageFailure(obj);
    }

    static void checkGeneric(ResultCodes.AckResult obj) {
        checkExtraPredicates(new AckResultTest.AckGenericTest(), obj);
    }

    static void checkLimitMessages(ResultCodes.AckResult obj) {
        checkExtraPredicates(new AckResultTest.LimitMessagesTest(), obj);
    }

    static void checkLimitBytes(ResultCodes.AckResult obj) {
        checkExtraPredicates(new AckResultTest.LimitBytesTest(), obj);
    }

    static void checkStorageFailure(ResultCodes.AckResult obj) {
        checkExtraPredicates(new AckResultTest.StorageFailureTest(), obj);
    }

    @Test
    void successTest() {
        checkIfNotGeneric(ResultCodes.AckResult.SUCCESS);
        checkSuccess(ResultCodes.AckResult.SUCCESS);
        checkGeneric(ResultCodes.AckResult.SUCCESS);
    }

    @Test
    void timeOutTest() {
        checkIfNotGeneric(ResultCodes.AckResult.TIMEOUT);
        checkTimeout(ResultCodes.AckResult.TIMEOUT);
        checkGeneric(ResultCodes.AckResult.TIMEOUT);
    }

    @Test
    void notConnectedTest() {
        checkIfNotGeneric(ResultCodes.AckResult.NOT_CONNECTED);
        checkNotConnected(ResultCodes.AckResult.NOT_CONNECTED);
        checkGeneric(ResultCodes.AckResult.NOT_CONNECTED);
    }

    @Test
    void canceledTest() {
        checkIfNotGeneric(ResultCodes.AckResult.CANCELED);
        checkCanceled(ResultCodes.AckResult.CANCELED);
        checkGeneric(ResultCodes.AckResult.CANCELED);
    }

    @Test
    void notSupportedTest() {
        checkIfNotGeneric(ResultCodes.AckResult.NOT_SUPPORTED);
        checkNotSupported(ResultCodes.AckResult.NOT_SUPPORTED);
        checkGeneric(ResultCodes.AckResult.NOT_SUPPORTED);
    }

    @Test
    void refusedTest() {
        checkIfNotGeneric(ResultCodes.AckResult.REFUSED);
        checkRefused(ResultCodes.AckResult.REFUSED);
        checkGeneric(ResultCodes.AckResult.REFUSED);
    }

    @Test
    void invalidArgumentTest() {
        checkIfNotGeneric(ResultCodes.AckResult.INVALID_ARGUMENT);
        checkInvalidArgument(ResultCodes.AckResult.INVALID_ARGUMENT);
        checkGeneric(ResultCodes.AckResult.INVALID_ARGUMENT);
    }

    @Test
    void notReadyTest() {
        checkIfNotGeneric(ResultCodes.AckResult.NOT_READY);
        checkNotReady(ResultCodes.AckResult.NOT_READY);
        checkGeneric(ResultCodes.AckResult.NOT_READY);
    }

    @Test
    void unknownTest() {
        checkIfNotGeneric(ResultCodes.AckResult.UNKNOWN);
        checkUnknown(ResultCodes.AckResult.UNKNOWN);
        checkGeneric(ResultCodes.AckResult.UNKNOWN);
    }

    @Test
    void limitMessagesTest() {
        checkIfNotGeneric(ResultCodes.AckResult.LIMIT_MESSAGES);
        checkLimitMessages(ResultCodes.AckResult.LIMIT_MESSAGES);
    }

    @Test
    void limitBytesTest() {
        checkIfNotGeneric(ResultCodes.AckResult.LIMIT_BYTES);
        checkLimitBytes(ResultCodes.AckResult.LIMIT_BYTES);
    }

    @Test
    void storageFailureTest() {
        checkIfNotGeneric(ResultCodes.AckResult.STORAGE_FAILURE);
        checkStorageFailure(ResultCodes.AckResult.STORAGE_FAILURE);
    }
}
