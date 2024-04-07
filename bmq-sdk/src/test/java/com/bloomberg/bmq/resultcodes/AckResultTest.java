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

import static com.bloomberg.bmq.resultcodes.AckResultChecks.checkGeneric;
import static com.bloomberg.bmq.resultcodes.AckResultChecks.checkLimitBytes;
import static com.bloomberg.bmq.resultcodes.AckResultChecks.checkLimitMessages;
import static com.bloomberg.bmq.resultcodes.AckResultChecks.checkStorageFailure;
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
public class AckResultTest {
    public static class AckGenericTest {
        public void isLimitMessages(ResultCodes.AckCode obj) {
            assertFalse(obj.isLimitMessages());
        }

        public void isLimitBytes(ResultCodes.AckCode obj) {
            assertFalse(obj.isLimitBytes());
        }

        public void isStorageFailure(ResultCodes.AckCode obj) {
            assertFalse(obj.isStorageFailure());
        }
    }

    public static class LimitMessagesTest extends AckGenericTest {
        @Override
        public void isLimitMessages(ResultCodes.AckCode obj) {
            assertTrue(obj.isLimitMessages());
        }
    }

    public static class LimitBytesTest extends AckGenericTest {
        @Override
        public void isLimitBytes(ResultCodes.AckCode obj) {
            assertTrue(obj.isLimitBytes());
        }
    }

    public static class StorageFailureTest extends AckGenericTest {
        @Override
        public void isStorageFailure(ResultCodes.AckCode obj) {
            assertTrue(obj.isStorageFailure());
        }
    }

    @Test
    public void successTest() {
        checkIfNotGeneric(ResultCodes.AckResult.SUCCESS);
        checkSuccess(ResultCodes.AckResult.SUCCESS);
        checkGeneric(ResultCodes.AckResult.SUCCESS);
    }

    @Test
    public void timeOutTest() {
        checkIfNotGeneric(ResultCodes.AckResult.TIMEOUT);
        checkTimeout(ResultCodes.AckResult.TIMEOUT);
        checkGeneric(ResultCodes.AckResult.TIMEOUT);
    }

    @Test
    public void notConnectedTest() {
        checkIfNotGeneric(ResultCodes.AckResult.NOT_CONNECTED);
        checkNotConnected(ResultCodes.AckResult.NOT_CONNECTED);
        checkGeneric(ResultCodes.AckResult.NOT_CONNECTED);
    }

    @Test
    public void canceledTest() {
        checkIfNotGeneric(ResultCodes.AckResult.CANCELED);
        checkCanceled(ResultCodes.AckResult.CANCELED);
        checkGeneric(ResultCodes.AckResult.CANCELED);
    }

    @Test
    public void notSupportedTest() {
        checkIfNotGeneric(ResultCodes.AckResult.NOT_SUPPORTED);
        checkNotSupported(ResultCodes.AckResult.NOT_SUPPORTED);
        checkGeneric(ResultCodes.AckResult.NOT_SUPPORTED);
    }

    @Test
    public void refusedTest() {
        checkIfNotGeneric(ResultCodes.AckResult.REFUSED);
        checkRefused(ResultCodes.AckResult.REFUSED);
        checkGeneric(ResultCodes.AckResult.REFUSED);
    }

    @Test
    public void invalidArgumentTest() {
        checkIfNotGeneric(ResultCodes.AckResult.INVALID_ARGUMENT);
        checkInvalidArgument(ResultCodes.AckResult.INVALID_ARGUMENT);
        checkGeneric(ResultCodes.AckResult.INVALID_ARGUMENT);
    }

    @Test
    public void notReadyTest() {
        checkIfNotGeneric(ResultCodes.AckResult.NOT_READY);
        checkNotReady(ResultCodes.AckResult.NOT_READY);
        checkGeneric(ResultCodes.AckResult.NOT_READY);
    }

    @Test
    public void unknownTest() {
        checkIfNotGeneric(ResultCodes.AckResult.UNKNOWN);
        checkUnknown(ResultCodes.AckResult.UNKNOWN);
        checkGeneric(ResultCodes.AckResult.UNKNOWN);
    }

    @Test
    public void limitMessagesTest() {
        checkIfNotGeneric(ResultCodes.AckResult.LIMIT_MESSAGES);
        checkLimitMessages(ResultCodes.AckResult.LIMIT_MESSAGES);
    }

    @Test
    public void limitBytesTest() {
        checkIfNotGeneric(ResultCodes.AckResult.LIMIT_BYTES);
        checkLimitBytes(ResultCodes.AckResult.LIMIT_BYTES);
    }

    @Test
    public void storageFailureTest() {
        checkIfNotGeneric(ResultCodes.AckResult.STORAGE_FAILURE);
        checkStorageFailure(ResultCodes.AckResult.STORAGE_FAILURE);
    }
}
