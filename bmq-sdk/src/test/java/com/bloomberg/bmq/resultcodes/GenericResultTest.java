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
import static com.bloomberg.bmq.resultcodes.GenericResultChecks.checkIfGeneric;
import static com.bloomberg.bmq.resultcodes.GenericResultChecks.checkInvalidArgument;
import static com.bloomberg.bmq.resultcodes.GenericResultChecks.checkNotConnected;
import static com.bloomberg.bmq.resultcodes.GenericResultChecks.checkNotReady;
import static com.bloomberg.bmq.resultcodes.GenericResultChecks.checkNotSupported;
import static com.bloomberg.bmq.resultcodes.GenericResultChecks.checkRefused;
import static com.bloomberg.bmq.resultcodes.GenericResultChecks.checkTimeout;
import static com.bloomberg.bmq.resultcodes.GenericResultChecks.checkUnknown;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bloomberg.bmq.ResultCodes;
import org.junit.jupiter.api.Test;

// ================================================================
// GenericResult tests
// ================================================================
class GenericResultTest {

    abstract static class BaseTest {
        void isSuccessTest(ResultCodes.GenericCode obj) {
            assertFalse(obj.isSuccess());
        }

        void isFailureTest(ResultCodes.GenericCode obj) {
            assertFalse(obj.isFailure());
        }

        void isCanceledTest(ResultCodes.GenericCode obj) {
            assertFalse(obj.isCanceled());
        }

        void isInvalidArgumentTest(ResultCodes.GenericCode obj) {
            assertFalse(obj.isInvalidArgument());
        }

        void isNotConnectedTest(ResultCodes.GenericCode obj) {
            assertFalse(obj.isNotConnected());
        }

        void isNotReadyTest(ResultCodes.GenericCode obj) {
            assertFalse(obj.isNotReady());
        }

        void isNotSupportedTest(ResultCodes.GenericCode obj) {
            assertFalse(obj.isNotSupproted());
        }

        void isRefusedTest(ResultCodes.GenericCode obj) {
            assertFalse(obj.isRefused());
        }

        void isTimeoutTest(ResultCodes.GenericCode obj) {
            assertFalse(obj.isTimeout());
        }

        void isUnknownTest(ResultCodes.GenericCode obj) {
            assertFalse(obj.isUnknown());
        }
    }

    static class SuccessTest extends BaseTest {
        @Override
        public void isSuccessTest(ResultCodes.GenericCode obj) {
            assertTrue(obj.isSuccess());
        }
    }

    abstract static class FailureStatusTest extends BaseTest {
        @Override
        public void isFailureTest(ResultCodes.GenericCode obj) {
            assertTrue(obj.isFailure());
        }
    }

    static class CanceledTest extends FailureStatusTest {
        @Override
        public void isCanceledTest(ResultCodes.GenericCode obj) {
            assertTrue(obj.isCanceled());
        }
    }

    static class InvalidArgumentTest extends FailureStatusTest {
        @Override
        public void isInvalidArgumentTest(ResultCodes.GenericCode obj) {
            assertTrue(obj.isInvalidArgument());
        }
    }

    static class NotConnectedTest extends FailureStatusTest {
        @Override
        public void isNotConnectedTest(ResultCodes.GenericCode obj) {
            assertTrue(obj.isNotConnected());
        }
    }

    static class NotReadyTest extends FailureStatusTest {
        @Override
        public void isNotReadyTest(ResultCodes.GenericCode obj) {
            assertTrue(obj.isNotReady());
        }
    }

    static class NotSupportedTest extends FailureStatusTest {
        @Override
        public void isNotSupportedTest(ResultCodes.GenericCode obj) {
            assertTrue(obj.isNotSupproted());
        }
    }

    static class RefusedTest extends FailureStatusTest {
        @Override
        public void isRefusedTest(ResultCodes.GenericCode obj) {
            assertTrue(obj.isRefused());
        }
    }

    static class TimeoutTest extends FailureStatusTest {
        @Override
        public void isTimeoutTest(ResultCodes.GenericCode obj) {
            assertTrue(obj.isTimeout());
        }
    }

    static class UnknownTest extends FailureStatusTest {
        @Override
        public void isUnknownTest(ResultCodes.GenericCode obj) {
            assertTrue(obj.isUnknown());
        }
    }

    @Test
    void timeOutTest() {
        checkIfGeneric(ResultCodes.GenericResult.TIMEOUT);
        checkTimeout(ResultCodes.GenericResult.TIMEOUT);
    }

    @Test
    void notConnectedTest() {
        checkIfGeneric(ResultCodes.GenericResult.NOT_CONNECTED);
        checkNotConnected(ResultCodes.GenericResult.NOT_CONNECTED);
    }

    @Test
    void canceledTest() {
        checkIfGeneric(ResultCodes.GenericResult.CANCELED);
        checkCanceled(ResultCodes.GenericResult.CANCELED);
    }

    @Test
    void notSupportedTest() {
        checkIfGeneric(ResultCodes.GenericResult.NOT_SUPPORTED);
        checkNotSupported(ResultCodes.GenericResult.NOT_SUPPORTED);
    }

    @Test
    void refusedTest() {
        checkIfGeneric(ResultCodes.GenericResult.REFUSED);
        checkRefused(ResultCodes.GenericResult.REFUSED);
    }

    @Test
    void invalidArgumentTest() {
        checkIfGeneric(ResultCodes.GenericResult.INVALID_ARGUMENT);
        checkInvalidArgument(ResultCodes.GenericResult.INVALID_ARGUMENT);
    }

    @Test
    void notReadyTest() {
        checkIfGeneric(ResultCodes.GenericResult.NOT_READY);
        checkNotReady(ResultCodes.GenericResult.NOT_READY);
    }

    @Test
    void unknownTest() {
        checkIfGeneric(ResultCodes.GenericResult.UNKNOWN);
        checkUnknown(ResultCodes.GenericResult.UNKNOWN);
    }
}
