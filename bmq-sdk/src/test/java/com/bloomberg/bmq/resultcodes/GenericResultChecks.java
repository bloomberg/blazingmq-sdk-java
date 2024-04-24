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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.bloomberg.bmq.ResultCodes;

class GenericResultChecks {

    static void checkPredicates(GenericResultTest.BaseTest test, ResultCodes.GenericCode obj) {
        test.isSuccessTest(obj);
        test.isFailureTest(obj);
        test.isCanceledTest(obj);
        test.isInvalidArgumentTest(obj);
        test.isNotConnectedTest(obj);
        test.isNotReadyTest(obj);
        test.isNotSupportedTest(obj);
        test.isRefusedTest(obj);
        test.isTimeoutTest(obj);
        test.isUnknownTest(obj);
    }

    static void checkIfGeneric(ResultCodes.GenericCode obj) {
        assertEquals(obj, obj.getGeneralResult());
    }

    static void checkIfNotGeneric(ResultCodes.GenericCode obj) {
        assertNotEquals(obj, obj.getGeneralResult());
    }

    static void checkSuccess(ResultCodes.GenericCode obj) {
        checkPredicates(new GenericResultTest.SuccessTest(), obj);
    }

    static void checkTimeout(ResultCodes.GenericCode obj) {
        checkPredicates(new GenericResultTest.TimeoutTest(), obj);
    }

    static void checkNotConnected(ResultCodes.GenericCode obj) {
        checkPredicates(new GenericResultTest.NotConnectedTest(), obj);
    }

    static void checkCanceled(ResultCodes.GenericCode obj) {
        checkPredicates(new GenericResultTest.CanceledTest(), obj);
    }

    static void checkNotSupported(ResultCodes.GenericCode obj) {
        checkPredicates(new GenericResultTest.NotSupportedTest(), obj);
    }

    static void checkRefused(ResultCodes.GenericCode obj) {
        checkPredicates(new GenericResultTest.RefusedTest(), obj);
    }

    static void checkInvalidArgument(ResultCodes.GenericCode obj) {
        checkPredicates(new GenericResultTest.InvalidArgumentTest(), obj);
    }

    static void checkNotReady(ResultCodes.GenericCode obj) {
        checkPredicates(new GenericResultTest.NotReadyTest(), obj);
    }

    static void checkUnknown(ResultCodes.GenericCode obj) {
        checkPredicates(new GenericResultTest.UnknownTest(), obj);
    }
}
