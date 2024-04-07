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

import com.bloomberg.bmq.ResultCodes;

public class OpenQueueResultChecks {

    public static void checkExtraPredicates(
            OpenQueueResultTest.OpenQueueGenericTest test, ResultCodes.OpenQueueCode obj) {
        test.isAlreadyInProgressTest(obj);
        test.isAlreadyOpenedTest(obj);
        test.isInvalidFlagsTest(obj);
        test.isInvalidUriTest(obj);
        test.isQueueIdNotUniqueTest(obj);
    }

    public static void checkGeneric(ResultCodes.OpenQueueCode obj) {
        checkExtraPredicates(new OpenQueueResultTest.OpenQueueGenericTest(), obj);
    }

    public static void checkAlreadyInProgress(ResultCodes.OpenQueueCode obj) {
        checkExtraPredicates(new OpenQueueResultTest.AlreadyInProgressTest(), obj);
    }

    public static void checkAlreadyOpened(ResultCodes.OpenQueueCode obj) {
        checkExtraPredicates(new OpenQueueResultTest.AlreadyOpenedTest(), obj);
    }

    public static void checkInvalidFlags(ResultCodes.OpenQueueCode obj) {
        checkExtraPredicates(new OpenQueueResultTest.InvalidFlagsTest(), obj);
    }

    public static void checkInvalidUri(ResultCodes.OpenQueueCode obj) {
        checkExtraPredicates(new OpenQueueResultTest.InvalidUriTest(), obj);
    }

    public static void checkQueueIdNotUnique(ResultCodes.OpenQueueCode obj) {
        checkExtraPredicates(new OpenQueueResultTest.QueueIdNotUniqueTest(), obj);
    }
}
