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

public class CloseQueueResultChecks {

    public static void checkPredicates(
            CloseQueueResultTest.CloseQueueGenericTest test, ResultCodes.CloseQueueResult obj) {
        test.isAlreadyClosed(obj);
        test.isAlreadyInProgressTest(obj);
        test.isUnknownQueueTest(obj);
        test.isInvalidQueueTest(obj);
    }

    public static void checkGeneric(ResultCodes.CloseQueueResult obj) {
        checkPredicates(new CloseQueueResultTest.CloseQueueGenericTest(), obj);
    }

    public static void checkAlreadyClosed(ResultCodes.CloseQueueResult obj) {
        checkPredicates(new CloseQueueResultTest.AlreadyClosedTest(), obj);
    }

    public static void checkAlreadyInProgress(ResultCodes.CloseQueueResult obj) {
        checkPredicates(new CloseQueueResultTest.AlreadyInProgressTest(), obj);
    }

    public static void checkUnknownQueue(ResultCodes.CloseQueueResult obj) {
        checkPredicates(new CloseQueueResultTest.UnknownQueueTest(), obj);
    }

    public static void checkInvalidQueue(ResultCodes.CloseQueueResult obj) {
        checkPredicates(new CloseQueueResultTest.InvalidQueueTest(), obj);
    }
}
