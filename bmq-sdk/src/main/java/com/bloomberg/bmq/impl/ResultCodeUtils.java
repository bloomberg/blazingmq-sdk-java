/*
 * Copyright 2022 Bloomberg Finance L.P.
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
package com.bloomberg.bmq.impl;

import com.bloomberg.bmq.ResultCodes;
import com.bloomberg.bmq.ResultCodes.GenericResult;
import com.bloomberg.bmq.impl.infr.msg.StatusCategory;

public class ResultCodeUtils {
    public static GenericResult toGenericResult(StatusCategory statusCategory) {
        switch (statusCategory) {
            case E_SUCCESS:
                return GenericResult.SUCCESS;
            case E_UNKNOWN:
                return GenericResult.UNKNOWN;
            case E_TIMEOUT:
                return GenericResult.TIMEOUT;
            case E_NOT_CONNECTED:
                return GenericResult.NOT_CONNECTED;
            case E_NOT_SUPPORTED:
                return GenericResult.NOT_SUPPORTED;
            case E_REFUSED:
                return GenericResult.REFUSED;
            case E_INVALID_ARGUMENT:
                return GenericResult.INVALID_ARGUMENT;
            case E_NOT_READY:
                return GenericResult.NOT_READY;
            case E_CANCELED:
                return GenericResult.CANCELED;
            default:
                throw new IllegalStateException("Unknown enum"); // Should never happen
        }
    }

    public static StatusCategory fromGenericResult(GenericResult genericResult) {

        switch (genericResult) {
            case SUCCESS:
                return StatusCategory.E_SUCCESS;
            case UNKNOWN:
                return StatusCategory.E_UNKNOWN;
            case TIMEOUT:
                return StatusCategory.E_TIMEOUT;
            case NOT_CONNECTED:
                return StatusCategory.E_NOT_CONNECTED;
            case NOT_SUPPORTED:
                return StatusCategory.E_NOT_SUPPORTED;
            case REFUSED:
                return StatusCategory.E_REFUSED;
            case INVALID_ARGUMENT:
                return StatusCategory.E_INVALID_ARGUMENT;
            case NOT_READY:
                return StatusCategory.E_NOT_READY;
            case CANCELED:
                return StatusCategory.E_CANCELED;
            default:
                throw new IllegalStateException("Unknown enum"); // Should never happen
        }
    }

    public static ResultCodes.AckResult ackResultFromInt(int result) {
        switch (result) {
            case 0:
                return ResultCodes.AckResult.SUCCESS;
            case 1:
                return ResultCodes.AckResult.LIMIT_MESSAGES;
            case 2:
                return ResultCodes.AckResult.LIMIT_BYTES;
            case 6:
                return ResultCodes.AckResult.STORAGE_FAILURE;
            case 5: // Value of '5' is reserved.
            default:
                return ResultCodes.AckResult.UNKNOWN;
        }
    }

    public static int intFromAckResult(ResultCodes.AckResult result) {
        switch (result) {
            case SUCCESS:
                return 0;
            case LIMIT_MESSAGES:
                return 1;
            case LIMIT_BYTES:
                return 2;
            case STORAGE_FAILURE:
                return 6;
            case UNKNOWN: // Value of '5' is reserved.
            default:
                return 5;
        }
    }

    private ResultCodeUtils() {
        throw new IllegalStateException("Utility class");
    }
}
