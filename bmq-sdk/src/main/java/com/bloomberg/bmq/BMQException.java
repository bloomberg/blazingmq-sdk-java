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
package com.bloomberg.bmq;

/**
 * A {@link RuntimeException} which is raised when any failure happens during BlazingMQ SDK call
 * execution.
 */
@SuppressWarnings("serial")
public class BMQException extends RuntimeException {

    private final ResultCodes.GenericCode result;

    public BMQException(Exception ex) {
        this(ex, ResultCodes.GenericResult.UNKNOWN);
    }

    public BMQException(String message) {
        this(message, ResultCodes.GenericResult.UNKNOWN);
    }

    public BMQException(String message, Throwable cause) {
        this(message, cause, ResultCodes.GenericResult.UNKNOWN);
    }

    public BMQException(Exception ex, ResultCodes.GenericCode code) {
        super(ex);
        result = code;
    }

    public BMQException(String message, ResultCodes.GenericCode code) {
        super(message);
        result = code;
    }

    public BMQException(String message, Throwable cause, ResultCodes.GenericCode code) {
        super(message, cause);
        result = code;
    }

    public ResultCodes.GenericCode code() {
        return result;
    }
}
