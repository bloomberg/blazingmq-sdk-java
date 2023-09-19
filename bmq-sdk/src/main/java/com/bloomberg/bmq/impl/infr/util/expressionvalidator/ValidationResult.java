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
package com.bloomberg.bmq.impl.infr.util.expressionvalidator;

import javax.annotation.concurrent.Immutable;

@Immutable
public class ValidationResult {
    private final String errorMessage;
    private final boolean success;

    public ValidationResult(boolean success, String errorMessage) {
        this.errorMessage = errorMessage;
        this.success = success;
    }

    /** @return String with error message if it exists or null otherwise */
    public String getErrorMessage() {
        return errorMessage;
    }

    /** @return boolean {@code true} on success, and {@code false} in case of failure */
    public boolean isSuccess() {
        return success;
    }
}
