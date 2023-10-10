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

    /** @return ValidationResult for successfull validation */
    public static ValidationResult makeSuccess() {
        return new ValidationResult(true, null);
    }

    /** @return ValidationResult for invalid character error */
    public static ValidationResult makeInvalidCharacter(String value, Long position) {
        StringBuilder sb = new StringBuilder("syntax error, unexpected invalid character \"");
        sb.append(value).append("\" at offset ").append(position);

        return new ValidationResult(false, sb.toString());
    }

    /** @return ValidationResult for unexpected character error */
    public static ValidationResult makeUnexpectedCharacter(String value, Long position) {
        StringBuilder sb = new StringBuilder("syntax error, unexpected \"");
        sb.append(value).append("\" at offset ").append(position);

        return new ValidationResult(false, sb.toString());
    }

    /** @return ValidationResult for integer overflow error */
    public static ValidationResult makeIntegerOverflow(Long position) {
        return new ValidationResult(false, "integer overflow at offset " + position);
    }

    /** @return ValidationResult for too many properties error */
    public static ValidationResult makeTooManyProperties() {
        return new ValidationResult(false, "expression uses too many properties");
    }

    /** @return ValidationResult for missed operation error */
    public static ValidationResult makeMissedOperation(Long position) {
        return new ValidationResult(false, "syntax error, missed operation at offset " + position);
    }

    /** @return ValidationResult for too many operators */
    public static ValidationResult makeTooManyOperators() {
        return new ValidationResult(false, "too many operators");
    }

    /** @return ValidationResult for unexpected end of expression error */
    public static ValidationResult makeUnexpectedEnd(Long position) {
        return new ValidationResult(
                false, "syntax error, unexpected end of expression at offset " + position);
    }

    /** @return ValidationResult for too no properties error */
    public static ValidationResult makeNoProperties() {
        return new ValidationResult(false, "expression does not use any properties");
    }

    /** @return ValidationResult for unmatched paranthesis error */
    public static ValidationResult makeUnmatchedParanthesis() {
        return new ValidationResult(
                false, "syntax error, unmatched number of open and close paranthesis");
    }
}
