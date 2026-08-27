/*
 * Copyright 2026 Bloomberg Finance L.P.
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
package com.bloomberg.bmq.fuzz;

import com.bloomberg.bmq.impl.infr.util.expressionvalidator.ExpressionValidator;
import com.bloomberg.bmq.impl.infr.util.expressionvalidator.ValidationResult;
import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import java.io.IOException;
import java.io.StringReader;

/**
 * Fuzz target for {@link ExpressionValidator}, the subscription-expression checker.
 *
 * <p>The validator pairs a JFlex-generated scanner with a hand-written token loop tracking
 * parenthesis depth, operator/property counts and the previous token. Several branches of that loop
 * dereference {@code prevToken} under assumptions justified only by a comment.
 */
public final class ExpressionValidatorFuzzer {

    private ExpressionValidatorFuzzer() {
        throw new IllegalStateException("Utility class");
    }

    public static void fuzzerTestOneInput(FuzzedDataProvider data) {
        final String expression = data.consumeRemainingAsString();

        final ValidationResult result;
        try {
            result = ExpressionValidator.validate(new StringReader(expression));
        } catch (IOException e) {
            return;
        }

        if (result == null) {
            throw new IllegalStateException("validate() returned null for: " + expression);
        }

        // The message is what gets surfaced to the user; an empty one turns a rejected
        // subscription into an unexplainable error.
        if (!result.isSuccess()
                && (result.getErrorMessage() == null || result.getErrorMessage().isEmpty())) {
            throw new IllegalStateException(
                    "Validation failed without a message for: " + expression);
        }
    }
}
