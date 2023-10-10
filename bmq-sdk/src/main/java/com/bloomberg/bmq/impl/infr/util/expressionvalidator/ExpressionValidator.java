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

import java.io.IOException;

public class ExpressionValidator {
    // This class provides a simple validation of a logical expression used in BlazingMQ
    // subscription.

    // The maximum number of operators allowed in expression.
    // NOTE: must be in sync with C++ implementation (SimpleEvaluator::k_MAX_OPERATORS)
    public static final int MAX_OPERATORS = 10;
    // The maximum number of properties allowed in expression.
    // NOTE: must be in sync with C++ implementation (SimpleEvaluator::k_MAX_PROPERTIES)
    public static final int MAX_PROPERTIES = 10;

    private ExpressionValidator() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * @param expression
     * @return ValidationResult result of expression validation
     * @throws IOException
     */
    public static ValidationResult validate(java.io.Reader expression) throws IOException {
        // Validate given 'expression' and return ValidationResult object with result.
        ExpressionScanner scanner = new ExpressionScanner(expression);

        // Loop through tokens from scanner
        Token prevToken = null;
        int parenthesisCounter = 0;
        int tokensCounter = 0;
        int operatorsCounter = 0;
        int propertiesCounter = 0;
        do {
            // Get token
            Token token = scanner.yylex();

            Token.Type tokenType = token.getType();
            if (tokenType == Token.Type.INVALID) { // Check syntax error detected by scanned
                return ValidationResult.makeInvalidCharacter(token.getValue(), token.getPosition());
            } else if (tokenType == Token.Type.LPAR) { // Check open and close parenthesis
                parenthesisCounter++;
            } else if (tokenType == Token.Type.RPAR) {
                // prevToken != null when parenthesisCounter > 0
                if (parenthesisCounter == 0 || prevToken.getType() == Token.Type.LPAR) {
                    return ValidationResult.makeUnexpectedCharacter(")", token.getPosition());
                }
                parenthesisCounter--;
            } else if (token.isLiteralOrProperty()) { // Check literal or property
                if (tokenType == Token.Type.INTEGER) { // Check integer overflow
                    try {
                        Long.parseLong(token.getValue());
                    } catch (NumberFormatException e) {
                        // Overflow occurred, other format issues are checked by scanner
                        return ValidationResult.makeIntegerOverflow(token.getPosition());
                    }
                } else if (tokenType == Token.Type.PROPERTY) {
                    if (++propertiesCounter > MAX_PROPERTIES) { // Check max number of properties
                        return ValidationResult.makeTooManyProperties();
                    }
                }
                if (prevToken != null
                        && prevToken.isLiteralOrProperty()) { // Check for two consequent literals
                    return ValidationResult.makeMissedOperation(token.getPosition());
                }
            } else if (token.isOperation()) { // Check operation
                // Check for consequent two arguments operations
                if (tokenType != Token.Type.LOGICAL_NOT_OP
                        && (prevToken != null && prevToken.isOperation())) {
                    return ValidationResult.makeUnexpectedCharacter(
                            token.getValue(), token.getPosition());
                }
                // Check for max operations
                if (++operatorsCounter > MAX_OPERATORS) {
                    return ValidationResult.makeTooManyOperators();
                }
            } else if (tokenType == Token.Type.END) { // Check expression end
                if (tokensCounter == 0) {
                    return ValidationResult.makeUnexpectedEnd(token.getPosition());
                }
            } else {
                return ValidationResult.makeUnexpectedCharacter(
                        token.getValue(), token.getPosition());
            }

            prevToken = token;
            tokensCounter++;
        } while (!scanner.yyatEOF());

        if (propertiesCounter == 0) {
            return ValidationResult.makeNoProperties();
        }
        if (parenthesisCounter > 0) {
            return ValidationResult.makeUnmatchedParanthesis();
        }

        return ValidationResult.makeSuccess();
    }
}
