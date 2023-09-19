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
    // This class provides a simple validation of a logical expression used in BMQ subscription.

    private String errorMessage;

    // The maximum number of operators allowed in expression.
    // NOTE: must be in sync with C++ implementation (SimpleEvaluator::k_MAX_OPERATORS)
    public static final int MAX_OPERATORS = 10;
    // The maximum number of properties allowed in expression.
    // NOTE: must be in sync with C++ implementation (SimpleEvaluator::k_MAX_PROPERTIES)
    public static final int MAX_PROPERTIES = 10;

    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * @param expression
     * @return 'true' if successfull or 'false' otherwise
     * @throws IOException
     */
    public boolean validate(java.io.Reader expression) throws IOException {
        // Validate given 'expression' and returns 'true' if successfull or 'false'
        // otherwise.

        errorMessage = null;
        ExpressionScanner scanner = new ExpressionScanner(expression);

        // Loop through tokens from scanner
        Token prevToken = null;
        int paranthesisCounter = 0;
        int tokensCounter = 0;
        int operatorsCounter = 0;
        int propertiesCounter = 0;
        do {
            // Get token
            Token token = scanner.yylex();

            Token.Type tokenType = token.getType();
            if (tokenType == Token.Type.INVALID) { // Check syntax error detected by scanned
                StringBuilder sb =
                        new StringBuilder("syntax error, unexpected invalid character \"");
                errorMessage =
                        sb.append(token.getValue())
                                .append("\" at offset ")
                                .append(token.getPosition())
                                .toString();
                return false;
            } else if (tokenType == Token.Type.LPAR) { // Check open and close paranthesis
                paranthesisCounter++;
            } else if (tokenType == Token.Type.RPAR) {
                if (paranthesisCounter == 0
                        || (prevToken != null && prevToken.getType() == Token.Type.LPAR)) {
                    errorMessage =
                            "syntax error, unexpected \")\" at offset " + token.getPosition();
                    return false;
                }
                paranthesisCounter--;
            } else if (token.isLiteralOrProperty()) { // Check literal or property
                if (tokenType == Token.Type.INTEGER) { // Check integer overflow
                    try {
                        Long.parseLong(token.getValue());
                    } catch (NumberFormatException e) {
                        // Overflow occured, other format issues are checked by scanner
                        errorMessage = "integer overflow at offset " + token.getPosition();
                        return false;
                    }
                } else if (tokenType == Token.Type.PROPERTY) {
                    if (++propertiesCounter > MAX_PROPERTIES) { // Check max number of properties
                        errorMessage = "expression uses too many properties";
                        return false;
                    }
                }
                if (prevToken != null
                        && prevToken.isLiteralOrProperty()) { // Check for two consequent literals
                    errorMessage =
                            "syntax error, missed operation at offset " + token.getPosition();
                    return false;
                }
            } else if (token.isOperation()) { // Check operation
                // Check for consequent two arguments operations
                if (tokenType != Token.Type.LOGICAL_NOT_OP
                        && (prevToken != null && prevToken.isOperation())) {
                    StringBuilder sb = new StringBuilder("syntax error, unexpected \"");
                    errorMessage =
                            sb.append(token.getValue())
                                    .append("\" at offset ")
                                    .append(token.getPosition())
                                    .toString();
                    return false;
                }
                // Check for max operations
                if (++operatorsCounter > MAX_OPERATORS) {
                    errorMessage = "too many operators";
                    return false;
                }
            } else if (tokenType == Token.Type.END) { // Check expression end
                if (tokensCounter == 0) {
                    errorMessage =
                            "syntax error, unexpected end of expression at offset "
                                    + token.getPosition();
                    return false;
                }
            } else {
                StringBuilder sb = new StringBuilder("syntax error, unexpected \"");
                errorMessage =
                        sb.append(token.getValue())
                                .append("\" at offset ")
                                .append(token.getPosition())
                                .toString();
                return false;
            }

            prevToken = token;
            tokensCounter++;
        } while (!scanner.yyatEOF());

        if (propertiesCounter == 0) {
            errorMessage = "expression does not use any properties";
            return false;
        }
        if (paranthesisCounter > 0) {
            errorMessage = "syntax error, unmatched number of open and close paranthesis";
            return false;
        }

        return true;
    }
}
