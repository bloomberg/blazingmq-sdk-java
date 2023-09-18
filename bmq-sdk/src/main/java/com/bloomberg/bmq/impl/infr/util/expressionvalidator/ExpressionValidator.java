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

    @SuppressWarnings("fallthrough")
    public boolean validate(java.io.Reader expression) throws IOException {
        // This class validates given 'expression' and returns 'true' if successfull or 'false'
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

            switch (token.getType()) {
                case INVALID:
                    errorMessage =
                            "syntax error, unexpected invalid character \""
                                    + token.getValue()
                                    + "\" at offset "
                                    + token.getPosition();
                    return false;

                case LPAR:
                    paranthesisCounter++;
                    break;
                case RPAR:
                    if (paranthesisCounter == 0
                            || (prevToken != null && prevToken.getType() == Token.Type.LPAR)) {
                        errorMessage =
                                "syntax error, unexpected \")\" at offset " + token.getPosition();
                        return false;
                    }
                    paranthesisCounter--;
                    break;
                case INTEGER:
                    try {
                        Long.parseLong(token.getValue());
                    } catch (NumberFormatException e) {
                        // Overflow occured, other format issues are checked by scanner
                        errorMessage = "integer overflow at offset " + token.getPosition();
                        return false;
                    }
                case PROPERTY:
                case BOOL:
                case STRING:
                    if (prevToken != null && prevToken.isLiteralOrProperty()) {
                        errorMessage =
                                "syntax error, missed operation at offset " + token.getPosition();
                        return false;
                    }
                    if (token.getType() == Token.Type.PROPERTY) propertiesCounter++;

                    break;
                case LOGICAL_OP:
                case COMPAR_OP:
                case MATH_OP:
                    if (prevToken != null && prevToken.isOperation()) {
                        StringBuilder sb = new StringBuilder("syntax error, unexpected \"");
                        errorMessage =
                                sb.append(token.getValue())
                                        .append("\" at offset ")
                                        .append(token.getPosition())
                                        .toString();
                        return false;
                    }
                case LOGICAL_NOT_OP:
                    if (++operatorsCounter > MAX_OPERATORS) {
                        errorMessage = "too many operators";
                        return false;
                    }
                    break;
                default:
                    break;
            }

            prevToken = token;
            tokensCounter++;
        } while (!scanner.yyatEOF());

        // Validate counters
        if (tokensCounter == 1 && prevToken.getType() == Token.Type.END) {
            errorMessage =
                    "syntax error, unexpected end of expression at offset "
                            + prevToken.getPosition();
            return false;
        }
        if (paranthesisCounter > 0) {
            errorMessage = "syntax error, unmatched number of open and close paranthesis";
            return false;
        }
        if (propertiesCounter == 0) {
            errorMessage = "expression does not use any properties";
            return false;
        }
        if (propertiesCounter > MAX_PROPERTIES) {
            errorMessage = "expression uses too many properties";
            return false;
        }

        return true;
    }
}
