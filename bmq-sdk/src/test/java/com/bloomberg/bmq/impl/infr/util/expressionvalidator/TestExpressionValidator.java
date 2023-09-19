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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestExpressionValidator {
    private String inputExpression;
    private Boolean expectedResult;
    private String expectedErrorMessage;

    public TestExpressionValidator(
            String inputExpression, Boolean expectedResult, String expectedErrorMessage) {
        this.inputExpression = inputExpression;
        this.expectedResult = expectedResult;
        this.expectedErrorMessage = expectedErrorMessage;
    }

    private static String makeTooManyOperators() {
        int numOperators = ExpressionValidator.MAX_OPERATORS + 1;
        StringBuilder builder = new StringBuilder(numOperators);
        for (int i = 0; i < numOperators; i++) builder.append("!");
        builder.append("var1");
        return builder.toString();
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testArguments() {
        return Arrays.asList(
                new Object[][] {

                    // -------------------------------------------------------------------------------------------------------------------
                    // Correct expressions
                    // -------------------------------------------------------------------------------------------------------------------
                    {"CustomerId == 1000", true, null},
                    {"CustomerId == 2000 && DestinationId == \"ABC\"", true, null},
                    {"CustomerId >= 1000 && CustomerId <= 2000", true, null},
                    {"((a*x)+b)>0", true, null},

                    // NOTE: the following tests must be in sync with C++ implementation
                    // (bmqeval_simpleevaluator.t.cpp, test3_evaluation() method)
                    {"b_true", true, null},

                    // integer comparisons
                    {"i64_42 == 42", true, null},
                    {"i64_42 != 42", true, null},
                    {"i64_42 < 41", true, null},
                    {"i64_42 < 42", true, null},
                    {"i64_42 < 43", true, null},
                    {"i64_42 > 41", true, null},
                    {"i64_42 > 42", true, null},
                    {"i64_42 > 43", true, null},
                    {"i64_42 <= 41", true, null},
                    {"i64_42 <= 42", true, null},
                    {"i64_42 <= 43", true, null},
                    {"i64_42 >= 41", true, null},
                    {"i64_42 >= 42", true, null},
                    {"i64_42 >= 43", true, null},

                    // reversed
                    {"42 == i64_42", true, null},
                    {"i64_42 != 42", true, null},
                    {"41 < i64_42", true, null},
                    {"42 < i64_42", true, null},
                    {"43 < i64_42", true, null},
                    {"41 > i64_42", true, null},
                    {"42 > i64_42", true, null},
                    {"43 > i64_42", true, null},
                    {"41 <= i64_42", true, null},
                    {"42 <= i64_42", true, null},
                    {"43 <= i64_42", true, null},
                    {"41 >= i64_42", true, null},
                    {"42 >= i64_42", true, null},
                    {"43 >= i64_42", true, null},

                    // mixed integer types
                    {"i_42 == 42", true, null},

                    // string comparisons
                    {"s_foo == \"foo\"", true, null},
                    {"s_foo != \"foo\"", true, null},
                    {"s_foo < \"bar\"", true, null},
                    {"s_foo < \"foo\"", true, null},
                    {"s_foo < \"zig\"", true, null},
                    {"s_foo > \"bar\"", true, null},
                    {"s_foo > \"foo\"", true, null},
                    {"s_foo > \"zig\"", true, null},
                    {"s_foo <= \"bar\"", true, null},
                    {"s_foo <= \"foo\"", true, null},
                    {"s_foo <= \"zig\"", true, null},
                    {"s_foo >= \"bar\"", true, null},
                    {"s_foo >= \"foo\"", true, null},
                    {"s_foo >= \"zig\"", true, null},

                    // string comparisons, reversed
                    {"\"foo\" == s_foo", true, null},
                    {"s_foo != \"foo\"", true, null},
                    {"\"bar\" < s_foo", true, null},
                    {"\"foo\" < s_foo", true, null},
                    {"\"zig\" < s_foo", true, null},
                    {"\"bar\" > s_foo", true, null},
                    {"\"foo\" > s_foo", true, null},
                    {"\"zig\" > s_foo", true, null},
                    {"\"bar\" <= s_foo", true, null},
                    {"\"foo\" <= s_foo", true, null},
                    {"\"zig\" <= s_foo", true, null},
                    {"\"bar\" >= s_foo", true, null},
                    {"\"foo\" >= s_foo", true, null},
                    {"\"zig\" >= s_foo", true, null},

                    // logical operators
                    {"!b_true", true, null},
                    {"~b_true", true, null},
                    {"b_true && i64_42 == 42", true, null},
                    {"b_true || i64_42 != 42", true, null},
                    // precedence
                    {"!(i64_42 == 42)", true, null},
                    {"b_false && b_false || b_true", true, null},
                    {"b_false && (b_false || b_true)", true, null},
                    {"s_foo == 42", true, null},
                    {"s_foo == 42 && b_false", true, null},
                    {"b_false || s_foo == 42", true, null},
                    {"!(s_foo == 42)", true, null},
                    {"non_existing_property", true, null},

                    // arithmetic operators
                    {"i_1 + 2 == 3", true, null},
                    {"i_1 - 2 == -1", true, null},
                    {"i_2 * 3 == 6", true, null},
                    {"i_42 / 2 == 21", true, null},
                    {"i_42 % 10 == 2", true, null},
                    {"-i_42 == -42", true, null},
                    // precedence
                    {"i_2 * 20 + 2 == 42", true, null},
                    {"i_2 + 20 * 2 == 42", true, null},
                    {"-i_1 + 1 == 0", true, null},

                    // boolean literals
                    // test literals on both sides
                    {"true || b_true", true, null},
                    {"(true && true) || b_true", true, null},
                    {"(true == true) || b_true", true, null},
                    {"(true == false) || !b_true", true, null},
                    {"false || !b_true", true, null},
                    {"(false && true) || !b_true", true, null},
                    {"b_true && true", true, null},
                    {"true && b_true", true, null},
                    {"b_true == true", true, null},
                    {"b_true < true", true, null},
                    // overflows
                    // supported_ints
                    {"i_0 != -32768", true, null}, // -(2 ** 15)
                    {"i_0 != 32767", true, null}, // 2 ** 15 - 1
                    {"i_0 != -2147483648", true, null}, // -(2 ** 31)
                    {"i_0 != 2147483647", true, null}, // 2 ** 31 - 1
                    {"i_0 != -9223372036854775807", true, null}, // -(2 ** 63) + 1
                    {"i_0 != 9223372036854775807", true, null}, // 2 ** 63 - 1
                    {"i_0 != -9223372036854775808", true, null}, // -(2 ** 63)

                    // unsupported_ints
                    {
                        "i_0 != 9223372036854775808", false, "integer overflow at offset 7"
                    }, // 2 ** 63
                    {
                        "i_0 != -170141183460469231731687303715884105728",
                        false,
                        "integer overflow at offset 7"
                    }, // -(2 ** 127)
                    {
                        "i_0 != 170141183460469231731687303715884105727",
                        false,
                        "integer overflow at offset 7"
                    }, // 2 ** 127 - 1
                    {
                        "i_0 != "
                                + "-5789604461865809771178549250434395392663499233282028201972879200395"
                                + "6564819968",
                        false,
                        "integer overflow at offset 7"
                    }, // -(2 ** 255)
                    {
                        "i_0 != "
                                + "57896044618658097711785492504343953926634992332820282019728792003956"
                                + "564819967",
                        false,
                        "integer overflow at offset 7"
                    }, // 2 ** 255 - 1

                    // -------------------------------------------------------------------------------------------------------------------
                    // Syntax errors
                    // -------------------------------------------------------------------------------------------------------------------
                    {"(a", false, "syntax error, unmatched number of open and close paranthesis"},
                    {"a)", false, "syntax error, unexpected \")\" at offset 1"},
                    {"()", false, "syntax error, unexpected \")\" at offset 1"},
                    {
                        "a1+a2+a3+a4+a5+a6+a7+a8+a9+a10+a11",
                        false,
                        "expression uses too many properties"
                    },
                    // NOTE: the following tests must be in sync with C++ implementation
                    // (bmqeval_simpleevaluator.t.cpp, test1_compilationErrors() method)
                    {"", false, "syntax error, unexpected end of expression at offset 0"},
                    {"42 @", false, "syntax error, unexpected invalid character \"@\" at offset 3"},
                    {makeTooManyOperators(), false, "too many operators"},
                    {"true && true", false, "expression does not use any properties"},
                    {
                        "val = 42",
                        false,
                        "syntax error, unexpected invalid character \"=\" at offset 4"
                    },
                    {
                        "val | true",
                        false,
                        "syntax error, unexpected invalid character \"|\" at offset 4"
                    },
                    {
                        "val & true",
                        false,
                        "syntax error, unexpected invalid character \"&\" at offset 4"
                    },
                    {"val <> 42", false, "syntax error, unexpected \">\" at offset 5"},

                    // -------------------------------------------------------------------------------------------------------------------
                    // Property names
                    // -------------------------------------------------------------------------------------------------------------------
                    // NOTE: the following tests must be in sync with C++ implementation
                    // (bmqeval_simpleevaluator.t.cpp, test2_propertyNames() method)
                    // letters
                    {"name > 0", true, null},
                    {"NAME > 0", true, null},
                    {"Name > 0", true, null},
                    {"nameName > 0", true, null},
                    {"NameName > 0", true, null},
                    {"n > 0", true, null},
                    {"N > 0", true, null},
                    {"aBaBaBaBaBaBaBaBaBaBaBaBaBaBaB > 0", true, null},
                    {"aaaBBBaaaBBBaaaBBBaaaBBBaaaBBB > 0", true, null},
                    {"BaBaBaBaBaBaBaBaBaBaBaBaBaBaBa > 0", true, null},

                    // letters + digits
                    {"n1a2m3e4 > 0", true, null},
                    {"N1A2M3E4 > 0", true, null},
                    {"N1a2m3e4 > 0", true, null},
                    {"n1a2m3e4N5a6m7e8 > 0", true, null},
                    {"N1a2m3e4N5a6m7e8 > 0", true, null},
                    {"n0 > 0", true, null},
                    {"N0 > 0", true, null},
                    {"aB1aB2aB3aB4aB5aB6aB7aB8aB9aB0aB > 0", true, null},
                    {"aaa111BBBaaa222BBBaaa333BBBaaa444 > 0", true, null},
                    {"Ba1Ba2Ba3Ba4Ba5Ba6Ba7Ba8Ba9Ba0Ba > 0", true, null},

                    // letters + underscore
                    {"name_ > 0", true, null},
                    {"NA_ME > 0", true, null},
                    {"Na_me_ > 0", true, null},
                    {"name_Name > 0", true, null},
                    {"Name_Name_ > 0", true, null},
                    {"n_ > 0", true, null},
                    {"N_ > 0", true, null},
                    {"aB_aB__aB___aB____aB_____ > 0", true, null},
                    {"aaa_BBBaaa__BBBaaa___BBBaaa > 0", true, null},
                    {"B_a_B_a_B_a_B_a_B_a_B_a_B_a_ > 0", true, null},

                    // letters + digits + underscore
                    {"n1a2m3e4_ > 0", true, null},
                    {"N1A2_M3E4 > 0", true, null},
                    {"N1a2_m3e4_ > 0", true, null},
                    {"n1a2m3e4_N5a6m7e8 > 0", true, null},
                    {"N1a2m3e4_N5a6m7e8_ > 0", true, null},
                    {"n0_ > 0", true, null},
                    {"N_0 > 0", true, null},
                    {"aB1_aB__2a___B3aB4____ > 0", true, null},
                    {"aaa_111BBBaaa222__BBBaaa3_3_3BBB > 0", true, null},
                    {"B_a_1_B_a_2_B_a_3_B_a_4_B_a_5_B_ > 0", true, null},

                    // readable examples
                    {"camelCase > 0", true, null},
                    {"snake_case > 0", true, null},
                    {"PascalCase > 0", true, null},
                    {"price > 0", true, null},
                    {"BetterLateThanNever > 0", true, null},
                    {"firmId > 0", true, null},
                    {"TheStandardAndPoor500 > 0", true, null},
                    {"SPX_IND > 0", true, null},

                    // all available characters
                    {"abcdefghijklmnopqrstuvwxyz_0123456789 > 0", true, null},
                    {"ABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789 > 0", true, null},

                    // negative examples
                    {"0name > 0", false, "syntax error, missed operation at offset 1"},
                    {"1NAME > 0", false, "syntax error, missed operation at offset 1"},
                    {"23456789Name > 0", false, "syntax error, missed operation at offset 8"},
                    {
                        "_nameName > 0",
                        false,
                        "syntax error, unexpected invalid character \"_\" at offset 0"
                    },
                    {
                        "0_NameName > 0",
                        false,
                        "syntax error, unexpected invalid character \"_\" at offset 1"
                    },
                    {
                        "_1n > 0",
                        false,
                        "syntax error, unexpected invalid character \"_\" at offset 0"
                    },
                    {
                        "1_N > 0",
                        false,
                        "syntax error, unexpected invalid character \"_\" at offset 1"
                    },
                    {
                        "_ > 0",
                        false,
                        "syntax error, unexpected invalid character \"_\" at offset 0"
                    },
                    {
                        "_11111111111111111111111111111 > 0",
                        false,
                        "syntax error, unexpected invalid character \"_\" at offset 0"
                    },
                    {"22222222222222222222222222222_ > 0", false, "integer overflow at offset 0"},
                });
    }

    @Test
    public void expressionValidatorTest() {
        java.io.Reader reader = new java.io.StringReader(inputExpression);
        try {
            ValidationResult result = ExpressionValidator.validate(reader);
            assertEquals(result.isSuccess(), expectedResult);
            assertEquals(result.getErrorMessage(), expectedErrorMessage);
        } catch (java.io.IOException e) {
            fail();
        }
    }
}
