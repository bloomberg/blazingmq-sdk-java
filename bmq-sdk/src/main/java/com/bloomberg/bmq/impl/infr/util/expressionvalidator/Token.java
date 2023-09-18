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

public class Token {
    // This data class holds token information from parsed expression.

    public enum Type {
        INVALID,
        END,
        BOOL,
        PROPERTY,
        INTEGER,
        STRING,
        LPAR,
        RPAR,
        LOGICAL_OP,
        LOGICAL_NOT_OP,
        COMPAR_OP,
        MATH_OP
    }

    private Type type;
    private String value;
    private long pos;

    public Token(Type type, String value, long pos) {
        this.type = type;
        this.value = value;
        this.pos = pos;
    }

    public Token(Type type, long pos) {
        this.type = type;
        this.pos = pos;
    }

    public Type getType() {
        // Return token type.
        return this.type;
    }

    public String getValue() {
        // Return token value.
        return this.value;
    }

    public long getPosition() {
        // Return token position in expression.
        return this.pos;
    }

    public boolean isLiteralOrProperty() {
        // Return 'true' if token is literal or property, 'false' otherwise.
        switch (type) {
            case BOOL:
            case INTEGER:
            case STRING:
            case PROPERTY:
                return true;
            default:
                return false;
        }
    }

    public boolean isOperation() {
        // Return 'true' if token is logical, comparison or math operation, 'false' otherwise.
        switch (type) {
            case LOGICAL_OP:
            case COMPAR_OP:
            case MATH_OP:
                return true;
            default:
                return false;
        }
    }
}
