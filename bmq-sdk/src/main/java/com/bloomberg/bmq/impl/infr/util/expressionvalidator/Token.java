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

    /** @return token type */
    public Type getType() {
        return this.type;
    }

    /** @return token value */
    public String getValue() {
        return this.value;
    }

    /** @return token position in expression */
    public long getPosition() {
        return this.pos;
    }

    /** @return 'true' if token is literal or property, 'false' otherwise */
    public boolean isLiteralOrProperty() {
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

    /** @return 'true' if token is logical, comparison or math operation, 'false' otherwise */
    public boolean isOperation() {
        switch (type) {
            case LOGICAL_OP:
            case LOGICAL_NOT_OP:
            case COMPAR_OP:
            case MATH_OP:
                return true;
            default:
                return false;
        }
    }
}
