/*
 * Copyright 2023 Bloomberg Finance L.P.
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

import com.bloomberg.bmq.impl.infr.util.Argument;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;

@Immutable
public class SubscriptionExpression {
    public enum Version {
        /** Empty. */
        e_UNDEFINED,

        /** Simple evaluator. */
        e_VERSION_1
    }

    /** Default expression text. */
    public static final String k_EXPRESSION_DEFAULT = "";

    /** Default expression version. */
    public static final Version k_VERSION_DEFAULT = Version.e_UNDEFINED;

    private final Optional<String> expression;
    private final Optional<Version> version;

    public SubscriptionExpression(String expression) {
        Argument.expectNonNull(expression, "expression");
        this.expression = Optional.of(expression);
        this.version =
                Optional.of((expression.length() > 0) ? Version.e_VERSION_1 : Version.e_UNDEFINED);
    }

    public SubscriptionExpression() {
        expression = Optional.empty();
        version = Optional.empty();
    }

    /**
     * Returns true if this {@code SubscriptionExpression} is equal to the specified {@code obj},
     * false otherwise. Two {@code SubscriptionExpression} are equal if they have equal properties.
     *
     * @param obj {@code SubscriptionExpression} object to compare with
     * @return boolean true if {@code SubscriptionExpression} are equal, false otherwise
     */
    @Override
    public boolean equals(Object obj) {

        if (obj == null) return false;
        if (obj == this) return true;
        if (!(obj instanceof SubscriptionExpression)) return false;
        SubscriptionExpression expression = (SubscriptionExpression) obj;

        return getExpression().equals(expression.getExpression())
                && getVersion() == expression.getVersion();
    }

    /**
     * Returns a hash code value for this {@code Subscription} object.
     *
     * @return int a hash code value for this object.
     */
    @Override
    public int hashCode() {

        Long hash = (long) getExpression().hashCode();
        hash <<= Integer.SIZE;
        hash |= (long) getVersion().hashCode();

        return hash.hashCode();
    }

    /**
     * Returns expression text. If not set, returns default value.
     *
     * @return String expression text
     */
    public String getExpression() {
        return expression.orElse(k_EXPRESSION_DEFAULT);
    }

    /**
     * Returns whether {@code expression} has been set for this object, or whether it implicitly
     * holds {@code k_EXPRESSION_DEFAULT}.
     *
     * @return boolean true if {@code expression} has been set.
     */
    public boolean hasExpression() {
        return expression.isPresent();
    }

    /**
     * Returns expression version. If not set returns default value.
     *
     * @return Version expression version
     */
    public Version getVersion() {
        return version.orElse(k_VERSION_DEFAULT);
    }

    /**
     * Returns whether {@code version} has been set for this object, or whether it implicitly holds
     * {@code k_VERSION_DEFAULT}.
     *
     * @return boolean true if {@code version} has been set.
     */
    public boolean hasVersion() {
        return version.isPresent();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ expression: \"")
                .append(getExpression())
                .append("\", version: ")
                .append(getVersion())
                .append(" ]");
        return sb.toString();
    }
}
