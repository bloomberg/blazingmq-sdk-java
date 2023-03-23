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
package com.bloomberg.bmq.impl.infr.msg;

public enum ClientLanguage {
    E_UNKNOWN(0),
    E_CPP(1),
    E_JAVA(2);

    private int id;

    private ClientLanguage(int id) {
        this.id = id;
    }

    public int toInt() {
        return id;
    }

    public static ClientLanguage fromInt(int i) {
        for (ClientLanguage l : ClientLanguage.values()) {
            if (l.toInt() == i) return l;
        }

        return null;
    }

    public static ClientLanguage fromString(String str) {
        /* This can be refactored to the following:
        / try {
        /     return ClientLanguage.valueOf(str.trim().toUpperCase());
        / catch (IllegalArgumentException e) {
        /     // log exception
        /     return null;
        / }
        /
        / But for consistency leave implementation as it's done in other enums
        /
        */

        switch (str) {
            case "E_UNKNOWN":
                return E_UNKNOWN;
            case "E_CPP":
                return E_CPP;
            case "E_JAVA":
                return E_JAVA;
            default:
                return null;
        }
    }
}
