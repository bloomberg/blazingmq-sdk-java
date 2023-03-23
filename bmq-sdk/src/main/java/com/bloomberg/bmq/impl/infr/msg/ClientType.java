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

public enum ClientType {
    E_UNKNOWN(0),
    E_TCPCLIENT(1),
    E_TCPBROKER(2);

    private int id;

    private ClientType(int id) {
        this.id = id;
    }

    public int toInt() {
        return id;
    }

    public static ClientType fromInt(int i) {
        for (ClientType t : ClientType.values()) {
            if (t.toInt() == i) return t;
        }
        return null;
    }

    public static ClientType fromString(String str) {
        switch (str) {
            case "E_UNKNOWN":
                return E_UNKNOWN;
            case "E_TCPCLIENT":
                return E_TCPCLIENT;
            case "E_TCPBROKER":
                return E_TCPBROKER;
            default:
                return null;
        }
    }
}
