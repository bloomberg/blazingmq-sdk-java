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

public enum StatusCategory {
    E_SUCCESS(0),
    E_UNKNOWN(-1),
    E_TIMEOUT(-2),
    E_NOT_CONNECTED(-3),
    E_CANCELED(-4),
    E_NOT_SUPPORTED(-5),
    E_REFUSED(-6),
    E_INVALID_ARGUMENT(-7),
    E_NOT_READY(-8);

    private int id;

    StatusCategory(int id) {
        this.id = id;
    }

    public int toInt() {
        return id;
    }

    public static StatusCategory fromInt(int i) {
        for (StatusCategory c : StatusCategory.values()) {
            if (c.toInt() == i) return c;
        }
        return null;
    }

    public boolean isSuccess() {
        return id == E_SUCCESS.id;
    }

    public boolean isTimeout() {
        return id == E_TIMEOUT.id;
    }

    public boolean isFailure() {
        return !isSuccess();
    }

    public boolean isCanceled() {
        return id == E_CANCELED.id;
    }
}
