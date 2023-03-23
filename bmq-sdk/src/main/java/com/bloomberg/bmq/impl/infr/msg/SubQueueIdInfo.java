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

import javax.annotation.concurrent.Immutable;

@Immutable
public class SubQueueIdInfo {
    public static final int DEFAULT_SUB_ID = 0;
    public static final String DEFAULT_APP_ID = "__default";

    private final int subId;
    private final String appId;

    public SubQueueIdInfo(int subId, String appId) {
        this.subId = subId;
        this.appId = appId;
    }

    public SubQueueIdInfo() {
        this.subId = DEFAULT_SUB_ID;
        this.appId = DEFAULT_APP_ID;
    }

    public int subId() {
        return subId;
    }

    public String appId() {
        return appId;
    }
}
