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

import com.bloomberg.bmq.SubscriptionHandle;

public class Subscription {
    private int sId;
    private Expression expression;
    private ConsumerInfo[] consumers;
    private transient com.bloomberg.bmq.Subscription origin;
    private transient SubscriptionHandle handle;

    public Subscription() {
        init();
    }

    public void reset() {
        init();
    }

    private void init() {
        sId = 0;
        expression = new Expression();
        consumers = new ConsumerInfo[] {};
        origin = null;
        handle = null;
    }

    public int id() {
        return sId;
    }

    public void setId(int id) {
        sId = id;
    };

    public Expression expression() {
        return expression;
    }

    public void setExpression(Expression val) {
        expression = val;
    }

    public ConsumerInfo[] consumers() {
        return consumers;
    }

    public void setConsumers(ConsumerInfo[] val) {
        consumers = val;
    }

    public com.bloomberg.bmq.Subscription origin() {
        return origin;
    }

    public void setOrigin(com.bloomberg.bmq.Subscription val) {
        origin = val;
    }

    public SubscriptionHandle handle() {
        return handle;
    }

    public void setHandle(SubscriptionHandle val) {
        handle = val;
    }

    public Object createNewInstance() {
        return new Subscription();
    }
}
