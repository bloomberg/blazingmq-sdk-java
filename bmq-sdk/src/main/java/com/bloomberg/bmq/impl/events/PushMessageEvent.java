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
package com.bloomberg.bmq.impl.events;

import com.bloomberg.bmq.impl.infr.proto.PushMessageImpl;
import com.bloomberg.bmq.impl.infr.util.UniqId;

public final class PushMessageEvent extends MessageEvent<PushMessageImpl> {
    public static final int TYPE_ID = UniqId.getNumber();

    @Override
    public int getDispatchId() {
        return TYPE_ID;
    }

    private PushMessageEvent(PushMessageImpl rawMsg) {
        super(rawMsg);
    }

    public static PushMessageEvent createInstance(PushMessageImpl rawMessage) {
        return new PushMessageEvent(rawMessage);
    }

    public PushMessageImpl rawMessage() {
        return super.getRawMessage();
    }

    @Override
    public void dispatch(EventHandler handler) {
        handler.handlePushMessageEvent(this);
    }
}
