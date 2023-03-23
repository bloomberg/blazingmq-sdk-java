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
package com.bloomberg.bmq.impl.intf;

import com.bloomberg.bmq.impl.infr.proto.AckMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.ConfirmEventImpl;
import com.bloomberg.bmq.impl.infr.proto.ControlEventImpl;
import com.bloomberg.bmq.impl.infr.proto.PushMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.PutEventImpl;

public interface SessionEventHandler {
    // Internal interface to handle BMQ wire protocol IO events.

    void handleControlEvent(ControlEventImpl controlEvent);

    void handleAckMessage(AckMessageImpl ackMsg);

    void handlePushMessage(PushMessageImpl pushMsg);

    void handlePutEvent(PutEventImpl putEvent);

    void handleConfirmEvent(ConfirmEventImpl confirmEvent);
}
