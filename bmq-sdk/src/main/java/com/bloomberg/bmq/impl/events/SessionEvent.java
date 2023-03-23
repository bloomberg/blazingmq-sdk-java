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

// Here SessionEvent is parent class only for QueueControl and BrokerSessionEvent, not for Message
// event
public abstract class SessionEvent<ENUM_TYPE> extends Event {

    private final ENUM_TYPE sessionEventType;
    private final String errorDescription;

    public ENUM_TYPE getEventType() {
        return sessionEventType;
    }

    public String getErrorDescription() {
        return errorDescription;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("SessionEvent Type: ")
                .append(sessionEventType)
                .append("; Error Desc: ")
                .append(errorDescription);
        return builder.toString();
    }

    protected SessionEvent(ENUM_TYPE sessionEventType, String errorDescription) {
        this.sessionEventType = sessionEventType;
        this.errorDescription = errorDescription;
    }
}
