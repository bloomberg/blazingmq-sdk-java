/*
 * Copyright 2026 Bloomberg Finance L.P.
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
package com.bloomberg.bmq.impl.infr.proto;

import com.bloomberg.bmq.impl.infr.codec.JsonDecoderUtil;
import com.bloomberg.bmq.impl.infr.msg.AuthenticationMessage;
import com.bloomberg.bmq.impl.intf.SessionEventHandler;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthenticationEventImpl extends EventImpl {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final AuthenticationMessage authnChoice;

    public AuthenticationEventImpl(ByteBuffer[] bbuf) {
        super(EventType.AUTHENTICATION, bbuf);
        AuthenticationMessage decoded = null;
        try {
            String json = JsonDecoderUtil.getJsonFromInputStream(blob);
            decoded = JsonDecoderUtil.decodeFromJson(json, AuthenticationMessage.class);
        } catch (IOException | JsonSyntaxException ex) {
            logger.error("Failed to decode Authentication event: ", ex);
        }
        authnChoice = decoded;
    }

    @Override
    public void dispatch(SessionEventHandler handler) {
        // Authentication events are handled directly by TcpBrokerConnection,
        // not dispatched through the session event handler.
    }

    public AuthenticationMessage authenticationChoice() {
        return authnChoice;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ AuthenticationEvent ").append(super.toString()).append(" ]");
        return sb.toString();
    }
}
