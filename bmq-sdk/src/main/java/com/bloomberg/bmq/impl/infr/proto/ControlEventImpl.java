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
package com.bloomberg.bmq.impl.infr.proto;

import com.bloomberg.bmq.impl.infr.codec.JsonDecoderUtil;
import com.bloomberg.bmq.impl.infr.msg.ControlMessageChoice;
import com.bloomberg.bmq.impl.infr.msg.NegotiationMessageChoice;
import com.bloomberg.bmq.impl.intf.SessionEventHandler;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControlEventImpl extends EventImpl {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private ControlMessageChoice ctrlChoice;
    private NegotiationMessageChoice negoChoice;
    private String json;

    public ControlEventImpl(ByteBuffer[] bbuf) {
        super(EventType.CONTROL, bbuf);
        try {
            json = JsonDecoderUtil.getJsonFromInputStream(blob);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void dispatch(SessionEventHandler handler) {
        handler.handleControlEvent(this);
    }

    public ControlMessageChoice controlChoice() throws JsonSyntaxException {
        if (ctrlChoice == null) {
            ctrlChoice = JsonDecoderUtil.decodeFromJson(json, ControlMessageChoice.class);
        }
        return ctrlChoice;
    }

    public NegotiationMessageChoice negotiationChoice() throws JsonSyntaxException {
        if (negoChoice == null) {
            negoChoice = JsonDecoderUtil.decodeFromJson(json, NegotiationMessageChoice.class);
        }
        return negoChoice;
    }

    public ControlMessageChoice tryControlChoice() {
        if (ctrlChoice == null) {
            try {
                ctrlChoice = JsonDecoderUtil.decodeFromJson(json, ControlMessageChoice.class);
                if (ctrlChoice == null || ctrlChoice.isEmpty()) {
                    ctrlChoice = null;
                    return null;
                }
            } catch (Exception e) {
                logger.error("Failed to get Control Message Choice: ", e);
                return null;
            }
        }
        return ctrlChoice;
    }

    public NegotiationMessageChoice tryNegotiationChoice() {
        if (negoChoice == null) {
            try {
                negoChoice = JsonDecoderUtil.decodeFromJson(json, NegotiationMessageChoice.class);
                if (negoChoice == null || negoChoice.isEmpty()) {
                    negoChoice = null;
                    return null;
                }
            } catch (Exception e) {
                logger.error("Failed to get Negotiation Message Choice");
                return null;
            }
        }
        return negoChoice;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ ControlEvent ").append(super.toString()).append(" ");
        Optional.ofNullable(tryControlChoice()).ifPresent(sb::append);
        if (this.ctrlChoice == null) {
            Optional.ofNullable(tryNegotiationChoice()).ifPresent(sb::append);
        }
        sb.append(" ]");
        return sb.toString();
    }
}
