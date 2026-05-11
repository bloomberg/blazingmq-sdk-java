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

public class ControlMessageChoice {

    private Integer rId;
    private Status status;
    private OpenQueue openQueue;
    private OpenQueueResponse openQueueResponse;
    private ConfigureQueueStream configureQueueStream;
    private ConfigureQueueStreamResponse configureQueueStreamResponse;
    private ConfigureStream configureStream;
    private ConfigureStreamResponse configureStreamResponse;
    private CloseQueue closeQueue;
    private CloseQueueResponse closeQueueResponse;
    private Disconnect disconnect;
    private DisconnectResponse disconnectResponse;

    public static final String CLASS_NAME = "ControlMessageChoice";

    public ControlMessageChoice() {
        init();
    }

    public Object createNewInstance() {
        return new ControlMessageChoice();
    }

    public final void reset() {
        init();
    }

    public final void makeStatus() {
        reset();
        status = new Status();
    }

    public final void makeOpenQueue() {
        reset();
        openQueue = new OpenQueue();
    }

    public final void makeOpenQueueResponse() {
        reset();
        openQueueResponse = new OpenQueueResponse();
    }

    public final void makeConfigureQueueStream() {
        reset();
        configureQueueStream = new ConfigureQueueStream();
    }

    public final void makeConfigureQueueStreamResponse() {
        reset();
        configureQueueStreamResponse = new ConfigureQueueStreamResponse();
    }

    public final void makeConfigureStream() {
        reset();
        configureStream = new ConfigureStream();
    }

    public final void makeConfigureStreamResponse() {
        reset();
        configureStreamResponse = new ConfigureStreamResponse();
    }

    public final void makeCloseQueue() {
        reset();
        closeQueue = new CloseQueue();
    }

    public final void makeCloseQueueResponse() {
        reset();
        closeQueueResponse = new CloseQueueResponse();
    }

    public final void makeDisconnect() {
        reset();
        disconnect = new Disconnect();
    }

    public final void makeDisconnectResponse() {
        reset();
        disconnectResponse = new DisconnectResponse();
    }

    public final boolean isStatusValue() {
        return status != null;
    }

    public final boolean isOpenQueueValue() {
        return openQueue != null;
    }

    public final boolean isOpenQueueResponseValue() {
        return openQueueResponse != null;
    }

    public final boolean isDisconnectValue() {
        return disconnect != null;
    }

    public final boolean isDisconnectResponseValue() {
        return disconnectResponse != null;
    }

    public final boolean isConfigureQueueStreamValue() {
        return configureQueueStream != null;
    }

    public final boolean isConfigureQueueStreamResponseValue() {
        return configureQueueStreamResponse != null;
    }

    public final boolean isConfigureStreamValue() {
        return configureStream != null;
    }

    public final boolean isConfigureStreamResponseValue() {
        return configureStreamResponse != null;
    }

    public final boolean isCloseQueueValue() {
        return closeQueue != null;
    }

    public final boolean isCloseQueueResponseValue() {
        return closeQueueResponse != null;
    }

    public final Status status() {
        return status;
    }

    public final OpenQueue openQueue() {
        return openQueue;
    }

    public final OpenQueueResponse openQueueResponse() {
        return openQueueResponse;
    }

    public final ConfigureQueueStream configureQueueStream() {
        return configureQueueStream;
    }

    public final ConfigureQueueStreamResponse configureQueueStreamResponse() {
        return configureQueueStreamResponse;
    }

    public final ConfigureStream configureStream() {
        return configureStream;
    }

    public final ConfigureStreamResponse configureStreamResponse() {
        return configureStreamResponse;
    }

    public final CloseQueue closeQueue() {
        return closeQueue;
    }

    public final CloseQueueResponse closeQueueResponse() {
        return closeQueueResponse;
    }

    public final Disconnect disconnect() {
        return disconnect;
    }

    public final DisconnectResponse disconnectResponse() {
        return disconnectResponse;
    }

    public final void init() {
        configureQueueStream = null;
        configureQueueStreamResponse = null;
        configureStream = null;
        configureStreamResponse = null;
        status = null;
        openQueue = null;
        openQueueResponse = null;
        closeQueue = null;
        closeQueueResponse = null;
        disconnect = null;
        disconnectResponse = null;
    }

    public void setId(int id) {
        rId = id;
    }

    public int id() {
        return rId;
    }

    public Boolean isEmpty() {
        return configureQueueStream == null
                && configureQueueStreamResponse == null
                && configureStream == null
                && configureStreamResponse == null
                && status == null
                && openQueue == null
                && openQueueResponse == null
                && closeQueue == null
                && closeQueueResponse == null
                && disconnect == null
                && disconnectResponse == null;
    }

    @Override
    public String toString() {
        return new com.google.gson.Gson().toJson(this);
    }
}
