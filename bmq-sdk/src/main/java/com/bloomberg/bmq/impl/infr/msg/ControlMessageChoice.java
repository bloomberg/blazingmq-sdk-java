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

public final class ControlMessageChoice {

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

    public void reset() {
        init();
    }

    public void makeStatus() {
        reset();
        status = new Status();
    }

    public void makeOpenQueue() {
        reset();
        openQueue = new OpenQueue();
    }

    public void makeOpenQueueResponse() {
        reset();
        openQueueResponse = new OpenQueueResponse();
    }

    public void makeConfigureQueueStream() {
        reset();
        configureQueueStream = new ConfigureQueueStream();
    }

    public void makeConfigureQueueStreamResponse() {
        reset();
        configureQueueStreamResponse = new ConfigureQueueStreamResponse();
    }

    public void makeConfigureStream() {
        reset();
        configureStream = new ConfigureStream();
    }

    public void makeConfigureStreamResponse() {
        reset();
        configureStreamResponse = new ConfigureStreamResponse();
    }

    public void makeCloseQueue() {
        reset();
        closeQueue = new CloseQueue();
    }

    public void makeCloseQueueResponse() {
        reset();
        closeQueueResponse = new CloseQueueResponse();
    }

    public void makeDisconnect() {
        reset();
        disconnect = new Disconnect();
    }

    public void makeDisconnectResponse() {
        reset();
        disconnectResponse = new DisconnectResponse();
    }

    public boolean isStatusValue() {
        return status != null;
    }

    public boolean isOpenQueueValue() {
        return openQueue != null;
    }

    public boolean isOpenQueueResponseValue() {
        return openQueueResponse != null;
    }

    public boolean isDisconnectValue() {
        return disconnect != null;
    }

    public boolean isDisconnectResponseValue() {
        return disconnectResponse != null;
    }

    public boolean isConfigureQueueStreamValue() {
        return configureQueueStream != null;
    }

    public boolean isConfigureQueueStreamResponseValue() {
        return configureQueueStreamResponse != null;
    }

    public boolean isConfigureStreamValue() {
        return configureStream != null;
    }

    public boolean isConfigureStreamResponseValue() {
        return configureStreamResponse != null;
    }

    public boolean isCloseQueueValue() {
        return closeQueue != null;
    }

    public boolean isCloseQueueResponseValue() {
        return closeQueueResponse != null;
    }

    public Status status() {
        return status;
    }

    public OpenQueue openQueue() {
        return openQueue;
    }

    public OpenQueueResponse openQueueResponse() {
        return openQueueResponse;
    }

    public ConfigureQueueStream configureQueueStream() {
        return configureQueueStream;
    }

    public ConfigureQueueStreamResponse configureQueueStreamResponse() {
        return configureQueueStreamResponse;
    }

    public ConfigureStream configureStream() {
        return configureStream;
    }

    public ConfigureStreamResponse configureStreamResponse() {
        return configureStreamResponse;
    }

    public CloseQueue closeQueue() {
        return closeQueue;
    }

    public CloseQueueResponse closeQueueResponse() {
        return closeQueueResponse;
    }

    public Disconnect disconnect() {
        return disconnect;
    }

    public DisconnectResponse disconnectResponse() {
        return disconnectResponse;
    }

    public void init() {
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
