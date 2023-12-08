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

public class ConfigureStream {
    private int qId;
    private StreamParameters streamParameters;

    public ConfigureStream() {
        init();
    }

    public void reset() {
        init();
    }

    private void init() {
        qId = 0;
        streamParameters = new StreamParameters();
    }

    // TODO these getter and setter are named with respect to the ConfigureQueueStream
    // implementation, but
    // it might be good to modify it to show the exact variable name qId, like qId(), setQId()
    public int id() {
        return qId;
    }

    public void setId(int val) {
        qId = val;
    }

    public StreamParameters streamParameters() {
        return streamParameters;
    }

    public void setStreamParameters(StreamParameters val) {
        streamParameters = val;
    }

    public Object createNewInstance() {
        return new ConfigureStream();
    }
}
