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

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringMessageProperty extends MessageProperty {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public StringMessageProperty() {
        super(PropertyType.STRING, MessagePropertyHeader.MAX_PROPERTY_VALUE_LENGTH);
    }

    @SuppressWarnings("this-escape") // setPropertyValue() is `final` and calls no non-final methods
    public StringMessageProperty(String val) {
        this();
        setPropertyValue(val.getBytes(StandardCharsets.US_ASCII));
    }

    @Override
    public void dispatch(MessagePropertyHandler handler) {
        handler.handleString(this);
    }

    @Override
    public String getValue() {
        return getValueAsString();
    }

    @Override
    public String getValueAsString() {
        byte[] b = value();
        return new String(b, StandardCharsets.US_ASCII);
    }
}
