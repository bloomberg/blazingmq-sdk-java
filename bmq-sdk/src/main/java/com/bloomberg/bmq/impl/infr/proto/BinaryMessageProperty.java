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

public final class BinaryMessageProperty extends MessageProperty {

    public BinaryMessageProperty() {
        super(PropertyType.BINARY, MessagePropertyHeader.MAX_PROPERTY_VALUE_LENGTH);
    }

    public BinaryMessageProperty(byte[] val) {
        this();
        setPropertyValue(val);
    }

    @Override
    public void dispatch(MessagePropertyHandler handler) {
        handler.handleBinary(this);
    }

    @Override
    public byte[] getValue() {
        return getValueAsBinary();
    }

    @Override
    public byte[] getValueAsBinary() {
        return value();
    }
}
