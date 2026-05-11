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

public class BoolMessageProperty extends MessageProperty {

    public BoolMessageProperty() {
        super(PropertyType.BOOL, Byte.SIZE);
    }

    @SuppressWarnings("this-escape") // setPropertyValue() is `final` and calls no non-final methods
    public BoolMessageProperty(boolean val) {
        this();
        byte[] b = {(byte) (val ? 1 : 0)};
        setPropertyValue(b);
    }

    @Override
    public Boolean getValue() {
        return getValueAsBool();
    }

    @Override
    public void dispatch(MessagePropertyHandler handler) {
        handler.handleBoolean(this);
    }

    @Override
    public boolean getValueAsBool() {
        return value()[0] == 1;
    }
}
