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

import java.nio.ByteBuffer;

public final class ShortMessageProperty extends MessageProperty {

    public ShortMessageProperty() {
        super(PropertyType.SHORT, Short.SIZE);
    }

    public ShortMessageProperty(short val) {
        this();
        ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
        buffer.putShort(val);
        setPropertyValue(buffer.array());
    }

    @Override
    public void dispatch(MessagePropertyHandler handler) {
        handler.handleShort(this);
    }

    @Override
    public Short getValue() {
        return getValueAsShort();
    }

    @Override
    public short getValueAsShort() {
        ByteBuffer b = ByteBuffer.wrap(value());
        return b.getShort();
    }
}
