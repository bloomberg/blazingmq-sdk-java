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

public final class Int32MessageProperty extends MessageProperty {

    public Int32MessageProperty() {
        super(PropertyType.INT32, Integer.SIZE);
    }

    public Int32MessageProperty(int val) {
        this();
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(val);
        setPropertyValue(buffer.array());
    }

    @Override
    public void dispatch(MessagePropertyHandler handler) {
        handler.handleInt32(this);
    }

    @Override
    public Integer getValue() {
        return getValueAsInt32();
    }

    @Override
    public int getValueAsInt32() {
        ByteBuffer b = ByteBuffer.wrap(value());
        return b.getInt();
    }
}
