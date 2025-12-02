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

public final class Int64MessageProperty extends MessageProperty {

    public Int64MessageProperty() {
        super(PropertyType.INT64, Long.SIZE);
    }

    public Int64MessageProperty(long val) {
        this();
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(val);
        setPropertyValue(buffer.array());
    }

    @Override
    public void dispatch(MessagePropertyHandler handler) {
        handler.handleInt64(this);
    }

    @Override
    public Long getValue() {
        return getValueAsInt64();
    }

    @Override
    public long getValueAsInt64() {
        ByteBuffer b = ByteBuffer.wrap(value());
        return b.getLong();
    }
}
