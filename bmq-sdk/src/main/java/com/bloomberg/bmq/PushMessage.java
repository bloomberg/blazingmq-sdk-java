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
package com.bloomberg.bmq;

import com.bloomberg.bmq.ResultCodes.GenericResult;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * PUSH message representation.
 *
 * <p>PUSH mesage comes from the broker which sends it to the consumer if it is a subscriber of some
 * {@link com.bloomberg.bmq.Queue} that has {@link com.bloomberg.bmq.PutMessage} posted by the
 * producer.
 */
public interface PushMessage extends QueueEvent {
    /**
     * Returns unique {@link com.bloomberg.bmq.MessageGUID} assigned to this message by the broker.
     *
     * @return MessageGUID unique message GUID
     */
    MessageGUID messageGUID();

    /**
     * Returns {@code true} if the application data of this message contains message properties.
     *
     * @return boolean {@code true} if the application data has message properties, {@code false}
     *     otherwise
     * @see com.bloomberg.bmq.MessageProperty
     * @see com.bloomberg.bmq.MessageProperties
     */
    boolean hasMessageProperties();

    /**
     * Returns a standart {@code Iterator} to iterate over existed properties.
     *
     * @return Iterator property iterator
     */
    Iterator<MessageProperty> propertyIterator();

    /**
     * Returns property with specified (@code name).
     *
     * @param name property name
     * @return (@code MessageProperty) if property with such name exists or (@code null).
     */
    MessageProperty getProperty(String name);

    /**
     * Returns user payload data of this message.
     *
     * @return ByteBuffer[] array of byte buffers that represents the user payload
     * @throws BMQException in case of any failure
     */
    ByteBuffer[] payload() throws BMQException;

    /**
     * Sends CONFIRM message to the broker that affirms the receiving of this message.
     *
     * @return GenericResult status of the CONFIRM message sending
     * @see com.bloomberg.bmq.ResultCodes.GenericResult
     */
    GenericResult confirm();
}
