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

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PushMessageIterator extends MessageIterator
        implements Iterator<PushMessageImpl> {

    static Logger logger = LoggerFactory.getLogger(PushMessageIterator.class);

    private PushMessageImpl message;
    private PushMessageImpl nextMessage;

    public PushMessageIterator(EventImpl ev) {
        super(ev);
        message = null;
        nextMessage = fetchNextMessage(new PushMessageImpl());
    }

    @Override
    public boolean hasNext() {
        return nextMessage != null;
    }

    @Override
    public PushMessageImpl next() {
        if (hasNext()) {
            message = nextMessage;
            nextMessage = fetchNextMessage(new PushMessageImpl());
            return message;
        } else {
            throw new NoSuchElementException("No PUSH messages");
        }
    }
}
