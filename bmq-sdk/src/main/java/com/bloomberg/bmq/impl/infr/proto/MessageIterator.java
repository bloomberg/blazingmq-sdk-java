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

import com.bloomberg.bmq.impl.infr.proto.intf.Streamable;
import com.bloomberg.bmq.impl.infr.util.Argument;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MessageIterator {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final EventImpl event;

    private int currentPosition;

    protected MessageIterator(EventImpl ev) {
        try {
            ev.reset();
            event = ev;
        } catch (IOException e) {
            throw new RuntimeException("Failed to reset event", e);
        }
    }

    protected final EventImpl event() {
        return event;
    }

    protected final <T extends Streamable> T fetchNextMessage(T message) {
        Argument.expectNonNull(message, "message");
        try {
            if (currentPosition > 0) {
                event().setPosition(currentPosition);
            }

            if (event().available() > 0) {
                // If there is more data then stream in and return the message
                currentPosition = event().streamIn(message);
                return message;
            } else {
                // Otherwise, then log DEBUG message and return null
                logger.debug("Input stream is empty");
                return null;
            }
        } catch (IOException e) {
            logger.debug("Fails to decode Message: ", e);
            return null;
        }
    }

    public final boolean isValid() {
        return event != null;
    }
}
