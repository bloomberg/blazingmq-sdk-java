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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfirmMessageIterator extends MessageIterator {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private ConfirmHeader header;

    public ConfirmMessageIterator(EventImpl ev) {
        super(ev);
        if (isValid()) {
            header = new ConfirmHeader();
            try {
                event().streamIn(header);
            } catch (IOException e) {
                logger.error("Failed to stream in ACK header: ", e);
                header = null;
            }
        }
    }

    public ConfirmHeader header() {
        return header;
    }

    public boolean hasNext() {
        return (header != null
                && isValid()
                && (event().available() >= ConfirmMessage.MESSAGE_SIZE));
    }

    public ConfirmMessage next() throws NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException("No CONFIRM messages");
        }
        ConfirmMessage msg = new ConfirmMessage();
        try {
            event().streamIn(msg);
        } catch (IOException e) {
            msg = null;
            logger.info("Fails to decode CONFIRM Message");
        }
        return msg;
    }
}
