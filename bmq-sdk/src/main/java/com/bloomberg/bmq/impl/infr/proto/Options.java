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

import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Options {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private SubQueueIdsOption subQueueIdsOption;
    private SubQueueInfosOption subQueueInfosOption;

    public SubQueueIdsOption subQueueIdsOption() {
        return subQueueIdsOption;
    }

    public SubQueueInfosOption subQueueInfosOption() {
        return subQueueInfosOption;
    }

    public void streamIn(int size, ByteBufferInputStream bbis) throws IOException {
        subQueueIdsOption = null;
        subQueueInfosOption = null;

        while (size > 0) {
            OptionHeader header = new OptionHeader();
            header.streamIn(bbis);
            switch (header.type()) {
                case SUB_QUEUE_INFOS:
                    if (subQueueInfosOption == null) {
                        subQueueInfosOption = new SubQueueInfosOption(header);
                        subQueueInfosOption.streamIn(bbis);
                        logger.debug("New options: {}", subQueueInfosOption);
                    } else {
                        logger.warn("Multiple SubQueueInfos option: {}", header.type());

                        int numSkip =
                                header.packed()
                                        ? 0
                                        : header.words() * Protocol.WORD_SIZE
                                                - OptionHeader.HEADER_SIZE;

                        bbis.skip(numSkip);
                    }
                    break;
                case SUB_QUEUE_IDS_OLD:
                    if (subQueueIdsOption == null) {
                        subQueueIdsOption = new SubQueueIdsOption(header);
                        subQueueIdsOption.streamIn(bbis);
                        logger.debug("Old options: {}", subQueueIdsOption);
                    } else {
                        logger.warn("Multiple SubQueueIds option: {}", header.type());
                        bbis.skip(header.words() * Protocol.WORD_SIZE - OptionHeader.HEADER_SIZE);
                    }
                    break;
                default:
                    logger.warn("Unsupported option type: {}", header.type());
                    bbis.skip(header.words() * Protocol.WORD_SIZE - OptionHeader.HEADER_SIZE);
                    break;
            }
            size -= header.words() * Protocol.WORD_SIZE;
        }

        if (subQueueIdsOption != null && subQueueInfosOption != null) {
            logger.warn("Both SubQueueInfos and SubQueueIds options");
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (subQueueInfosOption != null) {
            sb.append("[ SubQueueInfos ").append(subQueueInfosOption.toString()).append(" ]");
        }
        if (subQueueIdsOption != null) {
            sb.append("[ SubQueueIds ").append(subQueueIdsOption.toString()).append(" ]");
        }
        return sb.toString();
    }
}
