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
import com.bloomberg.bmq.impl.infr.util.Argument;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubQueueIdsOption {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private OptionHeader header;
    private ArrayList<Integer> subQueueIds;

    public SubQueueIdsOption(OptionHeader header) {
        this.header = Argument.expectNonNull(header, "header");
        subQueueIds = new ArrayList<>();

        Argument.expectCondition(
                header.type() == OptionType.SUB_QUEUE_IDS_OLD,
                "Option header type must be SUB_QUEUE_IDS_OLD");
    }

    public Integer[] subQueueIds() {
        Integer[] res = new Integer[subQueueIds.size()];
        return subQueueIds.toArray(res);
    }

    public void streamIn(ByteBufferInputStream bbis) throws IOException {
        subQueueIds.clear();
        int size = header.words() * Protocol.WORD_SIZE - OptionHeader.HEADER_SIZE;
        while (size >= Integer.BYTES) {
            subQueueIds.add(bbis.readInt());
            size -= Integer.BYTES;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ ").append(subQueueIds.toString()).append(" ]");
        return sb.toString();
    }
}
