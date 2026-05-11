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

import com.bloomberg.bmq.BMQException;
import com.bloomberg.bmq.MessageGUID;
import com.bloomberg.bmq.impl.QueueId;
import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.proto.intf.Streamable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PushMessageImpl implements Streamable {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final short INVALID_SCHEMA_WIRE_ID = (short) 1;
    private static final int HEADER_WORDS =
            PushHeader.HEADER_SIZE_FOR_SCHEMA_ID / Protocol.WORD_SIZE;

    private PushHeader header;
    private ApplicationData appData;
    private Options options;

    private CompressionAlgorithmType compressionType;

    public PushMessageImpl() {
        compressionType = CompressionAlgorithmType.E_NONE;
        reset();
    }

    public void reset() {
        header = new PushHeader();
        appData = new ApplicationData();
        options = new Options();
    }

    public void setQueueId(int id) {
        header.setQueueId(id);
    }

    public int queueId() {
        return header.queueId();
    }

    public Integer[] subQueueIds() {
        Integer[] res = new Integer[] {QueueId.k_DEFAULT_SUBQUEUE_ID};

        SubQueueInfosOption infos = options.subQueueInfosOption();
        SubQueueIdsOption ids = options.subQueueIdsOption();

        // Check new option first
        if (infos != null) {
            res = infos.subQueueIds();
        } else if (ids != null) { // check old option
            res = ids.subQueueIds();
        }

        return res;
    }

    public void setMessageGUID(MessageGUID guid) {
        header.setMessageGUID(guid);
    }

    public void setCompressionType(CompressionAlgorithmType compressionType) {
        this.compressionType = compressionType;
    }

    public MessageGUID messageGUID() {
        return header.messageGUID();
    }

    public void setFlags(int value) {
        header.setFlags(value);
    }

    public int flags() {
        return header.flags();
    }

    public ApplicationData appData() {
        return appData;
    }

    public PushHeader header() {
        return header;
    }

    public Options options() {
        return options;
    }

    @Override
    public void streamIn(ByteBufferInputStream bbis) throws IOException {
        header.streamIn(bbis);
        int totalSize = header.messageWords() * Protocol.WORD_SIZE;
        int headerSize = header.headerWords() * Protocol.WORD_SIZE;
        int appDataSize = totalSize - headerSize;
        if (totalSize <= 0 || headerSize <= 0 || bbis.available() < appDataSize) {
            logger.error(
                    "Total: {}, header: {}, required: {}, available: {}",
                    totalSize,
                    headerSize,
                    totalSize - headerSize,
                    bbis.available());
            throw new IOException("Cannot read application data.");
        }
        int optionSize = header.optionsWords() * Protocol.WORD_SIZE;
        int dataSize = appDataSize - optionSize;

        logger.debug(
                "Total: {}, header: {}, appData: {}, option: {}, data: {}",
                totalSize,
                headerSize,
                appDataSize,
                optionSize,
                dataSize);

        if (optionSize > 0) {
            options.streamIn(optionSize, bbis);
        }

        final boolean hasProperties =
                PushHeaderFlags.isSet(header.flags(), PushHeaderFlags.MESSAGE_PROPERTIES);
        final CompressionAlgorithmType inputCompressionType =
                CompressionAlgorithmType.fromInt(header.compressionType());
        final boolean isOldStyleProperties = header.schemaWireId() == 0;

        logger.debug(
                "Has properties: {}, compressionType: {}, schemaWireId: {}",
                hasProperties,
                inputCompressionType,
                header.schemaWireId());

        if (PushHeaderFlags.isSet(header.flags(), PushHeaderFlags.IMPLICIT_PAYLOAD)) {
            throw new BMQException("IMPLICIT_PAYLOAD flag is set");
        }

        appData.streamIn(dataSize, hasProperties, isOldStyleProperties, inputCompressionType, bbis);

        if (appData.unpackedSize() == 0) {
            throw new BMQException("Application data is empty");
        }
    }

    // TODO: move to test code
    public void compressData() throws IOException {
        if (appData.payloadSize() == 0) {
            logger.warn("Empty payload, IMPLICIT_PAYLOAD flag will be set when stream out.");
        }

        CompressionAlgorithmType finalCompressionType = this.compressionType;

        int dataToCompress = appData.payloadSize();
        // New style properties are not compressed.
        // TODO: remove after 2nd rollout of "new style" brokers.
        if (appData.hasProperties() && appData.isOldStyleProperties()) {
            dataToCompress += appData.propertiesSize();
        }

        // When data is less than a threshold, it is not compressed.
        if (dataToCompress < Protocol.COMPRESSION_MIN_APPDATA_SIZE) {
            finalCompressionType = CompressionAlgorithmType.E_NONE;
        }

        appData.compressData(finalCompressionType);
        header.setCompressionType(finalCompressionType.toInt());
    }

    // TODO: move to test code
    public void streamOut(ByteBufferOutputStream bbos) throws IOException {
        if (appData.hasProperties()) {
            int f = flags();
            if (!PushHeaderFlags.isSet(f, PushHeaderFlags.MESSAGE_PROPERTIES)) {
                f = PushHeaderFlags.setFlag(f, PushHeaderFlags.MESSAGE_PROPERTIES);
                header.setFlags(f);
            }

            // If properties are encoded using new style, we need to set
            // schema wire id to 1 (invalid schema wire id).
            // TODO: always set after 2nd rollout of "new style" brokers.
            if (!appData.isOldStyleProperties()) {
                header.setSchemaWireId(INVALID_SCHEMA_WIRE_ID);
            }
        }

        if (appData.payloadSize() == 0) {
            header.setFlags(
                    PushHeaderFlags.setFlag(header.flags(), PushHeaderFlags.IMPLICIT_PAYLOAD));
        }

        final int numWords = ProtocolUtil.calculateNumWords(appData.unpackedSize());
        header.setHeaderWords(HEADER_WORDS);
        header.setMessageWords(HEADER_WORDS + numWords);
        header.streamOut(bbos);
        appData.streamOut(bbos);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(header.toString())
                .append("[ PushMessage ")
                .append(options.toString())
                .append(appData.toString())
                .append(" ]");
        return sb.toString();
    }
}
