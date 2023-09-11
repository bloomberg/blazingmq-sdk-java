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
import com.bloomberg.bmq.CorrelationId;
import com.bloomberg.bmq.MessageProperties;
import com.bloomberg.bmq.impl.CorrelationIdImpl;
import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.proto.intf.Streamable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PutMessageImpl implements Streamable {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final short INVALID_SCHEMA_WIRE_ID = (short) 1;
    private static final int HEADER_WORDS = PutHeader.HEADER_SIZE / Protocol.WORD_SIZE;

    private final PutHeader header = new PutHeader();
    private final ApplicationData appData = new ApplicationData();

    private CompressionAlgorithmType compressionType;

    public PutMessageImpl() {
        compressionType = CompressionAlgorithmType.E_NONE;
    }

    public void setQueueId(int id) {
        header.setQueueId(id);
    }

    public int queueId() {
        return header.queueId();
    }

    public CorrelationId setCorrelationId(Object value) {
        return setupCorrelationId(CorrelationIdImpl.nextId(value));
    }

    public CorrelationId setCorrelationId() {
        return setupCorrelationId(CorrelationIdImpl.nextId());
    }

    public void setCompressionType(CompressionAlgorithmType compressionType) {
        this.compressionType = compressionType;
    }

    public CorrelationIdImpl setupCorrelationId(CorrelationIdImpl id) {
        header.setCorrelationId(id);
        requestAcknowledgement();
        return id;
    }

    public CorrelationId correlationId() {
        CorrelationIdImpl res = header.correlationId();
        if (!res.isValid()) {
            res = null;
        }
        return res;
    }

    private void requestAcknowledgement() {
        int putFlags = flags();
        putFlags = PutHeaderFlags.setFlag(putFlags, PutHeaderFlags.ACK_REQUESTED);
        setFlags(putFlags);
    }

    public MessageProperties messageProperties() {
        if (appData.properties() == null) {
            appData.setProperties(new MessagePropertiesImpl());
        }
        return appData.properties();
    }

    public void compressData() throws IOException {
        if (appData.payloadSize() == 0) {
            throw new IllegalStateException("Empty payload.");
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

    public void calculateAndSetCrc32c() throws IOException {
        header.setCrc32c(appData.calculateCrc32c());
    }

    public long crc32c() {
        return header.crc32c();
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

    public PutHeader header() {
        return header;
    }

    // TODO: move to test code
    @Override
    public void streamIn(ByteBufferInputStream bbis) throws IOException {
        header.streamIn(bbis);
        int totalSize = header.messageWords() * Protocol.WORD_SIZE;
        int headerSize = header.headerWords() * Protocol.WORD_SIZE;
        int appDataSize = totalSize - headerSize;
        if (totalSize <= 0 || headerSize <= 0 || bbis.available() < appDataSize) {
            throw new IOException("Cannot read application data.");
        }
        int optionSize = header.optionsWords() * Protocol.WORD_SIZE;
        int dataSize = appDataSize - optionSize;
        // Skip options. TODO: implement me
        if (bbis.skip(optionSize) != optionSize) {
            throw new IOException("Failed to skip " + optionSize + " bytes.");
        }

        final boolean hasProperties =
                PutHeaderFlags.isSet(header.flags(), PutHeaderFlags.MESSAGE_PROPERTIES);
        final CompressionAlgorithmType inputCompressionType =
                CompressionAlgorithmType.fromInt(header.compressionType());
        final boolean isOldStyleProperties = header.schemaWireId() == 0;

        appData.streamIn(dataSize, hasProperties, isOldStyleProperties, inputCompressionType, bbis);

        if (appData.unpackedSize() == 0) {
            throw new BMQException("Application data is empty.");
        }
    }

    public void streamOut(ByteBufferOutputStream bbos) throws IOException {
        if (appData.hasProperties()) {
            int f = flags();
            if (!PutHeaderFlags.isSet(f, PutHeaderFlags.MESSAGE_PROPERTIES)) {
                f = PutHeaderFlags.setFlag(f, PutHeaderFlags.MESSAGE_PROPERTIES);
                setFlags(f);
            }

            // If properties are encoded using new style, we need to set
            // schema wire id to 1 (invalid schema wire id) in order to tell the
            // broker that PUT message contains new style properties without
            // schema id.
            // TODO: always set after 2nd rollout of "new style" brokers.
            if (!appData.isOldStyleProperties()) {
                header.setSchemaWireId(INVALID_SCHEMA_WIRE_ID);
            }
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
                .append("[ PutMessage ")
                .append(appData.toString())
                .append(" ]");
        return sb.toString();
    }
}
