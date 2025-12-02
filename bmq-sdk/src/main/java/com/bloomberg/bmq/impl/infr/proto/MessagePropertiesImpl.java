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

import com.bloomberg.bmq.MessageProperties;
import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessagePropertiesImpl implements MessageProperties {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final LinkedHashMap<String, MessageProperty> propertyMap = new LinkedHashMap<>();

    static boolean isValidString(String str) {
        CharsetEncoder asciiEncoder = StandardCharsets.US_ASCII.newEncoder();
        return asciiEncoder.canEncode(str);
    }

    boolean addProperty(MessageProperty p, String name) {
        boolean res = false;
        if (p != null && isValidString(name)) {
            p.setPropertyName(name);
            propertyMap.put(name, p);
            res = true;
        }
        return res;
    }

    @Override
    public boolean setPropertyAsBool(String name, boolean val) {
        MessageProperty p = new BoolMessageProperty(val);
        return addProperty(p, name);
    }

    @Override
    public boolean setPropertyAsByte(String name, byte val) {
        MessageProperty p = new ByteMessageProperty(val);
        return addProperty(p, name);
    }

    @Override
    public boolean setPropertyAsShort(String name, short val) {
        MessageProperty p = new ShortMessageProperty(val);
        return addProperty(p, name);
    }

    @Override
    public boolean setPropertyAsInt32(String name, int val) {
        MessageProperty p = new Int32MessageProperty(val);
        return addProperty(p, name);
    }

    @Override
    public boolean setPropertyAsInt64(String name, long val) {
        MessageProperty p = new Int64MessageProperty(val);
        return addProperty(p, name);
    }

    @Override
    public boolean setPropertyAsString(String name, String val) {
        boolean res = false;
        if (isValidString(val)) {
            MessageProperty p = new StringMessageProperty(val);
            res = addProperty(p, name);
        }
        return res;
    }

    @Override
    public boolean setPropertyAsBinary(String name, byte[] val) {
        MessageProperty p = new BinaryMessageProperty(val);
        return addProperty(p, name);
    }

    public int numProperties() {
        return propertyMap.size();
    }

    public int totalSize() {
        if (numProperties() == 0) {
            return 0;
        }

        int len = MessagePropertiesHeader.HEADER_SIZE;
        Set<Map.Entry<String, MessageProperty>> set = propertyMap.entrySet();
        for (Map.Entry<String, MessageProperty> e : set) {
            final int nameLen = e.getKey().length();
            final int valLen = e.getValue().valueLength();
            len += (nameLen + valLen + MessagePropertyHeader.HEADER_SIZE);
        }

        return len + ProtocolUtil.calculatePadding(len);
    }

    public Iterator<Map.Entry<String, MessageProperty>> iterator() {
        return Collections.unmodifiableMap(propertyMap).entrySet().iterator();
    }

    public MessageProperty get(String name) {
        return propertyMap.get(name);
    }

    // TODO: remove after 2nd rollout of "new style" brokers
    public <T extends InputStream & DataInput> int streamInOld(T input) throws IOException {
        propertyMap.clear();
        MessagePropertiesHeader propsHeader = new MessagePropertiesHeader();
        propsHeader.streamIn(input);

        final int numProps = propsHeader.numProperties();
        MessagePropertyHeader[] propHeaderArray = new MessagePropertyHeader[numProps];

        final int propertyHeaderSize = propsHeader.messagePropertyHeaderSize();
        for (int i = 0; i < numProps; i++) {
            MessagePropertyHeader ph = new MessagePropertyHeader();
            ph.streamIn(input, propertyHeaderSize);
            propHeaderArray[i] = ph;
        }

        final int propsAreaSize = propsHeader.messagePropertiesAreaWords() * Protocol.WORD_SIZE;
        final int headersAreaSize = propsHeader.headerSize() + numProps * propertyHeaderSize;

        // Since the input stream type doesn't support setting of position,
        // we cannot determine padding bytes before we read all properties.
        int totalLength = headersAreaSize;

        for (int i = 0; i < numProps; ++i) {
            MessagePropertyHeader ph = propHeaderArray[i];
            MessageProperty mp;
            PropertyType t = PropertyType.fromInt(ph.propertyType());
            switch (t) {
                case BOOL:
                    mp = new BoolMessageProperty();
                    break;
                case BYTE:
                    mp = new ByteMessageProperty();
                    break;
                case SHORT:
                    mp = new ShortMessageProperty();
                    break;
                case INT32:
                    mp = new Int32MessageProperty();
                    break;
                case INT64:
                    mp = new Int64MessageProperty();
                    break;
                case STRING:
                    mp = new StringMessageProperty();
                    break;
                case BINARY:
                    mp = new BinaryMessageProperty();
                    break;
                default:
                    throw new IOException("Unknown property type");
            }
            final int nameLength = ph.propertyNameLength();
            final int valueLength = ph.propertyValueLength();

            totalLength += nameLength;
            totalLength += valueLength;

            byte[] n = new byte[nameLength];
            byte[] v = new byte[valueLength];

            // Since 'read(byte[])' might read just part of bytes, 'readFully(byte[])' is used
            // instead
            try {
                input.readFully(n);
            } catch (IOException e) {
                throw new IOException(
                        "Error when reading property name. Expected to read " + n.length + " bytes",
                        e);
            }
            mp.setPropertyName(new String(n, StandardCharsets.US_ASCII));

            // Since 'read(byte[])' might read just part of bytes, 'readFully(byte[])' is used
            // instead
            try {
                input.readFully(v);
            } catch (IOException e) {
                throw new IOException(
                        "Error when reading property value. Expected to read "
                                + v.length
                                + " bytes",
                        e);
            }
            mp.setPropertyValue(v);

            propertyMap.put(mp.name(), mp);
        }

        // Read padding bytes
        final int numPaddingBytes = input.readByte();

        // Skip padding bytes
        if (input.skip(numPaddingBytes - 1) != numPaddingBytes - 1) {
            throw new IOException("Failed to skip " + (numPaddingBytes - 1) + " bytes");
        }

        // Verify
        final int numPaddingBytesExp = ProtocolUtil.calculatePadding(totalLength);
        if (numPaddingBytesExp != numPaddingBytes) {
            throw new IOException(
                    "Unexpected padding: " + numPaddingBytes + ", should be " + numPaddingBytesExp);
        }

        // Add padding bytes
        totalLength += numPaddingBytes;

        if (totalLength != propsAreaSize) {
            throw new IOException(
                    "Invalid encoding: actual "
                            + totalLength
                            + " bytes, expected "
                            + propsAreaSize
                            + " bytes");
        }

        return totalLength;
    }

    public int streamIn(ByteBufferInputStream input) throws IOException {
        propertyMap.clear();
        final int initPos = input.position();
        MessagePropertiesHeader propsHeader = new MessagePropertiesHeader();
        propsHeader.streamIn(input);

        final int numProps = propsHeader.numProperties();
        MessagePropertyHeader[] propHeaderArray = new MessagePropertyHeader[numProps];

        final int propertyHeaderSize = propsHeader.messagePropertyHeaderSize();
        for (int i = 0; i < numProps; i++) {
            MessagePropertyHeader ph = new MessagePropertyHeader();
            ph.streamIn(input, propertyHeaderSize);
            propHeaderArray[i] = ph;
        }

        final int propsAreaSize = propsHeader.messagePropertiesAreaWords() * Protocol.WORD_SIZE;
        final int headersAreaSize = propsHeader.headerSize() + numProps * propertyHeaderSize;

        final int dataPos = input.position();
        if (headersAreaSize != (dataPos - initPos)) {
            throw new IOException("Invalid headers are size: " + headersAreaSize);
        }

        // Get padding bytes
        input.reset();
        if (input.skip(initPos) != initPos) {
            throw new IOException("Failed to skip " + initPos + " bytes.");
        }
        final int totalSize = ProtocolUtil.calculateUnpaddedLength(input, propsAreaSize);
        final int numPaddingBytes = propsAreaSize - totalSize;
        int totalLength = headersAreaSize;

        // Reset back to data position
        input.reset();
        if (input.skip(dataPos) != dataPos) {
            throw new IOException("Failed to skip " + dataPos + " bytes.");
        }

        // Read properties
        for (int i = 0; i < numProps; ++i) {
            final boolean isLastProperty = i == numProps - 1;

            MessagePropertyHeader ph = propHeaderArray[i];
            MessageProperty mp;
            PropertyType t = PropertyType.fromInt(ph.propertyType());
            switch (t) {
                case BOOL:
                    mp = new BoolMessageProperty();
                    break;
                case BYTE:
                    mp = new ByteMessageProperty();
                    break;
                case SHORT:
                    mp = new ShortMessageProperty();
                    break;
                case INT32:
                    mp = new Int32MessageProperty();
                    break;
                case INT64:
                    mp = new Int64MessageProperty();
                    break;
                case STRING:
                    mp = new StringMessageProperty();
                    break;
                case BINARY:
                    mp = new BinaryMessageProperty();
                    break;
                default:
                    throw new IOException("Unknown property type");
            }
            final int nameLength = ph.propertyNameLength();
            final int offset = ph.propertyValueLength();
            int valueLength;

            if (!isLastProperty) {
                // Calculate the length as delta between offsets minus
                // current property name length.
                final int nextOffset = propHeaderArray[i + 1].propertyValueLength();
                valueLength = nextOffset - offset - nameLength;
            } else {
                // Last property.
                // Calculate the length as delta between the total and property
                // offset  minus headers area minus property name length.
                valueLength = totalSize - offset - headersAreaSize - nameLength;
            }

            totalLength += nameLength;
            totalLength += valueLength;

            byte[] n = new byte[nameLength];
            byte[] v = new byte[valueLength];

            // Since 'read(byte[])' might read just part of bytes, 'readFully(byte[])' is used
            // instead
            try {
                input.readFully(n);
            } catch (IOException e) {
                throw new IOException(
                        "Error when reading property name. Expected to read " + n.length + " bytes",
                        e);
            }
            mp.setPropertyName(new String(n, StandardCharsets.US_ASCII));

            // Since 'read(byte[])' might read just part of bytes, 'readFully(byte[])' is used
            // instead
            try {
                input.readFully(v);
            } catch (IOException e) {
                throw new IOException(
                        "Error when reading property value. Expected to read "
                                + v.length
                                + " bytes",
                        e);
            }
            mp.setPropertyValue(v);

            propertyMap.put(mp.name(), mp);
        }

        // Skip padding bytes
        if (input.skip(numPaddingBytes) != numPaddingBytes) {
            throw new IOException("Failed to skip " + numPaddingBytes + " bytes");
        }

        // Add padding bytes
        totalLength += numPaddingBytes;

        if (totalLength != propsAreaSize) {
            throw new IOException(
                    "Invalid encoding: actual "
                            + totalLength
                            + " bytes, expected "
                            + propsAreaSize
                            + " bytes");
        }

        return totalLength;
    }

    // TODO: remove after 2nd rollout of "new style" brokers
    public void streamOutOld(DataOutput output) throws IOException {
        streamOut(output, true);
    }

    // TODO: remove after 2nd rollout of "new style" brokers
    public void streamOut(DataOutput output) throws IOException {
        streamOut(output, false);
    }

    // TODO: remove boolean after 2nd rollout of "new style" brokers
    private void streamOut(DataOutput output, boolean isOldStyleProperties) throws IOException {
        final int numProps = propertyMap.size();
        if (numProps == 0) {
            logger.info("No message properties to stream out");
            return; // RETURN
        }
        MessagePropertyHeader[] propHeaderArray = new MessagePropertyHeader[numProps];
        int i = 0;
        int offset = 0;
        int len = MessagePropertiesHeader.HEADER_SIZE;
        Set<Map.Entry<String, MessageProperty>> set = propertyMap.entrySet();
        for (Map.Entry<String, MessageProperty> e : set) {
            final int nameLen = e.getKey().length();
            final int valLen = e.getValue().valueLength();
            MessagePropertyHeader mph = new MessagePropertyHeader();
            mph.setPropertyType(e.getValue().type().toInt());

            if (isOldStyleProperties) {
                mph.setPropertyValueLength(valLen);
            } else {
                mph.setPropertyValueLength(offset);
            }

            mph.setPropertyNameLength(nameLen);

            int propLen = nameLen + valLen;

            len += (MessagePropertyHeader.HEADER_SIZE + propLen);
            offset += propLen;

            propHeaderArray[i] = mph;
            i++;
        }
        final int numPaddingBytes = ProtocolUtil.calculatePadding(len);
        final int numWords = ProtocolUtil.calculateNumWords(len);

        MessagePropertiesHeader propsHeader = new MessagePropertiesHeader();
        propsHeader.setNumProperties(numProps);
        propsHeader.setMessagePropertiesAreaWords(numWords);

        propsHeader.streamOut(output);
        for (MessagePropertyHeader mph : propHeaderArray) {
            mph.streamOut(output);
        }
        for (Map.Entry<String, MessageProperty> e : set) {
            output.write(e.getKey().getBytes(StandardCharsets.US_ASCII));
            output.write(e.getValue().value());
        }
        if (numPaddingBytes > 0) {
            output.write(ProtocolUtil.getPaddingBytes(numPaddingBytes), 0, numPaddingBytes);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ MessageProperties [ ");
        Set<Map.Entry<String, MessageProperty>> set = propertyMap.entrySet();
        for (Map.Entry<String, MessageProperty> e : set) {
            sb.append(e.getValue().toString());
        }
        sb.append(" ]");
        return sb.toString();
    }
}
