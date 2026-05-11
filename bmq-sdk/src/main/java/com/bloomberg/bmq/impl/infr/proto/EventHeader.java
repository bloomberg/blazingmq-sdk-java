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
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.infr.util.BitUtil;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class EventHeader {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // This class represents the header for all the events received by the
    // broker from local clients or from peer brokers.  A well-behaved event
    // header will always have its fragment bit set to zero.

    // EventHeader datagram [8 bytes]:
    // ..
    //   +---------------+---------------+---------------+---------------+
    //   |0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|0|1|2|3|4|5|6|7|
    //   +---------------+---------------+---------------+---------------+
    //   |F|                          Length                             |
    //   +---------------+---------------+---------------+---------------+
    //   |PV |   Type    |  HeaderWords  | TypeSpecific  |    Reserved   |
    //   +---------------+---------------+---------------+---------------+
    //       F..: Fragment
    //       PV.: Protocol Version
    //
    //  Fragment (F)..........: Always set to 0
    //  Length................: Total size (bytes) of this event
    //  Protocol Version (PV).: Protocol Version (up to 4 concurrent versions)
    //  Type..................: Type of the event (from EventType::Enum)
    //  HeaderWords...........: Number of words of this event header
    //  TypeSpecific..........: Content specific to the event's type, see below
    //  Reserved..............: For alignment and extension ~ must be 0
    // ..
    //
    // TypeSpecific content:
    // : o ControlMessage: represent the encoding used for that control message
    //      |0|1|2|3|4|5|6|7|
    //      +---------------+
    //      |CODEC| Reserved|
    //
    // NOTE: The HeaderWords allows to eventually put event level options
    //       (either by extending the EventHeader struct, or putting new struct
    //       after the EventHeader).  For now, this is left up for future
    //       enhancement as there are no use-case for an event level option.

    private int fragmentBitAndLength;
    private byte protocolVersionAndType;
    private byte headerWords;
    private byte typeSpecific;
    private byte reserved;

    private static final int FRAGMENT_NUM_BITS = 1;
    private static final int LENGTH_NUM_BITS = 31;
    private static final int PROTOCOL_VERSION_NUM_BITS = 2;
    private static final int TYPE_NUM_BITS = 6;

    private static final int FRAGMENT_START_IDX = 31;
    private static final int LENGTH_START_IDX = 0;
    private static final int PROTOCOL_VERSION_START_IDX = 6;
    private static final int TYPE_START_IDX = 0;

    private static final int FRAGMENT_MASK = BitUtil.oneMask(FRAGMENT_START_IDX, FRAGMENT_NUM_BITS);

    private static final int LENGTH_MASK = BitUtil.oneMask(LENGTH_START_IDX, LENGTH_NUM_BITS);

    private static final int PROTOCOL_VERSION_MASK =
            BitUtil.oneMask(PROTOCOL_VERSION_START_IDX, PROTOCOL_VERSION_NUM_BITS);

    private static final int TYPE_MASK = BitUtil.oneMask(TYPE_START_IDX, TYPE_NUM_BITS);

    public static final int MAX_SIZE_HARD = (1 << LENGTH_NUM_BITS) - 1;
    // Maximum size (bytes) of a full event, per protocol limitations.

    public static final int MAX_SIZE_SOFT = (64 + 2) * 1024 * 1024; // 66 MB
    // Maximum size (bytes) of a full event, enforced.  Note that this
    // constant needs to be greater than PutHeader.MAX_PAYLOAD_SIZE_SOFT.
    // This is because a PUT message sent on the wire contains EventHeader
    // as well as PutHeader, and some padding bytes, and
    // EventHeader.MAX_SIZE_SOFT needs to be large enough to allow those
    // headers as well.

    public static final int MAX_TYPE = (1 << TYPE_NUM_BITS) - 1;
    // Highest possible value for the type of an event.

    public static final int MAX_HEADER_SIZE = ((1 << 8) - 1) * Protocol.WORD_SIZE;
    // Maximum size (bytes) of an 'EventHeader'.

    public static final int MIN_HEADER_SIZE = 6;
    // Minimum size (bytes) of an 'EventHeader' (that is sufficient to
    // capture header words).  This value should *never* change.

    public static final int HEADER_SIZE = 8;
    // Current size (bytes) of the header.

    public static final int CONTROL_EVENT_ENCODING_NUM_BITS = 3;

    public static final int CONTROL_EVENT_ENCODING_START_IDX = 5;

    public static final byte CONTROL_EVENT_ENCODING_MASK = (byte) 0b11100000;

    // FIXME: same as MessageHeader.java
    // ctor, setters & getters for each field, streamIn, streamOut, equals(),
    //

    public EventHeader() {
        setLength(HEADER_SIZE);
        setProtocolVersion((byte) Protocol.VERSION);
        setType(EventType.UNDEFINED);
        setHeaderWords((byte) (HEADER_SIZE / Protocol.WORD_SIZE));
        typeSpecific = 0;
        reserved = 0;
    }

    public void setLength(int value) {
        fragmentBitAndLength = (fragmentBitAndLength & FRAGMENT_MASK) | (value & LENGTH_MASK);
    }

    public void setTypeSpecific(byte typeSpecific) {
        this.typeSpecific = typeSpecific;
    }

    public void setProtocolVersion(byte value) {
        protocolVersionAndType =
                (byte)
                        ((protocolVersionAndType & TYPE_MASK)
                                | (value << PROTOCOL_VERSION_START_IDX));
    }

    public void setType(EventType value) {
        protocolVersionAndType =
                (byte)
                        ((protocolVersionAndType & PROTOCOL_VERSION_MASK)
                                | (value.toInt() & TYPE_MASK));
    }

    public void setHeaderWords(byte value) {
        headerWords = value;
    }

    public byte typeSpecific() {
        return typeSpecific;
    }

    public int fragmentBit() {
        return (fragmentBitAndLength & FRAGMENT_MASK) >>> FRAGMENT_START_IDX;
    }

    public int length() {
        return fragmentBitAndLength & LENGTH_MASK;
    }

    public byte protocolVersion() {
        return (byte)
                ((protocolVersionAndType & PROTOCOL_VERSION_MASK)
                        >> // Spotbugs requires to use signed right shift here
                        PROTOCOL_VERSION_START_IDX);
    }

    public EventType type() {
        return EventType.fromInt(protocolVersionAndType & TYPE_MASK);
    }

    public byte headerWords() {
        return headerWords;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof EventHeader)) {
            return false; // RETURN
        }

        if (this == obj) {
            return true; // RETURN
        }
        EventHeader that = (EventHeader) obj;
        return (fragmentBit() == that.fragmentBit()
                && length() == that.length()
                && protocolVersion() == that.protocolVersion()
                && type() == that.type()
                && headerWords() == that.headerWords());
    }

    @Override
    public int hashCode() {
        Long l = (long) fragmentBitAndLength;
        l <<= Integer.SIZE;
        l |= (long) (headerWords & 0xff);
        l <<= Byte.SIZE;
        l |= (long) (protocolVersionAndType & 0xff);
        l <<= Byte.SIZE;
        l |= (long) (reserved & 0xff);
        l <<= Byte.SIZE;
        l |= (long) (typeSpecific & 0xff);
        return l.hashCode();
    }

    public void streamIn(ByteBufferInputStream bbis) throws IOException {
        fragmentBitAndLength = bbis.readInt();
        protocolVersionAndType = bbis.readByte();
        headerWords = bbis.readByte();
        typeSpecific = bbis.readByte();
        reserved = bbis.readByte();

        if (headerWords() * Protocol.WORD_SIZE > HEADER_SIZE) {
            // Read and ignore bytes that we don't know in the header.

            int numExtraBytes = headerWords() * Protocol.WORD_SIZE - HEADER_SIZE;
            byte[] headerExtra = new byte[numExtraBytes];
            int readBytes = bbis.read(headerExtra);
            assert readBytes == numExtraBytes;
        }
    }

    public void streamOut(ByteBufferOutputStream bbos) throws IOException {
        bbos.writeInt(fragmentBitAndLength);
        bbos.writeByte(protocolVersionAndType);
        bbos.writeByte(headerWords);
        bbos.writeByte(typeSpecific);
        bbos.writeByte(reserved);
    }

    public void setControlEventEncodingType(EncodingType type) {
        Argument.expectCondition(type != EncodingType.UNKNOWN, "Unexpected encoding type");
        if (this.type() != EventType.CONTROL) {
            throw new IllegalStateException("Unexpected call");
        }

        byte typeSpecificUpdate = typeSpecific();

        // Reset the bits for encoding type
        typeSpecificUpdate &= ~CONTROL_EVENT_ENCODING_MASK;

        byte typeAsByte = (byte) (type.toInt());
        // Set those bits to represent 'type'
        typeSpecificUpdate |= (byte) (typeAsByte << CONTROL_EVENT_ENCODING_START_IDX);

        setTypeSpecific(typeSpecificUpdate);
    }

    public EncodingType controlEventEncodingType() {
        // PRECONDITIONS
        if (this.type() != EventType.CONTROL) {
            throw new IllegalStateException("Unexpected call");
        }

        return EncodingType.fromInt(
                (typeSpecific() & CONTROL_EVENT_ENCODING_MASK)
                        >>> CONTROL_EVENT_ENCODING_START_IDX);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[ EventHeader [")
                .append(" FragmentBit=")
                .append(fragmentBit())
                .append(" Length=")
                .append(length())
                .append(" Type=")
                .append(type())
                .append(" HeaderWords=")
                .append(headerWords())
                .append(" TypeSpecific=")
                .append(typeSpecific())
                .append(" ] ]");
        return sb.toString();
    }
}
