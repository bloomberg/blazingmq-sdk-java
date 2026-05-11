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
package com.bloomberg.bmq.it.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.bloomberg.bmq.MessageGUID;
import com.bloomberg.bmq.ResultCodes;
import com.bloomberg.bmq.impl.CorrelationIdImpl;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.msg.ConsumerInfo;
import com.bloomberg.bmq.impl.infr.msg.QueueHandleParameters;
import com.bloomberg.bmq.impl.infr.msg.StreamParameters;
import com.bloomberg.bmq.impl.infr.msg.SubQueueIdInfo;
import com.bloomberg.bmq.impl.infr.proto.AckEventBuilder;
import com.bloomberg.bmq.impl.infr.proto.AckMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.EventBuilderResult;
import com.bloomberg.bmq.impl.infr.proto.MessagePropertiesImpl;
import com.bloomberg.bmq.impl.infr.proto.PutHeaderFlags;
import com.bloomberg.bmq.impl.infr.proto.PutMessageImpl;
import com.bloomberg.bmq.impl.infr.util.Argument;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTools {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static final int DEFAULT_ACQUIRE_TIMEOUT = 180;

    public static final int MESSAGE_PROPERTIES_SIZE = 60;

    public static void acquireSema(Semaphore sema) {
        acquireSema(sema, DEFAULT_ACQUIRE_TIMEOUT);
    }

    public static void acquireSema(Semaphore sema, int sec) {
        acquireSema(sema, 1, sec);
    }

    public static void acquireSema(Semaphore sema, int permits, int sec) {
        Argument.expectPositive(permits, "permits");
        Argument.expectPositive(sec, "sec");

        try {
            if (!sema.tryAcquire(permits, sec, TimeUnit.SECONDS)) {
                logger.error("Semaphore timeout");
                Thread.dumpStack();
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted: ", e);
            Thread.currentThread().interrupt();
        }
    }

    public static void sleepForSeconds(int seconds) {
        try {
            Thread.sleep(seconds * 1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void sleepForMilliSeconds(int millies) {
        try {
            Thread.sleep(millies);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static ByteBuffer prepareUnpaddedData(String msg, boolean isOldStyleProperties)
            throws IOException {
        MessagePropertiesImpl props = new MessagePropertiesImpl();

        props.setPropertyAsString("routingId", "abcd-efgh-ijkl");
        props.setPropertyAsInt64("timestamp", 123456789L);

        ByteBufferOutputStream bbos = new ByteBufferOutputStream();

        if (isOldStyleProperties) {
            props.streamOutOld(bbos);
        } else {
            props.streamOut(bbos);
        }

        bbos.writeAscii(msg);

        ByteBuffer content = ByteBuffer.allocate(bbos.size());

        ByteBuffer[] bb = bbos.reset();
        // Fill content buffer with the payload and the padding
        for (ByteBuffer b : bb) {
            content.put(b);
        }

        content.flip();
        return content;
    }

    public static boolean equalContent(ByteBuffer buf, ByteBuffer[] bbuf) {
        Argument.expectNonNull(buf, "buf");

        ByteBuffer bb = mergeBuffers(bbuf);
        return bb.compareTo(buf) == 0;
    }

    public static ByteBuffer mergeBuffers(ByteBuffer[] bbuf) {
        Argument.expectNonNull(bbuf, "bbuf");

        int size = 0;
        for (ByteBuffer b : bbuf) {
            size += b.limit();
        }
        Argument.expectPositive(size, "bbuf total limits");

        ByteBuffer bb = ByteBuffer.allocate(size);
        for (ByteBuffer b : bbuf) {
            bb.put(b);
        }
        bb.flip();

        return bb;
    }

    public static PutMessageImpl preparePutMessage(String payload, boolean isOldStyleProperties)
            throws IOException {
        ByteBuffer b = ByteBuffer.wrap(payload.getBytes());

        int putFlags = 0;
        putFlags = PutHeaderFlags.setFlag(putFlags, PutHeaderFlags.ACK_REQUESTED);

        // Add 2 properties to the message.
        MessagePropertiesImpl mp = new MessagePropertiesImpl();
        assertTrue(mp.setPropertyAsString("routingId", "abcd-efgh-ijkl"));
        assertTrue(mp.setPropertyAsInt64("timestamp", 123456789L));

        assertEquals(MESSAGE_PROPERTIES_SIZE, mp.totalSize());

        PutMessageImpl putMsg = new PutMessageImpl();
        putMsg.appData().setProperties(mp);
        putMsg.appData().setPayload(b);

        putMsg.appData().setIsOldStyleProperties(isOldStyleProperties);
        putMsg.compressData();

        logger.debug("Application data size: {}", putMsg.appData().unpackedSize());

        putMsg.setFlags(putFlags);
        putMsg.setCorrelationId();
        return putMsg;
    }

    public static ByteBuffer[] prepareAckEventData(
            ResultCodes.AckResult status,
            CorrelationIdImpl correlationId,
            MessageGUID guid,
            int queueId)
            throws IOException {

        AckMessageImpl ackMsg = new AckMessageImpl(status, correlationId, guid, queueId);

        AckEventBuilder builder = new AckEventBuilder();
        assertEquals(EventBuilderResult.SUCCESS, builder.packMessage(ackMsg));

        return builder.build();
    }

    public static Object getInternalState(Object target, String field) {
        Field f;
        try {
            f = target.getClass().getDeclaredField(field); // NoSuchFieldException
        } catch (NoSuchFieldException e) {
            logger.error("Exception: ", e);
            fail();
            return null;
        }
        f.setAccessible(true);
        try {
            return f.get(target);
        } catch (IllegalAccessException e) {
            logger.error("Exception: ", e);
            fail();
            return null;
        }
    }

    static long getCurrentlyAllocatedMemoryMB() {
        final Runtime runtime = Runtime.getRuntime();
        return (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
    }

    static long getGcCount() {
        long sum = 0;
        for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
            long count = b.getCollectionCount();
            if (count != -1) {
                sum += count;
            }
        }
        return sum;
    }

    public static long getUsedMemoryMB() {
        long before = getGcCount();
        System.gc();
        while (getGcCount() == before) ;
        return getCurrentlyAllocatedMemoryMB();
    }

    public static void assertQueueHandleParamsAreEqual(
            QueueHandleParameters p1, QueueHandleParameters p2) {
        // todo modify with respect to subscriptions or remove if not used
        assertNotNull(p1);
        assertNotNull(p2);
        assertEquals(p1.getAdminCount(), p2.getAdminCount());
        assertEquals(p1.getReadCount(), p2.getReadCount());
        assertEquals(p1.getWriteCount(), p2.getWriteCount());
        assertEquals(p1.getUri().canonical(), p2.getUri().canonical());
        assertEquals(p1.getQId(), p2.getQId());
        assertEquals(p1.getFlags(), p2.getFlags());

        SubQueueIdInfo subIdInfo1 = p1.getSubIdInfo();
        SubQueueIdInfo subIdInfo2 = p2.getSubIdInfo();

        if (subIdInfo1 == null) {
            assertNull(subIdInfo2);
        } else {
            assertNotNull(subIdInfo2);
            assertEquals(subIdInfo1.subId(), subIdInfo2.subId());
            assertEquals(subIdInfo1.appId(), subIdInfo2.appId());
        }
    }

    public static ConsumerInfo getDefaultConsumerInfo(StreamParameters parameters) {
        // todo is the name of the method good? default might mean default initialize
        if (parameters.subscriptions().length != 1) {
            return null;
        }
        if (parameters.subscriptions()[0].consumers().length != 1) {
            return null;
        }
        return parameters.subscriptions()[0].consumers()[0];
    }

    public static void assertCloseConfigurationStreamParameters(StreamParameters params) {
        assertNotNull(params);

        assertEquals(0, params.subscriptions().length);
    }

    public static ByteBuffer readFile(final String fileName) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (InputStream ins = bos.getClass().getResourceAsStream(fileName); ) {
            int value;
            do {
                value = ins.read();
                if (value >= 0) {
                    bos.write(value);
                }
            } while (value >= 0);
        }
        ByteBuffer res = ByteBuffer.allocateDirect(bos.size());
        res.put(bos.toByteArray());
        res.flip();
        return res;
    }

    public static String readFileContent(String fileName) throws IOException {
        ByteBuffer bb = readFile(fileName);
        return StandardCharsets.UTF_8.decode(bb).toString();
    }
}
