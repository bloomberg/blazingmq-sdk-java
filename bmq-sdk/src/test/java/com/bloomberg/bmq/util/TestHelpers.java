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
package com.bloomberg.bmq.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import com.bloomberg.bmq.BMQException;
import com.bloomberg.bmq.impl.infr.msg.MessagesTestSamples.SampleFileMetadata;
import com.bloomberg.bmq.impl.infr.util.Argument;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHelpers {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static ByteBuffer filledBuffer(int size) {
        byte[] buf = new byte[size];
        ByteBuffer filled = ByteBuffer.allocate(size);
        filled.put(buf);
        return filled;
    }

    public static ByteBuffer readFile(final String fileName) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (InputStream ins = TestHelpers.class.getResourceAsStream(fileName)) {
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

    public static byte[] buffersContents(ByteBuffer[] buffers) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        for (ByteBuffer buf : buffers) {
            ByteBuffer dup = buf.duplicate();
            if (dup.position() != 0 && dup.limit() == dup.capacity()) {
                dup.flip();
            }
            byte[] temp = new byte[dup.remaining()];
            dup.get(temp, 0, temp.length);
            os.write(temp);
        }
        return os.toByteArray();
    }

    public static byte[] resourceFileContents(String path, int size) throws IOException {
        try (InputStream in = TestHelpers.class.getResourceAsStream(path)) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            int pos = 0;
            while (pos++ < size) {
                int b = in.read();
                if (b < 0) break;
                os.write(b);
            }
            return os.toByteArray();
        }
    }

    public static void compareWithFileContent(
            ByteBuffer[] message, SampleFileMetadata messageSample) throws IOException {

        int contentLength = messageSample.length();
        String filePath = messageSample.filePath();

        // Read 'contentLength' bytes from the file at 'filePath', and compare
        // those bytes with 'message'.
        byte[] messageContent = buffersContents(message);
        byte[] fileContent = resourceFileContents(filePath, contentLength);
        assertArrayEquals(fileContent, messageContent);
    }

    public static void acquireSema(Semaphore sema, int sec) {
        Argument.expectPositive(sec, "sec");

        try {
            if (!sema.tryAcquire(sec, TimeUnit.SECONDS)) {
                throw new BMQException("Semaphore timeout");
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted: ", e);
            Thread.currentThread().interrupt();
        }
    }

    public static <T> T poll(BlockingQueue<T> queue, Duration timeout) {
        Argument.expectNonNull(queue, "queue");
        Argument.expectNonNull(timeout, "timeout");
        T obj = null;
        try {
            obj = queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);

            if (obj == null) {
                logger.info("Queue poll timeout");
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted: ", e);
            Thread.currentThread().interrupt();
        }

        return obj;
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

    public static void sleepForMilliSeconds(int millies) {
        try {
            Thread.sleep(millies);
        } catch (InterruptedException e) {
            logger.error("Interrupted: ", e);
            Thread.currentThread().interrupt();
        }
    }
}
