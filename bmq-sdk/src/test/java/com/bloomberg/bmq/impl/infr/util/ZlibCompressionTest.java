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
package com.bloomberg.bmq.impl.infr.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.io.LimitedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ZlibCompressionTest {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final String TEXT =
            "Some string with some text about something and another string with another text about something more";
    private final String ZIPPED64 =
            "eJwLzs9NVSguKcrMS1cozyzJUCgGCZSkVpQoJCbll5aA+SUZIOnEvBQgzi/JSC1C0QETw6opN78oFQBxgiXz";

    @Test
    void testCompress() throws IOException {
        // Prepare expected data
        ByteBufferOutputStream bbos = new ByteBufferOutputStream();

        // Write some data before
        bbos.writeUTF("compressed");

        byte[] zipped = Base64.getDecoder().decode(ZIPPED64);
        bbos.write(zipped);

        // Write some data after
        bbos.writeUTF("data");

        ByteBuffer[] expected = bbos.reset();

        // Prepare actual data
        bbos = new ByteBufferOutputStream();

        // Write some data before
        bbos.writeUTF("compressed");

        ZlibCompression compression = new ZlibCompression();

        // We need to close compressed stream in order to flush all compressed bytes
        // from compressor to compressed stream to underlying stream.
        //
        // The problem is that underlying stream`s close() method is called automatically.
        //
        // That's ok for now because ByteBufferOutputStream is not closeable (close() method is
        // empty).
        //
        // Later we will need to refactor the code in order to close compressed stream without
        // closing underlying stream
        try (OutputStream os = compression.compress(bbos)) {
            byte[] bytes = TEXT.getBytes(StandardCharsets.UTF_8);
            os.write(bytes);
        }

        // Write some data after
        bbos.writeUTF("data");

        ByteBuffer[] actual = bbos.reset();

        // Compare
        assertArrayEquals(expected, actual);
    }

    @Test
    void testDecompress() throws IOException {
        ZlibCompression compression = new ZlibCompression();

        byte[] zipped = Base64.getDecoder().decode(ZIPPED64);

        // Create array of ByteBuffers.
        // All three elements contain data to decompress.
        // The last ByteBuffer contains the last compressed byte and extra bytes which
        // should not be read and decompressed by decompressing stream.
        ByteBuffer[] data = new ByteBuffer[3];
        data[0] = ByteBuffer.wrap(Arrays.copyOfRange(zipped, 0, zipped.length / 2));
        data[1] = ByteBuffer.wrap(Arrays.copyOfRange(zipped, zipped.length / 2, zipped.length - 1));
        data[2] = ByteBuffer.wrap(new byte[] {zipped[zipped.length - 1], 1, 2, 3});

        ByteBufferInputStream bbis = new ByteBufferInputStream(data);

        // In order not to read extra bytes by decompressing stream,
        // ByteBufferInputStream is wrapped into LimitedInputStream,
        // which provides a limited number of input bytes to decompressing stream.
        // In other words, LimitedInputStream allows decompressing stream to read only first "n"
        // bytes.
        // The limit is set to the length of compressed data.
        LimitedInputStream lis = new LimitedInputStream(bbis, zipped.length);

        // Wrap compressed bytes into decompressing stream
        InputStream is = compression.decompress(lis);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        byte[] buf = new byte[1024];
        int read;

        while ((read = is.read(buf)) > 0) {
            baos.write(buf, 0, read);
        }

        // Check that decompressing stream has read all data from limited stream
        assertEquals(0, lis.available());

        // Check that extra bytes have not been read by decompressing stream and are
        // available to read from source stream
        assertEquals(3, bbis.available());
        assertEquals(1, bbis.read());
        assertEquals(2, bbis.read());
        assertEquals(3, bbis.read());
        assertEquals(0, bbis.available());

        byte[] uncompressed = baos.toByteArray();

        ByteBuffer b = ByteBuffer.wrap(uncompressed);
        String str = StandardCharsets.UTF_8.decode(b).toString();

        logger.info("Uncompressed string: {}", str);
        assertEquals(TEXT, str);
    }
}
