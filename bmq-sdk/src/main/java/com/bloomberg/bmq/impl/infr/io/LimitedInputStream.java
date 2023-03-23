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
package com.bloomberg.bmq.impl.infr.io;

import com.bloomberg.bmq.impl.infr.util.Argument;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps input stream and allows readers to read only first "n" bytes from it.
 *
 * <p>Can be used when you have an algorithm that reads bytes from input stream but for some reason
 * you don't want that algorithm to read the whole stream but just first "n" bytes.
 *
 * <p>In this case you may wrap source stream into {@code LimitedInputStream}, set the limit to some
 * "n" and use it as an input to alogirthm.
 *
 * <p>Here is an example:
 *
 * <pre>
 *
 * // Some algorithm from some library that takes input stream and writes its content into some file.
 * void writeToDisk(InputStream stream, String fileName) { ... }
 *
 * void func(InputStream stream) {
 *     String fileName = "some name";
 *
 *     // We want to write to disk only first 100 byte of the stream
 *     LimitedInputStream limitedStream = new LimitedInputStream(stream, 100);
 *
 *     writeToDisk(limitedStream, fileName);
 *
 *     // If the source stream was larger than 100 bytes, the rest bytes are available to read.
 *     // For instance, read next 50 bytes from source stream
 *     byte[50] bytes = new byte[50];
 *     stream.read(bytes);
 * }
 * </pre>
 */
public class LimitedInputStream extends FilterInputStream {
    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final int limit;
    private int read;

    public LimitedInputStream(InputStream in, int limit) throws IOException {
        super(in);

        Argument.expectNonNull(in, "source stream");
        Argument.expectNonNegative(limit, "limit");
        Argument.expectNotGreater(limit, in.available(), "limit");

        this.limit = limit;
    }

    @Override
    public int read() throws IOException {
        if (read >= limit) {
            return -1;
        }

        int i = in.read();
        read++;

        return i;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (read >= limit) {
            return -1;
        }

        int toRead = Math.min(limit - read, len);
        int i = in.read(b, off, toRead);
        read += i;

        return i;
    }

    @Override
    public int available() {
        return limit - read;
    }
}
