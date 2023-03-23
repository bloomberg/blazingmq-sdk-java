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
package com.bloomberg.bmq.impl.infr.codec;

import com.bloomberg.bmq.impl.infr.io.ByteBufferOutputStream;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.google.gson.Gson;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public final class JsonEncoderUtil {

    private static Gson gson = new Gson();

    public static void encode(ByteBufferOutputStream bbos, Object message) throws IOException {
        Argument.expectNonNull(message, "message");
        byte[] jsonBytes =
                gson.toJson(message, message.getClass())
                        .replace("/", "\\/")
                        .getBytes(StandardCharsets.US_ASCII);
        Argument.expectNonNull(bbos, "bbos").write(jsonBytes);
        // NOTE: to achieve binary compatibility with C++ BMQ SDK we escape
        // forward slashes as far as it is default BDE JSON lib behaviour.
    }

    private JsonEncoderUtil() {
        throw new IllegalStateException("Utility class");
    }
}
