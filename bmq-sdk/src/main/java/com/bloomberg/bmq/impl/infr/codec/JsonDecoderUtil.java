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

import com.bloomberg.bmq.impl.infr.io.ByteBufferInputStream;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

public final class JsonDecoderUtil {

    private static Gson gson = new Gson();

    public static <T> T decode(ByteBufferInputStream bbis, Type msgType) throws IOException {
        final String json = getJsonFromInputStream(bbis);
        return decodeFromJson(json, msgType);
    }

    public static String getJsonFromInputStream(ByteBufferInputStream bbis) throws IOException {
        final int bytesAvailable = bbis.available();
        byte[] bytes = new byte[bytesAvailable];

        final int bytesRead = bbis.read(bytes);

        if (bytesRead != bytesAvailable) {
            throw new IOException(
                    "Failed to read expected num bytes while decoding. "
                            + "Expected to read: "
                            + bytesAvailable
                            + " bytes, actually read: "
                            + bytesRead
                            + " bytes.");
        }

        final int padding = bytes[bytes.length - 1];
        if (padding < 0 || padding > 4) {
            throw new IOException("Invalid padding value encountered while decoding " + padding);
        }

        return new String(bytes, 0, bytes.length - padding, StandardCharsets.UTF_8);
    }

    public static <T> T decodeFromJson(String json, Type msgType) throws JsonSyntaxException {
        return gson.fromJson(json, msgType);
    }

    private JsonDecoderUtil() {
        throw new IllegalStateException("Utility class");
    }
}
