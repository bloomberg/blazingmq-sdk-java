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
import com.bloomberg.bmq.CompressionAlgorithm;
import com.bloomberg.bmq.impl.infr.util.Compression;
import com.bloomberg.bmq.impl.infr.util.ZlibCompression;

public enum CompressionAlgorithmType {
    E_NONE(0, CompressionAlgorithm.None),
    E_ZLIB(1, CompressionAlgorithm.Zlib);

    private final int type;
    private final CompressionAlgorithm algorithm;

    CompressionAlgorithmType(int type, CompressionAlgorithm algorithm) {
        this.type = type;
        this.algorithm = algorithm;
    }

    int toInt() {
        return type;
    }

    CompressionAlgorithm toAlgorithm() {
        return algorithm;
    }

    Compression getCompression() {
        Compression compression = null;

        switch (this) {
            case E_ZLIB:
                compression = new ZlibCompression();
                break;
            default:
                throw new BMQException(String.format("No compression for type '%s'", this));
        }

        return compression;
    }

    static CompressionAlgorithmType fromInt(int i) {
        for (CompressionAlgorithmType t : CompressionAlgorithmType.values()) {
            if (t.toInt() == i) return t;
        }

        String msg = String.format("'%d' - unknown compression algorithm type", i);
        throw new IllegalArgumentException(msg);
    }

    public static CompressionAlgorithmType fromAlgorithm(CompressionAlgorithm algorithm) {
        for (CompressionAlgorithmType t : CompressionAlgorithmType.values()) {
            if (t.algorithm == algorithm) return t;
        }

        String msg = String.format("'%s' - unknown compression algorithm type", algorithm);
        throw new IllegalArgumentException(msg);
    }
}
