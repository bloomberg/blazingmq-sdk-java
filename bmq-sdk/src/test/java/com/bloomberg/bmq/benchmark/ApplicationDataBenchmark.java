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
package com.bloomberg.bmq.benchmark;

import com.bloomberg.bmq.impl.infr.proto.ApplicationDataTest;
import com.bloomberg.bmq.impl.infr.proto.CompressionAlgorithmType;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
public class ApplicationDataBenchmark {
    private final ApplicationDataTest test = new ApplicationDataTest();

    @Benchmark
    @Warmup(iterations = 1, time = 500, timeUnit = TimeUnit.MILLISECONDS)
    @Measurement(iterations = 2, time = 500, timeUnit = TimeUnit.MILLISECONDS)
    public void testZlibStreamInOut() throws IOException {
        CompressionAlgorithmType compressionType = CompressionAlgorithmType.E_ZLIB;

        final int PAYLOAD_SIZE_BYTES = 1024 * 1024 * 2; // 2 Mb

        test.verifyStreamIn(
                test.generatePayload(PAYLOAD_SIZE_BYTES),
                test.generateProps(),
                false,
                compressionType);

        test.verifyStreamOut(
                test.generatePayload(PAYLOAD_SIZE_BYTES),
                test.generateProps(),
                false,
                compressionType);
    }
}
