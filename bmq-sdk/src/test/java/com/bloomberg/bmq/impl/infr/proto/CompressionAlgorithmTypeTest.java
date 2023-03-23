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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.bloomberg.bmq.BMQException;
import com.bloomberg.bmq.CompressionAlgorithm;
import com.bloomberg.bmq.impl.infr.util.ZlibCompression;
import java.util.EnumSet;
import java.util.Set;
import org.junit.Test;

public class CompressionAlgorithmTypeTest {

    @Test
    public void testToInt() {
        assertEquals(0, CompressionAlgorithmType.E_NONE.toInt());
        assertEquals(1, CompressionAlgorithmType.E_ZLIB.toInt());
    }

    @Test
    public void testToAlgorithm() {
        assertEquals(CompressionAlgorithm.None, CompressionAlgorithmType.E_NONE.toAlgorithm());
        assertEquals(CompressionAlgorithm.Zlib, CompressionAlgorithmType.E_ZLIB.toAlgorithm());
    }

    @Test
    public void testFromInt() {
        for (CompressionAlgorithmType t : CompressionAlgorithmType.values()) {
            int i = t.toInt();
            assertEquals(t, CompressionAlgorithmType.fromInt(i));
        }

        // for invalid values throw exception

        int below =
                EnumSet.allOf(CompressionAlgorithmType.class).stream()
                                .mapToInt(CompressionAlgorithmType::toInt)
                                .min()
                                .getAsInt()
                        - 1;

        try {
            CompressionAlgorithmType.fromInt(below);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals(
                    String.format("'%d' - unknown compression algorithm type", below),
                    e.getMessage());
        }

        int above =
                EnumSet.allOf(CompressionAlgorithmType.class).stream()
                                .mapToInt(CompressionAlgorithmType::toInt)
                                .max()
                                .getAsInt()
                        + 1;

        try {
            CompressionAlgorithmType.fromInt(above);
            fail(); // should not get here
        } catch (IllegalArgumentException e) {
            assertEquals(
                    String.format("'%d' - unknown compression algorithm type", above),
                    e.getMessage());
        }
    }

    @Test
    public void testFromAlgorithm() {
        Set<CompressionAlgorithm> toCover = EnumSet.allOf(CompressionAlgorithm.class);

        for (CompressionAlgorithmType t : CompressionAlgorithmType.values()) {
            CompressionAlgorithm a = t.toAlgorithm();
            assertEquals(t, CompressionAlgorithmType.fromAlgorithm(a));

            assertTrue(toCover.remove(a));
        }

        assertTrue(toCover.isEmpty());
    }

    @Test
    public void testGetCompression() {
        try {
            CompressionAlgorithmType.E_NONE.getCompression();
            fail(); // should not get here
        } catch (BMQException e) {
            assertEquals("No compression for type 'E_NONE'", e.getMessage());
        }

        assertThat(
                CompressionAlgorithmType.E_ZLIB.getCompression(),
                instanceOf(ZlibCompression.class));
    }
}
