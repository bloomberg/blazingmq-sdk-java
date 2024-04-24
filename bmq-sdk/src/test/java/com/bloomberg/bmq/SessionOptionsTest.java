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
package com.bloomberg.bmq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class SessionOptionsTest {

    @Test
    void testWaterMarks() {
        // Negative LWM
        try {
            SessionOptions.builder()
                    .setInboundBufferWaterMark(
                            new SessionOptions.InboundEventBufferWaterMark(-1, 2));
            fail(); // Should not get here
        } catch (IllegalArgumentException e) {
            // OK
        }

        // Negative HWM
        try {
            SessionOptions.builder()
                    .setInboundBufferWaterMark(
                            new SessionOptions.InboundEventBufferWaterMark(1, -2));
            fail(); // Should not get here
        } catch (IllegalArgumentException e) {
            // OK
        }

        // Zero LWM and HWM
        try {
            SessionOptions.builder()
                    .setInboundBufferWaterMark(
                            new SessionOptions.InboundEventBufferWaterMark(0, 0));
            fail(); // Should not get here
        } catch (IllegalArgumentException e) {
            // OK
        }

        // Zero HWM
        try {
            SessionOptions.builder()
                    .setInboundBufferWaterMark(
                            new SessionOptions.InboundEventBufferWaterMark(2, 0));
            fail(); // Should not get here
        } catch (IllegalArgumentException e) {
            // OK
        }

        // Equal LWM and HWM
        try {
            SessionOptions.builder()
                    .setInboundBufferWaterMark(
                            new SessionOptions.InboundEventBufferWaterMark(2, 2));
            fail(); // Should not get here
        } catch (IllegalArgumentException e) {
            // OK
        }

        // LWM is greater than HWM
        try {
            SessionOptions.builder()
                    .setInboundBufferWaterMark(
                            new SessionOptions.InboundEventBufferWaterMark(2, 1));
            fail(); // Should not get here
        } catch (IllegalArgumentException e) {
            // OK
        }

        // Valid LWM and HWM
        SessionOptions.builder()
                .setInboundBufferWaterMark(new SessionOptions.InboundEventBufferWaterMark(0, 1));

        SessionOptions.builder()
                .setInboundBufferWaterMark(new SessionOptions.InboundEventBufferWaterMark(3, 9));
    }

    @Test
    void testStatsDumpInterval() {
        // Negative SDI
        try {
            SessionOptions.builder().setStatsDumpInterval(Duration.ofNanos(-1));
            fail(); // Should not get here
        } catch (IllegalArgumentException e) {
            // OK
            assertEquals("'statsDumpInterval' must be non-negative.", e.getMessage());
        }

        // Valid SDI - Zero
        SessionOptions.builder().setStatsDumpInterval(Duration.ZERO);

        // Valid SDI - Positive
        SessionOptions.builder().setStatsDumpInterval(Duration.ofSeconds(15));
    }

    @Test
    void testHostHealthMonitor() {
        // Default value is null
        assertNull(SessionOptions.createDefault().hostHealthMonitor());

        // Builder: default value is null
        assertNull(SessionOptions.builder().build().hostHealthMonitor());

        // Builder: try to set null value
        try {
            SessionOptions.builder().setHostHealthMonitor(null);
            fail(); // Should not get here
        } catch (IllegalArgumentException e) {
            assertEquals("'host health monitor' must be non-null", e.getMessage());
        }

        // Builder: set valid value
        HostHealthMonitor monitor = mock(HostHealthMonitor.class);

        SessionOptions options = SessionOptions.builder().setHostHealthMonitor(monitor).build();

        assertEquals(monitor, options.hostHealthMonitor());
    }

    @Test
    void testBuilderWithOptions() {
        // GSON cannot serialize Duration type, so add simple adapter to convert Duration to string
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(
                Duration.class,
                new JsonSerializer<Duration>() {
                    @Override
                    public JsonElement serialize(
                            Duration src, Type typeOfSrc, JsonSerializationContext context) {
                        return new JsonPrimitive(src.toString());
                    }
                });
        Gson gson = gsonBuilder.create();

        SessionOptions defaultOptions = SessionOptions.createDefault();
        assertEquals(gson.toJson(defaultOptions), gson.toJson(SessionOptions.builder().build()));

        assertEquals(
                gson.toJson(defaultOptions),
                gson.toJson(SessionOptions.builder(defaultOptions).build()));

        Duration startTimeout = Duration.ofSeconds(1);
        Duration stopTimeout = Duration.ofMinutes(1);

        SessionOptions newOptions =
                SessionOptions.builder()
                        .setStartTimeout(startTimeout)
                        .setStopTimeout(stopTimeout)
                        .build();

        assertNotEquals(gson.toJson(defaultOptions), gson.toJson(newOptions));
        assertEquals(
                gson.toJson(newOptions), gson.toJson(SessionOptions.builder(newOptions).build()));
    }
}
