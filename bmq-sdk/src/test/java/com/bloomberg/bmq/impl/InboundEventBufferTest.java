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
package com.bloomberg.bmq.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.impl.events.BrokerSessionEvent;
import com.bloomberg.bmq.impl.events.Event;
import com.bloomberg.bmq.impl.infr.stat.EventQueueStats;
import com.bloomberg.bmq.impl.infr.util.Argument;
import java.lang.invoke.MethodHandles;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InboundEventBufferTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private boolean isHwmEvent(Event ev) {
        return isSessionEvent(
                Argument.expectNonNull(ev, "ev"),
                BrokerSessionEvent.Type.e_SLOWCONSUMER_HIGHWATERMARK);
    }

    private boolean isLwmEvent(Event ev) {
        return isSessionEvent(
                Argument.expectNonNull(ev, "ev"), BrokerSessionEvent.Type.e_SLOWCONSUMER_NORMAL);
    }

    private boolean isConnectedEvent(Event ev) {
        return isSessionEvent(
                Argument.expectNonNull(ev, "ev"), BrokerSessionEvent.Type.e_CONNECTED);
    }

    private boolean isSessionEvent(Event ev, BrokerSessionEvent.Type type) {
        assertTrue(ev instanceof BrokerSessionEvent);
        BrokerSessionEvent.Type evType = ((BrokerSessionEvent) ev).getEventType();
        assertEquals(type, evType);
        return true;
    }

    @Test
    void testErrors() throws InterruptedException {
        // Null watermarks object
        try {
            new InboundEventBuffer(null, new EventQueueStats());
            fail(); // Should not get here
        } catch (IllegalArgumentException e) {
            // OK
        }

        // Null event queue stats
        try {
            new InboundEventBuffer(new SessionOptions.InboundEventBufferWaterMark(0, 2), null);
            fail(); // Should not get here
        } catch (IllegalArgumentException e) {
            // OK
        }

        // Negative poll timeout
        try {
            InboundEventBuffer b =
                    new InboundEventBuffer(
                            new SessionOptions.InboundEventBufferWaterMark(0, 2),
                            new EventQueueStats());
            b.poll(-1);
            fail(); // Should not get here
        } catch (IllegalArgumentException e) {
            // OK
        }

        {
            // Putting unsupported events
            InboundEventBuffer b =
                    new InboundEventBuffer(
                            new SessionOptions.InboundEventBufferWaterMark(0, 2),
                            new EventQueueStats());

            final BrokerSessionEvent hwmEvent =
                    BrokerSessionEvent.createInstance(
                            BrokerSessionEvent.Type.e_SLOWCONSUMER_HIGHWATERMARK, "");
            final BrokerSessionEvent lwmEvent =
                    BrokerSessionEvent.createInstance(
                            BrokerSessionEvent.Type.e_SLOWCONSUMER_NORMAL, "");

            final Event[] events = {hwmEvent, lwmEvent};

            for (Event ev : events) {
                try {
                    b.put(ev);
                    fail(); // Should not get here
                } catch (IllegalArgumentException e) {
                    // OK
                }
            }
        }
    }

    @Test
    void testWatermarkEvents() throws InterruptedException {
        InboundEventBuffer b =
                new InboundEventBuffer(
                        new SessionOptions.InboundEventBufferWaterMark(0, 1),
                        new EventQueueStats());

        assertEquals(0, b.size());

        final BrokerSessionEvent event =
                BrokerSessionEvent.createInstance(BrokerSessionEvent.Type.e_CONNECTED, "");

        b.put(event);

        // HWM reached and HWM event added internally
        assertEquals(2, b.size());

        b.put(event);

        assertEquals(3, b.size());

        // Get the first event, it should be HWM event
        Event deqEvent = b.poll(1);
        assertNotNull(deqEvent);
        assertTrue(isHwmEvent(deqEvent));
        assertEquals(2, b.size());

        // The second and the third should be CONNECTED
        deqEvent = b.poll(1);
        assertNotNull(deqEvent);
        assertTrue(isConnectedEvent(deqEvent));
        assertEquals(1, b.size());

        deqEvent = b.poll(1);
        assertNotNull(deqEvent);
        assertTrue(isConnectedEvent(deqEvent));

        // Now we've reached LWM and the event should be added internally
        assertEquals(1, b.size());
        deqEvent = b.poll(1);
        assertNotNull(deqEvent);
        assertTrue(isLwmEvent(deqEvent));
        assertEquals(0, b.size());
    }

    @Test
    void testPollAfterDumpFinalStats() throws InterruptedException {
        EventQueueStats stats = new EventQueueStats();

        InboundEventBuffer b =
                new InboundEventBuffer(
                        new SessionOptions.InboundEventBufferWaterMark(500, 1000), stats);

        assertEquals(0, b.size());

        final BrokerSessionEvent event =
                BrokerSessionEvent.createInstance(BrokerSessionEvent.Type.e_CONNECTED, "");

        b.put(event);
        assertEquals(1, b.size());

        // Dump final stats
        StringBuilder builder = new StringBuilder();
        stats.dump(builder, true);

        logger.info("Final event queue stats:\n{}", builder.toString());

        // Get the event
        Event deqEvent = b.poll(1);
        assertNotNull(deqEvent);
        assertTrue(isConnectedEvent(deqEvent));

        assertEquals(0, b.size());
    }
}
