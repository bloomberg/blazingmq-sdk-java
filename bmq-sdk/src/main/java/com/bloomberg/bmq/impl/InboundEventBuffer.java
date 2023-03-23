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

import com.bloomberg.bmq.SessionOptions;
import com.bloomberg.bmq.impl.events.BrokerSessionEvent;
import com.bloomberg.bmq.impl.events.Event;
import com.bloomberg.bmq.impl.infr.stat.EventQueueStats;
import com.bloomberg.bmq.impl.infr.util.Argument;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class InboundEventBuffer {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final LinkedBlockingDeque<Event> eventFIFO = new LinkedBlockingDeque<>(); // thread-safe

    private final SessionOptions.InboundEventBufferWaterMark watermarks;
    private final EventQueueStats eventQueueStats;

    private boolean reportLwm = false;

    public InboundEventBuffer(
            SessionOptions.InboundEventBufferWaterMark wms, EventQueueStats eventQueueStats) {
        watermarks = Argument.expectNonNull(wms, "wms");
        this.eventQueueStats = Argument.expectNonNull(eventQueueStats, "eventQueueStats");
    }

    int size() {
        return eventFIFO.size();
    }

    private boolean isLwmOrHwmEvent(Event ev) {
        if (Argument.expectNonNull(ev, "ev") instanceof BrokerSessionEvent) {
            BrokerSessionEvent.Type t = ((BrokerSessionEvent) ev).getEventType();
            switch (t) {
                case e_SLOWCONSUMER_NORMAL: // fallthrough
                case e_SLOWCONSUMER_HIGHWATERMARK:
                    return true;
                default:
                    break;
            }
        }
        return false;
    }

    private String getBufferStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append(" LWM=")
                .append(watermarks.lowWaterMark())
                .append(" HWM=")
                .append(watermarks.highWaterMark())
                .append(" BUFFER_SIZE=")
                .append(eventFIFO.size());
        Event oldestEv = eventFIFO.peekFirst();
        if (oldestEv != null) {
            sb.append(" OLDEST_EVENT_MS=").append(oldestEv.getDuration().toMillis());
        }
        return sb.toString();
    }

    public void put(Event ev) throws InterruptedException {
        Argument.expectNonNull(ev, "ev");
        Argument.expectCondition(!isLwmOrHwmEvent(ev), "Unexpected LWM or HWM event: ", ev);

        // Since adding event to the queue and updating the stats is not
        // an atomic operation, the following case is possible when the queue size
        // is equal to zero:
        //
        // Thread A calls "poll" method and waits for the next event.
        // After that thread B calls "put" method and adds the event to the queue.

        // Thread A unblocks and updates the stats by calling "onDequeue" method before
        // thread B updates the stats by calling "onEnqueue" method.

        // Therefore, at some point of time, the queue size in the stats might be set to -1.

        // To avoid this, when adding new event to the queue, we at first
        // update the stats and after that add the event to the queue.
        eventQueueStats.onEnqueue();
        eventFIFO.put(ev);

        final int hwm = watermarks.highWaterMark();
        if (!reportLwm && eventFIFO.size() >= hwm) {
            String reason = "Inbound event buffer size exceeded HWM." + getBufferStatus();
            BrokerSessionEvent brokerSessionEvent =
                    BrokerSessionEvent.createInstance(
                            BrokerSessionEvent.Type.e_SLOWCONSUMER_HIGHWATERMARK, reason);
            logger.warn(reason);

            // First update the stats and after that add the event to the queue.
            eventQueueStats.onEnqueue();
            eventFIFO.putFirst(brokerSessionEvent);

            reportLwm = true;
        }
    }

    public Event poll(int timeoutSec) throws InterruptedException {
        Event ev =
                eventFIFO.poll(
                        Argument.expectNonNegative(timeoutSec, "timeoutSec"), TimeUnit.SECONDS);

        if (ev != null) {
            Duration d = ev.getDuration();
            logger.debug("Ev: {}, duration: {} ns", ev, d.toNanos());
            eventQueueStats.onDequeue(d);
        }

        final int lwm = watermarks.lowWaterMark();
        if (reportLwm && eventFIFO.size() <= lwm) {
            String reason = "Inbound event buffer size is back to normal." + getBufferStatus();
            BrokerSessionEvent brokerSessionEvent =
                    BrokerSessionEvent.createInstance(
                            BrokerSessionEvent.Type.e_SLOWCONSUMER_NORMAL, reason);
            logger.warn(reason);

            // First update the stats and after that add the event to the queue.
            eventQueueStats.onEnqueue();
            eventFIFO.putFirst(brokerSessionEvent);

            reportLwm = false;
        }
        return ev;
    }
}
