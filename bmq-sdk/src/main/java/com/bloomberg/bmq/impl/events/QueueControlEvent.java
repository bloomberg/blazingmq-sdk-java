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
package com.bloomberg.bmq.impl.events;

import com.bloomberg.bmq.ResultCodes.GenericCode;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.infr.util.UniqId;
import com.bloomberg.bmq.impl.intf.QueueHandle;

public class QueueControlEvent extends SessionEvent<QueueControlEvent.Type> {
    public static final int TYPE_ID = UniqId.getNumber();
    private final GenericCode resultCode;

    @Override
    public int getDispatchId() {
        return TYPE_ID;
    }

    private interface Dispatcher {
        void handle(QueueControlEventHandler handler, QueueControlEvent event);
    }

    public enum Type {
        e_QUEUE_OPEN_RESULT(QueueControlEventHandler::handleQueueOpenResult),
        e_QUEUE_REOPEN_RESULT(QueueControlEventHandler::handleQueueReopenResult),
        e_QUEUE_CLOSE_RESULT(QueueControlEventHandler::handleQueueCloseResult),
        e_QUEUE_CONFIGURE_RESULT(QueueControlEventHandler::handleQueueConfigureResult),
        e_QUEUE_SUSPEND_RESULT(Type::handleQueueSuspendResult),
        e_QUEUE_RESUME_RESULT(Type::handleQueueResumeResult);

        private final Dispatcher dispatcher;

        private static void handleQueueSuspendResult(
                QueueControlEventHandler handler, QueueControlEvent event) {
            // Block PUTs by setting 'QueueHandle.isSuspended()` property to
            // true right before dispatching the event.
            Argument.expectCondition(
                    !event.getQueue().getIsSuspended(), "'queue' must not be suspended");

            event.getQueue().setIsSuspended(true);
            handler.handleQueueSuspendResult(event);
        }

        private static void handleQueueResumeResult(
                QueueControlEventHandler handler, QueueControlEvent event) {
            // Unblock PUTs by setting 'QueueHandle.isSuspended()` property
            // to false right before dispatching the event.
            Argument.expectCondition(
                    event.getQueue().getIsSuspended(), "'queue' must be suspended");

            event.getQueue().setIsSuspended(false);
            handler.handleQueueResumeResult(event);
        }

        Type(Dispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        public void dispatch(QueueControlEventHandler handler, QueueControlEvent event) {
            dispatcher.handle(handler, event);
        }
    }

    public final void dispatch(QueueControlEventHandler handler) {
        getEventType().dispatch(handler, this);
    }

    private QueueHandle queue;

    private QueueControlEvent(
            Type queueEventType, GenericCode result, String errorDescription, QueueHandle queue) {
        super(queueEventType, errorDescription);
        resultCode = result;
        this.queue = queue;
    }

    @Override
    public void dispatch(EventHandler handler) {
        handler.handleQueueEvent(this);
    }

    public GenericCode getStatus() {
        return resultCode;
    }

    public QueueHandle getQueue() {
        return queue;
    }

    public static QueueControlEvent createInstance(
            Type sessionEventType, GenericCode result, String errorDescription, QueueHandle queue) {
        return new QueueControlEvent(sessionEventType, result, errorDescription, queue);
    }
}
