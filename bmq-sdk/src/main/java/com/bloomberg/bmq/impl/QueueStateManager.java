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

import com.bloomberg.bmq.Uri;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.intf.QueueState;
import java.util.Collection;

/*
    ------------------------------------------------------------------
    Queue manager scenario cases:
    ------------------------------------------------------------------
    (#1): openQueue request successfully sent upstream
    (#2): Failure due to local timeout
        (#2.1): of 1st(openQueue) stage of openQueue;
        (#2.2): of 2st(configureQueue) stage of openQueue;
        (#2.3): of 1st(configureQueue) stage of closeQueue;
    (#3): Failure due to server response
        (#3.1): of 1st(openQueue) stage of openQueue;
        (#3.2): of 2st(configureQueue) stage of openQueue;
        (#3.3): of 1st(configureQueue) stage of closeQueue;
    (#4): queue is fully closed
    (#5): processing late response
        (#5.1): for open queue request;
        (#5.2): for configure queue request during open queue sequence;
        (#5.3): for configure queue request during close queue sequence;

    ------------------------------------------------------------------
    Queue manager scenario cases grouped by actions:
    ------------------------------------------------------------------
    (INSERT_ACTIVE) Queue inserted to active queues when:
        (#1): openQueue request successfully sent upstream;
    (REMOVE_ACTIVE) Queue removed from active queues when:
        (#2.1), (#3.1): failure of 1st(openQueue) stage of openQueue;
        (#2.2), (#3.2): failure of 2st(configureQueue) stage of openQueue;
        (#4): queue is fully closed
    (INSERT_EXPIRED) Queue inserted to expired queues:
        (#2.1): failure due to local timeout 1st(openQueue) stage of openQueue;
        (#2.2): failure due to local timeout 2st(configureQueue) stage of openQueue;
        (#2.3): failure due to local timeout 1st(configureQueue) stage of closeQueue;
    (REMOVE_EXPIRED) Queue removed from expired queues:
        (#5): processing late response

    ------------------------------------------------------------------
    Queue manager scenario cases grouped by preconditions:
    ------------------------------------------------------------------
    QUEUE IS OPENED:
        (#1): openQueue request successfully sent upstream to reopen a queue
              after channel restored
    QUEUE IS CLOSED:
        (#1): openQueue request successfully sent upstream
    QUEUE IS OPENING:
        (#2.1) (#3.1): failure of 1st(openQueue) stage of openQueue;
        (#2.2) (#3.2): failure of 2st(configureQueue) stage of openQueue;
    QUEUE IS CLOSING:
        (#2.3) (#3.3): failure of 1st(configureQueue) stage of closeQueue;
        (#4): queue is fully closed;

    ------------------------------------------------------------------
    Queue manager scenario cases grouped by post conditions:
    ------------------------------------------------------------------
    QUEUE IS OPENING:
        (#1): openQueue request successfully sent upstream
    QUEUE IS CLOSED:
        (#2.1) (#3.1): failure of 1st(openQueue) stage of openQueue;
        (#2.2) (#3.2): failure of 2st(configureQueue) stage of openQueue;
        (#2.3) (#3.3): failure of 1st(configureQueue) stage of closeQueue;
        (#4): queue is fully closed;
        (#5): processing late response

*/
public class QueueStateManager {
    private final QueueManager queueManager;

    /*
     Scenario case:  (#1) openQueue request successfully sent upstream
     Actions:        INSERT_ACTIVE
     Precondition:   QUEUE IS CLOSED
     Post condition: QUEUE IS OPENING
    */
    public void onOpenQueueSent(QueueImpl queue) {
        Argument.expectNonNull(queue, "queue");
        Argument.expectCondition(queue.getState() == QueueState.e_CLOSED, "'queue' must be closed");

        boolean res = queueManager.insert(queue);
        assert res : "Queue insert failed" + queue;
        queue.setState(QueueState.e_OPENING);
    }

    /*
     Scenario case:  (#1) configureStream request successfully sent upstream
     Actions:        INSERT_ACTIVE
     Precondition:   QUEUE IS OPENED
     Post condition: QUEUE IS OPENED
    */
    public void onConfigureStreamSent(QueueImpl queue) {
        Argument.expectNonNull(queue, "queue");
        Argument.expectCondition(queue.getState() == QueueState.e_OPENED, "'queue' must be opened");

        // Update subscriptions
        boolean res = queueManager.update(queue);
        assert res : "Queue insert failed" + queue;
    }

    /*
     Scenario case:  (#1) openQueue request successfully sent upstream
     Actions:        NO-OP
     Precondition:   QUEUE IS OPENED
     Post condition: QUEUE IS OPENING
    */
    public void onReopenQueueSent(QueueImpl queue) {
        Argument.expectNonNull(queue, "queue");
        Argument.expectCondition(queue.getState() == QueueState.e_OPENED, "'queue' must be opened");

        QueueImpl q = queueManager.findByQueueId(queue.getFullQueueId());
        Argument.expectCondition(q != null, "Queue is not active: ", queue);
        queue.setState(QueueState.e_OPENING);
    }

    /*
     Scenario cases: (#2.1) Failure due to local timeout of 1st(openQueue)
                            stage of openQueue;
                     (#2.2) Failure due to local timeout of 2st(configureQueue)
                            stage of openQueue;
     Actions:        INSERT_EXPIRED, REMOVE_ACTIVE
     Precondition:   QUEUE IS OPENING
     Post condition: QUEUE IS CLOSED
    */
    public void onOpenQueueLocallyTimedOut(QueueImpl queue) {
        Argument.expectNonNull(queue, "queue");
        Argument.expectCondition(
                queue.getState() == QueueState.e_OPENING, "'queue' must be opening");

        queue.setState(QueueState.e_CLOSED);
        boolean res = queueManager.insertExpired(queue);
        assert res : "Queue insert to expired queue map failed";
        res = queueManager.remove(queue);
        assert res : "Queue removal from active queue map failed";
    }

    /*
     Scenario cases: (#3.1) Failure due  to server response of 1st(openQueue)
                           stage of openQueue;
                     (#3.2) Failure due to server response of 2st(configureQueue)
                            stage of openQueue;
     Actions:        REMOVE_ACTIVE
     Precondition:   QUEUE IS OPENING
     Post condition: QUEUE IS CLOSED
    */
    public void onOpenQueueFailed(QueueImpl queue) {
        Argument.expectNonNull(queue, "queue");
        Argument.expectCondition(
                queue.getState() == QueueState.e_OPENING, "'queue' must be opening");

        queue.setState(QueueState.e_CLOSED);
        boolean res = queueManager.remove(queue);
        assert res : "Queue removal from active queue map failed";
    }

    /*
     Scenario case:  (#2.3): failure due to local timeout 1st(configureQueue) stage of closeQueue;
     Actions:        INSERT_EXPIRED
     Precondition:   QUEUE IS CLOSING
     Post condition: QUEUE IS CLOSED
    */
    public void onCloseQueueConfigureFailed(QueueImpl queue) {
        Argument.expectNonNull(queue, "queue");
        Argument.expectCondition(
                queue.getState() == QueueState.e_CLOSING, "'queue' must be closing");

        queue.setState(QueueState.e_CLOSED);
        boolean res = queueManager.insertExpired(queue);
        assert res : "Queue insert to expired queue map failed";
    }

    /*
     Scenario case:   (#4): queue is fully closed;
     Actions:        REMOVE_EXPIRED
     Precondition:   QUEUE IS CLOSING
     Post condition: QUEUE IS CLOSED
    */
    public void onFullyClosed(QueueImpl queue) {
        Argument.expectNonNull(queue, "queue");
        Argument.expectCondition(
                queue.getState() == QueueState.e_CLOSING, "'queue' must be closing");

        queue.setState(QueueState.e_CLOSED);
        boolean res = queueManager.remove(queue);
        assert res : "Queue removal from active queue map failed";
    }

    /*
     Scenario case:   (#5.1): late response for open queue request;
                      (#5.2): late response for configure queue request during open queue sequence;
                      (#5.3): late response for configure queue request during close queue sequence;
     Actions:        REMOVE_EXPIRED
     Post condition: QUEUE IS CLOSED
    */
    public void onLateResponseProcessed(QueueImpl queue) {
        Argument.expectNonNull(queue, "queue");
        queue.setState(QueueState.e_CLOSED);
        boolean res = queueManager.removeExpired(queue);
        assert res : "Queue removal from expired queue map failed";
    }

    public static QueueStateManager createInstance() {
        return new QueueStateManager();
    }

    private QueueStateManager() {
        queueManager = QueueManager.createInstance();
    }

    public QueueImpl findByQueueId(QueueId queueId) {
        return queueManager.findByQueueId(queueId);
    }

    public QueueImpl findBySubscriptionId(int subscriptionId) {
        return queueManager.findBySubscriptionId(subscriptionId);
    }

    public QueueImpl findByUri(Uri uri) {
        return queueManager.findByUri(uri);
    }

    public QueueImpl findExpiredByQueueId(QueueId queueId) {
        return queueManager.findExpiredByQueueId(queueId);
    }

    public QueueId generateNextQueueId(Uri uri) {
        return queueManager.generateNextQueueId(uri);
    }

    public Collection<QueueImpl> getOpenedQueues() {
        return queueManager.getOpenedQueues();
    }

    public Collection<QueueImpl> getAllQueues() {
        return queueManager.getAllQueues();
    }

    public void reset() {
        queueManager.reset();
    }

    // In theory, the method below should not be called directly but be part of
    // scenarios and handlers above.
    // Should be refactored later.

    public void incrementSubStreamCount(QueueImpl queue) {
        Argument.expectNonNull(queue, "queue");
        Argument.expectNonNull(queue.getUri(), "queue uri");

        queueManager.incrementSubStreamCount(queue.getUri().canonical());
    }

    public void decrementSubStreamCount(QueueImpl queue) {
        Argument.expectNonNull(queue, "queue");
        Argument.expectNonNull(queue.getUri(), "queue uri");

        queueManager.decrementSubStreamCount(queue.getUri().canonical());
    }

    public int getSubStreamCount(QueueImpl queue) {
        Argument.expectNonNull(queue, "queue");
        Argument.expectNonNull(queue.getUri(), "queue uri");

        return queueManager.getSubStreamCount(queue.getUri().canonical());
    }

    public void resetSubStreamCount(QueueImpl queue) {
        Argument.expectNonNull(queue, "queue");
        Argument.expectNonNull(queue.getUri(), "queue uri");

        queueManager.resetSubStreamCount(queue.getUri().canonical());
    }
}
