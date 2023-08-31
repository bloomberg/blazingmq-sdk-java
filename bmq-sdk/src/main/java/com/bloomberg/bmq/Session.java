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

import com.bloomberg.bmq.ResultCodes.GenericResult;
import com.bloomberg.bmq.impl.BrokerSession;
import com.bloomberg.bmq.impl.QueueId;
import com.bloomberg.bmq.impl.QueueImpl;
import com.bloomberg.bmq.impl.events.AckMessageEvent;
import com.bloomberg.bmq.impl.events.BrokerSessionEvent;
import com.bloomberg.bmq.impl.events.BrokerSessionEventHandler;
import com.bloomberg.bmq.impl.events.Event;
import com.bloomberg.bmq.impl.events.PushMessageEvent;
import com.bloomberg.bmq.impl.events.QueueControlEvent;
import com.bloomberg.bmq.impl.events.QueueControlEventHandler;
import com.bloomberg.bmq.impl.infr.net.NettyTcpConnectionFactory;
import com.bloomberg.bmq.impl.infr.proto.AckMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.BinaryMessageProperty;
import com.bloomberg.bmq.impl.infr.proto.BoolMessageProperty;
import com.bloomberg.bmq.impl.infr.proto.ByteMessageProperty;
import com.bloomberg.bmq.impl.infr.proto.CompressionAlgorithmType;
import com.bloomberg.bmq.impl.infr.proto.Int32MessageProperty;
import com.bloomberg.bmq.impl.infr.proto.Int64MessageProperty;
import com.bloomberg.bmq.impl.infr.proto.MessagePropertyHandler;
import com.bloomberg.bmq.impl.infr.proto.PushHeaderFlags;
import com.bloomberg.bmq.impl.infr.proto.PushMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.PutMessageImpl;
import com.bloomberg.bmq.impl.infr.proto.ShortMessageProperty;
import com.bloomberg.bmq.impl.infr.proto.StringMessageProperty;
import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.intf.QueueHandle;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides access to the BlazingMQ broker.
 *
 * <p>This component provides access to a message queue broker. The broker manages named, persistent
 * queues of messages. This broker allows a client to open queues, post messages to them, or
 * retrieve messages from them. All of these operations take place within the context of the session
 * opened by the client application.
 *
 * <p>Messages received from a broker are communicated to the application by the session associated
 * with that broker in the form of events (see {@link com.bloomberg.bmq.Event}). Events can be of
 * two different types: (1) Messages and queue status events ({@link com.bloomberg.bmq.QueueEvent}),
 * or (2) Session status events ({@link com.bloomberg.bmq.SessionEvent}).
 *
 * <p>A {@code Session} dispatches events to the application in an asynchronous mode. The applicaton
 * must supply a concrete {@link com.bloomberg.bmq.SessionEventHandler} object at construction time.
 * The concrete {@code SessionEventHandler} provided by the application must implement the {@link
 * com.bloomberg.bmq.SessionEventHandler#handleSessionEvent
 * SessionEventHandler.handleSessionEvent()} method, which will be called by the {@code Session}
 * every time a session event is received.
 *
 * <p>Note that the concrete {@code SessionEventHandler} may implement a handler for the specific
 * session event type and it will be called by the {@code Session} for that event instead of the
 * default {@code handleSessionEvent}.
 *
 * <p>A {@code Session} allows {@code start} and {@code stop} operations in synchronous or
 * asynchronous version for the convenience of the programmer.
 *
 * <p>By default a {@code Session} connects to the local broker, which in turn may connect to a
 * remote cluster based on configuration.
 *
 * <p>After a {@code Session} started, the application has to open one or several queues in read
 * and/or write mode (see {@link com.bloomberg.bmq.AbstractSession#getQueue
 * AbstractSession.getQueue()}).
 *
 * <H2>Disclaimer </H2>
 *
 * A {@code Session} object is a heavy object representing the negotiated TCP session with the
 * broker, and the entire associated state (opened queues, etc.) Therefore, there should only be
 * <b>one</b> session per task, created at task startup and closed at task shutdown. It is extremely
 * inefficient, and an abuse of resource to create a session ad-hoc every time a message needs to be
 * posted.
 *
 * <H2>Thread-safety </H2>
 *
 * This session object is <b>thread enabled</b>, meaning that two threads can safely call any
 * methods on the <b>same instance</b> without external synchronization.
 *
 * <H2>Connecting to the Broker </H2>
 *
 * A {@code Session} establishes a communication with a broker service using TCP/IP. Each {@code
 * Session} object must be constructed with a {@link com.bloomberg.bmq.SessionOptions} object, which
 * provides the necessary information to connect to the broker. In particular, the {@code
 * SessionOptions} object must specify the IP address and port needed to connect to the broker. The
 * {@code SessionOptions} object may also provide extra parameters for tuning the TCP connection
 * behavior.
 *
 * <p>Note that in most cases the user does not need to explicitly construct a {@code
 * SessionOptions} object: the default factory method {@link
 * com.bloomberg.bmq.SessionOptions#createDefault} creates an instance that will connect to the
 * broker service on the local machine using the standard port.
 *
 * <p>A {@code Session} object is created in an unconnected state. The {@code start} or {@code
 * startAsync} method must be called to connect to the broker. Note that {@code start} method is
 * blocking, and returns either after connection to broker has been established (success), or after
 * specified timeout (failure). {@code startAsync} method, as the name suggests, connects to the
 * broker asynchronously (i.e., it returns immediately), and the result of the operation is notified
 * via {@link com.bloomberg.bmq.SessionEvent.StartStatus} session event.
 *
 * <p>When the {@code Session} is no longer needed, the application should call the {@code stop}
 * (blocking) or {@code stopAsync} (non-blocking) method to disconnect from the broker. Note that
 * the session can be restarted with a call to {@code start} or {@code startAsync} once it has been
 * fully stopped. To fully shut down the session {@code linger} should be called after stop, and
 * after that the session restart is not possible any more.
 *
 * <H2>Connection loss and reconnection </H2>
 *
 * If the connection between the application and the broker is lost, the {@code Session} will
 * automatically try to reconnect periodically. The {@code Session} will also notify the application
 * of the event of losing the connection via {@link com.bloomberg.bmq.SessionEvent.ConnectionLost}
 * session event.
 *
 * <p>Once the connection has been re-established with the broker (as a result of one of the
 * periodic reconnection attempts), the {@code Session} will notify the application via {@link
 * com.bloomberg.bmq.SessionEvent.Reconnected} session event. After the connection re-establishment,
 * the {@code Session} will attempt to reopen the queues that were in {@code OPEN} state prior to
 * connection loss. The {@code Session} will notify the application of the result of reopen
 * operation via {@link com.bloomberg.bmq.QueueControlEvent.ReopenQueueResult} for each queue. Note
 * that a reopen operation on a queue may fail (due to broker issue, machine issue, etc), so the
 * application must keep track on these session events, and stop posting on a queue that failed to
 * reopen.
 *
 * <p>After all reopen operations are complete and application has been notified with all {@code
 * ReopenQueueResult} events, the {@code Session} delivers a {@link
 * com.bloomberg.bmq.SessionEvent.StateRestored} session event to the application.
 *
 * <p>Note that if there were no opened queues prior to connection loss then the {@code Session}
 * will send {@code StateRestored} event immediately after {@code Reconnected} session event.
 *
 * <H2>Example 1 </H2>
 *
 * The following example illustrates how to create a {@code Session} start it, stop and shutdown it.
 *
 * <pre>
 * void runSession()  {
 *     SessionOptions sessionOptions = SessionOptions.builder()
 *                .setBrokerUri(URI.create(tcp://localhost:30114")
 *                .build();
 *     AbstractSession session = new Session(sessionOptions, null);
 *     try {
 *         session.start(Duration.ofSeconds(10));
 *         System.out.println("Session started");
 *
 *         // Open queue in READ or WRITE or READ/WRITE mode, and receive or
 *         // post messages, etc.
 *
 *         session.stop(Duration.ofSeconds(10));
 *     } catch (BMQException e) {
 *         System.out.println("Operation error: " + e);
 *     } finally {
 *         session.linger();
 *     }
 * }
 * </pre>
 *
 * This example can be simplified because the constructor for {@code Session} uses a default {@code
 * SessionOptions} object that will connect to the local broker service. The example may be
 * rewritten as follow:
 *
 * <pre>
 * void runSession()  {
 *     AbstractSession session = new Session(null);
 *     try {
 *         session.start(Duration.ofSeconds(10));
 *         System.out.println("Session started");
 *
 *         // Open queue in READ or WRITE or READ/WRITE mode, and receive or
 *         // post messages, etc.
 *
 *         session.stop(Duration.ofSeconds(10));
 *     } catch (BMQException e) {
 *         System.out.println("Operation error: " + e);
 *     } finally {
 *         session.linger();
 *     }
 * }
 * </pre>
 *
 * <H2>Processing session events </H2>
 *
 * If the {@code Session} is created with non null {@link com.bloomberg.bmq.SessionEventHandler}
 * then it will be used to notify the application about session events like results of {@link
 * com.bloomberg.bmq.Session#startAsync Session.startAsync()} and {@link
 * com.bloomberg.bmq.Session#stopAsync Session.stopAsync()} operations, and connection statuses like
 * {@code SessionEvent.ConnectionLost}, {@code SessionEvent.Reconnected}, {@code
 * SessionEvent.StateRestored}. Each event may be handled in its special callback if the application
 * implements related method, e.g. {@link
 * com.bloomberg.bmq.SessionEventHandler#handleStartStatusSessionEvent
 * SessionEventHandler.handleStartStatusSessionEvent()}, or it can use the default implementation
 * that calls {@link com.bloomberg.bmq.SessionEventHandler#handleSessionEvent
 * SessionEventHandler.handleSessionEvent()}, for each session event without user defined handler.
 *
 * <H2>Example 2 </H2>
 *
 * The following example demonstrates a usage of the simplest {@link
 * com.bloomberg.bmq.SessionEventHandler}
 *
 * <pre>
 *        Session session = new Session(new SessionEventHandler() {
 *
 *               public void handleSessionEvent(SessionEvent event) {
 *                   switch (event.type()) {
 *                   case START_STATUS_SESSION_EVENT:
 *                       GenericResult result = ((SessionEvent.StartStatus)event).result();
 *                       if (result.isSuccess()) {
 *                           System.out.println("The connection to the broker is established");
 *                           openQueues();
 *                           startPostingToQueues();
 *                       } else if (result.isTimeout()) {
 *                           System.out.println("The connection to the broker has timed out");
 *                       }
 *                       break;
 *                   case STOP_STATUS_SESSION_EVENT:
 *                       GenericResult result = ((SessionEvent.StopStatus)event).result();
 *                       if (result.isSuccess()) {
 *                           System.out.println("The connection to the broker is closed gracefully");
 *                       }
 *                       break;
 *                   case CONNECTION_LOST_SESSION_EVENT:
 *                       // The connection to the broker dropped. Stop posting to the queue.
 *                       stopPostingToQueues();
 *                       break;
 *                   case RECONNECTED_SESSION_EVENT:
 *                       // The connection to the broker has been restored, but the queue may not be ready yet.
 *                       break;
 *                   case STATE_RESTORED_SESSION_EVENT:
 *                       // The connection to the broker has been restored and all queues
 *                       // have been re-opened. Resume posting to the queue.
 *                       resumePostingToQueues();
 *                       break;
 *                   cae UNKNOWN_SESSION_EVENT:
 *                   default:
 *                       System.out.println("Unexpected session event: " + event);
 *                   }
 *               }
 *           });
 *
 *       session.startAsync(Duration.ofSeconds(10));
 * </pre>
 *
 * And in the following version session start status event will be handled in a separate callback
 * while all the other session events will be passed to {@code handleSessionEvent}
 *
 * <pre>
 *
 *       Session session = new Session( new SessionEventHandler() {
 *
 *               public void handleSessionEvent(SessionEvent event) {
 *                   System.out.println("Other session event: " + event);
 *               }
 *
 *               public void handleStartStatusSessionEvent(SessionEvent.StartStatus event) {
 *                   System.out.println("Session start status: " + event.result());
 *                   if (event.result().isSuccess()) {
 *                       System.out.println("The connection to the broker is established");
 *                   }
 *               }
 *            });
 *
 *      session.startAsync(Duration.ofSeconds(10));
 * </pre>
 *
 * Note that after the {@code Session} is associated with some session event handler, this
 * association cannot be changed or canceled. The event handler will be used for processing events
 * until the {@code Session.linger()} is called.
 *
 * <H2>Opening queues </H2>
 *
 * Once the {@code Session} has been created and started, the application can use it to open queues
 * for producing and/or consuming messages. A queue is associated with a domain. Domain metadata
 * must be deployed in the BlazingMQ infrastructure prior to opening queues under that domain,
 * because opening a queue actually loads the metadata deployed for the associated domain.
 *
 * <p>The metadata associated with a domain defines various parameters like maximum queue size and
 * capacity, persistent policy, routing policy, etc.
 *
 * <p>Queue are identified by URIs (Unified Resource Identifiers) that must follow the BlazingMQ
 * syntax, manipulated as {@link com.bloomberg.bmq.Uri} objects. A queue URI is typically formatted
 * as follows:
 *
 * <pre>
 *
 *  bmq://my.domain/my.queue
 * </pre>
 *
 * Note that domain names are unique in BlazingMQ infrastructure, which makes a fully qualified
 * queue URI unique too.
 *
 * <p>Queues in BlazingMQ infrastructure are created by applications on demand. Broker creates a
 * queue when it receives an open-queue request from an application for a queue that does not exist
 * currently.
 *
 * <p>Application can open a queue by calling {@link com.bloomberg.bmq.AbstractSession#getQueue
 * AbstractSession.getQueue()} method on a started session. Application must pass appropriate flags
 * to indicate if it wants to post messages to queue, consume messages from the queue, or both. Also
 * it has to provide concrete {@link com.bloomberg.bmq.QueueEventHandler} to recieve queue related
 * events. Depending on the queue flags it also needs to supply concrete {@link
 * com.bloomberg.bmq.AckMessageHandler} it the queue will be used for message posting. This handler
 * will be called each time when {@code ACK} event comes from the broker. And if the queue will be
 * used for comsuming the messages then {@link com.bloomberg.bmq.PushMessageHandler} should be
 * supplied. It will be called on each incoming {@link com.bloomberg.bmq.PushMessage}. Once the
 * application obtainns the {@link com.bloomberg.bmq.Queue} interface it can call {@link
 * com.bloomberg.bmq.Queue#open Queue.open()} or {@link com.bloomberg.bmq.Queue#openAsync
 * Queue.openAsync()}.
 *
 * <p>Note that {@code open} is a blocking method, and returns after the queue has been successfully
 * opened (success) or after specified timeout has expired (failure). {@code openAsync} method, as
 * the name suggests, is non blocking, and the result of the operation is notified via {@link
 * com.bloomberg.bmq.QueueControlEvent.OpenQueueResult} session event.
 *
 * <H2>Example 3 </H2>
 *
 * The following example demonstrates how to open a queue for posting messages. The code first opens
 * the queue with appropriate flags, and then uses {@link com.bloomberg.bmq.Queue#createPutMessage}
 * to create a message and post to the queue.
 *
 * <pre>{@code
 * // Session creation, startup logic and exception handling elided for brevity
 * Uri queueUri = new Uri("bmq://my.domain/my.queue");
 *
 * // Application can implement concrete QueueMessageHandler and AckMessageHandler,
 * // or e.g. use a functional interface feature with a standard collection.
 * ArrayList<QueueControlEvent> queueEvents = new ArrayList();
 * ArrayList<AckMessage> ackMessages = new ArrayList();
 *
 * long flags = QueueFlags.setWriter(0);
 * flags = QueueFlags.setAck(flags);
 *
 * Queue myQueue = session.getQueue(
 *                     queueUri,
 *                     flags,            // Setup queue flag for producer
 *                     queueEvents::add, // QueueEventHandler is a functional interface
 *                     ackMessages::add, // AckMessageHandler is a functional interface
 *                     null);            // PushMessageHandler is null since we don't want
 *                                       // to receive messages
 * myQueue.open(QueueOptions.createDefault(), Duration.ofSeconds(5));
 * }</pre>
 *
 * Note that apart from {@link com.bloomberg.bmq.QueueFlags#WRITE} flag, {@link
 * com.bloomberg.bmq.QueueFlags#ACK} flag has been passed to {@code getQueue} method above. This
 * indicates that application is interested in receiving {@code ACK} notification for each message
 * it posts to the queue, irrespective of whether or not the message was successfully received by
 * the broker and posted to the queue. Also note that if the queue has {@code QueueFlags.ACK}} flag
 * then every {@code PUT} message that is going to be posted via this queue should have a valid
 * {@code CorrelationId}, see {@link com.bloomberg.bmq.Queue#createPutMessage}.
 *
 * <p>Once the queue has been successfully opened for writing, messages can be posted to the queue
 * for consumption by interested applications.
 *
 * <pre>
 *    // Prepare user payload
 *    String userMessage = "Hello!";
 *    ByteBuffer byteBuffer = ByteBuffer.wrap(userMessage.getBytes());
 *
 *    // Get PUT message interface from the opened queue
 *    PutMessage msg = myQueue.createPutMessage(byteBuffer);
 *
 *    // Add unique correlation ID. The broker will send ACK in response to this message.
 *    // The ID can be used to bind this message with the corresponding ACK.
 *    msg.setAutoCorrelationId();
 *
 *    // Add some optional properties
 *    MessageProperties mp = msg.messageProperties();
 *    mp.setPropertyAsString("myId", "abcd-efgh-ijkl");
 *    mp.setPropertyAsInt64("timestamp", 123456789L);
 *
 *    // Post this message to the queue
 *    myQueue.post(msg);
 * </pre>
 *
 * To post multiple messages at once the application may use {@link com.bloomberg.bmq.Queue#pack
 * Queue.pack()} and {@link com.bloomberg.bmq.Queue#flush} methods:
 *
 * <pre>{@code
 * for (int i = 0; i < 10; i++) {
 *     PutMessage msg = myQueue.createPutMessage(payload);
 *     // ... Set CorrelationID message properties
 *
 *     myQueue.pack(msg);
 * }
 * // Now post the messages all together
 * myQueue.flush();
 * }</pre>
 *
 * <H2>Closing queues </H2>
 *
 * After an application no longer needs to produce or consume messages from a queue, it can be
 * closed by {@link com.bloomberg.bmq.Queue#close Queue.close()} or {@link
 * com.bloomberg.bmq.Queue#closeAsync Queue.closeAsync()} method. Note that closing a queue closes
 * an application's "view" on the queue, and may not lead to queue deletion in the broker. A {@code
 * Session} does not expose any method to explicitly delete a queue.
 *
 * <p>Note that {@code close} is a blocking method and returns after the queue has been successfully
 * closed (success) or after specified timeout has expired (failure). {@code closeAsync}, as the
 * name suggests, is a non-blocking method, and result of the operation is notified via {@link
 * com.bloomberg.bmq.QueueControlEvent.CloseQueueResult} queue event.
 */
public final class Session implements AbstractSession {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final ScheduledExecutorService scheduler;
    private final BrokerSession brokerSession;
    private final SessionEventHandler sessionEventHandler;
    private final BrokerSessionEventHandler dispatcher;
    private final EventDispatcher eventDispatcher;

    /**
     * Creates a new session object with default session options.
     *
     * @param eh session event handler callback interface
     * @see com.bloomberg.bmq.SessionOptions
     * @see com.bloomberg.bmq.SessionEventHandler
     */
    public Session(SessionEventHandler eh) {
        this(SessionOptions.createDefault(), eh);
    }

    /**
     * Creates a new session object.
     *
     * @param so specified options
     * @param eh session event handler callback interface
     * @throws IllegalArgumentException if the specified session options or event handler is null
     * @see com.bloomberg.bmq.SessionOptions
     * @see com.bloomberg.bmq.SessionEventHandler
     */
    public Session(SessionOptions so, SessionEventHandler eh) {
        Argument.expectNonNull(so, "session options");
        Argument.expectNonNull(eh, "event handler");

        sessionEventHandler = eh;
        scheduler = Executors.newSingleThreadScheduledExecutor();
        dispatcher = new BrokerSessionEventDispatcher();
        eventDispatcher = new EventDispatcher();
        brokerSession =
                BrokerSession.createInstance(
                        so,
                        new NettyTcpConnectionFactory(),
                        scheduler,
                        event -> event.dispatch(eventDispatcher));
    }

    /**
     * Connects to the BlazingMQ broker and start the message processing.
     *
     * <p>This method blocks until either the session is connected to the broker, fails to connect,
     * or the operation times out.
     *
     * @param timeout start timeout value
     * @throws BMQException if start attempt failed
     */
    @Override
    public void start(Duration timeout) {
        GenericResult res = brokerSession.start(timeout);
        if (res != GenericResult.SUCCESS) {
            throw new BMQException("Failed to start session: " + res, res);
        }
    }

    /**
     * Connects to the BlazingMQ broker and start the message processing
     *
     * <p>This method returns without blocking. The result of the operation is communicated with a
     * session event. If the optionally specified 'timeout' is not populated, use the one defined in
     * the session options.
     *
     * @param timeout start timeout value
     * @throws BMQException in case of failure
     */
    @Override
    public void startAsync(Duration timeout) {
        brokerSession.startAsync(timeout);
    }

    /**
     * Checks if the session is started.
     *
     * @return true if the session is started, otherwise false
     */
    @Override
    public boolean isStarted() {
        return brokerSession.isStarted();
    }

    /**
     * Gracefully disconnects from the BlazingMQ broker and stop the operation of this session.
     *
     * <p>This method blocks waiting for all already invoked event handlers to exit and all
     * session-related operations to be finished.
     *
     * @param timeout stop timeout value
     * @throws BMQException if stop attempt failed
     */
    @Override
    public void stop(Duration timeout) {
        brokerSession.stop(timeout);
    }

    /**
     * Gracefully disconnects from the BlazingMQ broker and stop the operation of this session.
     *
     * <p>This method returns without blocking. The result of the operation is communicated with a
     * session event. If the optionally specified {@code timeout} is not populated, use the one
     * defined in the session options.
     *
     * @param timeout stop timeout value
     * @throws BMQException in case of failure
     */
    @Override
    public void stopAsync(Duration timeout) {
        brokerSession.stopAsync(timeout);
    }

    /**
     * Shutdowns all connection and event handling threads.
     *
     * <p>This method must be called when the session is stopped. No other method may be used after
     * this method returns.
     */
    @Override
    public void linger() {
        try {
            brokerSession.linger();
        } catch (RuntimeException e) {
            logger.error("Failed to linger session: ", e);
        }
    }

    /**
     * Creates a queue representation.
     *
     * <p>Returned {@link com.bloomberg.bmq.Queue} may be in any state.
     *
     * @param uri URI of this queue, immutable
     * @param flags a combination of the values defined in {@link com.bloomberg.bmq.QueueFlags}
     * @param handler queue event handler callback interface, must be non-null for async operations
     *     and health sensitive queues
     * @param ackHandler callback handler for incoming ACK events, can be null if only consumer
     * @param pushHandler callback handler for incoming PUSH events, can be null if only producer
     * @return Queue interface to the queue instance
     * @throws IllegalArgumentException in case of a wrong combination of the queue flags and the
     *     handlers
     * @throws BMQException in case of other failures
     * @see com.bloomberg.bmq.Uri
     * @see com.bloomberg.bmq.QueueFlags
     * @see com.bloomberg.bmq.QueueEventHandler
     * @see com.bloomberg.bmq.AckMessageHandler
     * @see com.bloomberg.bmq.PushMessageHandler
     */
    @Override
    public Queue getQueue(
            Uri uri,
            long flags,
            QueueEventHandler handler,
            AckMessageHandler ackHandler,
            PushMessageHandler pushHandler) {

        // Currently queue event handler may be null.
        // In one of the upcoming releases it will be mandatory.
        if (handler == null) {
            String msg =
                    "Queue event handler was not provided. It will be mandatory in "
                            + "one of the the upcoming releases of BlazingMQ Java SDK. Please provide "
                            + "non-null and valid queue event handler.  Please reach out to BlazingMQ "
                            + "team with any questions.";
            logger.error(msg);
        }

        if (QueueFlags.isWriter(flags) && QueueFlags.isAck(flags)) {
            Argument.expectCondition(
                    ackHandler != null, "Null ACK handler but ACK flag is set: ", flags);
        }

        if (QueueFlags.isReader(flags)) {
            Argument.expectCondition(
                    pushHandler != null, "Null PUSH handler but READER flag is set: ", flags);
        }

        return new QueueAdapter(uri, flags, handler, ackHandler, pushHandler);
    }

    private class EventAdapter implements com.bloomberg.bmq.Event {
        @Override
        public AbstractSession session() {
            return Session.this;
        }
    }

    @Immutable
    abstract class SessionEventAdapter extends EventAdapter implements SessionEvent {
        private final long creationTime;
        private final String errorDescription;

        protected SessionEventAdapter(BrokerSessionEvent event) {
            Argument.expectNonNull(event, "event");

            creationTime = event.getCreationTime();
            errorDescription = event.getErrorDescription();
        }

        public abstract void dispatch();

        @Override
        public String toString() {
            return "Event [Type: "
                    + this.type()
                    + " Creation time in broker session: "
                    + creationTime
                    + " Description: "
                    + errorDescription
                    + "]";
        }
    }

    @Immutable
    class UnknownEvent extends SessionEventAdapter {

        UnknownEvent(BrokerSessionEvent event) {
            super(event);
        }

        @Override
        public void dispatch() {
            Session.this.sessionEventHandler.handleSessionEvent(this);
        }

        @Override
        public Type type() {
            return Type.UNKNOWN_SESSION_EVENT;
        }
    }

    @Immutable
    class ConnectionLostEvent extends SessionEventAdapter implements SessionEvent.ConnectionLost {

        ConnectionLostEvent(BrokerSessionEvent event) {
            super(event);
        }

        @Override
        public void dispatch() {
            Session.this.sessionEventHandler.handleConnectionLostSessionEvent(this);
        }
    }

    @Immutable
    class ReconnectedEvent extends SessionEventAdapter implements SessionEvent.Reconnected {

        ReconnectedEvent(BrokerSessionEvent event) {
            super(event);
        }

        @Override
        public void dispatch() {
            Session.this.sessionEventHandler.handleReconnectedSessionEvent(this);
        }
    }

    @Immutable
    class StateRestoredEvent extends SessionEventAdapter implements SessionEvent.StateRestored {

        StateRestoredEvent(BrokerSessionEvent event) {
            super(event);
        }

        @Override
        public void dispatch() {
            Session.this.sessionEventHandler.handleStateRestoredSessionEvent(this);
        }
    }

    @Immutable
    class SlowConsumerHighWatermarkEvent extends SessionEventAdapter
            implements SessionEvent.SlowConsumerHighWatermark {

        SlowConsumerHighWatermarkEvent(BrokerSessionEvent event) {
            super(event);
        }

        @Override
        public void dispatch() {
            Session.this.sessionEventHandler.handleSlowConsumerHighWatermarkEvent(this);
        }
    }

    @Immutable
    class SlowConsumerNormalEvent extends SessionEventAdapter
            implements SessionEvent.SlowConsumerNormal {

        SlowConsumerNormalEvent(BrokerSessionEvent event) {
            super(event);
        }

        @Override
        public void dispatch() {
            Session.this.sessionEventHandler.handleSlowConsumerNormalEvent(this);
        }
    }

    @Immutable
    class StartStatusEvent extends SessionEventAdapter implements SessionEvent.StartStatus {

        private final GenericResult result;

        StartStatusEvent(BrokerSessionEvent event, GenericResult res) {
            super(event);
            result = Argument.expectNonNull(res, "res");
        }

        @Override
        public void dispatch() {
            Session.this.sessionEventHandler.handleStartStatusSessionEvent(this);
        }

        @Override
        public GenericResult result() {
            return result;
        }
    }

    @Immutable
    class StopStatusEvent extends SessionEventAdapter implements SessionEvent.StopStatus {

        private final GenericResult result;

        StopStatusEvent(BrokerSessionEvent event, GenericResult res) {
            super(event);
            result = Argument.expectNonNull(res, "res");
        }

        @Override
        public void dispatch() {
            Session.this.sessionEventHandler.handleStopStatusSessionEvent(this);
        }

        @Override
        public GenericResult result() {
            return result;
        }
    }

    @Immutable
    class HostUnhealthyEvent extends SessionEventAdapter implements SessionEvent.HostUnhealthy {

        HostUnhealthyEvent(BrokerSessionEvent event) {
            super(event);
        }

        @Override
        public void dispatch() {
            Session.this.sessionEventHandler.handleHostUnhealthySessionEvent(this);
        }
    }

    @Immutable
    class HostHealthRestoredEvent extends SessionEventAdapter
            implements SessionEvent.HostHealthRestored {

        HostHealthRestoredEvent(BrokerSessionEvent event) {
            super(event);
        }

        @Override
        public void dispatch() {
            Session.this.sessionEventHandler.handleHostHealthRestoredSessionEvent(this);
        }
    }

    private static void handleEvent(SessionEventAdapter event) {
        event.dispatch();
    }

    @Immutable
    private class BrokerSessionEventDispatcher implements BrokerSessionEventHandler {
        @Override
        public void handleConnected(BrokerSessionEvent event) {
            handleEvent(new StartStatusEvent(event, GenericResult.SUCCESS));
        }

        @Override
        public void handleDisconnected(BrokerSessionEvent event) {
            handleEvent(new StopStatusEvent(event, GenericResult.SUCCESS));
        }

        @Override
        public void handleConnectionLost(BrokerSessionEvent event) {
            handleEvent(new ConnectionLostEvent(event));
        }

        @Override
        public void handleReconnected(BrokerSessionEvent event) {
            handleEvent(new ReconnectedEvent(event));
        }

        @Override
        public void handleStateRestored(BrokerSessionEvent event) {
            handleEvent(new StateRestoredEvent(event));
        }

        @Override
        public void handleConnectionTimeout(BrokerSessionEvent event) {
            handleEvent(new StartStatusEvent(event, GenericResult.TIMEOUT));
        }

        @Override
        public void handleSlowConsumerNormal(BrokerSessionEvent event) {
            handleEvent(new SlowConsumerNormalEvent(event));
        }

        @Override
        public void handleSlowConsumerHighWatermark(BrokerSessionEvent event) {
            handleEvent(new SlowConsumerHighWatermarkEvent(event));
        }

        @Override
        public void handleConnectionInProgress(BrokerSessionEvent event) {
            handleEvent(new StartStatusEvent(event, GenericResult.NOT_SUPPORTED));
        }

        @Override
        public void handleDisconnectionTimeout(BrokerSessionEvent event) {
            handleEvent(new StopStatusEvent(event, GenericResult.TIMEOUT));
        }

        @Override
        public void handleDisconnectionInProgress(BrokerSessionEvent event) {
            handleEvent(new StopStatusEvent(event, GenericResult.NOT_SUPPORTED));
        }

        @Override
        public void handleHostUnhealthy(BrokerSessionEvent event) {
            handleEvent(new HostUnhealthyEvent(event));
        }

        @Override
        public void handleHostHealthRestored(BrokerSessionEvent event) {
            handleEvent(new HostHealthRestoredEvent(event));
        }

        @Override
        public void handleError(BrokerSessionEvent event) {
            handleEvent(new UnknownEvent(event));
        }

        @Override
        public void handleCancelled(BrokerSessionEvent event) {
            handleEvent(new StartStatusEvent(event, GenericResult.CANCELED));
        }
    }

    @Immutable
    private class EventDispatcher implements com.bloomberg.bmq.impl.events.EventHandler {

        @Override
        public void handleEvent(Event event) {
            logger.error("Unexpected event caught: {}", event);
        }

        @Override
        public void handlePushMessageEvent(PushMessageEvent ev) {
            PushMessageImpl msg = ev.rawMessage();
            Integer[] subQueueIds = msg.subQueueIds();
            for (Integer subQId : subQueueIds) {
                //                QueueId qid = QueueId.createInstance(msg.queueId(), subQId);
                QueueId qid = QueueId.createInstance(msg.queueId(), 0);
                QueueHandle queue = brokerSession.lookupQueue(qid);
                if (queue != null) {
                    queue.handlePushMessage(msg);
                }
            }
        }

        @Override
        public void handleAckMessageEvent(AckMessageEvent ev) {
            AckMessageImpl msg = ev.rawMessage();
            QueueId qid = QueueId.createInstance(msg.queueId(), 0);
            QueueHandle queue = brokerSession.lookupQueue(qid);
            if (queue != null) {
                queue.handleAckMessage(msg);
            }
        }

        @Override
        public void handleQueueEvent(QueueControlEvent ev) {
            QueueHandle queue = ev.getQueue();
            if (queue == null)
                throw new RuntimeException("Failure: Queue is null. Ev: " + ev.toString());
            queue.handleQueueEvent(ev);
        }

        @Override
        public void handleBrokerSessionEvent(BrokerSessionEvent event) {
            event.dispatch(Session.this.dispatcher);
        }
    }

    private class QueueAdapter implements Queue {
        private final QueueImpl impl;
        private final boolean isAck;
        private final boolean isHandlerSet;

        private QueueAdapter(
                Uri uri,
                long flags,
                QueueEventHandler handler,
                AckMessageHandler ackHandler,
                PushMessageHandler pushHandler) {
            isHandlerSet = handler != null;
            impl =
                    new QueueImpl(
                            brokerSession,
                            uri,
                            flags,
                            new QueueEventDispatcher(handler),
                            new AckMessageDispatcher(ackHandler),
                            new PushMessageDispatcher(pushHandler));
            isAck = QueueFlags.isAck(flags);
        }

        private void checkAck(PutMessage msg) {
            if (isAck && msg.correlationId() == null) {
                throw new BMQException("CorrelationId is not set although the queue has ACK flag");
            }
        }

        private void checkQueueHandlerIsSet() throws BMQException {
            // Queue event handler must be set for:
            //   - asynchronous queue operation in order to deliver
            //     corresponding result event
            //   - sensitive to host health changes queues in order to deliver
            //     QUEUE_SUSPENDED and QUEUE_RESUMED events

            if (!isHandlerSet) {
                String msg = "Queue event handler must be set.";
                logger.error(msg);

                throw new BMQException(msg, ResultCodes.GenericResult.NOT_SUPPORTED);
            }
        }

        @Override
        public void open(QueueOptions options, Duration timeout) throws BMQException {
            if (options.hasSuspendsOnBadHostHealth() && options.getSuspendsOnBadHostHealth()) {
                checkQueueHandlerIsSet();
            }

            ResultCodes.OpenQueueCode res = impl.open(options, timeout);
            if (!res.isSuccess()) {
                throw new BMQException("Failed to open queue: " + res, res);
            }
        }

        @Override
        public void openAsync(QueueOptions options, Duration timeout) throws BMQException {
            checkQueueHandlerIsSet();
            impl.openAsync(options, timeout);
        }

        @Override
        public void configure(QueueOptions options, Duration timeout) throws BMQException {
            // In case the queue is being reconfigured as host health sensitive,
            // we need to check that queue event handler is set.
            if (options.getSuspendsOnBadHostHealth()) {
                checkQueueHandlerIsSet();
            }

            ResultCodes.ConfigureQueueCode res = impl.configure(options, timeout);
            if (!res.isSuccess()) {
                throw new BMQException("Failed to configure queue: " + res, res);
            }
        }

        @Override
        public void configureAsync(QueueOptions options, Duration timeout) throws BMQException {
            checkQueueHandlerIsSet();
            impl.configureAsync(options, timeout);
        }

        @Override
        public void close(Duration timeout) throws BMQException {
            ResultCodes.CloseQueueCode res = impl.close(timeout);
            if (!res.isSuccess()) {
                throw new BMQException("Failed to close queue: " + res, res);
            }
        }

        @Override
        public void closeAsync(Duration timeout) throws BMQException {
            checkQueueHandlerIsSet();
            impl.closeAsync(timeout);
        }

        @Override
        public PutMessage createPutMessage(ByteBuffer... payload) {
            PutMessageImpl msg = new PutMessageImpl();

            try {
                msg.appData().setPayload(payload);
            } catch (IOException e) {
                throw new BMQException("Failed to set payload", e);
            }

            return new PutMessageAdapter(msg);
        }

        @Override
        public void post(PutMessage message) throws BMQException {
            Argument.expectNonNull(message, "message");
            Argument.expectCondition(
                    message instanceof PutMessageAdapter,
                    "'message' must be castable to ",
                    PutMessageAdapter.class.getName(),
                    ": ",
                    message.getClass().getName());
            if (impl.getIsSuspended()) {
                throw new BMQException("The queue is in suspended state");
            }

            checkAck(message);
            PutMessageImpl msgImpl = ((PutMessageAdapter) message).impl;
            brokerSession.post(impl, msgImpl);
        }

        @Override
        public void pack(PutMessage message) throws BMQException {
            Argument.expectNonNull(message, "message");
            Argument.expectCondition(
                    message instanceof PutMessageAdapter,
                    "'message' must be castable to ",
                    PutMessageAdapter.class.getName(),
                    ": ",
                    message.getClass().getName());
            if (impl.getIsSuspended()) {
                throw new BMQException("The queue is in suspended state");
            }

            checkAck(message);
            PutMessageImpl msgImpl = ((PutMessageAdapter) message).impl;
            impl.pack(msgImpl);
        }

        @Override
        public void flush() throws BMQException {
            impl.flush();
        }

        @Override
        public Uri uri() {
            return impl.getUri();
        }

        @Override
        public boolean isOpen() {
            return impl.isOpened();
        }

        public QueueId queueId() {
            return impl.getFullQueueId();
        }

        private class QueueEventAdapter extends EventAdapter implements QueueEvent {
            @Override
            public Queue queue() {
                return QueueAdapter.this;
            }
        }

        @Immutable
        private abstract class QueueControlEventAdapter extends QueueEventAdapter
                implements com.bloomberg.bmq.QueueControlEvent {
            private final QueueControlEvent impl;

            private QueueControlEventAdapter(QueueControlEvent impl) {
                this.impl = Argument.expectNonNull(impl, "impl");
            }

            @Override
            public ResultCodes.GenericCode result() {
                return impl.getStatus();
            }
        }

        private class OpenQueueEventAdapter extends QueueControlEventAdapter
                implements com.bloomberg.bmq.QueueControlEvent.OpenQueueResult {
            private OpenQueueEventAdapter(QueueControlEvent impl) {
                super(impl);
            }

            @Override
            public ResultCodes.OpenQueueResult result() {
                return ResultCodes.OpenQueueResult.upcast(super.impl.getStatus());
            }
        }

        private class ReopenQueueEventAdapter extends QueueControlEventAdapter
                implements com.bloomberg.bmq.QueueControlEvent.ReopenQueueResult {
            private ReopenQueueEventAdapter(QueueControlEvent impl) {
                super(impl);
            }

            @Override
            public ResultCodes.OpenQueueResult result() {
                return ResultCodes.OpenQueueResult.upcast(super.impl.getStatus());
            }
        }

        private class CloseQueueEventAdapter extends QueueControlEventAdapter
                implements com.bloomberg.bmq.QueueControlEvent.CloseQueueResult {
            private CloseQueueEventAdapter(QueueControlEvent impl) {
                super(impl);
            }

            @Override
            public ResultCodes.CloseQueueResult result() {
                return ResultCodes.CloseQueueResult.upcast(super.impl.getStatus());
            }
        }

        private class ConfigureQueueEventAdapter extends QueueControlEventAdapter
                implements com.bloomberg.bmq.QueueControlEvent.ConfigureQueueResult {
            private ConfigureQueueEventAdapter(QueueControlEvent impl) {
                super(impl);
            }

            @Override
            public ResultCodes.ConfigureQueueResult result() {
                return ResultCodes.ConfigureQueueResult.upcast(super.impl.getStatus());
            }
        }

        private class SuspendQueueEventAdapter extends QueueControlEventAdapter
                implements com.bloomberg.bmq.QueueControlEvent.SuspendQueueResult {
            private SuspendQueueEventAdapter(QueueControlEvent impl) {
                super(impl);
            }

            @Override
            public ResultCodes.ConfigureQueueResult result() {
                return ResultCodes.ConfigureQueueResult.upcast(super.impl.getStatus());
            }
        }

        private class ResumeQueueEventAdapter extends QueueControlEventAdapter
                implements com.bloomberg.bmq.QueueControlEvent.ResumeQueueResult {
            private ResumeQueueEventAdapter(QueueControlEvent impl) {
                super(impl);
            }

            @Override
            public ResultCodes.ConfigureQueueResult result() {
                return ResultCodes.ConfigureQueueResult.upcast(super.impl.getStatus());
            }
        }

        private class PutMessageAdapter implements com.bloomberg.bmq.PutMessage {

            private final PutMessageImpl impl;

            PutMessageAdapter(PutMessageImpl impl) {
                this.impl = Argument.expectNonNull(impl, "impl");
            }

            @Override
            public CorrelationId setCorrelationId() {
                return impl.setCorrelationId();
            }

            @Override
            public CorrelationId setCorrelationId(Object obj) {
                return impl.setCorrelationId(obj);
            }

            @Override
            public void setCompressionAlgorithm(CompressionAlgorithm algorithm) {
                impl.setCompressionType(CompressionAlgorithmType.fromAlgorithm(algorithm));
            }

            @Override
            public CorrelationId correlationId() {
                return impl.correlationId();
            }

            @Override
            public MessageProperties messageProperties() {
                return impl.messageProperties();
            }

            @Override
            public Queue queue() {
                return QueueAdapter.this;
            }

            @Override
            public Session session() {
                return Session.this;
            }

            @Override
            public String toString() {
                StringBuilder sb = new StringBuilder();
                sb.append("[ Queue URI: ")
                        .append(queue().uri())
                        .append(" ]")
                        .append(impl.toString());
                return sb.toString();
            }
        }

        private class PushMessageAdapter implements com.bloomberg.bmq.PushMessage {

            private final PushMessageImpl impl;

            PushMessageAdapter(PushMessageImpl impl) {
                this.impl = Argument.expectNonNull(impl, "impl");
            }

            @Override
            public MessageGUID messageGUID() {
                return impl.messageGUID();
            }

            @Override
            public CorrelationId correlationId() {
                for (Integer sId : impl.subQueueIds()) {
                    Map.Entry<SubscriptionHandle, Subscription> entry =
                            QueueAdapter.this.impl.getQueueOptions().getSubscriptions().get(sId);
                    if (entry != null) {
                        return entry.getKey().getCorrelationId();
                    }
                }
                throw new BMQException("Undefined correlationId for PushMessage");
            };

            @Override
            public Queue queue() {
                return QueueAdapter.this;
            }

            @Override
            public Session session() {
                return Session.this;
            }

            @Override
            public GenericResult confirm() {
                return brokerSession.confirm(QueueAdapter.this.impl, impl);
            }

            @Override
            public boolean hasMessageProperties() {
                return PushHeaderFlags.isSet(impl.flags(), PushHeaderFlags.MESSAGE_PROPERTIES);
            }

            @Override
            public ByteBuffer[] payload() throws BMQException {
                try {
                    if (impl.appData().isCompressed()) {
                        logger.debug("Access compressed payload for the first time");
                    }

                    return impl.appData().payload();
                } catch (IOException ex) {
                    logger.error("Failed to read application data.");
                    throw new BMQException(ex);
                }
            }

            @Override
            public Iterator<MessageProperty> propertyIterator() {
                return new Iterator<MessageProperty>() {
                    private final StatefullPropertyUpcaster upcaster =
                            new StatefullPropertyUpcaster();
                    private final Iterator<
                                    Map.Entry<
                                            String,
                                            com.bloomberg.bmq.impl.infr.proto.MessageProperty>>
                            implIterator = impl.appData().properties().iterator();

                    @Override
                    public boolean hasNext() {
                        return implIterator.hasNext();
                    }

                    @Override
                    public MessageProperty next() {
                        implIterator.next().getValue().dispatch(upcaster);
                        return upcaster.property();
                    }
                };
            }

            @Override
            public MessageProperty getProperty(String name) {
                if (!hasMessageProperties()) {
                    throw new IllegalStateException("Message doesn't contain properties");
                }

                com.bloomberg.bmq.impl.infr.proto.MessageProperty property =
                        impl.appData().properties().get(name);

                if (property == null) {
                    return null;
                }

                final StatefullPropertyUpcaster upcaster = new StatefullPropertyUpcaster();

                property.dispatch(upcaster);
                return upcaster.property();
            }

            @Override
            public String toString() {
                StringBuilder sb = new StringBuilder();
                sb.append("[ Queue URI: ")
                        .append(queue().uri())
                        .append(" ]")
                        .append(impl.toString());
                return sb.toString();
            }
        }

        private class AckMessageAdapter implements com.bloomberg.bmq.AckMessage {

            private final AckMessageImpl impl;

            AckMessageAdapter(AckMessageImpl impl) {
                this.impl = Argument.expectNonNull(impl, "impl");
            }

            @Override
            public MessageGUID messageGUID() {
                return impl.messageGUID();
            }

            @Override
            public Queue queue() {
                return QueueAdapter.this;
            }

            @Override
            public Session session() {
                return Session.this;
            }

            @Override
            public ResultCodes.AckResult status() {
                return impl.status();
            }

            @Override
            public CorrelationId correlationId() {
                return impl.correlationId();
            }

            @Override
            public String toString() {
                StringBuilder sb = new StringBuilder();
                sb.append("[ Queue URI: ")
                        .append(queue().uri())
                        .append(" ]")
                        .append(impl.toString());
                return sb.toString();
            }
        }

        @Immutable
        private class QueueEventDispatcher implements QueueControlEventHandler {
            private final Optional<QueueEventHandler> handler;

            private QueueEventDispatcher(QueueEventHandler handler) {
                this.handler = Optional.ofNullable(handler);
            }

            @Override
            public void handleQueueOpenResult(QueueControlEvent event) {
                if (handler.isPresent()) {
                    handler.get()
                            .handleQueueEvent(QueueAdapter.this.new OpenQueueEventAdapter(event));
                }
            }

            @Override
            public void handleQueueReopenResult(QueueControlEvent event) {
                if (handler.isPresent()) {
                    handler.get()
                            .handleQueueEvent(QueueAdapter.this.new ReopenQueueEventAdapter(event));
                }
            }

            @Override
            public void handleQueueCloseResult(QueueControlEvent event) {
                if (handler.isPresent()) {
                    handler.get()
                            .handleQueueEvent(QueueAdapter.this.new CloseQueueEventAdapter(event));
                }
            }

            @Override
            public void handleQueueConfigureResult(QueueControlEvent event) {
                if (handler.isPresent()) {
                    handler.get()
                            .handleQueueEvent(
                                    QueueAdapter.this.new ConfigureQueueEventAdapter(event));
                }
            }

            @Override
            public void handleQueueSuspendResult(QueueControlEvent event) {
                if (handler.isPresent()) {
                    handler.get()
                            .handleQueueEvent(
                                    QueueAdapter.this.new SuspendQueueEventAdapter(event));
                }
            }

            @Override
            public void handleQueueResumeResult(QueueControlEvent event) {
                if (handler.isPresent()) {
                    handler.get()
                            .handleQueueEvent(QueueAdapter.this.new ResumeQueueEventAdapter(event));
                }
            }
        }

        @Immutable
        private class AckMessageDispatcher
                implements com.bloomberg.bmq.impl.events.AckMessageHandler {
            private final Optional<AckMessageHandler> handler;

            private AckMessageDispatcher(AckMessageHandler handler) {
                this.handler = Optional.ofNullable(handler);
            }

            @Override
            public void handleAckMessage(AckMessageImpl msg) {
                if (handler.isPresent()) {
                    handler.get().handleAckMessage(new AckMessageAdapter(msg));
                }
            }
        }

        @Immutable
        private class PushMessageDispatcher
                implements com.bloomberg.bmq.impl.events.PushMessageHandler {
            private final Optional<PushMessageHandler> handler;

            private PushMessageDispatcher(PushMessageHandler handler) {
                this.handler = Optional.ofNullable(handler);
            }

            @Override
            public void handlePushMessage(PushMessageImpl msg) {
                if (handler.isPresent()) {
                    handler.get().handlePushMessage(new PushMessageAdapter(msg));
                }
            }
        }
    }

    private static class StatefullPropertyUpcaster implements MessagePropertyHandler {
        private MessageProperty property;

        public MessageProperty property() {
            return property;
        }

        @Override
        public void handleUnknown(com.bloomberg.bmq.impl.infr.proto.MessageProperty property) {
            this.property =
                    new MessageProperty() {
                        @Override
                        public Type type() {
                            return Type.UNDEFINED;
                        }

                        @Override
                        public String name() {
                            return property.name();
                        }

                        @Override
                        public Object value() {
                            return property.getValue();
                        }

                        @Override
                        public String toString() {
                            return property.toString();
                        }
                    };
        }

        @Override
        public void handleBoolean(BoolMessageProperty property) {
            this.property =
                    new MessageProperty.BoolProperty() {
                        @Override
                        public Boolean value() {
                            return property.getValue();
                        }

                        @Override
                        public String name() {
                            return property.name();
                        }

                        @Override
                        public String toString() {
                            return property.toString();
                        }
                    };
        }

        @Override
        public void handleByte(ByteMessageProperty property) {
            this.property =
                    new MessageProperty.ByteProperty() {
                        @Override
                        public Byte value() {
                            return property.getValue();
                        }

                        @Override
                        public String name() {
                            return property.name();
                        }

                        @Override
                        public String toString() {
                            return property.toString();
                        }
                    };
        }

        @Override
        public void handleShort(ShortMessageProperty property) {
            this.property =
                    new MessageProperty.ShortProperty() {
                        @Override
                        public Short value() {
                            return property.getValue();
                        }

                        @Override
                        public String name() {
                            return property.name();
                        }

                        @Override
                        public String toString() {
                            return property.toString();
                        }
                    };
        }

        @Override
        public void handleInt32(Int32MessageProperty property) {
            this.property =
                    new MessageProperty.Int32Property() {
                        @Override
                        public Integer value() {
                            return property.getValue();
                        }

                        @Override
                        public String name() {
                            return property.name();
                        }

                        @Override
                        public String toString() {
                            return property.toString();
                        }
                    };
        }

        @Override
        public void handleInt64(Int64MessageProperty property) {
            this.property =
                    new MessageProperty.Int64Property() {
                        @Override
                        public Long value() {
                            return property.getValue();
                        }

                        @Override
                        public String name() {
                            return property.name();
                        }

                        @Override
                        public String toString() {
                            return property.toString();
                        }
                    };
        }

        @Override
        public void handleString(StringMessageProperty property) {
            this.property =
                    new MessageProperty.StringProperty() {
                        @Override
                        public String value() {
                            return property.getValue();
                        }

                        @Override
                        public String name() {
                            return property.name();
                        }

                        @Override
                        public String toString() {
                            return property.toString();
                        }
                    };
        }

        @Override
        public void handleBinary(BinaryMessageProperty property) {
            this.property =
                    new MessageProperty.BinaryProperty() {
                        @Override
                        public byte[] value() {
                            return property.getValue();
                        }

                        @Override
                        public String name() {
                            return property.name();
                        }

                        @Override
                        public String toString() {
                            return property.toString();
                        }
                    };
        }
    }
}
