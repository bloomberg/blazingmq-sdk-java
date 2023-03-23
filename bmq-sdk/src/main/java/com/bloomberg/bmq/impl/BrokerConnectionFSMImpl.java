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

import com.bloomberg.bmq.impl.infr.util.Argument;
import com.bloomberg.bmq.impl.intf.BrokerConnection.StartStatus;
import com.bloomberg.bmq.impl.intf.BrokerConnection.StopStatus;
import com.bloomberg.bmq.impl.intf.BrokerConnectionFSM;
import com.bloomberg.bmq.impl.intf.SessionStatusHandler.SessionStatus;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerConnectionFSMImpl implements BrokerConnectionFSM {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private interface Reactions {
        void onStartRequest();

        void onStopRequest();

        void onNegotiationResponse();

        void onNegotiationFailed();

        void onNegotiationTimeout();

        void onChannelStatusUp();

        void onChannelStatusWritable();

        void onChannelStatusDown();

        void onConnectStatusSuccess();

        void onConnectStatusFailed();

        void onConnectStatusCancelled();

        void onDisconnectChannelSuccess();

        void onDisconnectChannelFailed();

        void onDisconnectBrokerTimeout();

        void onDisconnectBrokerResponse();

        void onDisconnectBrokerFailed();

        void onBmqEvent();
    }

    private State[] states = {
        new ConnectionLost(),
        new Connecting(),
        new Negotiating(),
        new Connected(),
        new DisconnectingBroker(),
        new DisconnectingChannel(),
        new Stopped()
    };

    /* Inputs
       e0  START_REQUEST
       e1  STOP_REQUEST
       e2  NEGOTIATION_RESPONSE
       e3  NEGOTIATION_FAILURE
       e4  NEGOTIATION_TIMEOUT
       e5  CHANNEL_STATUS_UP
       e6  CHANNEL_STATUS_WRITABLE
       e7  CHANNEL_STATUS_DOWN
       e8  CONNECT_STATUS_SUCCESS
       e9  CONNECT_STATUS_FAILURE
       e10 CONNECT_STATUS_CANCELLED
       e11 DISCONNECT_CHANNEL_SUCCESS
       e12 DISCONNECT_CHANNEL_FAILURE
       e13 DISCONNECT_BROKER_TIMEOUT
       e14 DISCONNECT_BROKER_RESPONSE
       e15 DISCONNECT_BROKER_FAILURE
       e16 BMQ_EVENT
    */

    private static int[][] transitions = {
        // spotless:off
        /*  e0  e1  e2  e3  e4  e5  e6  e7  e8  e9  e10 e11 e12 e13 e14 e15 e16
         *
         * s0 CONNECTION_LOST */ {
            0,  5,  0,  0,  0,  2,  0,  0,  0,  0,  0,  6,  6,  0,  0,  0,  0,
        },
        /* s1 CONNECTING */ {
            1,  5,  1,  1,  1,  2,  1,  0,  1,  6,  6,  1,  1,  1,  1,  1,  1,
        },
        /* s2 NEGOTIATING */ {
            2,  5,  3,  5,  5,  2,  2,  0,  2,  2,  2,  2,  2,  2,  2,  2,  2,
        },
        /* s3 CONNECTED */ {
            3,  4,  3,  3,  3,  3,  3,  0,  3,  3,  3,  3,  3,  3,  3,  3,  3,
        },
        /* s4 DISCONNECTING_BROKER */ {
            4,  4,  4,  4,  4,  4,  4,  0,  4,  4,  4,  4,  4,  5,  5,  5,  4,
        },
        /* s5 DISCONNECTING_CHANNEL*/ {
            5,  5,  5,  5,  5,  5,  5,  0,  5,  5,  5,  6,  6,  5,  5,  5,  5,
        },
        /* s6 STOPPED*/ {
            1,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
        }
        // spotless:on
    };

    private FSMWorker fsmWorker;
    private boolean isOnceStarted = false;
    private StartStatus startErrorStatus = null;
    private StopStatus stopStatus = null;

    private volatile States currentState = States.STOPPED;

    public static BrokerConnectionFSM createInstance(FSMWorker w) {
        return new BrokerConnectionFSMImpl(w);
    }

    BrokerConnectionFSMImpl(FSMWorker w) {
        fsmWorker = Argument.expectNonNull(w, "worker");
    }

    private void changeState(Inputs ev) {
        final int newIdx = transitions[currentState.ordinal()][ev.ordinal()];
        final int curIdx = currentState.ordinal();
        logger.debug("{}: {} -> {}", ev, currentState, States.fromInt(newIdx));
        if (newIdx != curIdx) {
            states[curIdx].onExit();
            currentState = States.fromInt(newIdx);
            states[newIdx].onEnter();
        }
    }

    @Override
    public void handleInput(Inputs inp) {
        State currentState = states[this.currentState.ordinal()];
        switch (inp) {
            case START_REQUEST:
                currentState.onStartRequest();
                break;
            case STOP_REQUEST:
                currentState.onStopRequest();
                break;
            case CHANNEL_STATUS_UP:
                currentState.onChannelStatusUp();
                break;
            case CHANNEL_STATUS_DOWN:
                currentState.onChannelStatusDown();
                break;
            case CHANNEL_STATUS_WRITABLE:
                currentState.onChannelStatusWritable();
                break;
            case CONNECT_STATUS_SUCCESS:
                currentState.onConnectStatusSuccess();
                break;
            case CONNECT_STATUS_FAILURE:
                currentState.onConnectStatusFailed();
                break;
            case CONNECT_STATUS_CANCELLED:
                currentState.onConnectStatusCancelled();
                break;
            case NEGOTIATION_RESPONSE:
                currentState.onNegotiationResponse();
                break;
            case NEGOTIATION_FAILURE:
                currentState.onNegotiationFailed();
                break;
            case NEGOTIATION_TIMEOUT:
                currentState.onNegotiationTimeout();
                break;
            case DISCONNECT_CHANNEL_SUCCESS:
                currentState.onDisconnectChannelSuccess();
                break;
            case DISCONNECT_CHANNEL_FAILURE:
                currentState.onDisconnectChannelFailed();
                break;
            case DISCONNECT_BROKER_TIMEOUT:
                currentState.onDisconnectBrokerTimeout();
                break;
            case DISCONNECT_BROKER_RESPONSE:
                currentState.onDisconnectBrokerResponse();
                break;
            case DISCONNECT_BROKER_FAILURE:
                currentState.onDisconnectBrokerFailed();
                break;
            case BMQ_EVENT:
                currentState.onBmqEvent();
                break;
            default:
                throw new IllegalArgumentException("Unexpected input: " + inp);
        }
        changeState(inp);
    }

    @Override
    public States currentState() {
        return currentState;
    }

    public static int[][] transitionTable() {
        return Arrays.stream(transitions).map(int[]::clone).toArray(int[][]::new);
    }

    abstract class State implements Reactions {
        States state;

        protected State(States st) {
            state = st;
        }

        public void onEnter() {
            logger.debug("Current state: {}", this);
        }

        public void onExit() {
            logger.debug("Leaving {}", this);
        }

        @Override
        public void onStartRequest() {
            logger.debug("{}: onStartRequest", state);
            fsmWorker.reportStartStatus(StartStatus.NOT_SUPPORTED);
        }

        @Override
        public void onStopRequest() {
            logger.debug("{}: onStopRequest", state);
        }

        @Override
        public void onNegotiationResponse() {
            logger.debug("{} Default action: onNegotiationResponse", state);
        }

        @Override
        public void onNegotiationFailed() {
            logger.debug("{} Default action: onNegotiationFailed", state);
        }

        @Override
        public void onNegotiationTimeout() {
            logger.debug("{} Default action: onNegotiationTimeout", state);
        }

        @Override
        public void onChannelStatusUp() {
            logger.debug("{} Default action: onChannelStatusUp", state);
        }

        @Override
        public void onChannelStatusWritable() {
            logger.debug("{} Default action: onChannelStatusWritable", state);
        }

        @Override
        public void onChannelStatusDown() {
            logger.debug("{} Default action: onChannelStatusDown", state);
        }

        @Override
        public void onConnectStatusSuccess() {
            logger.debug("{} Default action: onConnectStatusSuccess", state);
        }

        @Override
        public void onConnectStatusFailed() {
            logger.debug("{} Default action: onConnectStatusFailed", state);
        }

        @Override
        public void onConnectStatusCancelled() {
            logger.debug("{} Default action: onConnectStatusCancelled", state);
        }

        @Override
        public void onDisconnectChannelSuccess() {
            logger.debug("{} Default action: onDisconnectChannelSuccess", state);
        }

        @Override
        public void onDisconnectChannelFailed() {
            logger.debug("{} Default action: onDisconnectChannelFailed", state);
        }

        @Override
        public void onDisconnectBrokerTimeout() {
            logger.debug("{} Default action: onDisconnectBrokerTimeout", state);
        }

        @Override
        public void onDisconnectBrokerResponse() {
            logger.debug("{} Default action: onDisconnectBrokerResponse", state);
        }

        @Override
        public void onDisconnectBrokerFailed() {
            logger.debug("{} Default action: onDisconnectBrokerFailed", state);
        }

        @Override
        public void onBmqEvent() {
            logger.debug("{} Default action: onBmqEvent", state);
        }

        @Override
        public String toString() {
            return state.toString();
        }
    }

    class ConnectionLost extends State {

        public ConnectionLost() {
            super(States.CONNECTION_LOST);
        }

        @Override
        public void onEnter() {
            logger.debug("onEnter: {}", this);
            fsmWorker.handleChannelDown();
        }

        @Override
        public void onDisconnectChannelSuccess() {
            logger.debug("onDisconnectChannelSuccess: {}", this);
            stopStatus = StopStatus.SUCCESS;
        }

        @Override
        public void onDisconnectChannelFailed() {
            logger.debug("onDisconnectChannelFailed: {}", this);
            stopStatus = StopStatus.FAILURE;
        }

        @Override
        public void onNegotiationTimeout() {
            logger.debug("onNegotiationTimeout: {}", this);
            if (!isOnceStarted) {
                startErrorStatus = StartStatus.NEGOTIATION_FAILURE;
                fsmWorker.doDisconnectChannel();
            }
        }
    }

    class Connecting extends State {

        public Connecting() {
            super(States.CONNECTING);
        }

        @Override
        public void onEnter() {
            logger.debug("onEnter: {}", this);
            fsmWorker.doConnect();
        }

        @Override
        public void onStopRequest() {
            logger.debug("onStopRequest: {}", this);
            if (!isOnceStarted) {
                startErrorStatus = StartStatus.CANCELLED;
            }
        }

        @Override
        public void onConnectStatusFailed() {
            logger.debug("onConnectStatusFailed: {}", this);
            startErrorStatus = StartStatus.CONNECT_FAILURE;
        }
    }

    class Negotiating extends State {

        public Negotiating() {
            super(States.NEGOTIATING);
        }

        @Override
        public void onEnter() {
            logger.debug("onEnter: {}", this);
            fsmWorker.doNegotiation();
        }

        @Override
        public void onBmqEvent() {
            logger.debug("onBmqEvent: {}", this);
            fsmWorker.handleNegotiationResponse();
        }

        @Override
        public void onNegotiationFailed() {
            logger.debug("onNegotiationFailed: {}", this);
            startErrorStatus = StartStatus.NEGOTIATION_FAILURE;
        }

        @Override
        public void onNegotiationTimeout() {
            logger.debug("onNegotiationTimeout: {}", this);
            startErrorStatus = StartStatus.NEGOTIATION_FAILURE;
        }

        @Override
        public void onStopRequest() {
            logger.debug("onStopRequest: {}", this);
            if (!isOnceStarted) {
                startErrorStatus = StartStatus.CANCELLED;
            }
        }
    }

    class Connected extends State {

        public Connected() {
            super(States.CONNECTED);
        }

        @Override
        public void onEnter() {
            logger.debug("Current state: {}", this);
            if (!isOnceStarted) {
                isOnceStarted = true;
                fsmWorker.reportStartStatus(StartStatus.SUCCESS);
            }
            fsmWorker.reportSessionStatus(SessionStatus.SESSION_UP);
        }

        @Override
        public void onExit() {
            logger.debug("Current state: {}", this);
            fsmWorker.reportSessionStatus(SessionStatus.SESSION_DOWN);
        }

        @Override
        public void onBmqEvent() {
            fsmWorker.handleBmqEvent();
        }

        @Override
        public void onStartRequest() {
            logger.debug("onStartRequest: {}", this);
            fsmWorker.reportStartStatus(StartStatus.SUCCESS);
        }
    }

    class DisconnectingBroker extends State {

        public DisconnectingBroker() {
            super(States.DISCONNECTING_BROKER);
        }

        @Override
        public void onEnter() {
            fsmWorker.doDisconnectSession();
        }

        @Override
        public void onBmqEvent() {
            logger.debug("onBmqEvent: {}", this);
            // Continue to handle all incoming bmq events until we receive
            // disconnect response
            fsmWorker.handleBmqEvent();
        }
    }

    class DisconnectingChannel extends State {

        public DisconnectingChannel() {
            super(States.DISCONNECTING_CHANNEL);
        }

        @Override
        public void onEnter() {
            fsmWorker.doDisconnectChannel();
        }

        @Override
        public void onDisconnectChannelSuccess() {
            logger.debug("onDisconnectChannelSuccess: {}", this);
            stopStatus = StopStatus.SUCCESS;
        }

        @Override
        public void onDisconnectChannelFailed() {
            logger.debug("onDisconnectChannelFailed: {}", this);
            stopStatus = StopStatus.FAILURE;
        }
    }

    class Stopped extends State {

        @Override
        public void onEnter() {
            logger.debug("Current state: {}", this);
            isOnceStarted = false;
            fsmWorker.handleStop();
            if (startErrorStatus != null) {
                fsmWorker.reportStartStatus(startErrorStatus);
                startErrorStatus = null;
            }
            if (stopStatus != null) {
                fsmWorker.reportStopStatus(stopStatus);
                stopStatus = null;
            }
        }

        public Stopped() {
            super(States.STOPPED);
        }

        @Override
        public void onStopRequest() {
            fsmWorker.reportStopStatus(StopStatus.SUCCESS);
        }

        @Override
        public void onStartRequest() {
            logger.debug("onStartRequest: {}", this);
        }
    }
}
