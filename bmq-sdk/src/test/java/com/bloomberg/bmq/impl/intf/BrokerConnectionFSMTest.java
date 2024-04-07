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
package com.bloomberg.bmq.impl.intf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.bloomberg.bmq.impl.BrokerConnectionFSMImpl;
import com.bloomberg.bmq.impl.intf.BrokerConnectionFSM.Inputs;
import com.bloomberg.bmq.impl.intf.BrokerConnectionFSM.States;
import java.lang.invoke.MethodHandles;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerConnectionFSMTest {

    static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static final int[][] transitions = BrokerConnectionFSMImpl.transitionTable();

    @Test
    void testTransitions() {
        // Check that transition table contains only existed states and each
        // state is reachable.

        final int numOfStates = States.values().length;
        final int numOfInputs = Inputs.values().length;

        assertTrue(numOfStates > 0);
        assertTrue(numOfInputs > 0);
        assertEquals(transitions.length, numOfStates);
        assertEquals(transitions[0].length, numOfInputs);

        TreeSet<Integer> stateSet = new TreeSet<>();
        for (int i = 0; i < numOfStates; i++) {
            boolean hasTransition = false;
            for (int j = 0; j < numOfInputs; j++) {
                int s = transitions[i][j];
                assertTrue(s < numOfStates);
                if (s != i) {
                    hasTransition = true;
                }
                stateSet.add(s);
            }
            assertTrue(hasTransition);
        }
        assertEquals(stateSet.size(), numOfStates);
    }

    States getState(States initState, Inputs signal) {
        int idx = transitions[initState.ordinal()][signal.ordinal()];
        return States.fromInt(idx);
    }

    private void testStopped() {
        // Check transitions fom the STOPPED state
        States initState = States.STOPPED;
        int[] transitions = BrokerConnectionFSMTest.transitions[initState.ordinal()];
        for (Inputs i : Inputs.values()) {
            States newState = getState(initState, i);
            switch (i) {
                case START_REQUEST:
                    assertEquals(States.CONNECTING, newState);
                    break;
                default:
                    if (newState != initState) {
                        logger.error(
                                "Unexpected transition: {} | {} -> {}", initState, i, newState);
                        fail();
                    }
            }
        }
    }

    private void testDisconnectingBroker() {
        // Check transitions fom the DISCONNECTING_BROKER state
        States initState = States.DISCONNECTING_BROKER;
        int[] transitions = BrokerConnectionFSMTest.transitions[initState.ordinal()];
        for (Inputs i : Inputs.values()) {
            States newState = getState(initState, i);
            switch (i) {
                case CHANNEL_STATUS_DOWN:
                    assertEquals(States.CONNECTION_LOST, newState);
                    break;
                case DISCONNECT_BROKER_RESPONSE: // fallthrough
                case DISCONNECT_BROKER_TIMEOUT: // fallthrough
                case DISCONNECT_BROKER_FAILURE:
                    assertEquals(States.DISCONNECTING_CHANNEL, newState);
                    break;
                default:
                    if (newState != initState) {
                        logger.error(
                                "Unexpected transition: {} | {} -> {}", initState, i, newState);
                        fail();
                    }
            }
        }
    }

    private void testDisconnectingChannel() {
        // Check transitions fom the DISCONNECTING_CHANNEL state
        States initState = States.DISCONNECTING_CHANNEL;
        int[] transitions = BrokerConnectionFSMTest.transitions[initState.ordinal()];
        for (Inputs i : Inputs.values()) {
            States newState = getState(initState, i);
            switch (i) {
                case CHANNEL_STATUS_DOWN:
                    assertEquals(States.CONNECTION_LOST, newState);
                    break;
                case DISCONNECT_CHANNEL_SUCCESS: // fallthrough
                case DISCONNECT_CHANNEL_FAILURE:
                    assertEquals(States.STOPPED, newState);
                    break;
                default:
                    if (newState != initState) {
                        logger.error(
                                "Unexpected transition: {} | {} -> {}", initState, i, newState);
                        fail();
                    }
            }
        }
    }

    private void testConnectionLost() {
        // Check transitions fom the CONNECTION_LOST state
        States initState = States.CONNECTION_LOST;
        int[] transitions = BrokerConnectionFSMTest.transitions[initState.ordinal()];
        for (Inputs i : Inputs.values()) {
            States newState = getState(initState, i);
            switch (i) {
                case STOP_REQUEST:
                    assertEquals(States.DISCONNECTING_CHANNEL, newState);
                    break;
                case CHANNEL_STATUS_UP:
                    assertEquals(States.NEGOTIATING, newState);
                    break;
                case DISCONNECT_CHANNEL_SUCCESS: // fallthrough
                case DISCONNECT_CHANNEL_FAILURE:
                    assertEquals(States.STOPPED, newState);
                    break;
                default:
                    if (newState != initState) {
                        logger.error(
                                "Unexpected transition: {} | {} -> {}", initState, i, newState);
                        fail();
                    }
            }
        }
    }

    private void testConnecting() {
        // Check transitions fom the CONNECTING state
        States initState = States.CONNECTING;
        int[] transitions = BrokerConnectionFSMTest.transitions[initState.ordinal()];
        for (Inputs i : Inputs.values()) {
            States newState = getState(initState, i);
            switch (i) {
                case STOP_REQUEST:
                    assertEquals(States.DISCONNECTING_CHANNEL, newState);
                    break;
                case CHANNEL_STATUS_UP:
                    assertEquals(States.NEGOTIATING, newState);
                    break;
                case CHANNEL_STATUS_DOWN:
                    assertEquals(States.CONNECTION_LOST, newState);
                    break;
                case CONNECT_STATUS_FAILURE: // fallthrough
                case CONNECT_STATUS_CANCELLED:
                    assertEquals(States.STOPPED, newState);
                    break;
                default:
                    if (newState != initState) {
                        logger.error(
                                "Unexpected transition: {} | {} -> {}", initState, i, newState);
                        fail();
                    }
            }
        }
    }

    private void testNegotiating() {
        // Check transitions fom the NEGOTIATING state
        States initState = States.NEGOTIATING;
        int[] transitions = BrokerConnectionFSMTest.transitions[initState.ordinal()];
        for (Inputs i : Inputs.values()) {
            States newState = getState(initState, i);
            switch (i) {
                case STOP_REQUEST: // fallthrough
                case NEGOTIATION_TIMEOUT: // fallthrough
                case NEGOTIATION_FAILURE:
                    assertEquals(States.DISCONNECTING_CHANNEL, newState);
                    break;
                case NEGOTIATION_RESPONSE:
                    assertEquals(States.CONNECTED, newState);
                    break;
                case CHANNEL_STATUS_DOWN:
                    assertEquals(States.CONNECTION_LOST, newState);
                    break;
                default:
                    if (newState != initState) {
                        logger.error(
                                "Unexpected transition: {} | {} -> {}", initState, i, newState);
                        fail();
                    }
            }
        }
    }

    private void testConnected() {
        // Check transitions fom the CONNECTED state
        States initState = States.CONNECTED;
        int[] transitions = BrokerConnectionFSMTest.transitions[initState.ordinal()];
        for (Inputs i : Inputs.values()) {
            States newState = getState(initState, i);
            switch (i) {
                case STOP_REQUEST:
                    assertEquals(States.DISCONNECTING_BROKER, newState);
                    break;
                case CHANNEL_STATUS_DOWN:
                    assertEquals(States.CONNECTION_LOST, newState);
                    break;
                default:
                    if (newState != initState) {
                        logger.error(
                                "Unexpected transition: {} | {} -> {}", initState, i, newState);
                        fail();
                    }
            }
        }
    }

    @Test
    void testStates() {
        for (States s : States.values()) {
            switch (s) {
                case CONNECTION_LOST:
                    testConnectionLost();
                    break;
                case CONNECTED:
                    testConnected();
                    break;
                case CONNECTING:
                    testConnecting();
                    break;
                case NEGOTIATING:
                    testNegotiating();
                    break;
                case DISCONNECTING_BROKER:
                    testDisconnectingBroker();
                    break;
                case DISCONNECTING_CHANNEL:
                    testDisconnectingChannel();
                    break;
                case STOPPED:
                    testStopped();
                    break;
                default:
                    logger.error("Unexpected state {}", s);
                    fail();
            }
        }
    }
}
