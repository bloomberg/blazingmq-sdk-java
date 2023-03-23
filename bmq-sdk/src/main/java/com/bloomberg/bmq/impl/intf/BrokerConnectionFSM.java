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

import com.bloomberg.bmq.impl.intf.BrokerConnection.StartStatus;
import com.bloomberg.bmq.impl.intf.BrokerConnection.StopStatus;
import com.bloomberg.bmq.impl.intf.SessionStatusHandler.SessionStatus;

public interface BrokerConnectionFSM {

    enum States {
        CONNECTION_LOST, // s0
        CONNECTING, // s1
        NEGOTIATING, // s2
        CONNECTED, // s3
        DISCONNECTING_BROKER, // s4
        DISCONNECTING_CHANNEL, // s5
        STOPPED; // s6

        public static States fromInt(int i) {
            for (States s : States.values()) {
                if (s.ordinal() == i) return s;
            }
            return null;
        }
    }

    enum Inputs {
        START_REQUEST, // e0
        STOP_REQUEST, // e1
        NEGOTIATION_RESPONSE, // e2
        NEGOTIATION_FAILURE, // e3
        NEGOTIATION_TIMEOUT, // e4
        CHANNEL_STATUS_UP, // e5
        CHANNEL_STATUS_WRITABLE, // e6
        CHANNEL_STATUS_DOWN, // e7
        CONNECT_STATUS_SUCCESS, // e8
        CONNECT_STATUS_FAILURE, // e9
        CONNECT_STATUS_CANCELLED, // e10
        DISCONNECT_CHANNEL_SUCCESS, // e11
        DISCONNECT_CHANNEL_FAILURE, // e12
        DISCONNECT_BROKER_TIMEOUT, // e13
        DISCONNECT_BROKER_RESPONSE, // e14
        DISCONNECT_BROKER_FAILURE, // e15
        BMQ_EVENT; // e16

        public static Inputs fromInt(int i) {
            for (Inputs e : Inputs.values()) {
                if (e.ordinal() == i) return e;
            }
            return null;
        }
    }

    interface FSMWorker {
        void doConnect();

        void doNegotiation();

        void doDisconnectChannel();

        void doDisconnectSession();

        void handleNegotiationResponse();

        void handleBmqEvent();

        void handleChannelDown();

        void handleStop();

        void reportSessionStatus(SessionStatus status);

        void reportStartStatus(StartStatus status);

        void reportStopStatus(StopStatus status);
    }

    void handleInput(Inputs inp);

    States currentState();
}
