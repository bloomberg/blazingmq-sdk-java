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

/**
 * Provides an interface for monitoring the health of the host. BlazingMQ sessions can use such
 * objects to conditionally suspend queue activity while the host is marked unhealthy
 */
public interface HostHealthMonitor {

    /** Provides an interface for handling changes detected by {@code HostHealthMonitor} */
    interface Handler {
        /**
         * Invoked as a response to the {@code HostHealthMonitor} detecting a change in the state of
         * the host health
         *
         * @param state current host health state
         */
        void onHostHealthStateChanged(HostHealthState state);
    }

    /**
     * Registers the specified `handler` to be invoked each time the health of the host changes
     *
     * @param handler Handler object to be registered
     * @return true if specified {@code handler} was successfully added, false if specified {@code
     *     handler} was added before
     * @throws NullPointerException if specified {@code handler} is null
     */
    boolean addHandler(Handler handler);

    /**
     * Unregisters the specified `handler'
     *
     * @param handler Handler object to be removed
     * @return true if specified {@code handler} was successfully removed, false if there were no
     *     such object to remove
     * @throws NullPointerException if specified {@code handler} is null
     */
    boolean removeHandler(Handler handler);

    /**
     * Queries the current health of the host
     *
     * @return HostHealthState current host health state
     */
    HostHealthState hostHealthState();
}
