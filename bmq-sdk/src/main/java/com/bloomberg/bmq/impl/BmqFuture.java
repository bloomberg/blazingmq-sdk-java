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

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BmqFuture<RESULT> {

    public static final Duration s_DEFAULT_TIMEOUT = Duration.ofSeconds(60);

    private final Future<RESULT> future;

    public BmqFuture(Future<RESULT> future) {
        this.future = future;
    }

    public RESULT get(Duration timeout) throws TimeoutException {
        return BmqFuture.get(future, timeout);
    }

    @SuppressWarnings("squid:S2142")
    public static <RESULT> RESULT get(Future<RESULT> future, Duration timeout)
            throws TimeoutException {
        if (timeout == null) {
            timeout = s_DEFAULT_TIMEOUT;
        }
        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
