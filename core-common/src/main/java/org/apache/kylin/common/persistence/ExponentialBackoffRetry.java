/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.common.persistence;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExponentialBackoffRetry {
    private static final Logger logger = LoggerFactory.getLogger(ExponentialBackoffRetry.class);

    private final ResourceStore store;
    private final KylinConfig config;
    private final int baseSleepTimeMs;
    private final int maxSleepTimeMs;
    private long firstSleepTime;
    private int retryCount;

    public ExponentialBackoffRetry(ResourceStore store) {
        this.store = store;
        this.config = store.getConfig();
        this.baseSleepTimeMs = config.getResourceStoreReconnectBaseMs();
        this.maxSleepTimeMs = config.getResourceStoreReconnectMaxMs();
        this.retryCount = 0;
    }

    public <V> V doWithRetry(Callable<V> callable) throws IOException {
        V result = null;
        boolean done = false;

        while (!done) {
            try {
                result = callable.call();
                done = true;
            } catch (Throwable ex) {
                boolean shouldRetry = checkIfAllowRetry(ex);
                if (!shouldRetry) {
                    throwIOException(ex);
                }
            }
        }

        return result;
    }

    private void throwIOException(Throwable ex) throws IOException {
        if (ex instanceof IOException)
            throw (IOException) ex;

        if (ex instanceof RuntimeException)
            throw (RuntimeException) ex;

        if (ex instanceof Error)
            throw (Error) ex;

        throw new IOException(ex);
    }

    private boolean checkIfAllowRetry(Throwable ex) {
        if (config.isResourceStoreReconnectEnabled() && store.isUnreachableException(ex)) {
            if (isTimeOut(config.getResourceStoreReconnectTimeoutMs())) {
                logger.error("Reconnect to resource store timeout, abandoning...", ex);
                return false;
            }

            long waitMs = getSleepTimeMs();
            long seconds = waitMs / 1000;
            logger.info("Will try to re-connect after {} seconds.", seconds);
            try {
                Thread.sleep(waitMs);
            } catch (InterruptedException e) {
                throw new RuntimeException("Current thread for resource store's CRUD is interrupted, abandoning...");
            }
            increaseRetryCount();
            return true;
        }

        return false;
    }

    private long getSleepTimeMs() {
        if (retryCount == 0)
            firstSleepTime = System.currentTimeMillis();

        long ms = baseSleepTimeMs * (1L << retryCount);

        if (ms > maxSleepTimeMs)
            ms = maxSleepTimeMs;
        return ms;
    }

    private void increaseRetryCount() {
        retryCount++;
    }

    private boolean isTimeOut(long timeoutMs) {
        return retryCount != 0 && (System.currentTimeMillis() - firstSleepTime >= timeoutMs);
    }
}
