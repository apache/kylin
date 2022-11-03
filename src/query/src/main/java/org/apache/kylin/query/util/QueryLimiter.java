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
package org.apache.kylin.query.util;

import java.util.concurrent.Semaphore;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.query.exception.BusyQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class QueryLimiter {
    private static final Logger logger = LoggerFactory.getLogger(QueryLimiter.class);

    private static volatile boolean isDowngrading;
    private static final ThreadLocal<Boolean> downgradeState = new ThreadLocal<>();

    private static final Semaphore semaphore;
    static {
        semaphore = new Semaphore(KylinConfig.getInstanceFromEnv().getDowngradeParallelQueryThreshold(), true);
    }

    private QueryLimiter() {
    }

    @VisibleForTesting
    public static Semaphore getSemaphore() {
        return semaphore;
    }

    public static synchronized void downgrade() {
        if (!isDowngrading) {
            isDowngrading = true;
            logger.info("Query server state changed to downgrade");
        } else {
            logger.debug("Query server is already in downgrading state");
        }
    }

    public static synchronized void recover() {
        if (isDowngrading) {
            isDowngrading = false;
            logger.info("Query server state changed to normal");
        } else {
            logger.debug("Query server is already in normal state");
        }
    }

    public static void tryAcquire() {
        downgradeState.set(isDowngrading);
        if (!isDowngrading) {
            return;
        }

        if (!semaphore.tryAcquire()) {
            // finally {} skip release lock
            downgradeState.set(false);

            logger.info("query: {} failed to get acquire", QueryContext.current().getQueryId());
            throw new BusyQueryException("Query rejected. Caused by query server is too busy");
        }

        logger.debug("query: {} success to get acquire", QueryContext.current().getQueryId());
    }

    public static void release() {
        if (!downgradeState.get()) {
            return;
        }

        logger.debug("query: {} release acquire, current server state: {}", QueryContext.current().getQueryId(),
                downgradeState.get());

        semaphore.release();
    }

    public static boolean getStatus() {
        return isDowngrading;
    }
}
