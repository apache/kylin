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

package org.apache.kylin.rest.util;

import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GlutenCacheRequestLimits implements AutoCloseable {
    private static final AtomicInteger RUNNING_STATS = new AtomicInteger(0);

    static boolean openQueryRequest(int maxConcurrentQuery) {
        if (maxConcurrentQuery == 0) {
            return true;
        }
        try {
            for (;;) {
                int running = RUNNING_STATS.get();
                if (running < maxConcurrentQuery) {
                    if (RUNNING_STATS.compareAndSet(running, running + 1)) {
                        return true;
                    }
                } else {
                    return false;
                }
            }
        } catch (Exception e) {
            Message msg = MsgPicker.getMsg();
            throw new KylinException(PERMISSION_DENIED, msg.getGlutenCacheTooManyRunning());
        }
    }

    static void closeQueryRequest(int maxConcurrentQuery) {
        if (maxConcurrentQuery == 0) {
            return;
        }
        if (RUNNING_STATS.get() != 0) {
            RUNNING_STATS.decrementAndGet();
        }
    }

    private final int maxConcurrentQuery;

    public GlutenCacheRequestLimits() {
        this.maxConcurrentQuery = KylinConfig.getInstanceFromEnv().getConcurrentRunningThresholdForGlutenCache();

        boolean ok = openQueryRequest(maxConcurrentQuery);
        checkRequest(ok);
    }

    private static void checkRequest(boolean ok) {
        if (!ok) {
            Message msg = MsgPicker.getMsg();
            throw new KylinException(PERMISSION_DENIED, msg.getGlutenCacheTooManyRunning());
        }
    }

    @Override
    public void close() {
        closeQueryRequest(maxConcurrentQuery);
    }
}
