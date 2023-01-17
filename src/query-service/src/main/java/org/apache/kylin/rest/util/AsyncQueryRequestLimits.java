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

import static org.apache.kylin.common.exception.QueryErrorCode.TOO_MANY_ASYNC_QUERY;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;

public class AsyncQueryRequestLimits implements AutoCloseable {
    private static volatile AtomicInteger asyncQueryCount = new AtomicInteger(0);

    private static final int MAX_COUNT = KylinConfig.getInstanceFromEnv().getAsyncQueryMaxConcurrentJobs();

    private static void openAsyncQueryRequest() {
        if (MAX_COUNT <= 0) {
            return;
        }
        asyncQueryCount.incrementAndGet();

    }

    public static void checkCount() {
        if (MAX_COUNT <= 0) {
            return;
        }
        if (asyncQueryCount.get() >= MAX_COUNT) {
            throw new KylinException(TOO_MANY_ASYNC_QUERY, MsgPicker.getMsg().getAsyncQueryTooManyRunning());
        }

    }

    private static void closeAsyncQueryRequest() {
        if (MAX_COUNT <= 0) {
            return;
        }
        asyncQueryCount.decrementAndGet();

    }

    public AsyncQueryRequestLimits() {
        openAsyncQueryRequest();
    }

    @Override
    public void close() {
        closeAsyncQueryRequest();
    }
}
