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

package org.apache.kylin.engine.spark;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kylin.guava30.shaded.common.cache.Cache;
import org.apache.kylin.guava30.shaded.common.cache.CacheBuilder;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryCostCollector {

    // use guava cache is for auto clean even if there are something went wrong in query
    public static final Cache<String, AtomicLong> QUERY_CPU_TIME_CACHE = CacheBuilder.newBuilder()
            .expireAfterAccess(10, TimeUnit.MINUTES).build();

    public static void addQueryMetrics(String queryId, long cpuCost) {
        long start = System.currentTimeMillis();
        try {
            addQueryCounter(queryId, cpuCost);
        } catch (Exception e) {
            log.error("Error when add query metrics.", e);
        }
        long end = System.currentTimeMillis();
        if ((end - start) > 100) {
            log.warn("QueryCost collector cost too much time: {} ms ", (end - start));
        }
    }

    private static void addQueryCounter(String queryId, long step) throws ExecutionException {
        AtomicLong totalBlocks = QUERY_CPU_TIME_CACHE.get(queryId, () -> new AtomicLong(0L));
        totalBlocks.addAndGet(step);
    }

    public static long getAndCleanStatus(String queryId) {
        try {
            AtomicLong cpuTime = QUERY_CPU_TIME_CACHE.get(queryId, () -> new AtomicLong(0L));
            QUERY_CPU_TIME_CACHE.invalidate(queryId);
            return cpuTime.get();
        } catch (ExecutionException e) {
            log.error("Error when log query metrics.", e);
        }
        return 0;
    }

    private QueryCostCollector() {
    }
}
