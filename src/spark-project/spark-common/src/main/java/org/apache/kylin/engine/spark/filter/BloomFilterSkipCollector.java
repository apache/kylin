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

package org.apache.kylin.engine.spark.filter;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class BloomFilterSkipCollector {
    public static final Logger LOGGER = LoggerFactory.getLogger(BloomFilterSkipCollector.class);

    // use guava cache is for auto clean even if there are something went wrong in query
    public static final Cache<String, AtomicLong> queryTotalBloomBlocks = CacheBuilder
            .newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build();
    public static final Cache<String, AtomicLong> querySkipBloomBlocks = CacheBuilder
            .newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build();
    public static final Cache<String, AtomicLong> querySkipBloomRows = CacheBuilder
            .newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build();
    public static final Cache<String, AtomicLong> queryFooterReadTime = CacheBuilder
            .newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build();
    public static final Cache<String, AtomicLong> queryFooterReadNumber = CacheBuilder
            .newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build();

    public static final AtomicLong logCounter = new AtomicLong(0);
    private static final AtomicLong globalBloomBlocks = new AtomicLong(0);
    private static final AtomicLong globalSkipBloomBlocks = new AtomicLong(0);

    private static final AutoReadWriteLock LOCK = new AutoReadWriteLock(new ReentrantReadWriteLock());

    public static void addQueryMetrics(
            String queryId, long totalBloomBlocks,
            long skipBloomBlocks, long skipBloomRows, long footerReadTime, long footerReadNumber) {
        long start = System.currentTimeMillis();
        try (AutoReadWriteLock.AutoLock writeLock = LOCK.lockForWrite()) {
            addQueryCounter(queryId, queryTotalBloomBlocks, totalBloomBlocks);
            addQueryCounter(queryId, querySkipBloomBlocks, skipBloomBlocks);
            addQueryCounter(queryId, querySkipBloomRows, skipBloomRows);
            addQueryCounter(queryId, queryFooterReadTime, footerReadTime);
            addQueryCounter(queryId, queryFooterReadNumber, footerReadNumber);
            globalBloomBlocks.addAndGet(totalBloomBlocks);
            globalSkipBloomBlocks.addAndGet(skipBloomBlocks);
        } catch (Exception e) {
            LOGGER.error("Error when add query metrics.", e);
        }
        long end = System.currentTimeMillis();
        if ((end - start) > 100) {
            LOGGER.warn("BloomFilter collector cost too much time: {} ms ", (end - start));
        }
    }

    private static void addQueryCounter(String queryId,
            Cache<String, AtomicLong> counter, long step) throws ExecutionException {
        AtomicLong totalBlocks = counter.get(queryId, () -> new AtomicLong(0L));
        totalBlocks.addAndGet(step);
    }

    public static void logAndCleanStatus(String queryId) {
        try {
            AtomicLong readTime = queryFooterReadTime.get(queryId, () -> new AtomicLong(0L));
            AtomicLong readNumber = queryFooterReadNumber.get(queryId, () -> new AtomicLong(1L));
            if (readNumber.get() > 0L && readTime.get() > 0L) {
                LOGGER.info("Reading footer avg time is {}, total read time is {}, number of row groups is {}",
                        readTime.get() / readNumber.get(), readTime.get(), readNumber);
            }
            AtomicLong totalBloomBlocks = queryTotalBloomBlocks.getIfPresent(queryId);
            if (KylinConfig.getInstanceFromEnv().isBloomCollectFilterEnabled()
                    && totalBloomBlocks != null && totalBloomBlocks.get() > 0) {
                AtomicLong skipBlocks = querySkipBloomBlocks.get(queryId, () -> new AtomicLong(0L));
                AtomicLong skipRows = querySkipBloomRows.get(queryId, () -> new AtomicLong(0L));
                LOGGER.info("BloomFilter total bloom blocks is {}, skip bloom blocks is {}, skip rows is {}",
                        totalBloomBlocks.get(), skipBlocks.get(), skipRows.get());
            }
            queryFooterReadTime.invalidate(queryId);
            queryFooterReadNumber.invalidate(queryId);
            queryTotalBloomBlocks.invalidate(queryId);
            querySkipBloomBlocks.invalidate(queryId);
            querySkipBloomRows.invalidate(queryId);
            logCounter.incrementAndGet();
            if (logCounter.get() >= 100) {
                LOGGER.info("Global BloomFilter total bloom blocks is {}, "
                                + " skip bloom blocks is {}",
                        globalBloomBlocks.get(), globalSkipBloomBlocks.get());
                logCounter.set(0);
            }
            if (globalBloomBlocks.get() < 0 || globalSkipBloomBlocks.get() < 0) {
                // globalBloomBlocks number > Long.MAX_VALUE, almost impossible to get here
                globalBloomBlocks.set(0);
                globalSkipBloomBlocks.set(0);
            }
        } catch (ExecutionException e) {
            LOGGER.error("Error when log query metrics.", e);
        }
    }

    private BloomFilterSkipCollector() {
    }
}
