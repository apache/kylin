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

import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.guava30.shaded.common.cache.Cache;
import org.apache.kylin.guava30.shaded.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetPageFilterCollector {

    public static final Logger LOGGER = LoggerFactory.getLogger(ParquetPageFilterCollector.class);
    public static final Cache<String, AtomicLong> queryTotalParquetPages = CacheBuilder.newBuilder()
            .expireAfterAccess(10, TimeUnit.MINUTES).build();
    public static final Cache<String, AtomicLong> queryFilteredParquetPages = CacheBuilder.newBuilder()
            .expireAfterAccess(10, TimeUnit.MINUTES).build();
    public static final Cache<String, AtomicLong> queryAfterFilterParquetPages = CacheBuilder.newBuilder()
            .expireAfterAccess(10, TimeUnit.MINUTES).build();
    private static final AutoReadWriteLock LOCK = new AutoReadWriteLock(new ReentrantReadWriteLock());

    private ParquetPageFilterCollector() {
        // for sonar
    }

    public static void addQueryMetrics(String queryId, long totalPages, long filteredPages, long afterFilterPages) {
        long start = System.currentTimeMillis();
        try (AutoReadWriteLock.AutoLock writeLock = LOCK.lockForWrite()) {
            addQueryCounter(queryId, queryTotalParquetPages, totalPages);
            addQueryCounter(queryId, queryFilteredParquetPages, filteredPages);
            addQueryCounter(queryId, queryAfterFilterParquetPages, afterFilterPages);
        } catch (Exception e) {
            LOGGER.error("Error when add query metrics.", e);
        }
        long end = System.currentTimeMillis();
        if ((end - start) > 100) {
            LOGGER.warn("Parquet page filter collector cost too much time: {} ms ", (end - start));
        }
    }

    private static void addQueryCounter(String queryId, Cache<String, AtomicLong> counter, long step)
            throws ExecutionException {
        AtomicLong pageCnt = counter.get(queryId, () -> new AtomicLong(0L));
        pageCnt.addAndGet(step);
    }

    public static void logParquetPages(String queryId) {
        try {
            AtomicLong totalPages = queryTotalParquetPages.getIfPresent(queryId);
            if (totalPages != null && totalPages.get() > 0) {
                AtomicLong filteredPages = queryFilteredParquetPages.get(queryId, () -> new AtomicLong(0L));
                AtomicLong afterFilteredPages = queryAfterFilterParquetPages.get(queryId, () -> new AtomicLong(0L));
                LOGGER.info("Query total parquet pages {}, filtered pages {}, after filter pages {}, filter rate {}",
                        totalPages.get(), filteredPages.get(), afterFilteredPages.get(),
                        (double) filteredPages.get() / totalPages.get());
            }
            queryTotalParquetPages.invalidate(queryId);
            queryFilteredParquetPages.invalidate(queryId);
            queryAfterFilterParquetPages.invalidate(queryId);

        } catch (ExecutionException e) {
            LOGGER.error("Error when log query metrics.", e);
        }

    }
}
