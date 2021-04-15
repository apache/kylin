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

package org.apache.kylin.stream.core.storage.columnar;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kylin.stream.core.storage.columnar.protocol.FragmentMetaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.cache.CacheBuilder;
import org.apache.kylin.shaded.com.google.common.cache.CacheLoader;
import org.apache.kylin.shaded.com.google.common.cache.CacheStats;
import org.apache.kylin.shaded.com.google.common.cache.LoadingCache;
import org.apache.kylin.shaded.com.google.common.cache.RemovalListener;
import org.apache.kylin.shaded.com.google.common.cache.RemovalNotification;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

/**
 * In streaming receiver side, data was divided into two part, memory store and fragment file. As the literal means,
 *  memory store is located at JVM heap (actually a SortedMap), and fragment file usually located at disk.
 *
 * Since the size of fragment file is often very large, reducing times of IO will improve performance remarkably. So we
 *  cache fragment file into off-heap memory as much as possible.
 */
public class ColumnarStoreCache {
    private static Logger logger = LoggerFactory.getLogger(ColumnarStoreCache.class);
    private static ColumnarStoreCache instance = new ColumnarStoreCache();

    private static final int INIT_CACHE_SIZE = 100;
    private static final int CACHE_SIZE = 10000;

    // temporary set it to Long.MAX_VALUE, just leave os to manage the buffer 
    private static final long MAX_BUFFERED_SIZE = Long.MAX_VALUE;
    private AtomicLong currentBufferedSize = new AtomicLong(0);

    private ConcurrentMap<DataSegmentFragment, AtomicLong> refCounters = Maps.newConcurrentMap();
    public LoadingCache<DataSegmentFragment, FragmentData> fragmentDataCache = CacheBuilder.newBuilder()
            .initialCapacity(INIT_CACHE_SIZE)
            .concurrencyLevel(8)
            .maximumSize(CACHE_SIZE)
            .expireAfterAccess(6, TimeUnit.HOURS)
            .removalListener(new RemovalListener<DataSegmentFragment, FragmentData>() {
                @Override
                public void onRemoval(RemovalNotification<DataSegmentFragment, FragmentData> notification) {
                    DataSegmentFragment fragment = notification.getKey();
                    logger.debug("Data fragment " + fragment + " is unloaded from Cache due to "
                            + notification.getCause());
                    FragmentData fragmentData = notification.getValue();
                    AtomicLong refCounter = refCounters.get(fragment);
                    if (refCounter != null) {
                        synchronized (refCounter) {
                            if (refCounter.get() <= 0) {
                                int bufferSize = fragmentData.getBufferCapacity();
                                currentBufferedSize.addAndGet(-bufferSize);
                                fragmentData.tryForceUnMapBuffer();
                                refCounters.remove(fragment);
                            } else {
                                logger.debug("Fragment mapped buffer " + fragment
                                        + " cannot be cleaned, because it has reference " + refCounter.get());
                            }
                        }
                    } else {
                        logger.debug("no ref counter found for fragment: " + fragment);
                    }
                }
            })
            .build(new CacheLoader<DataSegmentFragment, FragmentData>() {
                @Override
                public FragmentData load(DataSegmentFragment fragment) throws Exception {
                    if (currentBufferedSize.get() >= MAX_BUFFERED_SIZE) {
                        synchronized (fragmentDataCache) {
                            if (currentBufferedSize.get() >= MAX_BUFFERED_SIZE) {
                                long entrySize = fragmentDataCache.size();
                                logger.debug("Max buffer size exceeds {}, invalidate half of the cache, cacheSize {}",
                                        currentBufferedSize, entrySize);
                                long removed = 0;
                                for (DataSegmentFragment frag : fragmentDataCache.asMap().keySet()) {
                                    if (removed >= entrySize / 2) {
                                        break;
                                    }
                                    fragmentDataCache.invalidate(frag);
                                    removed++;
                                }
                            }
                        }
                    }
                    FragmentMetaInfo fragmentMetaInfo = fragment.getMetaInfo();
                    if (fragmentMetaInfo == null) {
                        throw new IllegalStateException("no metadata file exists for fragment:" + fragment);
                    }
                    FragmentData fragmentData = new FragmentData(fragmentMetaInfo, fragment.getDataFile());
                    int bufferSize = fragmentData.getBufferCapacity();
                    currentBufferedSize.addAndGet(bufferSize);
                    logger.debug("Data fragment {} cached, bufferSize {}, totalBufferSize {}", fragment, bufferSize,
                            currentBufferedSize.get());
                    return fragmentData;
                }
            });

    public static ColumnarStoreCache getInstance() {
        return instance;
    }

    public FragmentData startReadFragmentData(DataSegmentFragment fragment) throws IOException {
        try {
            AtomicLong refCounter = refCounters.putIfAbsent(fragment, new AtomicLong(1));
            if (refCounter != null) {
                //Additional synchronize is required for reference count check.
                synchronized (refCounter) {
                    refCounter.incrementAndGet();
                }
            }
            return fragmentDataCache.get(fragment);
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }

    public void finishReadFragmentData(DataSegmentFragment fragment) {
        AtomicLong refCounter = refCounters.get(fragment);
        if (refCounter != null) {
            refCounter.decrementAndGet();
        } else {
            logger.warn("Ref counter not exist for fragment:{}", fragment);
        }
    }

    public ColumnarStoreCacheStats getCacheStats() {
        ColumnarStoreCacheStats stats = new ColumnarStoreCacheStats();
        CacheStats cacheStats = fragmentDataCache.stats();

        stats.setHitCount(cacheStats.hitCount());
        stats.setMissCount(cacheStats.missCount());
        stats.setEvictionCount(cacheStats.evictionCount());
        stats.setLoadSuccessCount(cacheStats.loadSuccessCount());
        stats.setLoadExceptionCount(cacheStats.loadExceptionCount());
        stats.setTotalLoadTime(cacheStats.totalLoadTime());

        stats.setCacheEntriesNum(fragmentDataCache.size());
        stats.setCachedDataBufferSize(currentBufferedSize.get());
        return stats;
    }

    public void removeFragmentsCache(List<DataSegmentFragment> fragmentList) {
        if (fragmentList == null) {
            return;
        }
        for (DataSegmentFragment fragment : fragmentList) {
            fragmentDataCache.invalidate(fragment);
        }
    }

    public void removeFragmentCache(DataSegmentFragment fragment) {
        if (fragment == null) {
            return;
        }
        fragmentDataCache.invalidate(fragment);
    }

}
