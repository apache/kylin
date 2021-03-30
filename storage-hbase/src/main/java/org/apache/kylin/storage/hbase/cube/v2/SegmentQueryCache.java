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

package org.apache.kylin.storage.hbase.cube.v2;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kylin.cache.memcached.CacheStats;
import org.apache.kylin.cache.memcached.MemcachedCache;
import org.apache.kylin.cache.memcached.MemcachedCacheConfig;
import org.apache.kylin.cache.memcached.MemcachedChunkingCache;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentQueryCache {
    public static final Logger logger = LoggerFactory.getLogger(SegmentQueryCache.class);
    private static final String SEG_QUERY_CACHE_NAME = "query_segment_cache";
    private static SegmentQueryCache segmentQueryCacheInstance = new SegmentQueryCache();

    private MemcachedChunkingCache memcachedCache;

    public static SegmentQueryCache getInstance() {
        return segmentQueryCacheInstance;
    }

    private SegmentQueryCache() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        MemcachedCacheConfig memcachedCacheConfig = new MemcachedCacheConfig();
        String configHosts = kylinConfig.getMemCachedHosts();
        memcachedCacheConfig.setTimeout(kylinConfig.getQuerySegmentCacheTimeout());
        // set max object size a little less than 1024 * 1024, because the key of the segment result cache is long
        // if set to 1024 * 1024 will cause memcached client exceed max size error
        memcachedCacheConfig.setMaxObjectSize(1000000);
        memcachedCacheConfig.setHosts(configHosts);
        //Reverse the compression setting between Hbase coprocessor and memcached, if Hbase result is compressed, memcached will not compress.
        memcachedCacheConfig.setEnableCompression(!kylinConfig.getCompressionResult());
        String cacheName = SEG_QUERY_CACHE_NAME;
        memcachedCache = new MemcachedChunkingCache(MemcachedCache.create(memcachedCacheConfig, cacheName));
    }

    public void put(String key, SegmentQueryResult segmentQueryResult) {
        memcachedCache.put(key, segmentQueryResult);
    }

    public SegmentQueryResult get(String key) {
        byte[] value = memcachedCache.get(key);
        if (value == null) {
            return null;
        }
        return (SegmentQueryResult) (SerializationUtils.deserialize(value));
    }

    public CacheStats getCacheStats() {
        return memcachedCache.getStats();
    }

    /**
     * evict the segment cache by query key
     *
     * @param segmentQueryKey
     */
    public void evict(String segmentQueryKey) {
        memcachedCache.evict(segmentQueryKey);
    }
}
