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

package org.apache.kylin.storage.cache;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.Status;
import net.sf.ehcache.config.CacheConfiguration;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.realization.StreamSQLDigest;
import org.apache.kylin.metadata.tuple.TeeTupleItrListener;
import org.apache.kylin.storage.ICachableStorageQuery;
import org.apache.kylin.storage.IStorageQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public abstract class AbstractCacheFledgedQuery implements IStorageQuery, TeeTupleItrListener {
    private static final Logger logger = LoggerFactory.getLogger(AbstractCacheFledgedQuery.class);

    private static final String storageCacheTemplate = "StorageCache";

    protected static CacheManager CACHE_MANAGER;

    protected ICachableStorageQuery underlyingStorage;
    protected StreamSQLDigest streamSQLDigest;

    public AbstractCacheFledgedQuery(ICachableStorageQuery underlyingStorage) {
        this.underlyingStorage = underlyingStorage;
        this.makeCacheIfNecessary(underlyingStorage.getStorageUUID());
    }

    public static void setCacheManager(CacheManager cacheManager) {
        CACHE_MANAGER = cacheManager;
    }

    protected StreamSQLResult getStreamSQLResult(StreamSQLDigest streamSQLDigest) {

        Cache cache = CACHE_MANAGER.getCache(this.underlyingStorage.getStorageUUID());
        Element element = cache.get(streamSQLDigest.hashCode());//TODO: hash code cannot guarantee uniqueness
        if (element != null) {
            return (StreamSQLResult) element.getObjectValue();
        }
        return null;
    }

    protected boolean needSaveCache(long createTime) {
        long storageQueryTime = System.currentTimeMillis() - createTime;
        long durationThreshold = KylinConfig.getInstanceFromEnv().getQueryDurationCacheThreshold();
        //TODO: check scan count necessary?

        if (storageQueryTime < durationThreshold) {
            logger.info("Skip saving storage caching for storage cache because storage query time {} less than {}", storageQueryTime, durationThreshold);
            return false;
        }

        return true;
    }

    private void makeCacheIfNecessary(String storageUUID) {
        if (CACHE_MANAGER == null || (!(CACHE_MANAGER.getStatus().equals(Status.STATUS_ALIVE)))) {
            throw new RuntimeException("CACHE_MANAGER is not provided or not alive");
        }

        if (CACHE_MANAGER.getCache(storageUUID) == null) {
            logger.info("Cache for {} initializing...", storageUUID);

            //Create a Cache specifying its configuration.
            CacheConfiguration templateConf = CACHE_MANAGER.getCache(storageCacheTemplate).getCacheConfiguration();

            Cache storageCache = new Cache(new CacheConfiguration(storageUUID, 0).//
                    memoryStoreEvictionPolicy(templateConf.getMemoryStoreEvictionPolicy()).//
                    eternal(templateConf.isEternal()).//
                    timeToIdleSeconds(templateConf.getTimeToIdleSeconds()).//
                    //maxBytesLocalHeap(templateConf.getMaxBytesLocalHeap(), MemoryUnit.BYTES).//using pooled size
                    persistence(templateConf.getPersistenceConfiguration()));

            CACHE_MANAGER.addCacheIfAbsent(storageCache);
        }
    }
}
