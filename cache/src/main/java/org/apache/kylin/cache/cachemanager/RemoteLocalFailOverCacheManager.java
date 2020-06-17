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

package org.apache.kylin.cache.cachemanager;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.support.AbstractCacheManager;

import org.apache.kylin.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;

public class RemoteLocalFailOverCacheManager extends AbstractCacheManager {
    private static final Logger logger = LoggerFactory.getLogger(RemoteLocalFailOverCacheManager.class);

    @Autowired
    private MemcachedCacheManager remoteCacheManager;

    @Autowired
    private CacheManager localCacheManager;

    @Override
    public void afterPropertiesSet() {
        Preconditions.checkNotNull(localCacheManager, "localCacheManager is not injected yet");
    }

    @Override
    protected Collection<? extends Cache> loadCaches() {
        return null;
    }

    @Override
    public Cache getCache(String name) {
        if (remoteCacheManager == null || remoteCacheManager.isClusterDown()) {
            logger.info("use local cache, because remote cache is not configured or down");
            return localCacheManager.getCache(name);
        } else {
            return remoteCacheManager.getCache(name);
        }
    }

    @VisibleForTesting
    void disableRemoteCacheManager() {
        remoteCacheManager.setClusterHealth(false);
    }

    @VisibleForTesting
    void enableRemoteCacheManager() {
        remoteCacheManager.setClusterHealth(true);
    }

    @VisibleForTesting
    MemcachedCacheManager getRemoteCacheManager() {
        return remoteCacheManager;
    }
}