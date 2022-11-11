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

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.support.AbstractCacheManager;
import java.util.Collection;
import java.util.concurrent.Callable;

public class MultiLevelCacheManager extends AbstractCacheManager {

    private static final Logger logger = LoggerFactory.getLogger(MultiLevelCacheManager.class);

    @Autowired
    private AbstractRemoteCacheManager remoteCacheManager;

    @Autowired
    private CacheManager localCacheManager;

    @Override
    protected Collection<? extends Cache> loadCaches() {
        Cache successCache = new MultiLevelCacheManager.MultiLevelCacheAdaptor(remoteCacheManager, localCacheManager, CacheConstants.QUERY_CACHE);
        addCache(successCache);

        Collection<String> names = getCacheNames();
        Collection<Cache> caches = Lists.newArrayList();
        for (String name : names) {
            caches.add(getCache(name));
        }
        return caches;
    }

    public static class MultiLevelCacheAdaptor implements Cache {

        private AbstractRemoteCacheManager remoteCacheManager;
        private CacheManager localCacheManager;
        private String name;

        public MultiLevelCacheAdaptor(AbstractRemoteCacheManager remoteCacheManager, CacheManager localCacheManager, String name) {
            this.remoteCacheManager = remoteCacheManager;
            this.localCacheManager = localCacheManager;
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Object getNativeCache() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ValueWrapper get(Object key) {
            ValueWrapper localResult = localCacheManager.getCache(name).get(key);
            if (localResult != null) {
                return localResult;
            }
            if (!remoteCacheManager.isClusterDown()) {
                return remoteCacheManager.getCache(name).get(key);
            }
            return null;
        }

        @Override
        public <T> T get(Object key, Class<T> type) {
            T localResult = localCacheManager.getCache(name).get(key, type);
            if (localResult != null) {
                return localResult;
            }
            if (!remoteCacheManager.isClusterDown()) {
                return remoteCacheManager.getCache(name).get(key, type);
            }
            return null;
        }

        @Override
        public <T> T get(Object key, Callable<T> valueLoader) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void put(Object key, Object value) {
            localCacheManager.getCache(name).put(key, value);
            remoteCacheManager.getCache(name).put(key, value);
        }

        @Override
        public void evict(Object key) {
            remoteCacheManager.getCache(name).evict(key);
            localCacheManager.getCache(name).evict(key);
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ValueWrapper putIfAbsent(Object key, Object value) {
            ValueWrapper existing = get(key);
            if (existing == null) {
                put(key, value);
                return null;
            } else {
                return existing;
            }
        }

    }

}
