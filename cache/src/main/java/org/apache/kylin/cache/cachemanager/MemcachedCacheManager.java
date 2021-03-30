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

import java.net.SocketAddress;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kylin.cache.memcached.MemcachedCache;
import org.apache.kylin.cache.memcached.MemcachedCacheConfig;
import org.apache.kylin.cache.memcached.MemcachedChunkingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.support.AbstractCacheManager;
import org.springframework.cache.support.SimpleValueWrapper;

import org.apache.kylin.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;

import net.spy.memcached.MemcachedClientIF;

public class MemcachedCacheManager extends AbstractCacheManager {

    private static final Logger logger = LoggerFactory.getLogger(MemcachedCacheManager.class);
    private static final Long ONE_MINUTE = 60 * 1000L;

    @Autowired
    private MemcachedCacheConfig memcachedCacheConfig;

    private ScheduledExecutorService timer = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("Memcached-HealthChecker").build());
    private AtomicBoolean clusterHealth = new AtomicBoolean(true);

    @Override
    protected Collection<? extends Cache> loadCaches() {
        Cache successCache = new MemCachedCacheAdaptor(
                new MemcachedChunkingCache(MemcachedCache.create(memcachedCacheConfig, CacheConstants.QUERY_CACHE)));
        Cache userCache = new MemCachedCacheAdaptor(
                new MemcachedCache(MemcachedCache.create(memcachedCacheConfig, CacheConstants.USER_CACHE, 86400)));

        addCache(successCache);
        addCache(userCache);

        Collection<String> names = getCacheNames();
        Collection<Cache> caches = Lists.newArrayList();
        for (String name : names) {
            caches.add(getCache(name));
        }

        timer.scheduleWithFixedDelay(new MemcachedClusterHealthChecker(), ONE_MINUTE, ONE_MINUTE,
                TimeUnit.MILLISECONDS);
        return caches;
    }

    public boolean isClusterDown() {
        return !clusterHealth.get();
    }

    @VisibleForTesting
    void setClusterHealth(boolean ifHealth) {
        clusterHealth.set(ifHealth);
    }

    public static class MemCachedCacheAdaptor implements Cache {
        private MemcachedCache memcachedCache;

        public MemCachedCacheAdaptor(MemcachedCache memcachedCache) {
            this.memcachedCache = memcachedCache;
        }

        @Override
        public String getName() {
            return memcachedCache.getName();
        }

        @Override
        public Object getNativeCache() {
            return memcachedCache.getNativeCache();
        }

        @Override
        public ValueWrapper get(Object key) {
            byte[] value = memcachedCache.get(key);
            if (value == null) {
                return null;
            }
            return new SimpleValueWrapper(SerializationUtils.deserialize(value));
        }

        @Override
        public void put(Object key, Object value) {
            memcachedCache.put(key, value);
        }

        @Override
        public void evict(Object key) {
            memcachedCache.evict(key);
        }

        @Override
        public void clear() {
            memcachedCache.clear();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T get(Object key, Class<T> type) {
            byte[] value = memcachedCache.get(key);
            if (value == null) {
                return null;
            }
            Object obj = SerializationUtils.deserialize(value);
            if (obj != null && type != null && !type.isInstance(value)) {
                throw new IllegalStateException(
                        "Cached value is not of required type [" + type.getName() + "]: " + value);
            }
            return (T) obj;
        }

        @Override
        //TODO
        public <T> T get(Object key, Callable<T> valueLoader) {
            throw new UnsupportedOperationException();
        }

        @Override
        //TODO implementation here doesn't guarantee the atomicity.
        //Without atomicity, this method should not be invoked
        public ValueWrapper putIfAbsent(Object key, Object value) {
            byte[] existing = memcachedCache.get(key);
            if (existing == null) {
                memcachedCache.put(key, value);
                return null;
            } else {
                return new SimpleValueWrapper(SerializationUtils.deserialize(existing));
            }
        }

    }

    private class MemcachedClusterHealthChecker implements Runnable {
        @Override
        public void run() {
            Cache cache = getCache(CacheConstants.QUERY_CACHE);
            MemcachedClientIF cacheClient = (MemcachedClientIF) cache.getNativeCache();
            Collection<SocketAddress> liveServers = cacheClient.getAvailableServers();
            Collection<SocketAddress> deadServers = cacheClient.getUnavailableServers();
            if (liveServers.isEmpty()) {
                clusterHealth.set(false);
                logger.error("All the servers in MemcachedCluster is down, UnavailableServers: " + deadServers);
            } else {
                clusterHealth.set(true);
                if (deadServers.size() > liveServers.size()) {
                    logger.warn("Half of the servers in MemcachedCluster is down, LiveServers: " + liveServers
                            + ", UnavailableServers: " + deadServers);
                }
            }
        }
    }
}
