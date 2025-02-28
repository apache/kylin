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

package org.apache.kylin.rest.cache.memcached;

import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.cache.KylinCache;
import org.apache.kylin.rest.service.CommonQueryCacheSupporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * This is a cache manager for memcached which implements KylinCache.
 * It has a map contains memcached clients created for different types of cache constants.
 */
public class CompositeMemcachedCache implements KylinCache {

    private static final Logger logger = LoggerFactory.getLogger(CompositeMemcachedCache.class);

    private static final String PREFIX = "Kylin";

    private static final ConcurrentMap<String, Cache> cacheMap = new ConcurrentHashMap<>(16);

    private static final MemcachedCacheConfig memcachedCacheConfig = Singletons.getInstance(MemcachedCacheConfig.class);

    private static final Cache exceptionCache = new MemCachedCacheAdaptor(new MemcachedChunkingCache(MemcachedCache
            .create(memcachedCacheConfig, CommonQueryCacheSupporter.Type.EXCEPTION_QUERY_CACHE.rootCacheName, 86400)));

    private static final Cache schemaCache = new MemCachedCacheAdaptor(new MemcachedChunkingCache(MemcachedCache
            .create(memcachedCacheConfig, CommonQueryCacheSupporter.Type.SCHEMA_CACHE.rootCacheName, 86400)));

    private static final Cache successCache = new MemCachedCacheAdaptor(new MemcachedChunkingCache(MemcachedCache
            .create(memcachedCacheConfig, CommonQueryCacheSupporter.Type.SUCCESS_QUERY_CACHE.rootCacheName)));

    static {
        cacheMap.put(CommonQueryCacheSupporter.Type.EXCEPTION_QUERY_CACHE.rootCacheName, exceptionCache);
        cacheMap.put(CommonQueryCacheSupporter.Type.SCHEMA_CACHE.rootCacheName, schemaCache);
        cacheMap.put(CommonQueryCacheSupporter.Type.SUCCESS_QUERY_CACHE.rootCacheName, successCache);
    }

    public static KylinCache getInstance() {
        try {
            return Singletons.getInstance(CompositeMemcachedCache.class);
        } catch (RuntimeException e) {
            logger.error("Memcached init failed", e);
        }
        return null;
    }

    private void checkCacheType(String type) {
        if (type == null) {
            throw new NullPointerException("type can't be null");
        }

        if (!cacheMap.containsKey(type)) {
            throw new IllegalArgumentException("unsupported rootCacheName: " + type);
        }
    }

    private String getTypeProjectPrefix(String type, String project) {
        return String.format(Locale.ROOT, "%s-%s-%s-", PREFIX, type, project);
    }

    protected String serializeKey(Object key) {
        try {
            return JsonUtil.writeValueAsString(key);
        } catch (JsonProcessingException e) {
            logger.warn("Can not convert key to String.", e);
        }
        return null;
    }

    @Override
    public void put(String type, String project, Object key, Object value) {
        checkCacheType(type);
        String keyS = serializeKey(key);
        if (keyS == null) {
            logger.warn("write to cash failed for key can not convert to String");
            return;
        }
        keyS = getTypeProjectPrefix(type, project) + keyS;
        cacheMap.get(type).put(keyS, value);
    }

    @Override
    public void update(String type, String project, Object key, Object value) {
        checkCacheType(type);
        String keyS = serializeKey(key);
        if (keyS == null) {
            logger.warn("write to cache failed for key can not convert to String");
            return;
        }
        keyS = getTypeProjectPrefix(type, project) + keyS;
        cacheMap.get(type).put(keyS, value);
    }

    @Override
    public Object get(String type, String project, Object key) {
        checkCacheType(type);
        String keyS = serializeKey(key);
        if (keyS == null) {
            logger.warn("read from cache failed for key can not convert to String");
            return null;
        }
        keyS = getTypeProjectPrefix(type, project) + keyS;
        SimpleValueWrapper valueWrapper = (SimpleValueWrapper) cacheMap.get(type).get(keyS);
        return valueWrapper == null ? null : valueWrapper.get();
    }

    @Override
    public boolean remove(String type, String project, Object key) {
        checkCacheType(type);
        String keyS = serializeKey(key);
        if (keyS == null) {
            logger.warn("evict cache failed for key can not convert to String");
            return false;
        }
        keyS = getTypeProjectPrefix(type, project) + keyS;
        if (cacheMap.get(type).get(keyS) == null) {
            return false;
        }
        cacheMap.get(type).evict(keyS);
        return true;
    }

    @Override
    public void clearAll() {
        for (Cache cache : cacheMap.values()) {
            cache.clear();
        }
    }

    @Override
    public void clearByType(String type, String project) {
        checkCacheType(type);
        String pattern = getTypeProjectPrefix(type, project);
        Cache cache = cacheMap.get(type);
        if (cache instanceof MemCachedCacheAdaptor) {
            ((MemCachedCacheAdaptor) cache).clearByType(pattern);
        } else {
            logger.warn("cache do not support clear by project");
            cacheMap.clear();
        }
    }

    public String getName(String type) {
        checkCacheType(type);
        return cacheMap.get(type).getName();
    }

    public CacheStats getCacheStats(String type) {
        checkCacheType(type);
        Cache cache = cacheMap.get(type);
        if (cache instanceof MemCachedCacheAdaptor) {
            return ((MemCachedCacheAdaptor) cache).getCacheStats();
        } else {
            logger.warn("only support get cache stats with memcached adaptor, otherwise will return null");
            return null;
        }
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

        public CacheStats getCacheStats() {
            return memcachedCache.getStats();
        }

        @Override
        public ValueWrapper get(Object key) {
            byte[] value = memcachedCache.get(key);
            if (value == null || value.length == 0) {
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

        public void clearByType(String pattern) {
            memcachedCache.clearByType(pattern);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T get(Object key, Class<T> type) {
            byte[] value = memcachedCache.get(key);
            if (value == null || value.length == 0) {
                return null;
            }
            Object obj = SerializationUtils.deserialize(value);
            if (obj != null && type != null && !type.isInstance(value)) {
                throw new IllegalStateException(
                        "Cached value is not of required type [" + type.getName() + "]: " + Arrays.toString(value));
            }
            return (T) obj;
        }

        @Override
        public <T> T get(Object key, Callable<T> valueLoader) {
            throw new UnsupportedOperationException();
        }

        @Override
        //Without atomicity, this method should not be invoked
        public ValueWrapper putIfAbsent(Object key, Object value) {
            //implementation here doesn't guarantee the atomicity.
            byte[] existing = memcachedCache.get(key);
            if (existing == null || existing.length == 0) {
                memcachedCache.put(key, value);
                return null;
            } else {
                return new SimpleValueWrapper(SerializationUtils.deserialize(existing));
            }
        }
    }
}
