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

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kylin.cache.redis.RedisClient;
import org.apache.kylin.cache.redis.RedisConfig;
import org.apache.kylin.cache.redis.jedis.JedisPoolClient;
import org.apache.kylin.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.apache.kylin.cache.redis.RedisClientTypeEnum.JEDIS_POOL;


public class RedisManager extends AbstractRemoteCacheManager {

    private static final Logger logger = LoggerFactory.getLogger(RedisManager.class);
    private static final Long ONE_MINUTE = 60 * 1000L;

    @Autowired
    private RedisConfig redisConfig;

    private ScheduledExecutorService timer = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("Redis-HealthChecker").build());
    private AtomicBoolean clusterHealth = new AtomicBoolean(true);

    @Override
    protected Collection<? extends Cache> loadCaches() {
        Cache successCache = new RedisManager.RedisCacheAdaptor(createRedisClient(redisConfig), CacheConstants.QUERY_CACHE);

        addCache(successCache);

        Collection<String> names = getCacheNames();
        Collection<Cache> caches = Lists.newArrayList();
        for (String name : names) {
            caches.add(getCache(name));
        }
        timer.scheduleWithFixedDelay(new RedisManager.RedisClusterHealthChecker(), ONE_MINUTE, ONE_MINUTE,
                TimeUnit.MILLISECONDS);
        return caches;
    }

    public static class RedisCacheAdaptor implements Cache {

        private RedisClient redisClient;
        private String name;

        public RedisCacheAdaptor(RedisClient redisClient, String name) {
            this.redisClient = redisClient;
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Object getNativeCache() {
            return redisClient;
        }

        @Override
        public ValueWrapper get(Object key) {
            byte[] value = redisClient.get(key);
            if (value == null) {
                return null;
            }
            return new SimpleValueWrapper(SerializationUtils.deserialize(value));
        }

        @Override
        public <T> T get(Object key, Class<T> type) {
            byte[] value = redisClient.get(key);
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
        public <T> T get(Object key, Callable<T> valueLoader) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void put(Object key, Object value) {
            redisClient.put(key, value);
        }

        @Override
        public ValueWrapper putIfAbsent(Object key, Object value) {
            byte[] existing = redisClient.get(key);
            if (existing == null) {
                redisClient.put(key, value);
                return null;
            } else {
                return new SimpleValueWrapper(SerializationUtils.deserialize(existing));
            }
        }

        @Override
        public void evict(Object key) {
            redisClient.del(key);
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

    }

    protected RedisClient createRedisClient(RedisConfig redisConfig) {
        RedisClient redisClient = null;
        if (redisConfig.getRedisClientType() == JEDIS_POOL) {
            redisClient = new JedisPoolClient(redisConfig);
        }
        assert redisClient != null;
        return redisClient;
    }

    @Override
    public boolean isClusterDown() {
        return !clusterHealth.get();
    }

    @VisibleForTesting
    void setClusterHealth(boolean ifHealth) {
        clusterHealth.set(ifHealth);
    }

    private class RedisClusterHealthChecker implements Runnable {
        @Override
        public void run() {
            Cache cache = getCache(CacheConstants.QUERY_CACHE);
            RedisClient redisClient = (RedisClient) cache.getNativeCache();
            clusterHealth.set(isConnected(redisClient));
        }

        private boolean isConnected(RedisClient redisClient) {
            int success = 0;
            int total = 5;
            for (int i = 0; i < total; i++) {
                if (redisClient.ping()) {
                    success++;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.warn("RedisClusterHealthChecker sleep is Interrupted");
                }
            }
            boolean connected = success * 2 + 1 > total;
            logger.info("RedisClusterHealthChecker, connected:{}, success:{}, total:{}", connected, success, total);
            return connected;
        }
    }

}
