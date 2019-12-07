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

import org.springframework.cache.Cache;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.core.RedisOperations;

public class RedisCacheManagerAdaptor extends RedisCacheManager implements RemoteCacheManager {

    public RedisCacheManagerAdaptor(RedisOperations redisOperations) {
        super(redisOperations);
    }

    public RedisCacheManagerAdaptor(RedisOperations redisOperations, Collection<String> cacheNames) {
        super(redisOperations, cacheNames);
    }

    public RedisCacheManagerAdaptor(RedisOperations redisOperations, Collection<String> cacheNames,
            boolean cacheNullValues) {
        super(redisOperations, cacheNames, cacheNullValues);
    }

    /**
     * Get Redis Cache Object
     * @param name
     * @return
     */
    @Override
    public Cache getCache(String name) {
        return super.getCache(name);
    }

    @Override
    public boolean isClusterDown() {
        return clusterHealth.get();
    }

    @Override
    public void setClusterHealth(boolean ifHealth) {
        clusterHealth.set(ifHealth);
    }

}
