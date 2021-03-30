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
import java.util.Map;

import org.apache.kylin.cache.ehcache.InstrumentedEhCacheCache;
import org.apache.kylin.common.KylinConfig;
import org.springframework.cache.Cache;
import org.springframework.cache.ehcache.EhCacheCache;
import org.springframework.cache.support.AbstractCacheManager;
import org.springframework.util.Assert;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Status;

/**
 * CacheManager backed by an EhCache {@link net.sf.ehcache.CacheManager}.
 *
 */
public class InstrumentedEhCacheCacheManager extends AbstractCacheManager {

    private net.sf.ehcache.CacheManager cacheManager;
    private Map<String, String> metricsConfig = KylinConfig.getInstanceFromEnv().getKylinMetricsConf();
    private boolean enableMetrics = false;

    /**
     * Return the backing EhCache {@link net.sf.ehcache.CacheManager}.
     */
    public net.sf.ehcache.CacheManager getCacheManager() {
        return this.cacheManager;
    }

    /**
     * Set the backing EhCache {@link net.sf.ehcache.CacheManager}.
     */
    public void setCacheManager(net.sf.ehcache.CacheManager cacheManager) {
        this.cacheManager = cacheManager;
        if ("true".equalsIgnoreCase(metricsConfig.get("ehcache.enabled"))) {
            enableMetrics = true;
        }
    }

    @Override
    protected Collection<Cache> loadCaches() {
        Assert.notNull(this.cacheManager, "A backing EhCache CacheManager is required");
        Status status = this.cacheManager.getStatus();
        Assert.isTrue(Status.STATUS_ALIVE.equals(status),
                "An 'alive' EhCache CacheManager is required - current cache is " + status.toString());

        String[] names = this.cacheManager.getCacheNames();
        Collection<Cache> caches = Sets.newLinkedHashSetWithExpectedSize(names.length);
        for (String name : names) {
            if (enableMetrics) {
                caches.add(new InstrumentedEhCacheCache(this.cacheManager.getEhcache(name)));
            } else {
                caches.add(new EhCacheCache(this.cacheManager.getEhcache(name)));
            }
        }
        return caches;
    }

    @Override
    public Cache getCache(String name) {
        Cache cache = super.getCache(name);
        if (cache == null) {
            // check the EhCache cache again
            // (in case the cache was added at runtime)
            Ehcache ehcache = this.cacheManager.getEhcache(name);
            if (ehcache != null) {
                if (enableMetrics) {
                    cache = new InstrumentedEhCacheCache(ehcache);
                } else {
                    cache = new EhCacheCache(ehcache);
                }
                addCache(cache);
            }
        }
        return cache;
    }

}