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

package org.apache.kylin.rest.cache;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Locale;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;

public class KylinEhCache implements KylinCache {
    private static final Logger logger = LoggerFactory.getLogger(KylinEhCache.class);

    private CacheManager cacheManager;

    private KylinEhCache() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String cacheConfigLocation = kylinConfig.getEhCacheConfigPath();
        try {
            logger.info("Trying to load ehcache properties from {}.", cacheConfigLocation);
            this.cacheManager = CacheManager.create(new URL(cacheConfigLocation));
        } catch (MalformedURLException e) {
            logger.warn("Cannot use " + cacheConfigLocation, e);
        } catch (CacheException e) {
            logger.warn("Create cache manager failed with config path: {}.", cacheConfigLocation, e);
        } finally {
            logger.info("Use default ehcache.xml.");
            this.cacheManager = CacheManager.create(ClassLoader.getSystemResourceAsStream("ehcache.xml"));
        }
    }

    public static KylinCache getInstance() {
        return Singletons.getInstance(KylinEhCache.class);
    }

    public Ehcache getEhCache(String type, String project) {
        final String projectCacheName = String.format(Locale.ROOT, "%s-%s", type, project);
        if (cacheManager.getEhcache(projectCacheName) == null) {
            CacheConfiguration cacheConfiguration = cacheManager.getEhcache(type).getCacheConfiguration().clone();
            cacheConfiguration.setName(projectCacheName);
            cacheManager.addCacheIfAbsent(new Cache(cacheConfiguration));
        }

        return cacheManager.getEhcache(projectCacheName);
    }

    @Override
    public void put(String type, String project, Object key, Object value) {
        Ehcache ehcache = getEhCache(type, project);
        ehcache.put(new Element(key, value));
    }

    @Override
    public void update(String type, String project, Object key, Object value) {
        put(type, project, key, value);
    }

    @Override
    public Object get(String type, String project, Object key) {
        Ehcache ehcache = getEhCache(type, project);
        Element element = ehcache.get(key);
        return element == null ? null : element.getObjectValue();
    }

    @Override
    public boolean remove(String type, String project, Object key) {
        Ehcache ehcache = getEhCache(type, project);
        return ehcache.remove(key);
    }

    @Override
    public void clearAll() {
        cacheManager.clearAll();
    }

    @Override
    public void clearByType(String type, String project) {
        final String projectCacheName = String.format(Locale.ROOT, "%s-%s", type, project);
        String[] cacheNames = cacheManager.getCacheNames();
        for (String cacheName : cacheNames) {
            Ehcache ehcache = cacheManager.getEhcache(cacheName);
            if (ehcache != null && cacheName.contains(projectCacheName)) {
                ehcache.removeAll();
            }
        }
    }
}
