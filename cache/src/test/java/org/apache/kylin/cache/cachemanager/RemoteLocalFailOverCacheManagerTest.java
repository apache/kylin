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

import static org.apache.kylin.cache.cachemanager.CacheConstants.QUERY_CACHE;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.ehcache.EhCacheCache;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
//
//import net.spy.memcached.MemcachedClientIF;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:cacheContext.xml" })
@ActiveProfiles("testing-memcached")
public class RemoteLocalFailOverCacheManagerTest {

    @Autowired
    @Qualifier("cacheManager")
    RemoteLocalFailOverCacheManager cacheManager;

    @BeforeClass
    public static void setupResource() throws Exception {
        LocalFileMetadataTestCase.staticCreateTestMetadata();
    }

    @AfterClass
    public static void tearDownResource() {
    }

    @Test
    public void testCacheManager() {
        cacheManager.disableRemoteCacheManager();
        Assert.assertTrue("Memcached failover to ehcache", cacheManager.getCache(QUERY_CACHE) instanceof EhCacheCache);
        cacheManager.enableRemoteCacheManager();
        Assert.assertTrue("Memcached enabled",
                cacheManager.getCache(QUERY_CACHE) instanceof MemcachedCacheManager.MemCachedCacheAdaptor);
//
//        MemcachedCacheManager remoteCacheManager = cacheManager.getRemoteCacheManager();
//        for (int i = 0; i < 1000; i++) {
//            MemcachedClientIF client = (MemcachedClientIF) remoteCacheManager.getCache(QUERY_CACHE).getNativeCache();
//            System.out.println(i + " available servers: " + client.getAvailableServers() + "; unavailable servers: "
//                    + client.getUnavailableServers());
//            try {
//                client.get("key");
//                Thread.sleep(2000L);
//            } catch (Exception e) {
//            }
//        }
    }
}