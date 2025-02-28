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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.rest.cache.memcached.CompositeMemcachedCache.MemCachedCacheAdaptor;
import org.apache.kylin.rest.service.CommonQueryCacheSupporter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.GetFuture;

public class MemcachedCacheTest extends NLocalFileMetadataTestCase {

    private Map<String, String> keyValueMap;
    private MemCachedCacheAdaptor memCachedAdaptor;
    private MemcachedCacheConfig cacheConfig;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();

        keyValueMap = Maps.newHashMap();
        keyValueMap.put("sql1", "value1");
        keyValueMap.put("sql11", "value11");

        cacheConfig = new MemcachedCacheConfig();
        MemcachedClient memcachedClient = mock(MemcachedClient.class);
        MemcachedCache memcachedCache = new MemcachedCache(memcachedClient, cacheConfig,
                CommonQueryCacheSupporter.Type.SUCCESS_QUERY_CACHE.rootCacheName, 7 * 24 * 3600);
        memCachedAdaptor = new MemCachedCacheAdaptor(memcachedCache);

        //Mock put to cache
        for (String key : keyValueMap.keySet()) {
            String hashedKey = memcachedCache.computeKeyHash(key);

            String value = keyValueMap.get(key);
            byte[] valueE = memcachedCache.encodeValue(key, value);

            GetFuture<Object> future = mock(GetFuture.class);
            when(future.get(cacheConfig.getTimeout(), TimeUnit.MILLISECONDS)).thenReturn(valueE);
            when(memcachedClient.asyncGet(hashedKey)).thenReturn(future);
        }
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGet() {
        for (String key : keyValueMap.keySet()) {
            Assert.assertEquals("The value should not change", keyValueMap.get(key), memCachedAdaptor.get(key).get());
        }
    }

    @Test
    public void testGetResolvedAddrList() {
        String hostsStr = "localhost:11211,unresolvedHost1:11211,unresolvedHost2:11211";
        List<InetSocketAddress> addrList = MemcachedCache.getResolvedAddrList(hostsStr);
        Assert.assertEquals(1, addrList.size());
    }

    @Test
    public void testMemcachedConfig() {
        cacheConfig.setTimeout(1000);
        cacheConfig.setHosts("localhost:11211");
        cacheConfig.setMaxChunkSize(1024);
        cacheConfig.setMaxOperationQueueSize(1);
        cacheConfig.setReadBufferSize(DefaultConnectionFactory.DEFAULT_READ_BUFFER_SIZE);
        cacheConfig.setEnableCompression(false);
        Assert.assertEquals(1000, cacheConfig.getTimeout());
        Assert.assertEquals("localhost:11211", cacheConfig.getHosts());
        Assert.assertEquals(1024, cacheConfig.getMaxChunkSize());
        Assert.assertEquals(1, cacheConfig.getMaxOperationQueueSize());
        Assert.assertEquals(16384, cacheConfig.getReadBufferSize());
        Assert.assertFalse(cacheConfig.isEnableCompression());
    }
}
