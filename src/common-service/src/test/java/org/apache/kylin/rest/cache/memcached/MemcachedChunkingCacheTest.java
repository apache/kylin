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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.base.Charsets;
import org.apache.kylin.guava30.shaded.common.base.Strings;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.rest.cache.memcached.CompositeMemcachedCache.MemCachedCacheAdaptor;
import org.apache.kylin.rest.service.CommonQueryCacheSupporter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.GetFuture;

public class MemcachedChunkingCacheTest extends NLocalFileMetadataTestCase {

    private Map<String, String> smallValueMap;
    private Map<String, String> largeValueMap;
    private MemCachedCacheAdaptor memCachedAdaptor;
    private KeyHookLookup.KeyHook keyHook;
    private MemcachedChunkingCache memcachedChunkingCache;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        final int maxObjectSize = 300;

        smallValueMap = Maps.newHashMap();
        smallValueMap.put("sql1", "value1");

        largeValueMap = Maps.newHashMap();
        largeValueMap.put("sql2", Strings.repeat("value2", maxObjectSize));

        MemcachedCacheConfig cacheConfig = new MemcachedCacheConfig();
        cacheConfig.setMaxObjectSize(maxObjectSize);
        MemcachedClient memcachedClient = mock(MemcachedClient.class);
        MemcachedCache memcachedCache = new MemcachedCache(memcachedClient, cacheConfig,
                CommonQueryCacheSupporter.Type.SUCCESS_QUERY_CACHE.rootCacheName, 7 * 24 * 3600);
        memcachedChunkingCache = new MemcachedChunkingCache(memcachedCache);
        memCachedAdaptor = new MemCachedCacheAdaptor(memcachedChunkingCache);

        //Mock put to cache
        for (String key : smallValueMap.keySet()) {
            String hashedKey = memcachedCache.computeKeyHash(key);

            String value = smallValueMap.get(key);
            byte[] valueB = memcachedCache.serializeValue(value);
            KeyHookLookup.KeyHook keyHook = new KeyHookLookup.KeyHook(null, valueB);
            byte[] valueE = memcachedCache.encodeValue(key, keyHook);

            GetFuture<Object> future = mock(GetFuture.class);
            when(memcachedClient.asyncGet(hashedKey)).thenReturn(future);

            when(future.get(cacheConfig.getTimeout(), TimeUnit.MILLISECONDS)).thenReturn(valueE);
        }

        //Mock put large value to cache
        for (String key : largeValueMap.keySet()) {
            String hashedKey = memcachedCache.computeKeyHash(key);

            String value = largeValueMap.get(key);
            byte[] valueB = memcachedCache.serializeValue(value);
            int nSplit = MemcachedChunkingCache.getValueSplit(cacheConfig, key, valueB.length);
            Pair<KeyHookLookup.KeyHook, byte[][]> keyValuePair = MemcachedChunkingCache.getKeyValuePair(nSplit, key,
                    valueB);
            KeyHookLookup.KeyHook keyHook = keyValuePair.getFirst();
            this.keyHook = keyHook;
            byte[][] splitValueB = keyValuePair.getSecond();

            //For key
            byte[] valueE = memcachedCache.encodeValue(key, keyHook);
            GetFuture<Object> future = mock(GetFuture.class);
            when(memcachedClient.asyncGet(hashedKey)).thenReturn(future);
            when(future.get(cacheConfig.getTimeout(), TimeUnit.MILLISECONDS)).thenReturn(valueE);

            //For splits
            Map<String, String> keyLookup = memcachedChunkingCache
                    .computeKeyHash(Arrays.asList(keyHook.getChunkskey()));
            Map<String, Object> bulkResult = Maps.newHashMap();
            for (int i = 0; i < nSplit; i++) {
                String splitKeyS = keyHook.getChunkskey()[i];
                bulkResult.put(memcachedCache.computeKeyHash(splitKeyS),
                        memcachedCache.encodeValue(splitKeyS.getBytes(Charsets.UTF_8), splitValueB[i]));
            }

            BulkFuture<Map<String, Object>> bulkFuture = mock(BulkFuture.class);
            when(memcachedClient.asyncGetBulk(keyLookup.keySet())).thenReturn(bulkFuture);
            when(bulkFuture.get(cacheConfig.getTimeout(), TimeUnit.MILLISECONDS)).thenReturn(bulkResult);
        }
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGet() {
        for (String key : smallValueMap.keySet()) {
            Assert.assertEquals("The value should not change", smallValueMap.get(key), memCachedAdaptor.get(key).get());
        }
        for (String key : largeValueMap.keySet()) {
            Assert.assertEquals("The value should not change", largeValueMap.get(key), memCachedAdaptor.get(key).get());
        }
    }

    @Test
    public void testEvict() {
        memcachedChunkingCache.evict("");
        memcachedChunkingCache.evict(null);
        memcachedChunkingCache.put("first", "first_value");
        keyHook.setChunkskey(new String[] { "first", "second" });
        for (String key : keyHook.getChunkskey()) {
            memcachedChunkingCache.evict(key);
        }
        byte[] value = memcachedChunkingCache.get("first");
        Assert.assertEquals(0, value.length);
    }

    @Test
    public void testSplitBytes() {
        byte[] data = new byte[8];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        int nSplit;
        byte[][] dataSplits;

        nSplit = 2;
        dataSplits = MemcachedChunkingCache.splitBytes(data, nSplit);
        Assert.assertEquals(nSplit, dataSplits.length);
        Assert.assertArrayEquals(dataSplits[0], new byte[] { 0, 1, 2, 3 });
        Assert.assertArrayEquals(dataSplits[1], new byte[] { 4, 5, 6, 7 });

        nSplit = 3;
        dataSplits = MemcachedChunkingCache.splitBytes(data, nSplit);
        Assert.assertEquals(nSplit, dataSplits.length);
        Assert.assertArrayEquals(dataSplits[0], new byte[] { 0, 1, 2 });
        Assert.assertArrayEquals(dataSplits[1], new byte[] { 3, 4, 5 });
        Assert.assertArrayEquals(dataSplits[2], new byte[] { 6, 7 });
    }

    @Test
    public void testKeyHookFunc() {
        KeyHookLookup.KeyHook keyHookEq = new KeyHookLookup.KeyHook();
        keyHookEq.setChunkskey(keyHook.getChunkskey());
        keyHookEq.setValues(keyHook.getValues());

        Assert.assertEquals(keyHook, keyHookEq);
        Assert.assertEquals(keyHook.hashCode(), keyHookEq.hashCode());
        Assert.assertTrue(keyHook.equals(keyHookEq)
                && (keyHook.toString().equals(keyHookEq.toString()) && keyHook.equals(keyHook)));

        keyHookEq.setChunkskey(null);
        keyHookEq.setValues(null);

        Assert.assertNotEquals(keyHook, keyHookEq);
        Assert.assertFalse(
                keyHook.equals(keyHookEq) || keyHook.toString().equals(keyHookEq.toString()) || keyHook.equals(null));
    }
}
