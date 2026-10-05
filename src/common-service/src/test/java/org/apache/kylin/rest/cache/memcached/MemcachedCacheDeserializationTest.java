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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.cache.memcached.CompositeMemcachedCache.MemCachedCacheAdaptor;
import org.apache.kylin.rest.service.CommonQueryCacheSupporter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.spy.memcached.CachedData;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.GetFuture;

public class MemcachedCacheDeserializationTest extends NLocalFileMetadataTestCase {

    private static final int MAX_OBJECT_SIZE = 1024 * 1024;

    private MemcachedClient memcachedClient;
    private MemcachedCache memcachedCache;
    private MemCachedCacheAdaptor adaptor;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        MemcachedCacheConfig cacheConfig = new MemcachedCacheConfig();
        cacheConfig.setMaxObjectSize(MAX_OBJECT_SIZE);
        memcachedClient = mock(MemcachedClient.class);
        memcachedCache = new MemcachedCache(memcachedClient, cacheConfig,
                CommonQueryCacheSupporter.Type.SUCCESS_QUERY_CACHE.rootCacheName, 7 * 24 * 3600);
        adaptor = new MemCachedCacheAdaptor(new MemcachedChunkingCache(memcachedCache));
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    private void mockCachedValue(String keyS, byte[] encodedValue) throws Exception {
        GetFuture<Object> future = mock(GetFuture.class);
        when(memcachedClient.asyncGet(memcachedCache.computeKeyHash(keyS))).thenReturn(future);
        when(future.get(anyLong(), any(TimeUnit.class))).thenReturn(encodedValue);
    }

    @Test
    public void testAllowedValueRoundTrip() throws Exception {
        String keyS = "allowed-key";
        byte[] valueBytes = SerializationUtils.serialize("cached-value");
        KeyHookLookup.KeyHook keyHook = new KeyHookLookup.KeyHook(null, valueBytes);
        mockCachedValue(keyS, memcachedCache.encodeValue(keyS, keyHook));

        Assert.assertEquals("cached-value", adaptor.get(keyS).get());
    }

    @Test
    public void testRejectDisallowedValueClass() throws Exception {
        String keyS = "poisoned-value";
        byte[] valueBytes = SerializationUtils.serialize(new File("/tmp/kylin-gadget"));
        KeyHookLookup.KeyHook keyHook = new KeyHookLookup.KeyHook(null, valueBytes);
        mockCachedValue(keyS, memcachedCache.encodeValue(keyS, keyHook));

        Assert.assertThrows(IllegalStateException.class, () -> adaptor.get(keyS));
    }

    @Test
    public void testRejectDisallowedKeyHookClass() throws Exception {
        String keyS = "poisoned-keyhook";
        mockCachedValue(keyS, memcachedCache.encodeValue(keyS, new File("/tmp/kylin-gadget")));

        Assert.assertThrows(IllegalStateException.class, () -> adaptor.get(keyS));
    }

    @Test
    public void testTranscoderRejectsDisallowedSerializedPayload() {
        MemcachedCache.KylinSerializingTranscoder transcoder = new MemcachedCache.KylinSerializingTranscoder(
                MAX_OBJECT_SIZE);

        CachedData poisoned = transcoder.encode(new File("/tmp/kylin-gadget"));
        Assert.assertThrows(IllegalStateException.class, () -> transcoder.decode(poisoned));
    }
}
