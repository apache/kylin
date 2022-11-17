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
package org.apache.kylin.cache.redis;

import org.apache.kylin.cache.cachemanager.CacheConstants;
import org.apache.kylin.cache.cachemanager.RedisManager;
import org.apache.kylin.cache.redis.jedis.JedisPoolClient;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.shaded.com.google.common.base.Charsets;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import java.util.Map;
import static org.mockito.Mockito.when;

public class JedisPoolClientTest extends LocalFileMetadataTestCase {

    private Map<String, String> keyValueMap;

    private RedisManager.RedisCacheAdaptor redisCacheAdaptor;

    @Before
    public void setUp() {
        this.createTestMetadata();

        keyValueMap = Maps.newHashMap();
        keyValueMap.put("sql1", "value1");
        keyValueMap.put("sql11", "value11");

        RedisConfig redisConfig = new RedisConfig();
        JedisPoolClient jedisPoolClient = new JedisPoolClient(redisConfig);
        JedisPoolClient clientSpy = Mockito.spy(jedisPoolClient);
        redisCacheAdaptor = new RedisManager.RedisCacheAdaptor(clientSpy, CacheConstants.QUERY_CACHE);

        for (String key : keyValueMap.keySet()) {
            String keyS = jedisPoolClient.serializeKey(key);
            String hashedKey = jedisPoolClient.computeKeyHash(keyS);
            String value = keyValueMap.get(key);
            byte[] valueE = jedisPoolClient.encodeValue(keyS.getBytes(Charsets.UTF_8), jedisPoolClient.serializeValue(value));
            when(clientSpy.internalGet(hashedKey)).thenReturn(valueE);
        }

    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGet() {
        for (String key : keyValueMap.keySet()) {
            Assert.assertEquals("The value should not change", keyValueMap.get(key), redisCacheAdaptor.get(key).get());
        }
    }

}
