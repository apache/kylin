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

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.springframework.security.util.FieldUtils;

import net.sf.ehcache.CacheManager;

@FixMethodOrder()
public class KylinEhCacheTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void cleanup() {
        cleanupTestMetadata();
    }

    @Test
    public void testErrorCacheConfig() throws IllegalAccessException {
        String url = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        overwriteSystemProp("kylin.cache.config", "file://" + url + "not-exists-ehcache.xml");
        KylinCache cache = KylinEhCache.getInstance();
        CacheManager cacheManager = (CacheManager) FieldUtils.getFieldValue(cache, "cacheManager");
        Assert.assertEquals("DEFAULT_CACHE", cacheManager.getName());
        cacheManager.shutdown();
    }

    @Test
    public void testRightCacheConfig() throws IllegalAccessException {
        String url = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String location = "file://" + url + "user-defined-ehcache.xml";
        overwriteSystemProp("kylin.cache.config", location);
        KylinCache cache = KylinEhCache.getInstance();
        CacheManager cacheManager = (CacheManager) FieldUtils.getFieldValue(cache, "cacheManager");
        Assert.assertEquals("UserDefinedCache", cacheManager.getName());
        cacheManager.shutdown();
    }

}