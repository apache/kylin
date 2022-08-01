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

package org.apache.kylin.rest.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.rest.cache.KylinCache;
import org.apache.kylin.rest.cache.KylinEhCache;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.response.TableMetaCacheResult;
import org.apache.kylin.rest.response.TableMetaCacheResultV2;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import net.sf.ehcache.CacheManager;

@RunWith(MockitoJUnitRunner.class)
public class QueryEhCacheTest extends LocalFileMetadataTestCase {

    @Spy
    private CacheManager cacheManager = Mockito
            .spy(CacheManager.create(ClassLoader.getSystemResourceAsStream("ehcache.xml")));

    @Spy
    private KylinCache kylinEhCache = Mockito.spy(KylinEhCache.getInstance());

    @InjectMocks
    private QueryCacheManager queryCacheManager;

    @BeforeClass
    public static void setupResource() {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void tearDownResource() {
        staticCleanupTestMetadata();
    }

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void testProjectCacheQuery() {
        final String project = "default";
        final SQLRequest req1 = new SQLRequest();
        req1.setProject(project);
        req1.setSql("select a from b");
        final SQLResponse resp1 = new SQLResponse();
        List<List<String>> results = new ArrayList<>();
        resp1.setResults(results);

        queryCacheManager.cacheSuccessQuery(req1, resp1);

        queryCacheManager.doCacheSuccessQuery(req1, resp1);
        Assert.assertEquals(resp1, queryCacheManager.doSearchQuery(QueryCacheManager.Type.SUCCESS_QUERY_CACHE, req1));
        Assert.assertNull(queryCacheManager.searchQuery(req1));
        queryCacheManager.clearQueryCache(req1);
        Assert.assertNull(queryCacheManager.doSearchQuery(QueryCacheManager.Type.SUCCESS_QUERY_CACHE, req1));

        queryCacheManager.cacheFailedQuery(req1, resp1);
        Assert.assertEquals(resp1, queryCacheManager.searchQuery(req1));
        queryCacheManager.clearProjectCache(project);
        Assert.assertNull(queryCacheManager.searchQuery(req1));

        String username = "username";
        TableMetaCacheResult metaList = new TableMetaCacheResult();
        queryCacheManager.putSchemaCache(project, username, metaList);
        Assert.assertEquals(metaList, queryCacheManager.doGetSchemaCache(project, username));

        queryCacheManager.clearSchemaCache(project);
        Assert.assertNull(queryCacheManager.doGetSchemaCache(project, username));

        TableMetaCacheResultV2 metaWithTypeList = new TableMetaCacheResultV2();
        queryCacheManager.putSchemaV2Cache(project, null, username, metaWithTypeList);
        Assert.assertEquals(metaWithTypeList, queryCacheManager.doGetSchemaCacheV2(project, null, username));
    }

}
