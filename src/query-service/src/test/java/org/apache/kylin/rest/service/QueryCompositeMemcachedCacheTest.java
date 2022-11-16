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

import com.google.code.ssm.jmemcached.plugin.AbstractJmemcachedMojo;
import com.google.code.ssm.jmemcached.plugin.JmemcachedStartMojo;
import com.google.code.ssm.jmemcached.plugin.Server;
import org.apache.kylin.rest.cache.KylinCache;
import org.apache.kylin.rest.cache.memcached.CompositeMemcachedCache;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

public class QueryCompositeMemcachedCacheTest extends LocalFileMetadataTestCase {

    static {
        Server server = new Server();
        server.setPort(11211);
        server.setBinary(true);
        AbstractJmemcachedMojo embeddedMemcachedServerStarter = new JmemcachedStartMojo();
        List<Server> serverList = new ArrayList<>();
        serverList.add(server);
        embeddedMemcachedServerStarter.setServers(serverList);
        try {
            embeddedMemcachedServerStarter.execute();
        } catch (MojoExecutionException e) {
            e.printStackTrace();
        } catch (MojoFailureException e) {
            e.printStackTrace();
        }
    }

    @Spy
    private KylinCache memcachedCache = Mockito.spy(CompositeMemcachedCache.getInstance());

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
        overwriteSystemProp("kylin.cache.memcached.enabled", "true");
        createTestMetadata();
        MockitoAnnotations.initMocks(this);
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void testProjectCompositeMemcachedCacheQuery() {
        overwriteSystemProp("kylin.cache.memcached.enabled", "true");
        final String project = "default";
        final SQLRequest req1 = new SQLRequest();
        req1.setProject(project);
        req1.setSql("select a from b");
        final SQLResponse resp1 = new SQLResponse();
        List<List<String>> results = new ArrayList<>();
        resp1.setResults(results);
        resp1.setResultRowCount(1);
        //Single Node mode
        testHelper(req1, resp1, project);
        //TODO: Cluster mode
    }

    private void testHelper(SQLRequest req1, SQLResponse resp1, String project) {
        queryCacheManager.cacheSuccessQuery(req1, resp1);

        queryCacheManager.doCacheSuccessQuery(req1, resp1);
        Assert.assertEquals(resp1.getResultRowCount(),
                queryCacheManager.doSearchQuery(QueryCacheManager.Type.SUCCESS_QUERY_CACHE, req1).getResultRowCount());
        Assert.assertNull(queryCacheManager.searchQuery(req1));
        queryCacheManager.clearQueryCache(req1);
        Assert.assertNull(queryCacheManager.doSearchQuery(QueryCacheManager.Type.SUCCESS_QUERY_CACHE, req1));

        queryCacheManager.cacheFailedQuery(req1, resp1);
        Assert.assertEquals(resp1.getResultRowCount(), queryCacheManager.searchQuery(req1).getResultRowCount());
        queryCacheManager.clearProjectCache(project);
        Assert.assertNull(queryCacheManager.searchQuery(req1));

        queryCacheManager.recoverCache();
    }
}
