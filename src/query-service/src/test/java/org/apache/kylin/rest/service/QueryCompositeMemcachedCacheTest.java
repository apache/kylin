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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.rest.cache.KylinCache;
import org.apache.kylin.rest.cache.memcached.CacheStats;
import org.apache.kylin.rest.cache.memcached.CompositeMemcachedCache;
import org.apache.kylin.rest.cache.memcached.MemcachedCache;
import org.apache.kylin.rest.cache.memcached.MemcachedCacheConfig;
import org.apache.kylin.rest.cache.memcached.MemcachedConnectionFactory;
import org.apache.kylin.rest.cache.memcached.MemcachedConnectionFactoryBuilder;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import com.google.code.ssm.jmemcached.plugin.AbstractJmemcachedMojo;
import com.google.code.ssm.jmemcached.plugin.JmemcachedStartMojo;
import com.google.code.ssm.jmemcached.plugin.Server;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;
import net.spy.memcached.ops.LinkedOperationQueueFactory;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationQueueFactory;
import net.spy.memcached.transcoders.SerializingTranscoder;

@Ignore
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

    private static String PROJECT = "test_project";
    private static String TYPE = "test_type";
    private static String CACHE_KEY = "test_key";
    private static String CACHE_VAL = "test_val";

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
    public void testMemcachedConnection() throws IOException {
        SerializingTranscoder transcoder = new SerializingTranscoder(1048576);
        transcoder.setCompressionThreshold(Integer.MAX_VALUE);
        OperationQueueFactory opQueueFactory;
        opQueueFactory = new LinkedOperationQueueFactory();

        MemcachedCacheConfig config = new MemcachedCacheConfig();

        String memcachedPrefix = "testCli";
        String hostStr = "localhost:11211";
        String cacheKey = "test_cache_key";
        String cacheVal = "test_cache_val";
        int timeToLive = 7 * 24 * 3600;
        MemcachedConnectionFactoryBuilder builder = new MemcachedConnectionFactoryBuilder();

        ConnectionFactory connectionFactory = builder.setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                .setHashAlg(DefaultHashAlgorithm.KETAMA_HASH)
                .setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT).setDaemon(true)
                .setFailureMode(FailureMode.Redistribute).setTranscoder(transcoder).setShouldOptimize(true)
                .setOpQueueMaxBlockTime(config.getTimeout()).setOpTimeout(config.getTimeout())
                .setReadBufferSize(config.getReadBufferSize()).setOpQueueFactory(opQueueFactory).build();

        MemcachedConnectionFactory factory = new MemcachedConnectionFactory(connectionFactory);
        MemcachedClient memcachedClient = new MemcachedClient(factory, MemcachedCache.getResolvedAddrList(hostStr));
        MemcachedCache cache = new MemcachedCache(memcachedClient, config, memcachedPrefix, timeToLive);
        cache.put(cacheKey, cacheVal);
        cache.put("", cacheVal);
        cache.put(null, cacheVal);
        cache.evict(cacheKey);
        Assert.assertEquals(0, cache.get("").length);
        Assert.assertEquals(0, cache.get(null).length);
        Assert.assertEquals(0, cache.get(cacheKey).length);
        cache.clear();
    }

    @Test
    public void testMemcachedClient() throws IOException {
        SerializingTranscoder transcoder = new SerializingTranscoder(1048576);
        transcoder.setCompressionThreshold(Integer.MAX_VALUE);
        OperationQueueFactory opQueueFactory;
        opQueueFactory = new LinkedOperationQueueFactory();

        MemcachedCacheConfig config = new MemcachedCacheConfig();

        String hostStr = "localhost:11211";
        MemcachedConnectionFactoryBuilder builder = new MemcachedConnectionFactoryBuilder();

        ConnectionFactory connectionFactory = builder.setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                .setHashAlg(DefaultHashAlgorithm.FNV1A_64_HASH)
                .setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT).setDaemon(true)
                .setFailureMode(FailureMode.Redistribute).setTranscoder(transcoder).setShouldOptimize(true)
                .setOpQueueMaxBlockTime(config.getTimeout()).setOpTimeout(config.getTimeout())
                .setReadBufferSize(config.getReadBufferSize()).setOpQueueFactory(opQueueFactory).build();

        MemcachedConnectionFactory factory = new MemcachedConnectionFactory(connectionFactory);
        MemcachedClient memcachedClient = new MemcachedClient(factory, MemcachedCache.getResolvedAddrList(hostStr));
        List<MemcachedNode> nodeList = new ArrayList<>(memcachedClient.getNodeLocator().getReadonlyCopy().getAll());
        NodeLocator nodeLocator = memcachedClient.getNodeLocator();
        memcachedClient.getNodeLocator().updateLocator(nodeList);
        Assert.assertEquals(500, memcachedClient.getOperationTimeout());
        Assert.assertNotEquals(nodeLocator, memcachedClient.getNodeLocator());
    }

    @Test
    public void testMemcachedNode() throws IOException {
        SerializingTranscoder transcoder = new SerializingTranscoder(1048576);
        transcoder.setCompressionThreshold(Integer.MAX_VALUE);
        OperationQueueFactory opQueueFactory;
        opQueueFactory = new LinkedOperationQueueFactory();

        MemcachedCacheConfig config = new MemcachedCacheConfig();

        String hostStr = "localhost:11211";
        MemcachedConnectionFactoryBuilder builder = new MemcachedConnectionFactoryBuilder();

        ConnectionFactory connectionFactory = builder.setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                .setHashAlg(DefaultHashAlgorithm.FNV1A_64_HASH)
                .setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT).setDaemon(true)
                .setFailureMode(FailureMode.Redistribute).setTranscoder(transcoder).setShouldOptimize(true)
                .setOpQueueMaxBlockTime(config.getTimeout()).setOpTimeout(config.getTimeout())
                .setReadBufferSize(config.getReadBufferSize()).setOpQueueFactory(opQueueFactory).build();

        MemcachedConnectionFactory factory = new MemcachedConnectionFactory(connectionFactory);
        MemcachedClient memcachedClient = new MemcachedClient(factory, MemcachedCache.getResolvedAddrList(hostStr));
        List<MemcachedNode> nodeList = new ArrayList<>(memcachedClient.getNodeLocator().getReadonlyCopy().getAll());
        nodeList.forEach(node -> {
            Assert.assertThrows(UnsupportedOperationException.class, node::setupResend);
            Assert.assertThrows(UnsupportedOperationException.class, node::setupForAuth);
            Assert.assertThrows(UnsupportedOperationException.class, node::destroyInputQueue);
            Assert.assertThrows(UnsupportedOperationException.class, node::reconnecting);
            Operation op = Mockito.spy(Operation.class);
            Assert.assertThrows(UnsupportedOperationException.class, () -> node.insertOp(op));
            Assert.assertThrows(UnsupportedOperationException.class, () -> node.addOp(op));
        });
    }

    @Test()
    public void testTypeAsNull() {
        CompositeMemcachedCache compositeMemcachedCache = (CompositeMemcachedCache) CompositeMemcachedCache
                .getInstance();
        Assert.assertNotNull(compositeMemcachedCache);
        Assert.assertThrows(NullPointerException.class,
                () -> compositeMemcachedCache.put(null, PROJECT, CACHE_KEY, CACHE_VAL));
    }

    @Test()
    public void testUnsupportedCacheType() {
        CompositeMemcachedCache compositeMemcachedCache = (CompositeMemcachedCache) CompositeMemcachedCache
                .getInstance();
        Assert.assertNotNull(compositeMemcachedCache);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> compositeMemcachedCache.put(TYPE, PROJECT, CACHE_KEY, CACHE_VAL));
    }

    @Test
    public void testCompositeMemcachedCache() {
        CompositeMemcachedCache compositeMemcachedCache = (CompositeMemcachedCache) CompositeMemcachedCache
                .getInstance();
        Assert.assertNotNull(compositeMemcachedCache);
        Object mockItem = mock(Object.class);
        when(mockItem.toString()).thenReturn(mockItem.getClass().getName());
        // write bad case
        String type = CommonQueryCacheSupporter.Type.SUCCESS_QUERY_CACHE.rootCacheName;
        compositeMemcachedCache.put(type, PROJECT, mockItem, CACHE_VAL);
        compositeMemcachedCache.update(type, PROJECT, mockItem, CACHE_VAL);
        Object result1 = compositeMemcachedCache.get(type, PROJECT, mockItem);
        Assert.assertNull(result1);
        Assert.assertFalse(compositeMemcachedCache.remove(type, PROJECT, mockItem));

        compositeMemcachedCache.put(type, PROJECT, CACHE_KEY, CACHE_VAL);
        Object result2 = compositeMemcachedCache.get(type, PROJECT, CACHE_KEY);
        Assert.assertEquals("test_val", (String) result2);

        compositeMemcachedCache.update(type, PROJECT, CACHE_KEY, "update_val");
        Object result3 = compositeMemcachedCache.get(type, PROJECT, CACHE_KEY);
        Assert.assertEquals("update_val", (String) result3);

        compositeMemcachedCache.clearAll();
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
        testCacheStatus();
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
        queryCacheManager.clearProjectCache(null);

        queryCacheManager.recoverCache();
    }

    private void testCacheStatus() {
        CacheStats cacheStats = ((CompositeMemcachedCache) queryCacheManager.getCache()).getCacheStats("StorageCache");
        String name = ((CompositeMemcachedCache) queryCacheManager.getCache()).getName("StorageCache");
        System.out.println("Cache name is: " + name);
        System.out.println("AvgGetTime is: " + cacheStats.getAvgGetTime());
        System.out.println("HitRate is: " + cacheStats.hitRate());
        System.out.println("AvgGetBytes is :" + cacheStats.avgGetBytes());
        System.out.println("NumErrors is :" + cacheStats.getNumErrors());
        System.out.println("Get number is :" + cacheStats.getNumGet());
        System.out.println("NumEvictions is :" + cacheStats.getNumEvictions());
        System.out.println("NumGetBytes is :" + cacheStats.getNumGetBytes());
        System.out.println("Hit number is :" + cacheStats.getNumHits());
        System.out.println("Miss number is :" + cacheStats.getNumMisses());
        System.out.println("Put number is :" + cacheStats.getNumPut());
        System.out.println("Put bytes is :" + cacheStats.getNumPutBytes());
        System.out.println("Timeout number is :" + cacheStats.getNumTimeouts());
        System.out.println("Lookup number is :" + cacheStats.numLookups());

        Assert.assertEquals(0, cacheStats.getNumErrors());
        Assert.assertEquals(0, cacheStats.getNumEvictions());
        Assert.assertEquals(0, cacheStats.getNumTimeouts());
    }
}
