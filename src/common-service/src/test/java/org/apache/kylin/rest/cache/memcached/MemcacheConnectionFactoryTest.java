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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.FailureMode;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.metrics.MetricType;
import net.spy.memcached.metrics.NoopMetricCollector;
import net.spy.memcached.ops.LinkedOperationQueueFactory;
import net.spy.memcached.ops.OperationQueueFactory;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import net.spy.memcached.transcoders.SerializingTranscoder;

public class MemcacheConnectionFactoryTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        overwriteSystemProp("kylin.cache.memcached.enabled", "true");
        createTestMetadata();
    }

    @After
    public void destroy() {
        cleanupTestMetadata();
    }

    @Test
    public void testMemcachedConnectionFactory() {
        SerializingTranscoder transcoder = new SerializingTranscoder(1048576);
        // always no compression inside, we compress/decompress outside
        transcoder.setCompressionThreshold(Integer.MAX_VALUE);
        OperationQueueFactory opQueueFactory;
        opQueueFactory = new LinkedOperationQueueFactory();

        MemcachedConnectionFactoryBuilder builder = new MemcachedConnectionFactoryBuilder();

        ConnectionFactory connectionFactory = builder.setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                .setHashAlg(DefaultHashAlgorithm.FNV1A_64_HASH)
                .setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT).setDaemon(true)
                .setFailureMode(FailureMode.Redistribute).setTranscoder(transcoder).setShouldOptimize(true)
                .setOpQueueMaxBlockTime(500).setOpTimeout(500)
                .setReadBufferSize(DefaultConnectionFactory.DEFAULT_READ_BUFFER_SIZE).setOpQueueFactory(opQueueFactory)
                .build();

        MemcachedConnectionFactory factory = new MemcachedConnectionFactory(connectionFactory);

        factory.createOperationQueue();
        factory.createReadOperationQueue();
        factory.createWriteOperationQueue();
        Assert.assertEquals(transcoder, factory.getDefaultTranscoder());
        Assert.assertEquals(FailureMode.Redistribute, factory.getFailureMode());
        Assert.assertEquals(DefaultHashAlgorithm.FNV1A_64_HASH, factory.getHashAlg());
        Assert.assertNotNull(factory.getOperationFactory());
        Assert.assertNotNull(factory.getMetricCollector());
        Assert.assertNotNull(factory.getListenerExecutorService());
        Assert.assertNotNull(factory.getInitialObservers());
        Assert.assertEquals(500, factory.getOperationTimeout());
        Assert.assertEquals(16384, factory.getReadBufSize());
        Assert.assertEquals(500, factory.getOpQueueMaxBlockTime());
        Assert.assertEquals(MetricType.OFF, factory.enableMetrics());
        Assert.assertTrue(factory.isDefaultExecutorService());
        Assert.assertTrue(factory.shouldOptimize());
        Assert.assertFalse(factory.useNagleAlgorithm());
        Assert.assertEquals(30, factory.getMaxReconnectDelay());
        Assert.assertEquals(1000, factory.getAuthWaitTime());
        Assert.assertEquals(500, factory.getOperationTimeout());
    }

    @Test
    public void testMemcachedConnectionConfig() {
        SerializingTranscoder transcoder = new SerializingTranscoder(1048576);
        // always no compression inside, we compress/decompress outside
        transcoder.setCompressionThreshold(Integer.MAX_VALUE);
        OperationQueueFactory opQueueFactory;
        opQueueFactory = new LinkedOperationQueueFactory();
        OperationFactory operationFactory = new BinaryOperationFactory();
        ExecutorService executorService;
        executorService = Executors.newSingleThreadExecutor();

        MemcachedConnectionFactoryBuilder builder = new MemcachedConnectionFactoryBuilder();

        ConnectionFactory connectionFactory = builder.setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                .setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT).setDaemon(true)
                .setFailureMode(FailureMode.Redistribute).setTranscoder(transcoder).setShouldOptimize(true)
                .setListenerExecutorService(executorService).setOpFact(operationFactory)
                .setEnableMetrics(MetricType.OFF).setMetricCollector(new NoopMetricCollector())
                .setOpQueueFactory(opQueueFactory).build();

        MemcachedConnectionFactory factory = new MemcachedConnectionFactory(connectionFactory);

        factory.createOperationQueue();
        factory.createReadOperationQueue();
        factory.createWriteOperationQueue();
        Assert.assertEquals(transcoder, factory.getDefaultTranscoder());
        Assert.assertEquals(FailureMode.Redistribute, factory.getFailureMode());
        Assert.assertEquals(DefaultHashAlgorithm.NATIVE_HASH, factory.getHashAlg());
        Assert.assertNotNull(factory.getOperationFactory());
        Assert.assertNotNull(factory.getMetricCollector());
        Assert.assertNotNull(factory.getListenerExecutorService());
        Assert.assertNotNull(factory.getInitialObservers());
        Assert.assertEquals(2500, factory.getOperationTimeout());
        Assert.assertEquals(16384, factory.getReadBufSize());
        Assert.assertEquals(10000, factory.getOpQueueMaxBlockTime());
        Assert.assertEquals(MetricType.OFF, factory.enableMetrics());
        Assert.assertFalse(factory.isDefaultExecutorService());
        Assert.assertTrue(factory.shouldOptimize());
        Assert.assertFalse(factory.useNagleAlgorithm());
        Assert.assertEquals(30, factory.getMaxReconnectDelay());
        Assert.assertEquals(1000, factory.getAuthWaitTime());
        Assert.assertEquals(2500, factory.getOperationTimeout());
    }
}
