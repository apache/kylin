package org.apache.kylin.rest.cache.memcached;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.FailureMode;
import net.spy.memcached.metrics.MetricType;
import net.spy.memcached.ops.LinkedOperationQueueFactory;
import net.spy.memcached.ops.OperationQueueFactory;
import net.spy.memcached.transcoders.SerializingTranscoder;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

public class MemcacheConnectionFactoryTest extends NLocalFileMetadataTestCase {

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
    public void testMemcachedConnectionFactory() {
        SerializingTranscoder transcoder = new SerializingTranscoder(1048576);
        // always no compression inside, we compress/decompress outside
        transcoder.setCompressionThreshold(Integer.MAX_VALUE);
        OperationQueueFactory opQueueFactory;
        opQueueFactory = new LinkedOperationQueueFactory();

        MemcachedConnectionFactoryBuilder builder = new MemcachedConnectionFactoryBuilder();

        ConnectionFactory connectionFactory = builder
                .setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                .setHashAlg(DefaultHashAlgorithm.FNV1A_64_HASH)
                .setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT).setDaemon(true)
                .setFailureMode(FailureMode.Redistribute).setTranscoder(transcoder).setShouldOptimize(true)
                .setOpQueueMaxBlockTime(500).setOpTimeout(500)
                .setReadBufferSize(DefaultConnectionFactory.DEFAULT_READ_BUFFER_SIZE).setOpQueueFactory(opQueueFactory).build();

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
        Assert.assertEquals(500, factory.getOperationTimeout());
        Assert.assertEquals(16384, factory.getReadBufSize());
        Assert.assertEquals(500, factory.getOpQueueMaxBlockTime());
        Assert.assertEquals(MetricType.OFF, factory.enableMetrics());
        Assert.assertTrue(factory.isDefaultExecutorService());
    }
}
