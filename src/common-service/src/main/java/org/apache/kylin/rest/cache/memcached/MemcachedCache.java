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

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.DataFormatException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.base.Joiner;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.base.Strings;
import org.apache.kylin.guava30.shaded.common.base.Throwables;
import org.apache.kylin.guava30.shaded.common.primitives.Ints;
import org.apache.kylin.guava30.shaded.common.primitives.Shorts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.DefaultHashAlgorithm;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedClientIF;
import net.spy.memcached.ops.ArrayOperationQueueFactory;
import net.spy.memcached.ops.LinkedOperationQueueFactory;
import net.spy.memcached.ops.OperationQueueFactory;
import net.spy.memcached.transcoders.SerializingTranscoder;

/**
 * This is a cache backend of Memcached. The implementation leverages spymemcached client to talk to the servers.
 * Memcached itself has a limitation to the size of the key.
 * So the real key for cache lookup is hashed from the orginal key.
 * The implementation provides a way for hash collsion detection.
 * It can also compress/decompress the value bytes based on the preconfigured
 * compression threshold to save network bandwidth and storage space.
 */
public class MemcachedCache {
    public static final int MAX_PREFIX_LENGTH = MemcachedClientIF.MAX_KEY_LENGTH - 40 // length of namespace hash
            - 40 // length of key hash
            - 2; // length of separators
    private static final Logger logger = LoggerFactory.getLogger(MemcachedCache.class);
    private static final int DEFAULT_TTL = 7 * 24 * 3600;

    private static final String UNABLE_TO_QUEUE_CACHE_OPERATION = "Unable to queue cache operation.";

    protected final MemcachedCacheConfig config;
    protected final MemcachedClientIF client;
    protected final String memcachedPrefix;
    protected final int compressThreshold;
    protected final AtomicLong hitCount = new AtomicLong(0);
    protected final AtomicLong missCount = new AtomicLong(0);
    protected final AtomicLong readBytes = new AtomicLong(0);
    protected final AtomicLong timeoutCount = new AtomicLong(0);
    protected final AtomicLong errorCount = new AtomicLong(0);
    protected final AtomicLong putCount = new AtomicLong(0);
    protected final AtomicLong putBytes = new AtomicLong(0);
    private final int timeToLiveSeconds;
    protected AtomicLong cacheGetTime = new AtomicLong(0);

    public MemcachedCache(final MemcachedClientIF client, final MemcachedCacheConfig config,
            final String memcachedPrefix, int timeToLiveSeconds) {
        Preconditions.checkArgument(memcachedPrefix.length() <= MAX_PREFIX_LENGTH,
                "memcachedPrefix length [%d] exceeds maximum length [%d]", memcachedPrefix.length(), MAX_PREFIX_LENGTH);
        this.memcachedPrefix = memcachedPrefix;
        this.client = client;
        this.config = config;
        this.compressThreshold = config.getMaxObjectSize() / 2;
        this.timeToLiveSeconds = timeToLiveSeconds;
    }

    public MemcachedCache(MemcachedCache cache) {
        this(cache.client, cache.config, cache.memcachedPrefix, cache.timeToLiveSeconds);
    }

    /**
     * Create and return the MemcachedCache. Each time call this method will create a new instance.
     * @param config            The MemcachedCache configuration to control the cache behavior.
     * @return
     */
    public static MemcachedCache create(final MemcachedCacheConfig config, String memcachedPrefix) {
        return create(config, memcachedPrefix, DEFAULT_TTL);
    }

    public static MemcachedCache create(final MemcachedCacheConfig config, String memcachedPrefix, int timeToLive) {
        try {
            SerializingTranscoder transcoder = new SerializingTranscoder(config.getMaxObjectSize());
            // always no compression inside, we compress/decompress outside
            transcoder.setCompressionThreshold(Integer.MAX_VALUE);

            OperationQueueFactory opQueueFactory;
            int maxQueueSize = config.getMaxOperationQueueSize();
            if (maxQueueSize > 0) {
                opQueueFactory = new ArrayOperationQueueFactory(maxQueueSize);
            } else {
                opQueueFactory = new LinkedOperationQueueFactory();
            }
            String hostsStr = config.getHosts();
            ConnectionFactory connectionFactory = new MemcachedConnectionFactoryBuilder()
                    .setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                    .setHashAlg(DefaultHashAlgorithm.FNV1A_64_HASH)
                    .setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT).setDaemon(true)
                    .setFailureMode(FailureMode.Redistribute).setTranscoder(transcoder).setShouldOptimize(true)
                    .setOpQueueMaxBlockTime(config.getTimeout()).setOpTimeout(config.getTimeout())
                    .setReadBufferSize(config.getReadBufferSize()).setOpQueueFactory(opQueueFactory).build();
            return new MemcachedCache(new MemcachedClient(new MemcachedConnectionFactory(connectionFactory),
                    getResolvedAddrList(hostsStr)), config, memcachedPrefix, timeToLive);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static List<InetSocketAddress> getResolvedAddrList(String hostsStr) {
        List<InetSocketAddress> addrs = AddrUtil.getAddresses(hostsStr);
        Iterator<InetSocketAddress> addrIterator = addrs.iterator();
        while (addrIterator.hasNext()) {
            if (addrIterator.next().isUnresolved()) {
                addrIterator.remove();
            }
        }
        return addrs;
    }

    public String getName() {
        return memcachedPrefix;
    }

    public Object getNativeCache() {
        return client;
    }

    protected byte[] serializeValue(Object value) {
        return SerializationUtils.serialize((Serializable) value);
    }

    @VisibleForTesting
    byte[] encodeValue(String keyS, Object value) {
        if (keyS == null) {
            return new byte[0];
        }
        return encodeValue(keyS.getBytes(StandardCharsets.UTF_8), serializeValue(value));
    }

    /**
     * This method is used to get value object based on key from the Cache. The key should have been convert into
     * json string in previous procession. it calls getBinary() method to compute hashed key from the original key
     * string, and use this as the real key to do lookup from internal Cache. Then decodes the real values bytes
     * from the cache lookup result and leverages object serializer to convert value bytes to object.
     */
    public byte[] get(Object key) {
        return get((String) key);
    }

    /**
     * @param keyS should be the serialized string
     */
    public byte[] get(String keyS) {
        return getBinary(keyS);
    }

    /**
     * This method is used to put key/value objects to the Cache. The key should have been convert into
     * json string in previous procession. it will call putBinary() method to compute hashed key from
     * the original key string and encode the original key bytes into value bytes for hash conflicts detection.
     */
    public void put(Object key, Object value) {
        put((String) key, value);
    }

    /**
     * @param keyS should be the serialized string
     */
    public void put(String keyS, Object value) {
        if (keyS != null) {
            putBinary(keyS, serializeValue(value), timeToLiveSeconds);
        }
    }

    public void evict(Object key) {
        if (key == null)
            return;
        evict((String) key);
    }

    /**
     * @param keyS should be the serialized string
     */
    public void evict(String keyS) {
        if (keyS == null)
            return;
        client.delete(computeKeyHash(keyS));
    }

    //currently memcached not support fuzzy matching. this method will clear remote cache of all project.
    public void clearByType(String pattern) {
        logger.debug("clear by pattern: {} will caused clear all method here", pattern);
        this.clear();
    }

    public void clear() {
        logger.warn("Clear Remote Cache!");
        Future<Boolean> resultFuture = client.flush();
        try {
            boolean result = resultFuture.get();
            logger.warn("Clear Remote Cache returned with result: {}", result);
        } catch (ExecutionException | InterruptedException e) {
            logger.warn("Can't clear Remote Cache.", e);
            // Restore interrupted state...
            Thread.currentThread().interrupt();
        }
    }

    public CacheStats getStats() {
        return new CacheStats(readBytes.get(), cacheGetTime.get(), putBytes.get(), new CacheStats.CacheStatsCounter(
                putCount.get(), hitCount.get(), missCount.get(), 0, timeoutCount.get(), errorCount.get()));
    }

    /**
     * @param keyS should be the serialized string
     * @return the serialized value
     */
    protected byte[] getBinary(String keyS) {
        if (Strings.isNullOrEmpty(keyS)) {
            return new byte[0];
        }
        byte[] bytes = internalGet(computeKeyHash(keyS));
        return decodeValue(keyS.getBytes(StandardCharsets.UTF_8), bytes);
    }

    /**
     * @param keyS should be the serialized string
     * @param valueB should be the serialized value
     */
    protected void putBinary(String keyS, byte[] valueB, int expiration) {
        if (Strings.isNullOrEmpty(keyS)) {
            return;
        }
        internalPut(computeKeyHash(keyS), encodeValue(keyS.getBytes(StandardCharsets.UTF_8), valueB), expiration);
    }

    protected byte[] internalGet(String hashedKey) {
        Future<Object> future;
        long start = System.currentTimeMillis();
        try {
            future = client.asyncGet(hashedKey);
        } catch (IllegalStateException e) {
            // operation did not get queued in time (queue is full)
            errorCount.incrementAndGet();
            logger.error(UNABLE_TO_QUEUE_CACHE_OPERATION, e);
            return new byte[0];
        } catch (Throwable t) {
            errorCount.incrementAndGet();
            logger.error(UNABLE_TO_QUEUE_CACHE_OPERATION, t);
            return new byte[0];
        }

        try {
            byte[] result;
            if (future != null) {
                result = (byte[]) future.get(config.getTimeout(), TimeUnit.MILLISECONDS);
            } else {
                result = null;
            }
            cacheGetTime.addAndGet(System.currentTimeMillis() - start);
            if (result != null) {
                hitCount.incrementAndGet();
                readBytes.addAndGet(result.length);
            } else {
                missCount.incrementAndGet();
            }
            return result;
        } catch (TimeoutException e) {
            timeoutCount.incrementAndGet();
            future.cancel(false);
            return new byte[0];
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        } catch (ExecutionException e) {
            errorCount.incrementAndGet();
            logger.error("ExecutionException when pulling key meta from cache.", e);
            return new byte[0];
        }
    }

    private void internalPut(String hashedKey, byte[] encodedValue, int expiration) {
        try {
            client.set(hashedKey, expiration, encodedValue);
            putCount.incrementAndGet();
            putBytes.addAndGet(encodedValue.length);
        } catch (IllegalStateException e) {
            // operation did not get queued in time (queue is full)
            errorCount.incrementAndGet();
            logger.error(UNABLE_TO_QUEUE_CACHE_OPERATION, e);
        } catch (Throwable t) {
            errorCount.incrementAndGet();
            logger.error(UNABLE_TO_QUEUE_CACHE_OPERATION, t);
        }
    }

    protected byte[] encodeValue(byte[] key, byte[] valueB) {
        byte[] compressed = null;
        if (config.isEnableCompression() && (valueB.length + Ints.BYTES + key.length > compressThreshold)) {
            try {
                compressed = CompressionUtils.compress(ByteBuffer.allocate(Ints.BYTES + key.length + valueB.length)
                        .putInt(key.length).put(key).put(valueB).array());
            } catch (IOException e) {
                compressed = null;
                logger.warn("Compressing value bytes error.", e);
            }
        }
        if (compressed != null) {
            return ByteBuffer.allocate(Shorts.BYTES + compressed.length).putShort((short) 1).put(compressed).array();
        } else {
            return ByteBuffer.allocate(Shorts.BYTES + Ints.BYTES + key.length + valueB.length).putShort((short) 0)
                    .putInt(key.length).put(key).put(valueB).array();
        }
    }

    protected byte[] decodeValue(byte[] key, byte[] valueE) {
        if (valueE == null || valueE.length == 0)
            return new byte[0];
        ByteBuffer buf = ByteBuffer.wrap(valueE);
        short enableCompression = buf.getShort();
        byte[] uncompressed = null;
        if (enableCompression == 1) {
            byte[] value = new byte[buf.remaining()];
            buf.get(value);
            try {
                uncompressed = CompressionUtils.decompress(value);
            } catch (IOException | DataFormatException e) {
                logger.error("Decompressing value bytes error.", e);
                return new byte[0];
            }
        }
        if (uncompressed != null) {
            buf = ByteBuffer.wrap(uncompressed);
        }
        final int keyLength = buf.getInt();
        byte[] keyBytes = new byte[keyLength];
        buf.get(keyBytes);
        if (!Arrays.equals(keyBytes, key)) {
            logger.error("Keys do not match, possible hash collision!");
            return new byte[0];
        }
        byte[] value = new byte[buf.remaining()];
        buf.get(value);
        return value;
    }

    @SuppressWarnings({ "squid:S4790" })
    protected String computeKeyHash(String key) {
        // hash keys to keep things under 250 characters for net.spy.memcached
        return Joiner.on(":").skipNulls().join(KylinConfig.getInstanceFromEnv().getDeployEnv(), this.memcachedPrefix,
                DigestUtils.sha1Hex(key));

    }

}
