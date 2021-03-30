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

package org.apache.kylin.cache.memcached;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
import org.apache.kylin.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kylin.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.kylin.shaded.com.google.common.base.Charsets;
import org.apache.kylin.shaded.com.google.common.base.Joiner;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.base.Strings;
import org.apache.kylin.shaded.com.google.common.base.Throwables;
import org.apache.kylin.shaded.com.google.common.primitives.Ints;
import org.apache.kylin.shaded.com.google.common.primitives.Shorts;

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
 * Cache backend by Memcached. The implementation leverages spymemcached client to talk to the servers.
 * Memcached itself has a limitation to the size of the key. So the real key for cache lookup is hashed from the orginal key.
 * The implementation provdes a way for hash collsion detection. It can also compress/decompress the value bytes based on the preconfigred compression threshold to save network bandwidth and storage space.
 *
 * @author mingmwang
 *
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
            logger.error("Unable to create MemcachedCache instance.", e);
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

    protected String serializeKey(Object key) {
        try {
            return JsonUtil.writeValueAsString(key);
        } catch (JsonProcessingException e) {
            logger.warn("Can not convert key to String.", e);
        }
        return null;
    }

    protected byte[] serializeValue(Object value) {
        return SerializationUtils.serialize((Serializable) value);
    }

    @VisibleForTesting
    byte[] encodeValue(String keyS, Object value) {
        if (keyS == null) {
            return null;
        }
        return encodeValue(keyS.getBytes(Charsets.UTF_8), serializeValue(value));
    }

    /**
     * This method is used to get value object based on key from the Cache. It converts key to json string first.
     * And then it calls getBinary() method to compute hashed key from the original key string, and use this as the real key to do lookup from internal Cache.
     * Then decodes the real values bytes from the cache lookup result, and leverages object serializer to convert value bytes to object.
     */
    public byte[] get(Object key) {
        return get(serializeKey(key));
    }

    /**
     * @param keyS should be the serialized string
     */
    public byte[] get(String keyS) {
        return getBinary(keyS);
    }

    /**
     * This method is used to put key/value objects to the Cache. It converts key to json string and leverages object serializer to convert value object to bytes.
     * And then it calls putBinary() method to compute hashed key from the original key string and encode the original key bytes into value bytes for hash conflicts detection.
     */
    public void put(Object key, Object value) {
        put(serializeKey(key), value);
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
        evict(serializeKey(key));
    }

    /**
     * @param keyS should be the serialized string
     */
    public void evict(String keyS) {
        if (keyS == null)
            return;
        client.delete(computeKeyHash(keyS));
    }

    public void clear() {
        logger.warn("Clear Remote Cache!");
        Future<Boolean> resultFuture = client.flush();
        try {
            boolean result = resultFuture.get();
            logger.warn("Clear Remote Cache returned with result: " + result);
        } catch (Exception e) {
            logger.warn("Can't clear Remote Cache.", e);
        }
    }

    public CacheStats getStats() {
        return new CacheStats(readBytes.get(), cacheGetTime.get(), putCount.get(), putBytes.get(), hitCount.get(),
                missCount.get(), 0, timeoutCount.get(), errorCount.get());
    }

    /**
     * @param keyS should be the serialized string
     * @return the serialized value
     */
    protected byte[] getBinary(String keyS) {
        if (Strings.isNullOrEmpty(keyS)) {
            return null;
        }
        byte[] bytes = internalGet(computeKeyHash(keyS));
        return decodeValue(keyS.getBytes(Charsets.UTF_8), bytes);
    }

    /**
     * @param keyS should be the serialized string
     * @param valueB should be the serialized value
     */
    protected void putBinary(String keyS, byte[] valueB, int expiration) {
        if (Strings.isNullOrEmpty(keyS)) {
            return;
        }
        internalPut(computeKeyHash(keyS), encodeValue(keyS.getBytes(Charsets.UTF_8), valueB), expiration);
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
            return null;
        } catch (Throwable t) {
            errorCount.incrementAndGet();
            logger.error(UNABLE_TO_QUEUE_CACHE_OPERATION, t);
            return null;
        }

        try {
            byte[] result = (byte[]) future.get(config.getTimeout(), TimeUnit.MILLISECONDS);
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
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        } catch (ExecutionException e) {
            errorCount.incrementAndGet();
            logger.error("ExecutionException when pulling key meta from cache.", e);
            return null;
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
        if (valueE == null)
            return null;
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
                return null;
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
            return null;
        }
        byte[] value = new byte[buf.remaining()];
        buf.get(value);
        return value;
    }

    protected String computeKeyHash(String key) {
        // hash keys to keep things under 250 characters for memcached
        return Joiner.on(":").skipNulls().join(KylinConfig.getInstanceFromEnv().getDeployEnv(), this.memcachedPrefix,
                DigestUtils.shaHex(key));

    }

}