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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.DataFormatException;

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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;

/**
 * Cache backend by Memcached. The implementation leverages spymemcached client to talk to the servers.
 * Memcached itself has a limitation to the size of the key. So the real key for cache lookup is hashed from the orginal key. 
 * The implementation provdes a way for hash collsion detection. It can also compress/decompress the value bytes based on the preconfigred compression threshold to save network bandwidth and storage space.
 * 
 * @author mingmwang
 *
 */
public class MemcachedCache{
    private static final Logger logger = LoggerFactory.getLogger(MemcachedCache.class);

    private static final int DEFAULT_TTL = 7 * 24 * 3600;
    
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
                    .setLocatorType(ConnectionFactoryBuilder.Locator.CONSISTENT)
                    .setDaemon(true)
                    .setFailureMode(FailureMode.Redistribute)
                    .setTranscoder(transcoder)
                    .setShouldOptimize(true)
                    .setOpQueueMaxBlockTime(config.getTimeout())
                    .setOpTimeout(config.getTimeout())
                    .setReadBufferSize(config.getReadBufferSize())
                    .setOpQueueFactory(opQueueFactory).build();
            return new MemcachedCache(new MemcachedClient(new MemcachedConnectionFactory(connectionFactory), AddrUtil.getAddresses(hostsStr)), config, memcachedPrefix, timeToLive);
        } catch (IOException e) {
            logger.error("Unable to create MemcachedCache instance.", e);
            throw Throwables.propagate(e);
        }
    }

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

    protected AtomicLong cacheGetTime = new AtomicLong(0);

    private final int timeToLiveSeconds;

    public MemcachedCache(final MemcachedClientIF client, final MemcachedCacheConfig config, final String memcachedPrefix, int timeToLiveSeconds) {
        Preconditions.checkArgument(memcachedPrefix .length() <= MAX_PREFIX_LENGTH, "memcachedPrefix length [%d] exceeds maximum length [%d]", memcachedPrefix.length(), MAX_PREFIX_LENGTH);
        this.memcachedPrefix = memcachedPrefix;
        this.client = client;
        this.config = config;
        this.compressThreshold = config.getMaxObjectSize()/2;
        this.timeToLiveSeconds = timeToLiveSeconds;
    }
    
    public MemcachedCache(MemcachedCache cache){
        this(cache.client, cache.config, cache.memcachedPrefix, cache.timeToLiveSeconds);
    }
    
    
    public String getName() {
        return memcachedPrefix;
    }

    public Object getNativeCache() {
        return client;
    }

    /**
     * This method is used to get value object based on key from the Cache. It converts key to json string first.
     * And then it calls getBinary() method to compute hashed key from the original key string, and use this as the real key to do lookup from internal Cache.
     * Then decodes the real values bytes from the cache lookup result, and leverages object serializer to convert value bytes to object.  
     */
    public byte[] get(Object key) {
        byte[] value = null;
        try {
            value = get(JsonUtil.writeValueAsString(key));
        } catch (JsonProcessingException e) {
            logger.warn("Can not convert key to String.", e);
        }
        return value;
    }

    public byte[] get(String key) {
        byte[] value = getBinary(key);
        return value;
    }

    /**
     * This method is used to put key/value objects to the Cache. It converts key to json string and leverages object serializer to convert value object to bytes. 
     * And then it calls putBinary() method to compute hashed key from the original key string and encode the original key bytes into value bytes for hash conflicts detection.
     */
    public void put(Object key, Object value) {
        try {
            put(JsonUtil.writeValueAsString(key), value);
        } catch (JsonProcessingException e) {
            logger.warn("Can not convert key to String.", e);
        }
    }

    public void put(String key, Object value) {
        putBinary(key, SerializationUtils.serialize((Serializable)value), timeToLiveSeconds);
    }

    public void evict(Object key) {
        if(key == null)
            return;
        try {
            evict(JsonUtil.writeValueAsString(key));
        } catch (JsonProcessingException e) {
            logger.warn("Can not convert key to String.", e);
        }
    }

    public void evict(String key) {
        if(key == null)
            return;
        client.delete(computeKeyHash(key));
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
        return new CacheStats(readBytes.get(), cacheGetTime.get(), putCount.get(), putBytes.get(), hitCount.get(), missCount.get(), 0, timeoutCount.get(), errorCount.get());
    }

    public byte[] getBinary(String key) {
        if(Strings.isNullOrEmpty(key)){
            return null;
        }
        byte[] bytes =  internalGet(computeKeyHash(key));
        return decodeValue(key.getBytes(Charsets.UTF_8), bytes);
    }
    

    public void putBinary(String key, byte[] value, int expiration) {
        if(Strings.isNullOrEmpty(key)){
            return;
        }
        internalPut(computeKeyHash(key), encodeValue(key.getBytes(Charsets.UTF_8), value), expiration);
    }

    protected byte[] internalGet(String hashedKey){
        Future<Object> future;
        long start = System.currentTimeMillis();
        try {
            future = client.asyncGet(hashedKey);
        } catch (IllegalStateException e) {
            // operation did not get queued in time (queue is full)
            errorCount.incrementAndGet();
            logger.error("Unable to queue cache operation.", e);
            return null;
        } catch (Throwable t) {
            errorCount.incrementAndGet();
            logger.error("Unable to queue cache operation.", t);
            return null;
        }
        
        try {
            byte[] result = (byte[])future.get(config.getTimeout(), TimeUnit.MILLISECONDS);
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
    
    private void internalPut(String hashedKey, byte[] serializedValue, int expiration) {
        try {
            client.set(hashedKey, expiration, serializedValue);
            putCount.incrementAndGet();
            putBytes.addAndGet(serializedValue.length);
        } catch (IllegalStateException e) {
            // operation did not get queued in time (queue is full)
            errorCount.incrementAndGet();
            logger.error("Unable to queue cache operation.", e);
        } catch (Throwable t) {
            errorCount.incrementAndGet();
            logger.error("Unable to queue cache operation.", t);
        }
    }

    protected byte[] encodeValue(byte[] key, byte[] value) {
        byte[] compressed = null;
        if (config.isEnableCompression() && (value.length + Ints.BYTES + key.length > compressThreshold)) {
            try {
                compressed = CompressionUtils.compress(ByteBuffer.allocate(Ints.BYTES + key.length + value.length).putInt(key.length).put(key).put(value).array());
            } catch (IOException e) {
                compressed = null;
                logger.warn("Compressing value bytes error.", e);
            }
        }
        if (compressed != null) {
            return ByteBuffer.allocate(Shorts.BYTES + compressed.length).putShort((short) 1).put(compressed).array();
        } else {
            return ByteBuffer.allocate(Shorts.BYTES + Ints.BYTES + key.length + value.length).putShort((short) 0).putInt(key.length).put(key).put(value).array();
        }
    }
    
    protected byte[] decodeValue(byte[] key, byte[] bytes) {
        if(bytes == null)
            return null;
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        short enableCompression = buf.getShort();
        byte[] uncompressed = null;
        if(enableCompression == 1){
            byte[] value = new byte[buf.remaining()];
            buf.get(value);
            try {
                uncompressed = CompressionUtils.decompress(value);
            } catch (IOException | DataFormatException e) {
                logger.error("Decompressing value bytes error.", e);
                return null;
            }
        }
        if(uncompressed != null){
            buf = ByteBuffer.wrap(uncompressed);
        }
        final int keyLength = buf.getInt();
        byte[] keyBytes = new byte[keyLength];
        buf.get(keyBytes);
        if(!Arrays.equals(keyBytes, key)){
            logger.error("Keys do not match, possible hash collision!");
            return null;
        }
        byte[] value = new byte[buf.remaining()];
        buf.get(value);
        return value;
    }

    protected String computeKeyHash(String key) {
        // hash keys to keep things under 250 characters for memcached
        return Joiner.on(":").skipNulls().join(KylinConfig.getInstanceFromEnv().getDeployEnv(), this.memcachedPrefix , DigestUtils.shaHex(key));

    }
    
    public static final int MAX_PREFIX_LENGTH = MemcachedClientIF.MAX_KEY_LENGTH - 40 // length of namespace hash
    - 40 // length of key hash
    - 2; // length of separators

}
