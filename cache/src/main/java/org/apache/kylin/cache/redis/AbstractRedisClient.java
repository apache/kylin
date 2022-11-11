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

import com.codahale.metrics.Gauge;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.directory.api.util.Strings;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.shaded.com.google.common.base.Charsets;
import org.apache.kylin.shaded.com.google.common.base.Joiner;
import org.apache.kylin.shaded.com.google.common.primitives.Ints;
import org.apache.kylin.shaded.com.google.common.primitives.Shorts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.DataFormatException;
import static com.codahale.metrics.MetricRegistry.name;
import static org.apache.kylin.metrics.lib.impl.MetricsSystem.Metrics;


public abstract class AbstractRedisClient implements RedisClient {

    private static final Logger logger = LoggerFactory.getLogger(AbstractRedisClient.class);

    protected final RedisConfig config;
    protected final String redisPrefix;
    protected final int compressThreshold;
    protected final int timeToLiveSeconds;

    protected final AtomicLong hitCount = new AtomicLong(0);
    protected final AtomicLong missCount = new AtomicLong(0);
    protected final AtomicLong readBytes = new AtomicLong(0);
    protected final AtomicLong errorCount = new AtomicLong(0);
    protected final AtomicLong putCount = new AtomicLong(0);
    protected final AtomicLong putBytes = new AtomicLong(0);
    protected final AtomicLong delCount = new AtomicLong(0);

    public AbstractRedisClient(RedisConfig redisConfig) {
        this.config = redisConfig;
        compressThreshold = redisConfig.getMaxObjectSize() / 2;
        redisPrefix = redisConfig.getPrefix();
        timeToLiveSeconds = redisConfig.getTtl();

        Map<String, String> metricsConfig = KylinConfig.getInstanceFromEnv().getKylinMetricsConf();
        if ("true".equalsIgnoreCase(metricsConfig.get("redis.enabled"))) {
            final String prefix = name(config.getRedisClientType().name(), redisPrefix);
            Metrics.register(name(prefix, "hits"), new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return hitCount.longValue();
                }
            });

            Metrics.register(name(prefix, "misses"), new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return missCount.longValue();
                }
            });

            Metrics.register(name(prefix, "eviction-count"), new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return delCount.longValue();
                }
            });

            Metrics.register(name(prefix, "put-count"), new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return putCount.longValue();
                }
            });
        }
    }

    public abstract byte[] internalGet(String key);
    public abstract void internalPut(String hashedKey, byte[] encodedValue, int expiration);
    public abstract void internalDel(String key);

    @Override
    public void put(Object key, Object value) {
        String keyString = serializeKey(key);
        if (Strings.isEmpty(keyString)) {
            return;
        }
        String hashKey = computeKeyHash(keyString);
        byte[] encodedValue = encodeValue(keyString.getBytes(Charsets.UTF_8), serializeValue(value));
        if (encodedValue.length > config.getMaxObjectSize()) {
            logger.info("AbstractRedisClient#put, value oversize, value size:{}, maxObjectSize:{}", encodedValue.length, config.getMaxObjectSize());
            return;
        }
        internalPut(hashKey, encodedValue, timeToLiveSeconds);
        putCount.incrementAndGet();
        putBytes.addAndGet(encodedValue.length);
    }

    @Override
    public byte[] get(Object key) {
        String keyString = serializeKey(key);
        if (Strings.isEmpty(keyString)) {
            return null;
        }
        byte[] result = internalGet(computeKeyHash(keyString));
        if (result == null) {
            missCount.incrementAndGet();
        } else {
            hitCount.incrementAndGet();
            readBytes.addAndGet(result.length);
        }
        return decodeValue(keyString.getBytes(Charsets.UTF_8), result);
    }

    @Override
    public void del(Object key) {
        if (key == null) {
            return;
        }
        String keyString = computeKeyHash(serializeKey(key));
        if (Strings.isEmpty(keyString)) {
            return;
        }
        internalDel(keyString);
        delCount.incrementAndGet();
    }

    @Override
    public String getName(){
        return redisPrefix;
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
            logger.error("Keys do not match, possible hash collision!, keyBytes:{}, key:{}", Arrays.toString(keyBytes), Arrays.toString(key));
            errorCount.incrementAndGet();
            return null;
        }
        byte[] value = new byte[buf.remaining()];
        buf.get(value);
        return value;
    }

    protected String computeKeyHash(String key) {
        // hash keys to keep things under 250 characters for redis
        return Joiner.on(":").skipNulls().join(redisPrefix, KylinConfig.getInstanceFromEnv().getDeployEnv(),
                DigestUtils.shaHex(key));
    }

}
