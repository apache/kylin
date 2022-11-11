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
package org.apache.kylin.cache.redis.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.kylin.cache.redis.AbstractRedisClient;
import org.apache.kylin.cache.redis.RedisClient;
import org.apache.kylin.cache.redis.RedisConfig;
import org.apache.kylin.shaded.com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class JedisPoolClient extends AbstractRedisClient implements RedisClient {

    private static final Logger logger = LoggerFactory.getLogger(JedisPoolClient.class);

    private final JedisPool jedisPool;

    public JedisPoolClient(RedisConfig redisConfig) {
        super(redisConfig);
        logger.info("JedisPoolClient init, redisConfig:{}", redisConfig);
        // init jedis config
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxIdle(redisConfig.getMaxIdle());
        poolConfig.setMinIdle(redisConfig.getMinIdle());
        poolConfig.setMaxTotal(redisConfig.getMaxTotal());
        poolConfig.setMaxWaitMillis(redisConfig.getMaxWaitMillis());
        jedisPool = new JedisPool(poolConfig, redisConfig.getHost(), redisConfig.getPort(), redisConfig.getTimeout());
    }

    @Override
    public void internalPut(String hashedKey, byte[] encodedValue, int expiration) {
        logger.debug("JedisPoolClient internalPut, key:{}, value size:{}, expiration:{}", hashedKey, encodedValue.length, expiration);
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.setex(hashedKey.getBytes(Charsets.UTF_8), expiration, encodedValue);
        } catch (Exception e) {
            errorCount.incrementAndGet();
            logger.error("JedisPoolClient put error, ", e);
        }
    }

    @Override
    public byte[] internalGet(String key) {
        logger.debug("JedisPoolClient internalGet, key:{}", key);
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(key.getBytes(Charsets.UTF_8));
        } catch (Exception e) {
            errorCount.incrementAndGet();
            logger.error("JedisPoolClient Get error", e);
        }
        return null;
    }

    @Override
    public void internalDel(String key) {
        logger.debug("JedisPoolClient internalDel, key:{}", key);
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(key);
        } catch (Exception e) {
            errorCount.incrementAndGet();
            logger.error("JedisPoolClient del error, ", e);
        }
    }

    @Override
    public boolean ping() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.ping();
            return true;
        } catch (Exception e) {
            errorCount.incrementAndGet();
            logger.error("JedisPoolClient isConnected error, ", e);
            return false;
        }
    }

}
