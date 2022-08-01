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

package org.apache.kylin.rest.cache;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.rest.util.SerializeUtil;
import org.apache.kylin.common.util.EncryptUtil;
import org.apache.kylin.common.util.ThrowableUtils;
import org.apache.kylin.rest.service.CommonQueryCacheSupporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisClusterException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.util.JedisClusterCRC16;

public class RedisCache implements KylinCache {

    private static final Logger logger = LoggerFactory.getLogger(RedisCache.class);

    private static final String NX = "NX";
    private static final String XX = "XX";
    private static final String PREFIX = "Kyligence-";
    private static final String CHARSET_NAME = "UTF-8";
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private static final String SCAN_POINTER_START_STR = new String(ScanParams.SCAN_POINTER_START_BINARY, CHARSET);
    private static JedisPool jedisPool;
    private static JedisCluster jedisCluster;

    //for flush and ping opt
    private static Set<String> masters = Sets.newHashSet();

    private String redisExpireTimeUnit;
    private long redisExpireTime;
    private long redisExpireTimeForException;
    private boolean redisClusterEnabled;
    private static final AtomicBoolean isRecovering = new AtomicBoolean(false);

    public static KylinCache getInstance() {
        try {
            return Singletons.getInstance(RedisCache.class);
        } catch (RuntimeException e) {
            logger.error("Jedis init failed: ", e);
        }
        return null;
    }

    public static boolean checkRedisClient() {
        Jedis jedis = null;
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        try {
            String result = "";
            if (kylinConfig.isRedisClusterEnabled()) {
                getRedisClusterInfo();
                for (String host : masters) {
                    jedis = jedisCluster.getClusterNodes().get(host).getResource();
                    String pong = jedis.ping();
                    if ("PONG".equals(pong)) {
                        return true;
                    }
                }
            } else {
                jedis = jedisPool.getResource();
                result = jedis.ping();
            }
            return "PONG".equals(result);
        } catch (Exception e) {
            logger.error("redis connect failed!", e);
            return false;
        } finally {
            close(jedis);
        }
    }

    public static void getRedisClusterInfo() {
        if (jedisCluster == null) {
            return;
        }
        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
        StringBuilder masterNodes = new StringBuilder();
        StringBuilder slaveNodes = new StringBuilder();
        masters = Sets.newHashSet();
        Jedis resource = null;
        for (String host : clusterNodes.keySet()) {
            try {
                resource = clusterNodes.get(host).getResource();
                if (resource.info("replication").contains("role:slave")) {
                    slaveNodes.append(host).append(" ");
                } else {
                    masters.add(host);
                    masterNodes.append(host).append(" ");
                }
            } catch (Exception e) {
                logger.error("redis {} is not reachable", host);
            } finally {
                close(resource);
            }
        }
        Preconditions.checkState(!masters.isEmpty(), "there is no master node alive for redis.");
        logger.info("redis cluster master nodes: {}", Joiner.on(" ").join(masters));
        logger.info("redis cluster slave node: {}", slaveNodes);
    }

    private static void close(Jedis redisClient) {
        try {
            if (redisClient != null)
                redisClient.close();
        } catch (Exception e) {
            logger.error("ignore error closing redis client", e);
        }
    }

    // ============================================================================

    private RedisCache() throws JedisException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        redisClusterEnabled = kylinConfig.isRedisClusterEnabled();
        redisExpireTimeUnit = kylinConfig.getRedisExpireTimeUnit();
        redisExpireTime = kylinConfig.getRedisExpireTime();
        redisExpireTimeForException = kylinConfig.getRedisExpireTimeForException();
        String redisHosts = kylinConfig.getRedisHosts();
        String redisPassword = kylinConfig.getRedisPassword();
        if (EncryptUtil.isEncrypted(redisPassword)) {
            redisPassword = EncryptUtil.getDecryptedValue(redisPassword);
        }
        String[] hostAndPorts = redisHosts.split(",");
        if (hostAndPorts == null || hostAndPorts.length < 1) {
            throw new RuntimeException(
                    "redis client init failed because there are some errors in kylin.properties for 'kylin.cache.redis.hosts'");
        }
        logger.info("The 'kylin.cache.redis.cluster-enabled' is {}", redisClusterEnabled);
        if (kylinConfig.isRedisClusterEnabled()) {
            logger.info("ke will use redis cluster");
            Set<HostAndPort> hosts = Sets.newHashSet();
            for (String hostAndPort : hostAndPorts) {
                String host = hostAndPort.substring(0, hostAndPort.lastIndexOf(":"));
                int port = Integer.parseInt(hostAndPort.substring(hostAndPort.lastIndexOf(":") + 1));
                hosts.add(new HostAndPort(host, port));
            }
            GenericObjectPoolConfig config = new GenericObjectPoolConfig();
            config.setMaxTotal(kylinConfig.getRedisMaxTotal());
            config.setMaxIdle(kylinConfig.getRedisMaxIdle());
            config.setMinIdle(kylinConfig.getRedisMinIdle());
            config.setMaxWaitMillis(kylinConfig.getMaxWaitMillis());
            if (StringUtils.isNotBlank(redisPassword)) {
                jedisCluster = new JedisCluster(hosts, kylinConfig.getRedisConnectionTimeout(),
                        kylinConfig.getRedisSoTimeout(), kylinConfig.getRedisMaxAttempts(), redisPassword, config);
            } else {
                jedisCluster = new JedisCluster(hosts, kylinConfig.getRedisConnectionTimeout(),
                        kylinConfig.getRedisSoTimeout(), kylinConfig.getRedisMaxAttempts(), config);
            }
            logger.warn("jedis cluster is not support ping");
        } else {
            logger.info("ke will use redis pool. The redis host ke will connect to is {}", hostAndPorts[0]);
            String host = hostAndPorts[0].substring(0, hostAndPorts[0].lastIndexOf(":"));
            int port = Integer.parseInt(hostAndPorts[0].substring(hostAndPorts[0].lastIndexOf(":") + 1));
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(kylinConfig.getRedisMaxTotal());
            config.setMaxIdle(kylinConfig.getRedisMaxIdle());
            config.setMinIdle(kylinConfig.getRedisMinIdle());
            config.setTestOnBorrow(true);
            if (!StringUtils.isEmpty(redisPassword)) {
                this.jedisPool = new JedisPool(config, host, port, kylinConfig.getRedisConnectionTimeout(),
                        redisPassword);
            } else {
                this.jedisPool = new JedisPool(config, host, port, kylinConfig.getRedisConnectionTimeout());
            }
        }
        logger.info("Jedis init success.");
    }

    public static KylinCache recoverInstance() {
        isRecovering.set(true);
        KylinCache cache;
        try {
            synchronized (RedisCache.class) {
                logger.info("Destroy RedisCache.");
                if (jedisCluster != null) {
                    jedisCluster.close();
                }
                if (jedisPool != null) {
                    jedisPool.close();
                }
                logger.info("Initiate RedisCache.");
                cache = new RedisCache();
            }
        } finally {
            isRecovering.set(false);
        }
        return cache;
    }

    private String getTypeProjectPrefix(String type, String project) {
        return String.format(Locale.ROOT, "%s-%s", type, project);
    }

    private boolean isExceptionQuery(String type) {
        return type.equals(CommonQueryCacheSupporter.Type.EXCEPTION_QUERY_CACHE.rootCacheName);
    }

    @Override
    public void put(String type, String project, Object key, Object value) {
        if (isRecovering.get()) {
            return;
        }

        if (isExceptionQuery(type)) {
            put(getTypeProjectPrefix(type, project), key, value, NX, redisExpireTimeUnit, redisExpireTimeForException);
            return;
        }
        put(getTypeProjectPrefix(type, project), key, value, NX, redisExpireTimeUnit, redisExpireTime);
    }

    @Override
    public void update(String type, String project, Object key, Object value) {
        if (isRecovering.get()) {
            return;
        }

        if (isExceptionQuery(type)) {
            put(getTypeProjectPrefix(type, project), key, value, XX, redisExpireTimeUnit, redisExpireTimeForException);
            return;
        }
        put(getTypeProjectPrefix(type, project), key, value, XX, redisExpireTimeUnit, redisExpireTime);
    }

    public void put(String type, Object key, Object value, String expireTimeUnit, long expireTime) {
        put(type, key, value, NX, expireTimeUnit, expireTime);
    }

    public void put(String type, Object key, Object value, String nx, String expireTimeUnit, long expireTime) {
        byte[] realKey = convertKeyToByte(type, key);
        byte[] valueBytes = convertValueToByte(value);
        if (redisClusterEnabled) {
            if (jedisCluster == null) {
                logger.error("[Redis cache log] Jedis Cluster failed to initiate.");
                return;
            }
            jedisCluster.set(realKey, valueBytes, getJedisSetParams(nx, expireTimeUnit, expireTime));
        } else {
            singleRedisPut(realKey, valueBytes, expireTimeUnit, expireTime);
        }
    }

    private void singleRedisPut(byte[] key, byte[] value, String expireTimeUnit, long expireTime) {
        try (Jedis redisClient = jedisPool.getResource()) {
            if (redisClient.exists(key)) {
                redisClient.del(key);
            }
            redisClient.set(key, value, getJedisSetParams(NX, expireTimeUnit, expireTime));
        }
    }

    private SetParams getJedisSetParams(String ifExist, String expireTimeUnit, Long expireTime) {
        SetParams params = new SetParams();
        if (expireTimeUnit.equals("EX")) {
            params.ex(expireTime); // expire time in second
        } else {
            params.px(expireTime); // expire time in microsecond
        }
        if (ifExist.equals(NX)) {
            params.nx(); // Only set the key if it does not already exist.
        } else {
            params.xx(); // Only set the key if it already exist.
        }
        return params;
    }

    @Override
    public Object get(String type, String project, Object key) {
        if (isRecovering.get()) {
            return null;
        }

        byte[] realKey = convertKeyToByte(getTypeProjectPrefix(type, project), key);
        byte[] sqlResp = null;
        try {
            if (redisClusterEnabled) {
                if (jedisCluster == null) {
                    logger.error("[Redis cache log] Jedis Cluster failed to initiate.");
                    return null;
                }
                if (jedisCluster.getClusterNodes().isEmpty()) {
                    recoverInstance();
                }
                logger.trace("redis get start");
                sqlResp = jedisCluster.get(realKey);
                logger.trace("redis get done, size = {}bytes", sqlResp.length);
            } else {
                logger.trace("redis get start");
                sqlResp = singleRedisGet(realKey);
                logger.trace("redis get done, size = {}bytes", sqlResp.length);
            }
        } catch (JedisConnectionException | JedisClusterException e) {
            logger.error("Get jedis connection failed: ", e);
            if (ThrowableUtils.isInterruptedException(e.getCause())) {
                Thread.currentThread().interrupt();
            }
        } catch (JedisMovedDataException e) {
            logger.error("Failed to get redis data: ", e);
        } catch (Exception e) {
            logger.error("Unknown redis cache error: ", e);
        }
        if (sqlResp != null) {
            Object result = convertByteToObject(sqlResp);
            logger.trace("redis result deserialized");
            return result;
        }
        return null;
    }

    private boolean removeImpl(String type, Object key) {
        byte[] realKey = convertKeyToByte(type, key);
        boolean res = false;
        if (redisClusterEnabled) {
            res = jedisCluster.del(realKey) > 0;
        } else {
            res = singleRedisRemove(realKey);
        }
        return res;
    }

    private byte[] singleRedisGet(byte[] key) {
        try (Jedis redisClient = jedisPool.getResource()) {
            return redisClient.get(key);
        }
    }

    private boolean singleRedisRemove(byte[] key) {
        try (Jedis redisClient = jedisPool.getResource()) {
            return redisClient.del(key) > 0;
        }
    }

    @Override
    public void clearAll() {
        clearByType("", "");
    }

    private void clusterRedisClearByPattern(String pattern) {
        Map<Integer, List<byte[]>> slotToKeys;
        if (masters.isEmpty()) {
            logger.error("[Redis cache log] No masters in the cluster, fail to clear by pattern.");
            return;
        }
        for (String host : masters) {
            slotToKeys = Maps.newHashMap();
            Jedis master = null;
            try {
                master = jedisCluster.getClusterNodes().get(host).getResource();
                for (byte[] k : getScannedKeys(pattern, master)) {
                    int slot = JedisClusterCRC16.getSlot(k);
                    if (slotToKeys.containsKey(slot)) {
                        slotToKeys.get(slot).add(k);
                    } else {
                        slotToKeys.put(slot, Lists.newArrayList(k));
                    }
                }
                for (int slot : slotToKeys.keySet()) {
                    master.del(slotToKeys.get(slot).toArray(new byte[slotToKeys.get(slot).size()][]));
                }
            } finally {
                close(master);
            }
        }
    }

    @Override
    public void clearByType(String type, String project) {
        if (isRecovering.get()) {
            return;
        }

        String prefixAndType = PREFIX;
        if (!type.isEmpty()) {
            prefixAndType += getTypeProjectPrefix(type, project);
        }
        if (redisClusterEnabled) {
            clusterRedisClearByPattern(prefixAndType + "*");
        } else {
            singleRedisClearByType(prefixAndType);
        }
    }

    private void singleRedisClearByType(String prefixAndType) {
        try (Jedis redisClient = jedisPool.getResource()) {
            delByProject(prefixAndType + "*", redisClient);
        }
    }

    private void delByProject(String key, Jedis redisClient) {
        List<byte[]> keys = getScannedKeys(key, redisClient);
        if (CollectionUtils.isNotEmpty(keys)) {
            redisClient.del((byte[][]) keys.toArray(new byte[keys.size()][]));
        }
    }

    private List<byte[]> getScannedKeys(String key, Jedis redisClient) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        byte[] cursor = ScanParams.SCAN_POINTER_START_BINARY;
        ScanParams scanParams = new ScanParams();
        scanParams.match(getBytesFromString(key));
        scanParams.count(config.getRedisScanKeysBatchCount());
        List<byte[]> keys = new ArrayList<>();
        while (true) {
            ScanResult<byte[]> scanResult = redisClient.scan(cursor, scanParams);
            cursor = scanResult.getCursorAsBytes();
            List<byte[]> list = scanResult.getResult();
            if (CollectionUtils.isNotEmpty(list)) {
                keys.addAll(list);
            }
            if (SCAN_POINTER_START_STR.equals(new String(cursor, CHARSET))) {
                break;
            }
        }
        return keys;
    }

    private byte[] convertValueToByte(Object value) {
        try {
            return CompressionUtils.compress(SerializeUtil.serialize(value));
        } catch (Exception e) {
            logger.error("serialize failed!", e);
            return null;
        }
    }

    private byte[] getBytesFromString(String str) {
        try {
            return str.getBytes(CHARSET_NAME);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("unsupported encoding:" + CHARSET_NAME, e);
        }
    }

    private byte[] convertKeyToByte(String type, Object key) {
        try {
            String prefixAndType = PREFIX + type;
            byte[] typeBytes = getBytesFromString(prefixAndType);
            byte[] keyBytes = SerializeUtil.serialize(key);
            byte[] trueKeyBytes = new byte[keyBytes.length + typeBytes.length];
            System.arraycopy(typeBytes, 0, trueKeyBytes, 0, typeBytes.length);
            System.arraycopy(keyBytes, 0, trueKeyBytes, typeBytes.length, keyBytes.length);
            return trueKeyBytes;
        } catch (Exception e) {
            logger.error("serialize fail!", e);
            return null;
        }
    }

    private Object convertByteToObject(byte[] bytes) {
        try {
            return SerializeUtil.deserialize(CompressionUtils.decompress(bytes));
        } catch (Exception e) {
            logger.error("deserialize fail!", e);
            return null;
        }
    }

    @Override
    public boolean remove(String type, String project, Object key) {
        if (isRecovering.get()) {
            return false;
        }

        return removeImpl(getTypeProjectPrefix(type, project), key);
    }
}
