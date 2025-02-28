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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.base.Strings;
import org.apache.kylin.guava30.shaded.common.base.Throwables;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.primitives.Ints;
import org.apache.kylin.guava30.shaded.common.primitives.Shorts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.spy.memcached.internal.BulkFuture;

/**
 * Subclass of MemcachedCache. It supports storing large objects.  
 * Memcached itself has a limitation to the value size with default value of 1M.
 * This implement extends the limit to 1G and can split huge bytes to multiple chunks. 
 * It will take care of the data integrity if part of the chunks lost(due to server restart or other reasons)
 *
 * @author mingmwang
 */
public class MemcachedChunkingCache extends MemcachedCache implements KeyHookLookup {
    private static final Logger logger = LoggerFactory.getLogger(MemcachedChunkingCache.class);

    public MemcachedChunkingCache(MemcachedCache cache) {
        super(cache);
        Preconditions.checkArgument(config.getMaxChunkSize() > 1, "maxChunkSize [%d] must be greater than 1",
                config.getMaxChunkSize());
        Preconditions.checkArgument(config.getMaxObjectSize() > 261, "maxObjectSize [%d] must be greater than 261",
                config.getMaxObjectSize());
    }

    protected static byte[][] splitBytes(final byte[] data, final int nSplit) {
        byte[][] dest = new byte[nSplit][];

        final int splitSize = (data.length - 1) / nSplit + 1;
        for (int i = 0; i < nSplit - 1; i++) {
            dest[i] = Arrays.copyOfRange(data, i * splitSize, (i + 1) * splitSize);
        }
        dest[nSplit - 1] = Arrays.copyOfRange(data, (nSplit - 1) * splitSize, data.length);

        return dest;
    }

    protected static int getValueSplit(MemcachedCacheConfig config, String keyS, int valueBLen) {
        // the number 6 means the chunk number size never exceeds 6 bytes
        final int valueSize = config.getMaxObjectSize() - Shorts.BYTES - Ints.BYTES
                - keyS.getBytes(StandardCharsets.UTF_8).length - 6;
        final int maxValueSize = config.getMaxChunkSize() * valueSize;
        Preconditions.checkArgument(valueBLen <= maxValueSize,
                "the value bytes length [%d] exceeds maximum value size [%d]", valueBLen, maxValueSize);
        return (valueBLen - 1) / valueSize + 1;
    }

    protected static Pair<KeyHook, byte[][]> getKeyValuePair(int nSplit, String keyS, byte[] valueB) {
        KeyHook keyHook;
        byte[][] splitValueB = null;
        if (nSplit > 1) {
            String[] chunkKeySs = new String[nSplit];
            for (int i = 0; i < nSplit; i++) {
                chunkKeySs[i] = keyS + i;
            }
            keyHook = new KeyHook(chunkKeySs, null);
            splitValueB = splitBytes(valueB, nSplit);
        } else {
            keyHook = new KeyHook(null, valueB);
        }

        return new Pair<>(keyHook, splitValueB);
    }

    /**
     * This method overrides the parent getBinary(), it gets the KeyHook from the Cache first
     * and check the KeyHook that whether chunking is enabled or not.
     */
    @Override
    @SuppressWarnings({ "squid:S3776" })
    public byte[] getBinary(String keyS) {
        if (Strings.isNullOrEmpty(keyS)) {
            return new byte[0];
        }
        KeyHook keyHook = lookupKeyHook(keyS);
        if (keyHook == null) {
            return new byte[0];
        }

        if (keyHook.getChunkskey() == null || keyHook.getChunkskey().length == 0) {
            if (logger.isDebugEnabled() || config.isEnableDebugLog()) {
                logger.debug(
                        "Chunking not enabled, return the value bytes in the keyhook directly, value bytes size = {}",
                        keyHook.getValues().length);
            }
            return keyHook.getValues();
        }

        BulkFuture<Map<String, Object>> bulkFuture;
        long start = System.currentTimeMillis();

        if (logger.isDebugEnabled() || config.isEnableDebugLog()) {
            logger.debug("Chunking enabled, chunk size = {}", keyHook.getChunkskey().length);
        }

        Map<String, String> keyLookup = computeKeyHash(Arrays.asList(keyHook.getChunkskey()));
        try {
            bulkFuture = client.asyncGetBulk(keyLookup.keySet());
        } catch (IllegalStateException e) {
            // operation did not get queued in time (queue is full)
            errorCount.incrementAndGet();
            logger.error("Unable to queue cache operation.", e);
            return new byte[0];
        } catch (Throwable t) {
            errorCount.incrementAndGet();
            logger.error("Unable to queue cache operation.", t);
            return new byte[0];
        }

        try {
            Map<String, Object> bulkResult = bulkFuture.get(config.getTimeout(), TimeUnit.MILLISECONDS);
            cacheGetTime.addAndGet(System.currentTimeMillis() - start);
            if (bulkResult.size() != keyHook.getChunkskey().length) {
                missCount.incrementAndGet();
                logger.warn("Some paritial chunks missing for query key: {}", keyS);
                //remove all the partital chunks here.
                for (String partitalKey : bulkResult.keySet()) {
                    client.delete(partitalKey);
                }
                deleteKeyHook(keyS);
                return new byte[0];
            }
            hitCount.getAndAdd(keyHook.getChunkskey().length);
            byte[][] bytesArray = new byte[keyHook.getChunkskey().length][];
            for (Map.Entry<String, Object> entry : bulkResult.entrySet()) {
                byte[] bytes = (byte[]) entry.getValue();
                readBytes.addAndGet(bytes.length);
                String originalKeyS = keyLookup.get(entry.getKey());
                int idx = Integer.parseInt(originalKeyS.substring(keyS.length()));
                bytesArray[idx] = decodeValue(originalKeyS.getBytes(StandardCharsets.UTF_8), bytes);
            }
            return concatBytes(bytesArray);
        } catch (TimeoutException e) {
            timeoutCount.incrementAndGet();
            bulkFuture.cancel(false);
            return new byte[0];
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        } catch (ExecutionException e) {
            errorCount.incrementAndGet();
            logger.error("ExecutionException when pulling item from cache.", e);
            return new byte[0];
        }
    }

    /**
     * This method overrides the parent putBinary() method.
     * It will split the large value bytes into multiple chunks to fit into the internal Cache.
     * It generates a KeyHook to store the splitted chunked keys.
     */
    @Override
    public void putBinary(String keyS, byte[] valueB, int expiration) {
        if (Strings.isNullOrEmpty(keyS)) {
            return;
        }
        int nSplit = getValueSplit(config, keyS, valueB.length);
        Pair<KeyHook, byte[][]> keyValuePair = getKeyValuePair(nSplit, keyS, valueB);
        KeyHook keyHook = keyValuePair.getFirst();
        byte[][] splitValueB = keyValuePair.getSecond();

        if (logger.isDebugEnabled() || config.isEnableDebugLog()) {
            logger.debug("put key hook:{} to cache for hash key", keyHook);
        }
        super.putBinary(keyS, serializeValue(keyHook), expiration);
        if (nSplit > 1) {
            for (int i = 0; i < nSplit; i++) {
                if (logger.isDebugEnabled() || config.isEnableDebugLog()) {
                    logger.debug(String.format(Locale.ROOT, "Chunk[ %d ] bytes size before encoding  = %d", i,
                            splitValueB[i].length));
                }
                super.putBinary(keyHook.getChunkskey()[i], splitValueB[i], expiration);
            }
        }
    }

    @Override
    public void evict(String keyS) {
        if (Strings.isNullOrEmpty(keyS)) {
            return;
        }
        KeyHook keyHook = lookupKeyHook(keyS);
        if (keyHook == null) {
            return;
        }

        if (keyHook.getChunkskey() != null && keyHook.getChunkskey().length > 0) {
            String[] chunkKeys = keyHook.getChunkskey();
            for (String chunkKey : chunkKeys) {
                super.evict(chunkKey);
            }
        }
        super.evict(keyS);
    }

    protected Map<String, String> computeKeyHash(List<String> keySList) {
        return Maps.uniqueIndex(keySList, this::computeKeyHash);
    }

    private void deleteKeyHook(String keyS) {
        try {
            super.evict(keyS);
        } catch (IllegalStateException e) {
            // operation did not get queued in time (queue is full)
            errorCount.incrementAndGet();
            logger.error("Unable to queue cache operation: ", e);
        }
    }

    private byte[] concatBytes(byte[]... bytesArray) {
        int length = 0;
        for (byte[] bytes : bytesArray) {
            length += bytes.length;
        }
        byte[] result = new byte[length];
        int destPos = 0;
        for (byte[] bytes : bytesArray) {
            System.arraycopy(bytes, 0, result, destPos, bytes.length);
            destPos += bytes.length;
        }
        if (logger.isDebugEnabled() || config.isEnableDebugLog()) {
            logger.debug("Original value bytes size for all chunks  = {}", result.length);
        }

        return result;
    }

    @Override
    public KeyHook lookupKeyHook(String keyS) {
        byte[] bytes = super.getBinary(keyS);
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        return SerializationUtils.deserialize(bytes);
    }
}
