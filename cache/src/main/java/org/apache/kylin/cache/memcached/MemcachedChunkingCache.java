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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.internal.BulkFuture;

import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;

/**
 * Subclass of MemcachedCache. It supports storing large objects.  Memcached itself has a limitation to the value size with default value of 1M.
 * This implement extends the limit to 1G and can split huge bytes to multiple chunks. It will take care of the data integrity if part of the chunks lost(due to server restart or other reasons)
 * 
 * @author mingmwang
 *
 */
public class MemcachedChunkingCache extends MemcachedCache implements KeyHookLookup{
    private static final Logger logger = LoggerFactory.getLogger(MemcachedChunkingCache.class);



    public MemcachedChunkingCache(MemcachedCache cache){
        super(cache);
        Preconditions.checkArgument(config.getMaxChunkSize() > 1, "maxChunkSize [%d] must be greater than 1", config.getMaxChunkSize());
        Preconditions.checkArgument(config.getMaxObjectSize() > 261, "maxObjectSize [%d] must be greater than 261", config.getMaxObjectSize());
    }
    
    /**
     * This method overrides the parent getBinary(), it gets the KeyHook from the Cache first and check the KeyHook that whether chunking is enabled or not.
     */
    @Override
    public byte[] getBinary(String key) {
        if(Strings.isNullOrEmpty(key)){
            return null;
        }
        KeyHook keyHook = lookupKeyHook(key);
        if (keyHook == null) {
            return null;
        }
        
        if(keyHook.getChunkskey() == null || keyHook.getChunkskey().length == 0) {
            if(logger.isDebugEnabled()){
                logger.debug("Chunking not enabled, return the value bytes in the keyhook directly, value bytes size = " + keyHook.getValues().length);
            }
            return keyHook.getValues();
        }
        
        BulkFuture<Map<String, Object>> bulkFuture;
        long start = System.currentTimeMillis();
        
        if(logger.isDebugEnabled()){
            logger.debug("Chunking enabled, chunk size = " + keyHook.getChunkskey().length);
        }
        
        Map<String, String> keyLookup = computeKeyHash(Arrays.asList(keyHook.getChunkskey()));
        try {
            bulkFuture = client.asyncGetBulk(keyLookup.keySet());
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
            Map<String, Object> bulkResult = bulkFuture.get(config.getTimeout(), TimeUnit.MILLISECONDS);
            cacheGetTime.addAndGet(System.currentTimeMillis() - start);
            if (bulkResult.size() != keyHook.getChunkskey().length) {
                missCount.incrementAndGet();
                logger.warn("Some paritial chunks missing for query key:" + key);
                //remove all the partital chunks here.
                for (String partitalKey : bulkResult.keySet()) {
                    client.delete(partitalKey);
                }
                deleteKeyHook(key);
                return null;
            }
            hitCount.getAndAdd(keyHook.getChunkskey().length);
            byte[][] bytesArray = new byte[keyHook.getChunkskey().length][];
            for (Map.Entry<String, Object> entry : bulkResult.entrySet()) {
                byte[] bytes = (byte[]) entry.getValue();
                readBytes.addAndGet(bytes.length);
                String originalKey = keyLookup.get(entry.getKey());
                int idx = Integer.parseInt(originalKey.substring(key.length()));
                bytesArray[idx] = decodeValue(originalKey.getBytes(Charsets.UTF_8), bytes);
            }
            return concatBytes(bytesArray);
        } catch (TimeoutException e) {
            timeoutCount.incrementAndGet();
            bulkFuture.cancel(false);
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        } catch (ExecutionException e) {
            errorCount.incrementAndGet();
            logger.error("ExecutionException when pulling item from cache.", e);
            return null;
        }
    }
    
    /**
     * This method overrides the parent putBinary() method. It will split the large value bytes into multiple chunks to fit into the internal Cache.
     * It generates a KeyHook to store the splitted chunked keys. 
     */
    @Override
    public void putBinary(String key, byte[] value, int expiration) {
        if(Strings.isNullOrEmpty(key)){
            return;
        }
        // the number 6 means the chunk number size never exceeds 6 bytes
        final int VALUE_SIZE = config.getMaxObjectSize() - Shorts.BYTES - Ints.BYTES - key.getBytes(Charsets.UTF_8).length - 6;
        final int MAX_VALUE_SIZE = config.getMaxChunkSize() * VALUE_SIZE;
        Preconditions.checkArgument(value.length<=MAX_VALUE_SIZE, "the value bytes length [%d] exceeds maximum value size [%d]", value.length, MAX_VALUE_SIZE);
        int split = (value.length%VALUE_SIZE == 0) ? (value.length/VALUE_SIZE) : (value.length/VALUE_SIZE + 1);
        String[] chunkskey = new String[split];
        byte[][] byteArray = splitBytes(value, VALUE_SIZE);
        KeyHook keyHook = null;
        if (split > 1) {
            if(logger.isDebugEnabled()){
                logger.debug("Enable chunking for putting large cached object values, chunk size = " + split + ", original value bytes size = " + value.length);
            }
            for (int i = 0; i < split; i++){
                chunkskey[i] = key + i;
            }
            keyHook = new KeyHook(chunkskey, null);
        }else{
            if(logger.isDebugEnabled()){
                logger.debug("Chunking not enabled, put the orignal value bytes to keyhook directly, original value bytes size = " + value.length);
            }
            keyHook = new KeyHook(null, value);
        }
        if(logger.isDebugEnabled()){
            logger.debug("put key hook:{} to cache for hash key", keyHook);
        }
        super.putBinary(key, SerializationUtils.serialize(keyHook), expiration);
        if (split > 1) {
            for (int i = 0; i < split; i++) {
                if(logger.isDebugEnabled()){
                    logger.debug("Chunk[" + i + "] bytes size before encoding  = " + byteArray[i].length);
                }
                super.putBinary(chunkskey[i], byteArray[i], expiration);
            }
        }
    }

    public void evict(String key) {
        if(Strings.isNullOrEmpty(key)){
            return;
        }
        KeyHook keyHook = lookupKeyHook(key);
        if (keyHook == null) {
            return;
        }

        if(keyHook.getChunkskey() != null && keyHook.getChunkskey().length > 0) {
            String[] chunkKeys = keyHook.getChunkskey();
            for (String chunkKey : chunkKeys) {
                super.evict(chunkKey);
            }
        }
        super.evict(key);
    }
    
    protected Map<String, String> computeKeyHash(List<String> keyList) {
        return Maps.uniqueIndex(keyList, new Function<String, String>() {
            @Override
            public String apply(String key) {
                return computeKeyHash(key);
            }
        });
    }
    
    private void deleteKeyHook(String key) {
        try {
            super.evict(key);
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
        if(logger.isDebugEnabled()){
            logger.debug("Original value bytes size for all chunks  = " + result.length);
        }
        
        return result;
    }

    private byte[][] splitBytes(final byte[] data, final int valueSize) {
        final int length = data.length;
        final byte[][] dest = new byte[(length + valueSize - 1) / valueSize][];
        int destIndex = 0;
        int stopIndex = 0;

        for (int startIndex = 0; startIndex + valueSize <= length; startIndex += valueSize) {
            stopIndex += valueSize;
            dest[destIndex++] = Arrays.copyOfRange(data, startIndex, stopIndex);
        }
        if (stopIndex < length)
            dest[destIndex] = Arrays.copyOfRange(data, stopIndex, length);
        return dest;
    }

    @Override
    public KeyHook lookupKeyHook(String key) {
        byte[] bytes = super.getBinary(key);
        if(bytes == null){
            return null;
        }
        return (KeyHook) SerializationUtils.deserialize(bytes);
    }
}
