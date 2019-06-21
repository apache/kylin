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

package org.apache.kylin.dict;

import java.lang.ref.SoftReference;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;

/**
 */
public abstract class CacheDictionary<T> extends Dictionary<T> {
    private static final long serialVersionUID = 1L;

    private transient LoadingCache<T, Integer> valueToIdCache;

    private transient SoftReference<byte[][]> idToValueByteCache;

    protected transient int baseId;

    protected BytesConverter<T> bytesConvert;

    CacheDictionary() {

    }

    //value --> id
    @Override
    protected final int getIdFromValueImpl(T value, int roundingFlag) {
        try {
            if (this.valueToIdCache != null && roundingFlag == 0) {
                cacheHitCount++;
                return valueToIdCache.get(value);
            }
        } catch (Exception th) {
            throw new IllegalArgumentException("Error to get Id From Value from Cache", th);
        }
        byte[] valueBytes = bytesConvert.convertToBytes(value);
        return getIdFromValueBytesWithoutCache(valueBytes, 0, valueBytes.length, roundingFlag);
    }

    //id --> value
    @Override
    protected final T getValueFromIdImpl(int id) {
        byte[] valueBytes = getValueBytesCacheFromIdImpl(id);
        return bytesConvert.convertFromBytes(valueBytes, 0, valueBytes.length);
    }

    private byte[] getValueBytesCacheFromIdImpl(int id) {
        byte[][] bytes = this.idToValueByteCache != null ? idToValueByteCache.get(): null;
        if (bytes != null) {
            int seq = calcSeqNoFromId(id);
            byte[] valueBytes = bytes[seq];
            if (valueBytes != null) {
                cacheHitCount++;
            } else {
                cacheMissCount++;
                valueBytes = getValueBytesFromIdWithoutCache(id);
                //add it to cache
                bytes[seq] = valueBytes;
            }
            return valueBytes;
        }
        return getValueBytesFromIdWithoutCache(id);
    }

    @Override
    protected byte[] getValueBytesFromIdImpl(int id) {
        byte[] valueBytes = getValueBytesCacheFromIdImpl(id);
        return bytesConvert.convertBytesValueFromBytes(valueBytes, 0, valueBytes.length);
    }

    final int calcSeqNoFromId(int id) {
        int seq = id - baseId;
        if (seq < 0 || seq >= getSize()) {
            throw new IllegalArgumentException("Not a valid ID: " + id);
        }
        return seq;
    }

    public final void enableCache() {
        if (this.valueToIdCache == null) {
            this.valueToIdCache = CacheBuilder
                    .newBuilder().softValues().expireAfterAccess(30, TimeUnit.MINUTES)
                    .maximumSize(KylinConfig.getInstanceFromEnv().getCachedDictionaryMaxEntrySize())
                    .build(new CacheLoader<T, Integer>() {
                        @Override
                        public Integer load(T value) {
                            cacheMissCount++;
                            cacheHitCount--;
                            byte[] valueBytes = bytesConvert.convertToBytes(value);
                            return getIdFromValueBytesWithoutCache(valueBytes, 0, valueBytes.length, 0);
                        }
                    });
        }
        if (this.idToValueByteCache == null)
            this.idToValueByteCache = new SoftReference<>(new byte[getSize()][]);
    }

    public final void disableCache() {
        this.valueToIdCache = null;
        this.idToValueByteCache = null;
    }

    protected abstract byte[] getValueBytesFromIdWithoutCache(int id);

    protected abstract int getIdFromValueBytesWithoutCache(byte[] valueBytes, int offset, int length, int roundingFlag);

}
