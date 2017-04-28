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

import org.apache.kylin.common.util.Dictionary;

import java.lang.ref.SoftReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public abstract class CacheDictionary<T> extends Dictionary<T> {
    private static final long serialVersionUID = 1L;

    private transient SoftReference<ConcurrentHashMap> valueToIdCache;

    private transient SoftReference<Object[]> idToValueCache;

    protected transient int baseId;

    protected BytesConverter<T> bytesConvert;

    public CacheDictionary() {

    }

    //value --> id
    @Override
    protected final int getIdFromValueImpl(T value, int roundingFlag) {
        if (this.valueToIdCache != null && roundingFlag == 0) {
            Map cache = valueToIdCache.get(); // SoftReference to skip cache gracefully when short of memory
            if (cache != null) {
                Integer id;
                id = (Integer) cache.get(value);
                if (id != null)
                    return id.intValue();
                byte[] valueBytes = bytesConvert.convertToBytes(value);
                id = getIdFromValueBytesWithoutCache(valueBytes, 0, valueBytes.length, roundingFlag);
                cache.put(value, id);
                return id;
            }
        }
        byte[] valueBytes = bytesConvert.convertToBytes(value);
        return getIdFromValueBytesWithoutCache(valueBytes, 0, valueBytes.length, roundingFlag);
    }

    //id --> value
    @Override
    protected final T getValueFromIdImpl(int id) {
        if (this.idToValueCache != null) {
            Object[] cache = idToValueCache.get();
            if (cache != null) {
                int seq = calcSeqNoFromId(id);
                if (cache[seq] != null)
                    return (T) cache[seq];
                byte[] valueBytes = getValueBytesFromIdWithoutCache(id);
                T value = bytesConvert.convertFromBytes(valueBytes, 0, valueBytes.length);
                cache[seq] = value;
                return value;
            }
        }
        byte[] valueBytes = getValueBytesFromIdWithoutCache(id);
        return bytesConvert.convertFromBytes(valueBytes, 0, valueBytes.length);
    }

    protected final int calcSeqNoFromId(int id) {
        int seq = id - baseId;
        if (seq < 0 || seq >= getSize()) {
            throw new IllegalArgumentException("Not a valid ID: " + id);
        }
        return seq;
    }

    public final void enableCache() {
        if (this.valueToIdCache == null)
            this.valueToIdCache = new SoftReference<>(new ConcurrentHashMap());
        if (this.idToValueCache == null)
            this.idToValueCache = new SoftReference<>(new Object[getSize()]);
    }

    public final void disableCache() {
        this.valueToIdCache = null;
        this.idToValueCache = null;
    }

    abstract protected byte[] getValueBytesFromIdWithoutCache(int id);

    abstract protected int getIdFromValueBytesWithoutCache(byte[] valueBytes, int offset, int length, int roundingFlag);

}
