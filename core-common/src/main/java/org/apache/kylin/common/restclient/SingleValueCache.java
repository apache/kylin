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

package org.apache.kylin.common.restclient;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author xjiang
 * 
 */
public abstract class SingleValueCache<K, V> extends AbstractRestCache<K, V> {

    private final ConcurrentMap<K, V> innerCache;

    public SingleValueCache(Broadcaster.TYPE syncType) {
        this(syncType, new ConcurrentHashMap<K, V>());
    }

    public SingleValueCache(Broadcaster.TYPE syncType, ConcurrentMap<K, V> innerCache) {
        super(syncType);
        this.innerCache = innerCache;
    }

    public void put(K key, V value) {
        boolean exists = innerCache.containsKey(key);

        if (!exists) {
            cacheUpdater.updateCache(key, value, Broadcaster.EVENT.CREATE, syncType, this);
        } else {
            cacheUpdater.updateCache(key, value, Broadcaster.EVENT.UPDATE, syncType, this);
        }
    }

    public void putLocal(K key, V value) {
        innerCache.put(key, value);
    }

    public void remove(K key) {
        if (innerCache.containsKey(key)) {
            cacheUpdater.updateCache(key,null,Broadcaster.EVENT.DROP,syncType,this);
        }
    }

    public void removeLocal(K key) {
         innerCache.remove(key);
    }

    public void clear() {
        innerCache.clear();
    }

    public int size() {
        return innerCache.size();
    }

    public V get(K key) {
        return innerCache.get(key);
    }

    public Collection<V> values() {
        return innerCache.values();
    }

    public boolean containsKey(String key) {
        return innerCache.containsKey(key);
    }

    public Map<K, V> getMap() {
        return Collections.unmodifiableMap(innerCache);
    }

    public Set<K> keySet() {
        return innerCache.keySet();
  }
}
