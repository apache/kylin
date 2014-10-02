/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.common.restclient;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author xjiang
 */
public class SingleValueCache<K, V> extends AbstractRestCache<K, V> {

    private final Map<K, V> innerCache;

    public SingleValueCache(Broadcaster.TYPE syncType) {
        super(syncType);
        innerCache = new ConcurrentHashMap<K, V>();
    }

    public void put(K key, V value) {
        Broadcaster.EVENT eventType =
                innerCache.containsKey(key) ? Broadcaster.EVENT.UPDATE : Broadcaster.EVENT.CREATE;
        innerCache.put(key, value);
        syncRemote(key, eventType);
    }

    public void putLocal(K key, V value) {
        innerCache.put(key, value);
    }

    public void remove(K key) {
        if (innerCache.containsKey(key)) {
            innerCache.remove(key);
            syncRemote(key, Broadcaster.EVENT.DROP);
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
