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

import java.util.Set;

import com.google.common.collect.HashMultimap;

/**
 * @author xjiang
 * 
 */
public class MultiValueCache<K, V> extends AbstractRestCache<K, V> {

    private final HashMultimap<K, V> innerCache;

    public MultiValueCache(Broadcaster.TYPE syncType) {
        super(syncType);
        innerCache = HashMultimap.create();
    }

    public void put(K key, V value) {
        Broadcaster.EVENT eventType = innerCache.containsKey(key) ? Broadcaster.EVENT.UPDATE : Broadcaster.EVENT.CREATE;
        synchronized (this) {
            innerCache.put(key, value);
        }
        syncRemote(key, eventType);
    }

    public void putLocal(K key, V value) {
        synchronized (this) {
            innerCache.put(key, value);
        }
    }

    public void remove(K key) {
        if (innerCache.containsKey(key)) {
            innerCache.removeAll(key);
            syncRemote(key, Broadcaster.EVENT.DROP);
        }
    }

    public void removeLocal(K key) {
        if (innerCache.containsKey(key)) {
            innerCache.removeAll(key);
        }
    }

    public void clear() {
        innerCache.clear();
    }

    public int size() {
        return innerCache.size();
    }

    public Set<V> get(K key) {
        return innerCache.get(key);
    }

    public Set<K> keySet() {
        return innerCache.keySet();
    }

    public boolean containsKey(Object key) {
        return innerCache.containsKey(key);
    }

    public boolean containsEntry(Object key, Object value) {
        return innerCache.containsEntry(key, value);
    }
}
