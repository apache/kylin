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

import org.apache.kylin.common.cache.CacheUpdater;
import org.apache.kylin.common.cache.LocalCacheUpdater;

/**
 * @author xjiang
 * 
 */
public abstract class AbstractRestCache<K, V> {

    protected static CacheUpdater cacheUpdater = new LocalCacheUpdater();

    public static void setCacheUpdater(CacheUpdater cu) {
        cacheUpdater = cu;
    }

    protected final Broadcaster.TYPE syncType;

    protected AbstractRestCache(Broadcaster.TYPE syncType) {
        this.syncType = syncType;
    }

    public abstract void put(K key, V value);

    public abstract void putLocal(K key, V value);

    public abstract void remove(K key);

    public abstract void removeLocal(K key);

    public abstract void clear();

    public abstract int size();
}
