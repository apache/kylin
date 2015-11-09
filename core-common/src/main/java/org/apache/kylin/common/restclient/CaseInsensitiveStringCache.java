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

import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.kylin.common.KylinConfig;

/**
 */
public class CaseInsensitiveStringCache<V> extends SingleValueCache<String, V> {

    public CaseInsensitiveStringCache(KylinConfig config, Broadcaster.TYPE syncType) {
        super(config, syncType, new ConcurrentSkipListMap<String, V>(String.CASE_INSENSITIVE_ORDER));
    }

    @Override
    public void put(String key, V value) {
        super.put(key, value);
    }

    @Override
    public void putLocal(String key, V value) {
        super.putLocal(key, value);
    }
}
