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

package org.apache.kylin.storage;

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ImplementationSwitch;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.realization.IRealization;

/**
 */
public class StorageFactory {

    private static ImplementationSwitch<IStorage> storages;
    static {
        Map<Integer, String> impls = KylinConfig.getInstanceFromEnv().getStorageEngines();
        storages = new ImplementationSwitch<IStorage>(impls, IStorage.class);
    }

    public static IStorage storage(IStorageAware aware) {
        return storages.get(aware.getStorageType());
    }

    public static IStorageQuery createQuery(IRealization realization) {
        return storage(realization).createQuery(realization);
    }

    public static <T> T createEngineAdapter(IStorageAware aware, Class<T> engineInterface) {
        return storage(aware).adaptToBuildEngine(engineInterface);
    }

}
