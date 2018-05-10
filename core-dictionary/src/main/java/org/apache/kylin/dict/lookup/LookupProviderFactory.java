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

package org.apache.kylin.dict.lookup;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Map;

import org.apache.kylin.dict.lookup.IExtLookupTableCache.CacheState;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class LookupProviderFactory {
    private static final Logger logger = LoggerFactory.getLogger(LookupProviderFactory.class);
    private static Map<String, String> lookupProviderImplClassMap = Maps.newConcurrentMap();

    static {
        registerLookupProvider(ExtTableSnapshotInfo.STORAGE_TYPE_HBASE,
                "org.apache.kylin.storage.hbase.lookup.HBaseLookupProvider");
    }

    public static void registerLookupProvider(String storageType, String implClassName) {
        lookupProviderImplClassMap.put(storageType, implClassName);
    }

    public static IExtLookupProvider getExtLookupProvider(String storageType) {
        String className = lookupProviderImplClassMap.get(storageType);
        if (className == null) {
            throw new IllegalStateException("no implementation class found for storage type:" + storageType);
        }
        try {
            Class clazz = Class.forName(className);
            Constructor constructor = clazz.getConstructor();
            return (IExtLookupProvider) constructor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("the lookup implementation class is invalid for storage type:"
                    + storageType, e);
        }
    }

    public static ILookupTable getInMemLookupTable(TableDesc tableDesc, String[] pkCols, IReadableTable readableTable)
            throws IOException {
        return new LookupStringTable(tableDesc, pkCols, readableTable);
    }

    public static ILookupTable getExtLookupTable(TableDesc tableDesc, ExtTableSnapshotInfo extTableSnapshot) {
        IExtLookupTableCache extLookupTableCache = getExtLookupProvider(extTableSnapshot.getStorageType()).getLocalCache();
        if (extLookupTableCache == null) {
            return getExtLookupTableWithoutCache(tableDesc, extTableSnapshot);
        }
        ILookupTable cachedLookupTable = extLookupTableCache.getCachedLookupTable(tableDesc, extTableSnapshot, true);
        if (cachedLookupTable != null) {
            logger.info("try to use cached lookup table:{}", extTableSnapshot.getResourcePath());
            return cachedLookupTable;
        }
        logger.info("use ext lookup table:{}", extTableSnapshot.getResourcePath());
        return getExtLookupTableWithoutCache(tableDesc, extTableSnapshot);
    }

    public static ILookupTable getExtLookupTableWithoutCache(TableDesc tableDesc, ExtTableSnapshotInfo extTableSnapshot) {
        IExtLookupProvider provider = getExtLookupProvider(extTableSnapshot.getStorageType());
        return provider.getLookupTable(tableDesc, extTableSnapshot);
    }

    public static <T> T createEngineAdapter(String lookupStorageType, Class<T> engineInterface) {
        IExtLookupProvider provider = getExtLookupProvider(lookupStorageType);
        return provider.adaptToBuildEngine(engineInterface);
    }

    public static void rebuildLocalCache(TableDesc tableDesc, ExtTableSnapshotInfo extTableSnapshotInfo) {
        IExtLookupTableCache tablesCache = getExtLookupProvider(extTableSnapshotInfo.getStorageType()).getLocalCache();
        if (tablesCache != null) {
            tablesCache.buildSnapshotCache(tableDesc, extTableSnapshotInfo, getExtLookupTableWithoutCache(tableDesc, extTableSnapshotInfo));
        }
    }

    public static void removeLocalCache(ExtTableSnapshotInfo extTableSnapshotInfo) {
        IExtLookupTableCache tablesCache = getExtLookupProvider(extTableSnapshotInfo.getStorageType()).getLocalCache();
        if (tablesCache != null) {
            tablesCache.removeSnapshotCache(extTableSnapshotInfo);
        }
    }

    public static CacheState getCacheState(ExtTableSnapshotInfo extTableSnapshotInfo) {
        IExtLookupTableCache tablesCache = getExtLookupProvider(extTableSnapshotInfo.getStorageType()).getLocalCache();
        if (tablesCache != null) {
            return tablesCache.getCacheState(extTableSnapshotInfo);
        }
        return CacheState.NONE;
    }

}
