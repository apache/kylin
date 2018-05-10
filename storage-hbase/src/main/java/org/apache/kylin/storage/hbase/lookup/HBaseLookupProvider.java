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

package org.apache.kylin.storage.hbase.lookup;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfo;
import org.apache.kylin.dict.lookup.IExtLookupProvider;
import org.apache.kylin.dict.lookup.IExtLookupTableCache;
import org.apache.kylin.dict.lookup.ILookupTable;
import org.apache.kylin.dict.lookup.cache.RocksDBLookupTableCache;
import org.apache.kylin.engine.mr.ILookupMaterializer;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Use HBase as lookup table storage
 */
public class HBaseLookupProvider implements IExtLookupProvider{
    protected static final Logger logger = LoggerFactory.getLogger(HBaseLookupProvider.class);


    @Override
    public ILookupTable getLookupTable(TableDesc tableDesc, ExtTableSnapshotInfo extTableSnapshot) {
        return new HBaseLookupTable(tableDesc, extTableSnapshot);
    }

    @Override
    public IExtLookupTableCache getLocalCache() {
        return RocksDBLookupTableCache.getInstance(KylinConfig.getInstanceFromEnv());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        if (engineInterface == ILookupMaterializer.class) {
            return (I) new HBaseLookupMaterializer();
        } else {
            throw new RuntimeException("Cannot adapt to " + engineInterface);
        }
    }
}
