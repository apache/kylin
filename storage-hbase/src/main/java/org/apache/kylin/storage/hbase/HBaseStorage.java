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

package org.apache.kylin.storage.hbase;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.engine.mr.IMROutput;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.ICachableStorageQuery;
import org.apache.kylin.storage.IStorage;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.cache.CacheFledgedDynamicQuery;
import org.apache.kylin.storage.cache.CacheFledgedStaticQuery;
import org.apache.kylin.storage.hbase.steps.HBaseMROutput;
import org.apache.kylin.storage.hbase.steps.HBaseMROutput2;
import org.apache.kylin.storage.hbase.steps.HBaseMROutput2Transition;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.apache.kylin.storage.hybrid.HybridStorageQuery;

import com.google.common.base.Preconditions;

@SuppressWarnings("unused")//used by reflection
public class HBaseStorage implements IStorage {

    private final static boolean allowStorageLayerCache = true;
    private final static String defaultCubeStorageQuery = "org.apache.kylin.storage.hbase.cube.v1.CubeStorageQuery";
    private final static String defaultIIStorageQuery = "org.apache.kylin.storage.hbase.ii.InvertedIndexStorageQuery";

    @Override
    public IStorageQuery createQuery(IRealization realization) {
        if (realization.getType() == RealizationType.INVERTED_INDEX) {
            ICachableStorageQuery ret;
            try {
                ret = (ICachableStorageQuery) Class.forName(defaultIIStorageQuery).getConstructor(IIInstance.class).newInstance((IIInstance) realization);
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize storage query for " + defaultIIStorageQuery, e);
            }

            if (allowStorageLayerCache) {
                return wrapWithCache(ret, realization);
            } else {
                return ret;
            }
        } else if (realization.getType() == RealizationType.CUBE) {
            ICachableStorageQuery ret;
            try {
                ret = (ICachableStorageQuery) Class.forName(defaultCubeStorageQuery).getConstructor(CubeInstance.class).newInstance((CubeInstance) realization);
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize storage query for " + defaultCubeStorageQuery, e);
            }

            if (allowStorageLayerCache) {
                return wrapWithCache(ret, realization);
            } else {
                return ret;
            }
        } else {
            throw new IllegalArgumentException("Unknown realization type " + realization.getType());
        }
    }
    
    private static IStorageQuery wrapWithCache(ICachableStorageQuery underlyingStorageEngine, IRealization realization) {
        if (underlyingStorageEngine.isDynamic()) {
            return new CacheFledgedDynamicQuery(underlyingStorageEngine, getPartitionCol(realization));
        } else {
            return new CacheFledgedStaticQuery(underlyingStorageEngine);
        }
    }

    private static TblColRef getPartitionCol(IRealization realization) {
        String modelName = realization.getModelName();
        DataModelDesc dataModelDesc = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv()).getDataModelDesc(modelName);
        PartitionDesc partitionDesc = dataModelDesc.getPartitionDesc();
        Preconditions.checkArgument(partitionDesc != null, "PartitionDesc for " + realization + " is null!");
        TblColRef partitionColRef = partitionDesc.getPartitionDateColumnRef();
        Preconditions.checkArgument(partitionColRef != null, "getPartitionDateColumnRef for " + realization + " is null");
        return partitionColRef;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        if (engineInterface == IMROutput.class) {
            return (I) new HBaseMROutput();
        } else if (engineInterface == IMROutput2.class) {
            return (I) new HBaseMROutput2Transition();
        } else {
            throw new RuntimeException("Cannot adapt to " + engineInterface);
        }
    }
}
