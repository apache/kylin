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

package org.apache.kylin.metadata.cube.storage;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.optimization.IndexOptimizer;
import org.apache.kylin.metadata.cube.optimization.IndexOptimizerFactory;
import org.apache.kylin.metadata.model.NDataModel;

import com.google.common.collect.Maps;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GarbageStorageCollector implements StorageInfoCollector {

    @Override
    public void collect(KylinConfig config, String project, StorageVolumeInfo storageVolumeInfo) {
        Map<String, Set<Long>> garbageIndexMap = Maps.newHashMap();
        long storageSize = 0L;

        for (val model : getModels(project)) {
            val dataflow = getDataflow(model).copy();

            final IndexOptimizer indexOptimizer = IndexOptimizerFactory.getOptimizer(dataflow, false);
            val garbageLayouts = indexOptimizer.getGarbageLayoutMap(dataflow).keySet();
            if (CollectionUtils.isNotEmpty(garbageLayouts)) {
                storageSize += calculateLayoutSize(garbageLayouts, dataflow);
                garbageIndexMap.put(model.getId(), garbageLayouts);
            }
        }

        storageVolumeInfo.setGarbageModelIndexMap(garbageIndexMap);
        storageVolumeInfo.setGarbageStorageSize(storageSize);
    }

    private List<NDataModel> getModels(String project) {
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        return dataflowManager.listUnderliningDataModels();
    }

    private NDataflow getDataflow(NDataModel model) {
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        return dataflowManager.getDataflow(model.getUuid());
    }

    private long calculateLayoutSize(Set<Long> cuboidLayoutIdSet, NDataflow dataflow) {
        long cuboidLayoutSize = 0L;
        for (NDataSegment segment : dataflow.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING)) {
            for (Long cuboidLayoutId : cuboidLayoutIdSet) {
                NDataLayout dataCuboid = segment.getSegDetails().getLayoutById(cuboidLayoutId);
                if (dataCuboid != null) {
                    cuboidLayoutSize += dataCuboid.getByteSize();
                }
            }
        }
        return cuboidLayoutSize;
    }

}
