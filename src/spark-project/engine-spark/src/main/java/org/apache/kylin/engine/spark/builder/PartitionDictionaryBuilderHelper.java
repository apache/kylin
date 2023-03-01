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

package org.apache.kylin.engine.spark.builder;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.PartitionStatusEnum;
import org.apache.kylin.metadata.cube.model.SegmentPartition;

import com.google.common.collect.Lists;

public class PartitionDictionaryBuilderHelper extends DictionaryBuilderHelper {

    public static Set<TblColRef> extractTreeRelatedGlobalDictToBuild(NDataSegment seg,
            Collection<IndexEntity> toBuildIndexEntities) {
        List<LayoutEntity> toBuildCuboids = Lists.newArrayList();
        for (IndexEntity desc : toBuildIndexEntities) {
            toBuildCuboids.addAll(desc.getLayouts());
        }

        List<LayoutEntity> buildedLayouts = Lists.newArrayList();
        if (seg.getSegDetails() != null) {
            Set<SegmentPartition> newPartitions = seg.getMultiPartitions().stream()
                    .filter(partition -> !partition.getStatus().equals(PartitionStatusEnum.READY))
                    .collect(Collectors.toSet());
            if (CollectionUtils.isEmpty(newPartitions)) {
                for (NDataLayout cuboid : seg.getSegDetails().getEffectiveLayouts()) {
                    buildedLayouts.add(cuboid.getLayout());
                }
            }
        }
        Set<TblColRef> buildedColRefSet = findNeedDictCols(buildedLayouts);
        Set<TblColRef> toBuildColRefSet = findNeedDictCols(toBuildCuboids);
        toBuildColRefSet.removeIf(buildedColRefSet::contains);
        return toBuildColRefSet;
    }
}
