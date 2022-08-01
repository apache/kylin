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

package org.apache.kylin.metadata.cube.optimization;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.utils.IndexPlanReduceUtil;

import com.google.common.collect.Sets;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * IncludedLayoutGcStrategy will search all garbage agg-layouts of the inputLayouts by default, but if
 * `kylin.garbage.remove-included-table-index` is true, it will also search garbage tableIndex-layouts.
 */
@Slf4j
public class IncludedLayoutOptStrategy extends AbstractOptStrategy {

    public IncludedLayoutOptStrategy() {
        this.setType(GarbageLayoutType.INCLUDED);
    }

    @Override
    public Set<Long> doCollect(List<LayoutEntity> inputLayouts, NDataflow dataflow, boolean needLog) {
        Set<Long> garbageLayouts = Sets.newHashSet();
        val fromGarbageToAliveMap = IndexPlanReduceUtil.collectIncludedLayouts(inputLayouts, true);
        fromGarbageToAliveMap.forEach((redundant, reserved) -> garbageLayouts.add(redundant.getId()));
        shiftLayoutHitCount(fromGarbageToAliveMap, dataflow);
        if (needLog) {
            log.info("In dataflow({}), IncludedLayoutGcStrategy found garbage laoyouts: {}", dataflow.getId(),
                    fromGarbageToAliveMap);
        }
        return garbageLayouts;
    }

    @Override
    protected void skipOptimizeTableIndex(List<LayoutEntity> inputLayouts) {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (!kylinConfig.isIncludedStrategyConsiderTableIndex()) {
            inputLayouts.removeIf(layout -> IndexEntity.isTableIndex(layout.getId()));
        }
    }

    /**
     * Put hit frequency of removed layouts to reserved layouts.
     */
    private void shiftLayoutHitCount(Map<LayoutEntity, LayoutEntity> removedToReservedMap, NDataflow dataflow) {
        Map<Long, FrequencyMap> layoutHitCount = dataflow.getLayoutHitCount();
        removedToReservedMap.forEach((removedLayout, reservedLayout) -> {
            FrequencyMap removedFreqMap = layoutHitCount.get(removedLayout.getId());
            if (removedFreqMap == null) {
                return;
            }
            layoutHitCount.putIfAbsent(reservedLayout.getId(), new FrequencyMap());
            layoutHitCount.get(reservedLayout.getId()).merge(removedFreqMap);
        });
        dataflow.setLayoutHitCount(layoutHitCount);
    }

}
