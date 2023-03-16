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
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.project.NProjectManager;

import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

/**
 * LowFreqLayoutGcStrategy will search all garbage layout in inputLayouts.
 */
@Slf4j
public class LowFreqLayoutOptStrategy extends AbstractOptStrategy {

    public LowFreqLayoutOptStrategy() {
        this.setType(GarbageLayoutType.LOW_FREQUENCY);
    }

    @Override
    public Set<Long> doCollect(List<LayoutEntity> inputLayouts, NDataflow dataflow, boolean needLog) {
        ProjectInstance projectInstance = NProjectManager.getInstance(dataflow.getConfig())
                .getProject(dataflow.getProject());
        Map<Long, FrequencyMap> hitFrequencyMap = dataflow.getLayoutHitCount();
        int days = projectInstance.getConfig().getFrequencyTimeWindowInDays();
        Set<Long> garbageLayouts = Sets.newHashSet();
        inputLayouts.forEach(layout -> {
            if (TimeUtil.minusDays(System.currentTimeMillis(), days) >= layout.getUpdateTime()) {
                FrequencyMap frequencyMap = hitFrequencyMap.get(layout.getId());
                if (frequencyMap == null) {
                    frequencyMap = new FrequencyMap();
                }

                if (frequencyMap.isLowFrequency(dataflow.getProject())) {
                    garbageLayouts.add(layout.getId());
                }
            }
        });

        if (needLog) {
            log.info("In dataflow({}), LowFreqLayoutGcStrategy found garbageLayouts: {}", dataflow.getId(),
                    garbageLayouts);
        }
        return garbageLayouts;
    }

    @Override
    protected void skipOptimizeTableIndex(List<LayoutEntity> inputLayouts) {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (!kylinConfig.isLowFreqStrategyConsiderTableIndex()) {
            inputLayouts.removeIf(layout -> IndexEntity.isTableIndex(layout.getId()));
        }
    }
}
