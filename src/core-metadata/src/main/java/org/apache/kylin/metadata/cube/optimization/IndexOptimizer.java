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
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.project.NProjectManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.Getter;

@Clarification(priority = Clarification.Priority.MAJOR, msg = "Enterprise")
public class IndexOptimizer {

    // if true, every strategy will print log in details
    private boolean needLog;

    @Getter
    private final List<AbstractOptStrategy> strategiesForAuto = Lists.newArrayList();

    @Getter
    private final List<AbstractOptStrategy> strategiesForManual = Lists.newArrayList();

    public IndexOptimizer(boolean needLog) {
        this.needLog = needLog;
    }

    protected List<LayoutEntity> filterAutoLayouts(NDataflow dataflow) {
        return dataflow.extractReadyLayouts().stream() //
                .filter(layout -> !layout.isManual() && layout.isAuto()) //
                .filter(layout -> !layout.isBase()).collect(Collectors.toList());
    }

    protected List<LayoutEntity> filterManualLayouts(NDataflow dataflow) {
        return dataflow.extractReadyLayouts().stream() //
                .filter(LayoutEntity::isManual) //
                .filter(layout -> !layout.isBase()).collect(Collectors.toList());
    }

    public Map<Long, GarbageLayoutType> getGarbageLayoutMap(NDataflow dataflow) {
        if (NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()) //
                .getProject(dataflow.getProject()).isExpertMode()) {
            return Maps.newHashMap();
        }

        Map<Long, GarbageLayoutType> garbageLayoutTypeMap = Maps.newHashMap();
        for (AbstractOptStrategy strategy : getStrategiesForAuto()) {
            strategy.collectGarbageLayouts(filterAutoLayouts(dataflow), dataflow, needLog)
                    .forEach(id -> garbageLayoutTypeMap.put(id, strategy.getType()));
        }

        for (AbstractOptStrategy strategy : getStrategiesForManual()) {
            strategy.collectGarbageLayouts(filterManualLayouts(dataflow), dataflow, needLog)
                    .forEach(id -> garbageLayoutTypeMap.put(id, strategy.getType()));
        }

        return garbageLayoutTypeMap;
    }

    @VisibleForTesting
    public static Set<Long> findGarbageLayouts(NDataflow dataflow, AbstractOptStrategy strategy) {
        List<LayoutEntity> allLayouts = dataflow.getIndexPlan().getAllLayouts();
        return strategy.collectGarbageLayouts(allLayouts, dataflow, false);
    }
}
