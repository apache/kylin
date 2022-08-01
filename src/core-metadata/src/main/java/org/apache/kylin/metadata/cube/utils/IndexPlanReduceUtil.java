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

package org.apache.kylin.metadata.cube.utils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.model.NDataModel;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class IndexPlanReduceUtil {

    private IndexPlanReduceUtil() {
    }

    public static Map<LayoutEntity, LayoutEntity> collectIncludedLayouts(List<LayoutEntity> inputLayouts,
            boolean isGarbageCleaning) {
        Map<LayoutEntity, LayoutEntity> redundantMap = Maps.newHashMap();
        Set<LayoutEntity> tableIndexGroup = Sets.newHashSet();
        Map<List<Integer>, Set<LayoutEntity>> aggIndexDimGroup = Maps.newHashMap();
        inputLayouts.forEach(layout -> {
            // layout.getOrderedDimensions() maybe more better, but difficult to cover with simple UT
            if (IndexEntity.isTableIndex(layout.getId())) {
                tableIndexGroup.add(layout);
            } else {
                List<Integer> aggIndexDims = layout.getColOrder().stream()
                        .filter(idx -> idx < NDataModel.MEASURE_ID_BASE).collect(Collectors.toList());
                aggIndexDimGroup.putIfAbsent(aggIndexDims, Sets.newHashSet());
                aggIndexDimGroup.get(aggIndexDims).add(layout);
            }
        });

        List<LayoutEntity> tableIndexsShareSameDims = descSortByColOrderSize(Lists.newArrayList(tableIndexGroup));
        redundantMap.putAll(findIncludedLayoutMap(tableIndexsShareSameDims, isGarbageCleaning));

        aggIndexDimGroup.forEach((dims, layouts) -> {
            List<LayoutEntity> aggIndexsShareSameDims = descSortByColOrderSize(Lists.newArrayList(layouts));
            redundantMap.putAll(findIncludedLayoutMap(aggIndexsShareSameDims, isGarbageCleaning));
        });

        return redundantMap;
    }

    /**
     * Collect a redundant map from included layout to reserved layout.
     * @param sortedLayouts sorted by layout's colOrder
     * @param isGarbageCleaning if true for gc, otherwise for auto-modeling tailor layout
     */
    private static Map<LayoutEntity, LayoutEntity> findIncludedLayoutMap(List<LayoutEntity> sortedLayouts,
            boolean isGarbageCleaning) {
        Map<LayoutEntity, LayoutEntity> includedMap = Maps.newHashMap();
        if (sortedLayouts.size() <= 1) {
            return includedMap;
        }

        for (int i = 0; i < sortedLayouts.size(); i++) {
            LayoutEntity target = sortedLayouts.get(i);
            if (includedMap.containsKey(target)) {
                continue;
            }
            for (int j = i + 1; j < sortedLayouts.size(); j++) {
                LayoutEntity current = sortedLayouts.get(j);
                // In the process of garbage cleaning all existing layouts were taken into account,
                // but in the process of propose only layouts with status of inProposing were taken into account.
                if ((!isGarbageCleaning && !current.isInProposing())
                        || (target.getColOrder().size() == current.getColOrder().size())
                        || includedMap.containsKey(current)
                        || !Objects.equals(current.getShardByColumns(), target.getShardByColumns())) {
                    continue;
                }

                if (isContained(current, target)) {
                    includedMap.put(current, target);
                }
            }
        }
        return includedMap;
    }

    /**
     * When two layouts comes from the same group, judge whether the current is contained by the target.
     * For AggIndex, only need to judge measures included; for TableIndex, compare colOrder.
     */
    private static boolean isContained(LayoutEntity current, LayoutEntity target) {
        boolean isTableIndex = IndexEntity.isTableIndex(target.getId());
        if (isTableIndex) {
            return isSubPartColOrder(current.getColOrder(), target.getColOrder());
        }
        Set<Integer> currentMeasures = Sets.newHashSet(current.getIndex().getMeasures());
        Set<Integer> targetMeasures = Sets.newHashSet(target.getIndex().getMeasures());
        return targetMeasures.containsAll(currentMeasures);
    }

    /**
     * Check whether current sequence is a part of target sequence.
     */
    public static boolean isSubPartColOrder(List<Integer> curSeq, List<Integer> targetSeq) {
        int i = 0;
        int j = 0;
        while (i < curSeq.size() && j < targetSeq.size()) {
            if (curSeq.get(i).intValue() == targetSeq.get(j).intValue()) {
                i++;
            }
            j++;
        }
        return i == curSeq.size() && j <= targetSeq.size();
    }

    // sort layout first to get a stable result for problem diagnosis
    public static List<LayoutEntity> descSortByColOrderSize(List<LayoutEntity> allLayouts) {
        allLayouts.sort((o1, o2) -> {
            if (o2.getColOrder().size() - o1.getColOrder().size() == 0) {
                return (int) (o1.getId() - o2.getId());
            }
            return o2.getColOrder().size() - o1.getColOrder().size();
        }); // desc by colOrder size
        return allLayouts;
    }
}
