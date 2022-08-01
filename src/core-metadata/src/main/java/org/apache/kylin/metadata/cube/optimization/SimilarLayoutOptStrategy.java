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
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.utils.IndexPlanReduceUtil;
import org.apache.kylin.metadata.model.NDataModel;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SimilarLayoutOptStrategy extends AbstractOptStrategy {

    public SimilarLayoutOptStrategy() {
        this.setType(GarbageLayoutType.SIMILAR);
    }

    @Override
    protected Set<Long> doCollect(List<LayoutEntity> inputLayouts, NDataflow dataflow, boolean needLog) {
        NDataSegment latestReadySegment = dataflow.getLatestReadySegment();
        if (latestReadySegment == null) {
            return Sets.newHashSet();
        }

        Map<Long, NDataLayout> dataLayoutMap = latestReadySegment.getLayoutsMap();
        Set<Long> garbageLayouts = Sets.newHashSet();
        Set<Pair<LayoutEntity, LayoutEntity>> sonToFatherLineageMap = buildLineage(inputLayouts);
        List<Pair<LayoutEntity, LayoutEntity>> similarList = retainSimilarLineage(sonToFatherLineageMap, dataLayoutMap);
        similarList.forEach(pair -> garbageLayouts.add(pair.getFirst().getId()));
        shiftLayoutHitCount(similarList, dataflow);
        if (needLog) {
            log.info("In dataflow({}), SimilarLayoutGcStrategy found garbage laoyouts: {}", dataflow.getId(),
                    similarList);
        }
        return garbageLayouts;
    }

    @Override
    protected void skipOptimizeTableIndex(List<LayoutEntity> inputLayouts) {
        inputLayouts.removeIf(layout -> IndexEntity.isTableIndex(layout.getId()));
    }

    private void shiftLayoutHitCount(List<Pair<LayoutEntity, LayoutEntity>> pairs, NDataflow dataflow) {
        Map<Long, Set<Long>> ancestorToChildren = Maps.newHashMap();
        pairs.forEach(pair -> {
            Long ancestor = pair.getSecond().getId();
            Long descendant = pair.getFirst().getId();
            ancestorToChildren.putIfAbsent(ancestor, Sets.newHashSet());
            ancestorToChildren.get(ancestor).add(descendant);
        });

        Map<Long, FrequencyMap> layoutHitCount = dataflow.getLayoutHitCount();

        // calculate frequencyMap from children
        Map<Long, FrequencyMap> ancestorFreqFromChildren = Maps.newHashMap();
        ancestorToChildren.forEach((ancestorId, children) -> {
            ancestorFreqFromChildren.putIfAbsent(ancestorId, new FrequencyMap());
            FrequencyMap ancestorFreqMap = ancestorFreqFromChildren.get(ancestorId);
            children.forEach(child -> {
                FrequencyMap frequencyMap = layoutHitCount.get(child);
                if (frequencyMap != null) {
                    NavigableMap<Long, Integer> tmp = frequencyMap.getDateFrequency();
                    tmp.forEach((date, cnt) -> ancestorFreqMap.getDateFrequency().merge(date, cnt, Integer::sum));
                }
            });
        });

        // merge frequencyMap from  children to ancestor
        ancestorToChildren.forEach((ancestorId, children) -> {
            layoutHitCount.putIfAbsent(ancestorId, new FrequencyMap());
            FrequencyMap frequencyMap = layoutHitCount.get(ancestorId);
            ancestorFreqFromChildren.get(ancestorId).getDateFrequency()
                    .forEach((date, cnt) -> frequencyMap.getDateFrequency().merge(date, cnt, Integer::sum));

        });
        dataflow.setLayoutHitCount(layoutHitCount);
    }

    private List<Pair<LayoutEntity, LayoutEntity>> retainSimilarLineage(
            Set<Pair<LayoutEntity, LayoutEntity>> sonToFatherLineageSet, Map<Long, NDataLayout> dataLayoutMap) {
        double relativeSimilarity = KylinConfig.getInstanceFromEnv().getLayoutSimilarityThreshold();
        double rejectSimilarThreshold = KylinConfig.getInstanceFromEnv().getSimilarityStrategyRejectThreshold();
        List<Pair<LayoutEntity, LayoutEntity>> retainedMap = Lists.newArrayList();
        sonToFatherLineageSet.forEach(pair -> {
            LayoutEntity son = pair.getFirst();
            LayoutEntity father = pair.getSecond();
            if (!dataLayoutMap.containsKey(son.getId()) || !dataLayoutMap.containsKey(father.getId())) {
                return;
            }
            NDataLayout sonData = dataLayoutMap.get(son.getId());
            NDataLayout fatherData = dataLayoutMap.get(father.getId());

            // for TableIndex, ignore similarity.
            if (IndexEntity.isTableIndex(son.getId())) {
                retainedMap.add(new Pair<>(son, father));
                return;
            }
            if (isSimilar(sonData, fatherData, relativeSimilarity, rejectSimilarThreshold)) {
                retainedMap.add(new Pair<>(son, father));
            }
        });
        return retainedMap;
    }

    private boolean isSimilar(NDataLayout son, NDataLayout father, double similarityThreshold, double rejectSimilar) {
        if (father.getRows() - son.getRows() > rejectSimilar) {
            return false;
        }
        double similarity = 1.0 * son.getRows() / father.getRows();
        return similarity >= similarityThreshold;
    }

    private Set<Pair<LayoutEntity, LayoutEntity>> buildLineage(List<LayoutEntity> inputLayouts) {
        Set<Pair<LayoutEntity, LayoutEntity>> lineageSet = Sets.newHashSet();
        Map<Set<Integer>, Set<LayoutEntity>> layoutsGroupByMeasures = Maps.newHashMap();
        inputLayouts.forEach(layout -> {
            Set<Integer> dimGroup = IndexEntity.isTableIndex(layout.getId()) //
                    ? Sets.newHashSet() // all table index add to one group
                    : layout.getColOrder().stream().filter(idx -> idx >= NDataModel.MEASURE_ID_BASE)
                            .collect(Collectors.toSet());
            layoutsGroupByMeasures.putIfAbsent(dimGroup, Sets.newHashSet());
            layoutsGroupByMeasures.get(dimGroup).add(layout);
        });

        layoutsGroupByMeasures.forEach((measures, layouts) -> {
            List<LayoutEntity> sortedLayouts = IndexPlanReduceUtil.descSortByColOrderSize(Lists.newArrayList(layouts));
            lineageSet.addAll(findLineage(sortedLayouts));
        });
        return lineageSet;
    }

    /**
     * Find a lineage from son layout to father layout.
     */
    private Set<Pair<LayoutEntity, LayoutEntity>> findLineage(List<LayoutEntity> sortedLayouts) {
        Set<Pair<LayoutEntity, LayoutEntity>> lineageSet = Sets.newHashSet();
        for (int i = 0; i < sortedLayouts.size(); i++) {
            LayoutEntity father = sortedLayouts.get(i);
            for (int j = i + 1; j < sortedLayouts.size(); j++) {
                LayoutEntity son = sortedLayouts.get(j);
                if (father.getColOrder().size() == son.getColOrder().size()
                        || !Objects.equals(son.getShardByColumns(), father.getShardByColumns())) {
                    continue;
                }

                List<Integer> fatherDims = father.getOrderedDimensions().keySet().asList();
                List<Integer> sonDims = son.getOrderedDimensions().keySet().asList();
                if (IndexPlanReduceUtil.isSubPartColOrder(sonDims, fatherDims)) {
                    lineageSet.add(new Pair<>(son, father));
                }
            }
        }

        return lineageSet;
    }
}
