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

package org.apache.kylin.rec.index;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.utils.IndexPlanReduceUtil;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.common.AccelerateInfo;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class IndexReducer extends AbstractIndexProposer {

    IndexReducer(AbstractContext.ModelContext context) {
        super(context);
    }

    @Override
    public IndexPlan execute(IndexPlan indexPlan) {

        log.debug("Start to reduce redundant layouts...");
        List<IndexEntity> allProposedIndexes = indexPlan.getIndexes();

        // collect redundant layouts
        List<LayoutEntity> layoutsToHandle = indexPlan.getAllLayouts();
        if (!KylinConfig.getInstanceFromEnv().isIncludedStrategyConsiderTableIndex()) {
            layoutsToHandle.removeIf(layout -> IndexEntity.isTableIndex(layout.getId()));
        }
        Map<LayoutEntity, LayoutEntity> redundantToReservedMap = Maps.newHashMap();
        redundantToReservedMap.putAll(IndexPlanReduceUtil.collectIncludedLayouts(layoutsToHandle, false));

        redundantToReservedMap.forEach((redundant, reserved) -> {
            val indexEntityOptional = allProposedIndexes.stream()
                    .filter(index -> index.getId() == redundant.getIndexId()) //
                    .findFirst();
            indexEntityOptional.ifPresent(entity -> entity.getLayouts().remove(redundant));
        });

        Set<String> redundantRecord = Sets.newHashSet();
        redundantToReservedMap.forEach((key, value) -> redundantRecord.add(key.getId() + "->" + value.getId()));
        log.trace("In this round, IndexPlan({}) found redundant layout(s) is|are: {}", indexPlan.getUuid(),
                String.join(", ", redundantRecord));

        // remove indexes without layouts
        List<IndexEntity> allReservedIndexList = allProposedIndexes.stream()
                .filter(indexEntity -> !indexEntity.getLayouts().isEmpty()) //
                .collect(Collectors.toList());
        log.debug("Proposed {} indexes, {} indexes will be reserved.", allProposedIndexes.size(),
                allReservedIndexList.size());
        allReservedIndexList.forEach(index -> index.getLayouts().forEach(layout -> layout.setInProposing(false)));
        indexPlan.setIndexes(allReservedIndexList);
        cleanRedundantLayoutRecommendations(redundantToReservedMap);

        // remove reduced index for indexRexItem
        Set<Long> layoutIds = indexPlan.getAllLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        context.setIndexRexItemMap(context.getIndexRexItemMap().entrySet().stream()
                .filter(e -> layoutIds.contains(e.getValue().getLayout().getId()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

        // adjust acceleration info
        adjustAccelerationInfo(redundantToReservedMap);
        log.debug("End of reduce indexes and layouts!");
        return indexPlan;
    }

    private void cleanRedundantLayoutRecommendations(Map<LayoutEntity, LayoutEntity> redundantToReserveMap) {
        if (context.getProposeContext().skipCollectRecommendations()) {
            return;
        }
        redundantToReserveMap
                .forEach((redundant, reserved) -> context.getIndexRexItemMap().remove(redundant.genUniqueContent()));
    }

    private void adjustAccelerationInfo(Map<LayoutEntity, LayoutEntity> redundantToReservedMap) {
        Map<String, AccelerateInfo> accelerateInfoMap = context.getProposeContext().getAccelerateInfoMap();
        Map<Long, LayoutEntity> redundantMap = Maps.newHashMap();
        redundantToReservedMap.forEach((redundant, reserved) -> redundantMap.putIfAbsent(redundant.getId(), redundant));
        accelerateInfoMap.forEach((key, value) -> {
            if (value.getRelatedLayouts() == null) {
                return;
            }
            value.getRelatedLayouts().forEach(relatedLayout -> {
                if (redundantMap.containsKey(relatedLayout.getLayoutId())) {
                    LayoutEntity entity = redundantMap.get(relatedLayout.getLayoutId());
                    // confirm layoutInfo in accelerationInfoMap equals to redundant layout
                    if (entity.getIndex().getIndexPlan().getUuid().equalsIgnoreCase(relatedLayout.getModelId())) {
                        LayoutEntity reserved = redundantToReservedMap.get(entity);
                        relatedLayout.setLayoutId(reserved.getId());
                        relatedLayout.setModelId(reserved.getIndex().getIndexPlan().getUuid());
                    }
                }
            });
            value.setRelatedLayouts(Sets.newHashSet(value.getRelatedLayouts())); // may exist equal objects
        });
    }

}
