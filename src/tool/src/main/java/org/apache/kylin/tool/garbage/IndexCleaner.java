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

package org.apache.kylin.tool.garbage;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.optimization.GarbageLayoutType;
import org.apache.kylin.metadata.cube.optimization.IndexOptimizerFactory;
import org.apache.kylin.metadata.cube.optimization.event.ApproveRecsEvent;
import org.apache.kylin.metadata.cube.optimization.event.BuildIndexEvent;
import org.apache.kylin.metadata.cube.utils.IndexPlanReduceUtil;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.recommendation.ref.OptRecManagerV2;
import org.apache.kylin.metadata.recommendation.ref.OptRecV2;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Clarification(priority = Clarification.Priority.MAJOR, msg = "Enterprise")
public class IndexCleaner extends MetadataCleaner {

    private List<String> needUpdateModels = Lists.newArrayList();

    private Map<NDataflow, Map<Long, GarbageLayoutType>> needOptAggressivelyModels = Maps.newHashMap();

    private final boolean needAggressiveOpt;

    public IndexCleaner(String project, boolean needAggressiveOpt) {
        super(project);
        this.needAggressiveOpt = needAggressiveOpt;
    }

    @Override
    public void prepare() {
        log.info("Start to clean index in project {}", project);
        val config = KylinConfig.getInstanceFromEnv();
        val dataflowManager = NDataflowManager.getInstance(config, project);
        val projectInstance = NProjectManager.getInstance(config).getProject(project);

        if (projectInstance.isExpertMode()) {
            log.info("not semiautomode, can't run index clean");
            return;
        }

        OptRecManagerV2 recManagerV2 = OptRecManagerV2.getInstance(project);
        for (val model : dataflowManager.listUnderliningDataModels()) {
            val dataflow = dataflowManager.getDataflow(model.getId()).copy();
            Map<Long, GarbageLayoutType> garbageLayouts = IndexOptimizerFactory
                    .getOptimizer(dataflow, needAggressiveOpt, true).getGarbageLayoutMap(dataflow);

            if (needAggressiveOpt) {
                needOptAggressivelyModels.put(dataflow, garbageLayouts);
                continue;
            }

            if (MapUtils.isEmpty(garbageLayouts)) {
                continue;
            }

            boolean hasNewRecItem = recManagerV2.genRecItemsFromIndexOptimizer(project, model.getUuid(),
                    garbageLayouts);
            if (hasNewRecItem) {
                needUpdateModels.add(model.getId());
            }
        }

        log.info("Clean index in project {} finished", project);
    }

    @Override
    public void beforeExecute() {
        if (MapUtils.isEmpty(needOptAggressivelyModels)) {
            return;
        }

        approveRec();
        mergeSameDimAggLayout();
    }

    @Override
    public void execute() {
        if (MapUtils.isNotEmpty(needOptAggressivelyModels)) {
            cleanUpIndexAggressively();
        }

        if (needUpdateModels.isEmpty()) {
            return;
        }
        NDataModelManager mgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        needUpdateModels.forEach(modelId -> {
            NDataModel dataModel = mgr.getDataModelDesc(modelId);
            if (dataModel != null && !dataModel.isBroken()) {
                OptRecV2 optRecV2 = OptRecManagerV2.getInstance(project).loadOptRecV2(modelId);
                int newSize = optRecV2.getAdditionalLayoutRefs().size() + optRecV2.getRemovalLayoutRefs().size();
                if (dataModel.getRecommendationsCount() != newSize) {
                    mgr.updateDataModel(modelId, copyForWrite -> copyForWrite.setRecommendationsCount(newSize));
                }
            }
        });
    }

    @Override
    public void afterExecute() {
        if (MapUtils.isEmpty(needOptAggressivelyModels)) {
            return;
        }

        buildIndexes();
    }

    private List<Set<LayoutEntity>> getMergedLayouts(IndexPlan indexPlan, Map<Long, GarbageLayoutType> garbageLayouts) {
        List<LayoutEntity> merged = Lists.newArrayList();
        garbageLayouts.forEach(((layoutId, garbageLayoutType) -> {
            LayoutEntity layout = indexPlan.getLayoutEntity(layoutId);
            if (GarbageLayoutType.MERGED == garbageLayoutType && !Objects.isNull(layout)) {
                merged.add(layout);
            }
        }));

        return IndexPlanReduceUtil.collectSameDimAggLayouts(merged);
    }

    private void approveRec() {
        EventBusFactory eventBusF = EventBusFactory.getInstance();
        eventBusF.callService(new ApproveRecsEvent(project, needOptAggressivelyModels));
    }

    private void mergeSameDimAggLayout() {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        needOptAggressivelyModels.forEach((dataflow, garbageLayouts) -> {
            IndexPlan indexPlan = indexPlanManager.getIndexPlan(dataflow.getId());
            List<Set<LayoutEntity>> mergedLayouts = getMergedLayouts(indexPlan, garbageLayouts);
            if (CollectionUtils.isEmpty(mergedLayouts)) {
                return;
            }

            log.info("merge same dimension index for model: {} under project: {}", dataflow.getModelAlias(), project);
            indexPlanManager.updateIndexPlan(dataflow.getId(), copyForWrite -> {
                IndexPlan indexPlanMerged = IndexPlanReduceUtil.mergeSameDimLayout(indexPlan, mergedLayouts);
                copyForWrite.setIndexes(indexPlanMerged.getIndexes());
                copyForWrite.setLastModified(System.currentTimeMillis());
            });
        });
    }

    private void cleanUpIndexAggressively() {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        needOptAggressivelyModels.forEach((dataflow, garbageLayouts) -> {
            if (MapUtils.isEmpty(garbageLayouts)) {
                return;
            }
            log.info("aggressively clean up index for model: {} under project: {}", dataflow.getModelAlias(), project);
            IndexPlan indexPlan = indexPlanManager.getIndexPlan(dataflow.getId());
            deleteIndexes(indexPlan, garbageLayouts.keySet());
        });
    }

    private void deleteIndexes(IndexPlan indexPlan, Set<Long> garbageLayouts) {
        garbageLayouts.stream().map(layoutId -> indexPlan.getLayoutEntity(layoutId).getIndex())
                .forEachOrdered(index -> {
                    indexPlan.getIndexes().remove(index);
                    indexPlan.getToBeDeletedIndexes().add(index);
                });

        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        indexPlanManager.updateIndexPlan(indexPlan.getId(), copyForWrite -> {
            copyForWrite.setLastModified(System.currentTimeMillis());
            copyForWrite.getToBeDeletedIndexes()
                    .addAll(indexPlan.getToBeDeletedIndexes().stream()
                            .filter(index -> !copyForWrite.getToBeDeletedIndexes().contains(index))
                            .collect(Collectors.toList()));
            copyForWrite.getIndexes().removeAll(indexPlan.getToBeDeletedIndexes());
        });
    }

    private void buildIndexes() {
        EventBusFactory eventBusFactory = EventBusFactory.getInstance();
        eventBusFactory
                .callService(new BuildIndexEvent(project, Lists.newArrayList(needOptAggressivelyModels.keySet())));
    }
}
