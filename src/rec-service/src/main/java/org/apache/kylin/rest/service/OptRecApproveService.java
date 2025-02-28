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

package org.apache.kylin.rest.service;

import static org.apache.kylin.common.exception.ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE;
import static org.apache.kylin.common.exception.ServerErrorCode.UNSUPPORTED_REC_OPERATION_TYPE;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.constant.LogConstant;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.guava30.shaded.common.eventbus.Subscribe;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.optimization.FrequencyMap;
import org.apache.kylin.metadata.cube.optimization.GarbageLayoutType;
import org.apache.kylin.metadata.cube.optimization.IndexOptimizerFactory;
import org.apache.kylin.metadata.cube.optimization.event.ApproveRecsEvent;
import org.apache.kylin.metadata.cube.utils.IndexPlanReduceUtil;
import org.apache.kylin.metadata.job.JobTokenItem;
import org.apache.kylin.metadata.job.JobTokenManager;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.candidate.RawRecManager;
import org.apache.kylin.metadata.recommendation.entity.CCRecItemV2;
import org.apache.kylin.metadata.recommendation.ref.CCRef;
import org.apache.kylin.metadata.recommendation.ref.DimensionRef;
import org.apache.kylin.metadata.recommendation.ref.LayoutRef;
import org.apache.kylin.metadata.recommendation.ref.MeasureRef;
import org.apache.kylin.metadata.recommendation.ref.ModelColumnRef;
import org.apache.kylin.metadata.recommendation.ref.OptRecManagerV2;
import org.apache.kylin.metadata.recommendation.ref.OptRecV2;
import org.apache.kylin.metadata.recommendation.ref.RecommendationRef;
import org.apache.kylin.metadata.recommendation.util.RawRecUtil;
import org.apache.kylin.rest.request.OptRecRequest;
import org.apache.kylin.rest.response.OpenRecApproveResponse;
import org.apache.kylin.rest.response.OptRecLayoutResponse;
import org.apache.kylin.rest.response.OptRecLayoutsResponse;
import org.apache.kylin.rest.response.OptRecResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.stereotype.Component;

import lombok.Getter;

@EnableDiscoveryClient
@Component("optRecApproveService")
public class OptRecApproveService extends BasicService {
    private static final Logger log = LoggerFactory.getLogger(LogConstant.SMART_CATEGORY);

    @Autowired
    public AclEvaluate aclEvaluate;

    @Autowired
    private ModelService modelService;

    @Autowired
    private IndexPlanService indexPlanService;

    @Autowired
    private OptRecService optRecService;

    private static final class RecApproveContext {
        private final Map<Integer, NDataModel.NamedColumn> columns = Maps.newHashMap();
        private final Map<Integer, NDataModel.NamedColumn> dimensions = Maps.newHashMap();
        private final Map<Integer, NDataModel.Measure> measures = Maps.newHashMap();
        private final List<Long> addedLayoutIdList = Lists.newArrayList();
        private final List<Long> removedLayoutIdList = Lists.newArrayList();
        private final Map<String, NDataModel.Measure> functionToMeasureMap = Maps.newHashMap();
        private Set<Integer> abnormalRecIds = Sets.newHashSet();

        @Getter
        private final OptRecV2 recommendation;
        private final Map<Integer, String> userDefinedRecNameMap;
        private final String project;

        private RecApproveContext(String project, String modelId, Map<Integer, String> userDefinedRecNameMap) {
            this.project = project;
            this.userDefinedRecNameMap = userDefinedRecNameMap;
            this.recommendation = OptRecManagerV2.getInstance(project).loadOptRecV2(modelId);
            RecNameChecker.checkCCRecConflictWithColumn(recommendation, project, userDefinedRecNameMap);
            RecNameChecker.checkUserDefinedRecNames(recommendation, project, userDefinedRecNameMap);
        }

        public List<RawRecItem> approveRawRecItems(List<Integer> recItemIds, boolean isAdd) {
            recItemIds.forEach(id -> OptRecService.checkRecItemIsValidAndReturn(recommendation, id, isAdd));
            List<RawRecItem> rawRecItems = getAllRelatedRecItems(recItemIds, isAdd);
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                if (isAdd) {
                    rawRecItems.sort(Comparator.comparing(RawRecItem::getId));
                    abnormalRecIds = reduceDupCCRecItems(rawRecItems, abnormalRecIds);
                    rewriteModel(rawRecItems);
                    Map<Long, RawRecItem> addedLayouts = rewriteIndexPlan(rawRecItems);
                    addedLayouts = checkAndRemoveDirtyLayouts(addedLayouts);
                    addedLayoutIdList.addAll(addedLayouts.keySet());
                    addLayoutHitCount(recommendation.getProject(), recommendation.getUuid(), addedLayouts);
                } else {
                    shiftLayoutHitCount(recommendation.getProject(), recommendation.getUuid(), rawRecItems);
                    List<Long> removedLayouts = reduceIndexPlan(rawRecItems);
                    removedLayoutIdList.addAll(removedLayouts);
                }
                return null;
            }, project);
            return rawRecItems;
        }

        private void addLayoutHitCount(String project, String modelUuid, Map<Long, RawRecItem> rawRecItems) {
            if (MapUtils.isEmpty(rawRecItems)) {
                return;
            }
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            NDataflowManager dfMgr = NDataflowManager.getInstance(config, project);
            dfMgr.updateDataflow(modelUuid, copyForWrite -> {
                Map<Long, FrequencyMap> layoutHitCount = copyForWrite.getLayoutHitCount();
                rawRecItems.forEach((id, rawRecItem) -> {
                    if (rawRecItem.getLayoutMetric() != null) {
                        layoutHitCount.put(id, rawRecItem.getLayoutMetric().getFrequencyMap());
                    }
                });
            });
        }

        private Map<Long, RawRecItem> checkAndRemoveDirtyLayouts(Map<Long, RawRecItem> addedLayouts) {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            NDataModelManager modelManager = NDataModelManager.getInstance(kylinConfig, project);
            NDataModel model = modelManager.getDataModelDesc(recommendation.getUuid());
            Set<Integer> queryScopes = Sets.newHashSet();
            model.getEffectiveDimensions().forEach((id, col) -> queryScopes.add(id));
            model.getEffectiveMeasures().forEach((id, col) -> queryScopes.add(id));

            NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, project);
            IndexPlan indexPlan = indexPlanManager.getIndexPlan(recommendation.getUuid());
            Map<Long, LayoutEntity> allLayoutsMap = indexPlan.getAllLayoutsMap();

            return addedLayouts.entrySet().stream().filter(entry -> {
                long layoutId = entry.getKey();
                return allLayoutsMap.containsKey(layoutId)
                        && queryScopes.containsAll(allLayoutsMap.get(layoutId).getColOrder());
            }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        public List<RawRecItem> getAllRelatedRecItems(List<Integer> layoutIds, boolean isAdd) {
            Set<RawRecItem> allRecItems = Sets.newLinkedHashSet();
            Map<Integer, LayoutRef> layoutRefs = isAdd //
                    ? recommendation.getAdditionalLayoutRefs()
                    : recommendation.getRemovalLayoutRefs();
            layoutIds.forEach(id -> {
                if (layoutRefs.containsKey(-id)) {
                    collect(allRecItems, layoutRefs.get(-id));
                }
            });
            return Lists.newArrayList(allRecItems);
        }

        private void collect(Set<RawRecItem> recItemsCollector, RecommendationRef ref) {
            if (ref instanceof ModelColumnRef) {
                return;
            }

            RawRecItem recItem = recommendation.getRawRecItemMap().get(-ref.getId());
            if (recItem == null || recItemsCollector.contains(recItem)) {
                return;
            }
            if (!ref.isBroken() && !ref.isExisted()) {
                ref.getDependencies().forEach(dep -> collect(recItemsCollector, dep));
                recItemsCollector.add(recItem);
            }
        }

        private void shiftLayoutHitCount(String project, String modelUuid, List<RawRecItem> rawRecItems) {
            if (CollectionUtils.isEmpty(rawRecItems)) {
                return;
            }
            Set<Long> layoutsToRemove = Sets.newHashSet();
            rawRecItems.forEach(rawRecItem -> {
                long layoutId = RawRecUtil.getLayout(rawRecItem).getId();
                layoutsToRemove.add(layoutId);
            });

            KylinConfig config = KylinConfig.getInstanceFromEnv();
            NDataflowManager dfMgr = NDataflowManager.getInstance(config, project);
            NDataflow originDf = dfMgr.getDataflow(modelUuid);
            NDataflow copiedDf = originDf.copy();
            IndexOptimizerFactory.getOptimizer(copiedDf, false, true).getGarbageLayoutMap(copiedDf);
            Map<Long, FrequencyMap> layoutHitCount = copiedDf.getLayoutHitCount();
            layoutHitCount.forEach((id, freqMap) -> {
                if (!layoutsToRemove.contains(id)) {
                    FrequencyMap oriMap = originDf.getLayoutHitCount().get(id);
                    if (oriMap != null) {
                        layoutHitCount.put(id, oriMap);
                    }
                }
            });
            dfMgr.updateDataflow(copiedDf.getUuid(), copyForWrite -> copyForWrite.setLayoutHitCount(layoutHitCount));
        }

        private void rewriteModel(List<RawRecItem> recItems) {
            if (CollectionUtils.isEmpty(recItems)) {
                return;
            }
            logBeginRewrite("Model");
            NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            modelManager.updateDataModel(recommendation.getUuid(), copyForWrite -> {
                prepareColsMeasData(copyForWrite);

                for (RawRecItem rawRecItem : recItems) {
                    if (isAbnormal(rawRecItem)) {
                        abnormalRecIds.add(rawRecItem.getId());
                        continue;
                    }
                    switch (rawRecItem.getType()) {
                    case DIMENSION:
                        writeDimensionToModel(copyForWrite, rawRecItem);
                        break;
                    case COMPUTED_COLUMN:
                        writeCCToModel(copyForWrite, rawRecItem);
                        break;
                    case MEASURE:
                        writeMeasureToModel(copyForWrite, rawRecItem);
                        break;
                    case ADDITIONAL_LAYOUT:
                    case REMOVAL_LAYOUT:
                    default:
                        break;
                    }
                }

                // Protect the model from being damaged
                try (SetLogCategory ignored = new SetLogCategory(LogConstant.SMART_CATEGORY)) {
                    log.info(copyForWrite.getUuid());
                }
                copyForWrite.keepColumnOrder();
                copyForWrite.keepMeasureOrder();
                List<NDataModel.NamedColumn> existedColumns = copyForWrite.getAllNamedColumns().stream()
                        .filter(NDataModel.NamedColumn::isExist).collect(Collectors.toList());
                List<NDataModel.Measure> existedMeasure = copyForWrite.getAllMeasures().stream()
                        .filter(measure -> !measure.isTomb()).collect(Collectors.toList());
                NDataModel.changeNameIfDup(existedColumns);
                NDataModel.checkDuplicateColumn(existedColumns);
                NDataModel.checkDuplicateMeasure(existedMeasure);
                NDataModel.checkDuplicateCC(copyForWrite.getComputedColumnDescs());
            });
            logFinishRewrite("Model");
        }

        private void prepareColsMeasData(NDataModel copyForWrite) {
            copyForWrite.getAllNamedColumns().forEach(column -> {
                if (column.isExist()) {
                    columns.putIfAbsent(column.getId(), column);
                }
                if (column.isDimension()) {
                    dimensions.putIfAbsent(column.getId(), column);
                }
            });

            copyForWrite.getAllMeasures().forEach(measure -> {
                if (!measure.isTomb()) {
                    measures.putIfAbsent(measure.getId(), measure);
                }
            });

            copyForWrite.getAllMeasures().forEach(measure -> {
                if (!measure.isTomb()) {
                    functionToMeasureMap.put(measure.getFunction().toString(), measure);
                }
            });
        }

        private Set<Integer> reduceDupCCRecItems(List<RawRecItem> recItems, Set<Integer> abnormalRecIds) {
            List<RawRecItem> ccRecItems = recItems.stream()
                    .filter(rawRecItem -> rawRecItem.getType() == RawRecItem.RawRecType.COMPUTED_COLUMN)
                    .collect(Collectors.toList());
            if (ccRecItems.isEmpty()) {
                return abnormalRecIds;
            }
            List<RawRecItem> sortedCCRecItems = ccRecItems.stream()
                    .sorted(Comparator.comparing(o -> ((CCRecItemV2) o.getRecEntity()).getCc().getInnerExpression()))
                    .collect(Collectors.toList());

            HashMap<Integer, Integer> dupCCRecItemMap = Maps.newHashMap();
            int resId = sortedCCRecItems.get(0).getId();
            for (int i = 1; i < sortedCCRecItems.size(); i++) {
                RawRecItem curRecItem = sortedCCRecItems.get(i);
                RawRecItem prevRecItem = sortedCCRecItems.get(i - 1);
                String expr1 = ((CCRecItemV2) curRecItem.getRecEntity()).getCc().getInnerExpression();
                String expr2 = ((CCRecItemV2) prevRecItem.getRecEntity()).getCc().getInnerExpression();
                if (expr1.equalsIgnoreCase(expr2)) {
                    int curRecItemId = curRecItem.getId();
                    dupCCRecItemMap.put(curRecItemId, resId);
                    abnormalRecIds.add(curRecItemId);
                } else {
                    resId = curRecItem.getId();
                }
            }

            return abnormalRecIds;
        }

        private void writeMeasureToModel(NDataModel model, RawRecItem rawRecItem) {
            Map<Integer, RecommendationRef> measureRefs = recommendation.getMeasureRefs();
            int negRecItemId = -rawRecItem.getId();
            RecommendationRef recommendationRef = measureRefs.get(negRecItemId);
            if (recommendationRef.isExisted()) {
                return;
            }

            MeasureRef measureRef = (MeasureRef) recommendationRef;
            NDataModel.Measure measure = measureRef.getMeasure();
            if (functionToMeasureMap.containsKey(measure.getFunction().toString())) {
                try (SetLogCategory ignored = new SetLogCategory(LogConstant.SMART_CATEGORY)) {
                    log.error("Fail to rewrite RawRecItem({}) for conflicting function ({})", rawRecItem.getId(),
                            measure.getFunction());
                }
                return;
            }
            int maxMeasureId = model.getMaxMeasureId();
            if (userDefinedRecNameMap.containsKey(negRecItemId)) {
                measureRef.rebuild(userDefinedRecNameMap.get(negRecItemId));
            } else {
                measureRef.rebuild(measure.getName());
            }
            measure = measureRef.getMeasure();
            measure.setId(++maxMeasureId);
            model.getAllMeasures().add(measure);
            measures.put(negRecItemId, measure);
            measures.put(measure.getId(), measure);
            functionToMeasureMap.put(measure.getFunction().toString(), measure);
            logWriteProperty(rawRecItem, measure);
        }

        private void writeDimensionToModel(NDataModel model, RawRecItem rawRecItem) {
            Map<Integer, NDataModel.NamedColumn> mapIdToColumn = model.getAllNamedColumns().stream()
                    .collect(Collectors.toMap(NDataModel.NamedColumn::getId, Function.identity()));
            Map<Integer, RecommendationRef> dimensionRefs = recommendation.getDimensionRefs();
            int negRecItemId = -rawRecItem.getId();
            RecommendationRef dimensionRef = dimensionRefs.get(negRecItemId);
            if (dimensionRef.isExisted()) {
                return;
            }
            DimensionRef dimRef = (DimensionRef) dimensionRef;
            NDataModel.NamedColumn column = null;
            if (dimRef.getEntity() instanceof ModelColumnRef) {
                ModelColumnRef columnRef = (ModelColumnRef) dimensionRef.getEntity();
                column = columnRef.getColumn();
            } else if (dimRef.getEntity() instanceof CCRef) {
                CCRef ccRef = (CCRef) dimensionRef.getEntity();
                column = columns.get(ccRef.getId());
            }
            Preconditions.checkArgument(column != null,
                    "Dimension can only depend on a computed column or an existing column");
            if (userDefinedRecNameMap.containsKey(negRecItemId)) {
                column.setName(userDefinedRecNameMap.get(negRecItemId));
            }
            column.setStatus(NDataModel.ColumnStatus.DIMENSION);
            mapIdToColumn.get(column.getId()).setName(column.getName());
            dimensions.putIfAbsent(negRecItemId, column);
            columns.get(column.getId()).setStatus(column.getStatus());
            columns.get(column.getId()).setName(column.getName());
            logWriteProperty(rawRecItem, column);
        }

        private void writeCCToModel(NDataModel model, RawRecItem rawRecItem) {
            Map<Integer, RecommendationRef> ccRefs = recommendation.getCcRefs();
            int negRecItemId = -rawRecItem.getId();
            RecommendationRef recommendationRef = ccRefs.get(negRecItemId);
            if (recommendationRef.isExisted()) {
                return;
            }
            CCRef ccRef = (CCRef) recommendationRef;
            ComputedColumnDesc cc = ccRef.getCc();
            if (userDefinedRecNameMap.containsKey(negRecItemId)) {
                ccRef.rebuild(userDefinedRecNameMap.get(negRecItemId));
                cc = ccRef.getCc();
            }
            int lastColumnId = model.getMaxColumnId();
            NDataModel.NamedColumn columnInModel = new NDataModel.NamedColumn();
            columnInModel.setId(++lastColumnId);
            columnInModel.setName(cc.getTableAlias() + "_" + cc.getColumnName());
            columnInModel.setAliasDotColumn(cc.getTableAlias() + "." + cc.getColumnName());
            columnInModel.setStatus(NDataModel.ColumnStatus.EXIST);
            model.getAllNamedColumns().add(columnInModel);
            model.getComputedColumnDescs().add(cc);
            columns.put(negRecItemId, columnInModel);
            columns.put(lastColumnId, columnInModel);
            logWriteProperty(rawRecItem, columnInModel);
        }

        private Map<Long, RawRecItem> rewriteIndexPlan(List<RawRecItem> recItems) {
            if (CollectionUtils.isEmpty(recItems)) {
                return Maps.newHashMap();
            }
            List<Long> layoutIds = Lists.newArrayList();
            logBeginRewrite("augment IndexPlan");
            NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            NDataModel model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .getDataModelDesc(recommendation.getUuid());

            HashMap<Long, RawRecItem> approvedLayouts = Maps.newHashMap();
            indexMgr.updateIndexPlan(recommendation.getUuid(), copyForWrite -> {
                IndexPlan.IndexPlanUpdateHandler updateHandler = copyForWrite.createUpdateHandler();
                for (RawRecItem rawRecItem : recItems) {
                    if (isAbnormal(rawRecItem)) {
                        abnormalRecIds.add(rawRecItem.getId());
                        continue;
                    }
                    if (!rawRecItem.isAddLayoutRec()) {
                        continue;
                    }
                    LayoutEntity layout = RawRecUtil.getLayout(rawRecItem);
                    List<Integer> colOrder = layout.getColOrder();
                    List<Integer> shardBy = Lists.newArrayList(layout.getShardByColumns());
                    List<Integer> sortBy = Lists.newArrayList(layout.getSortByColumns());
                    List<Integer> partitionBy = Lists.newArrayList(layout.getPartitionByColumns());

                    List<Integer> nColOrder = translateToRealIds(colOrder, "ColOrder");
                    List<Integer> nShardBy = translateToRealIds(shardBy, "ShardByColumns");
                    List<Integer> nSortBy = translateToRealIds(sortBy, "SortByColumns");
                    List<Integer> nPartitionBy = translateToRealIds(partitionBy, "PartitionByColumns");

                    if (isInvalidColId(nColOrder, model) || isInvalidColId(nShardBy, model)
                            || isInvalidColId(nSortBy, model) || isInvalidColId(nPartitionBy, model)
                            || (Sets.newHashSet(nColOrder).size() != colOrder.size())) {
                        try (SetLogCategory ignored = new SetLogCategory(LogConstant.SMART_CATEGORY)) {
                            log.error("Fail to rewrite illegal RawRecItem({})", rawRecItem.getId());
                        }
                        continue;
                    }

                    layout.setColOrder(nColOrder);
                    layout.setShardByColumns(nShardBy);
                    layout.setPartitionByColumns(nPartitionBy);
                    updateHandler.add(layout, rawRecItem.isAgg());
                    try (SetLogCategory ignored = new SetLogCategory(LogConstant.SMART_CATEGORY)) {
                        approvedLayouts.put(layout.getId(), rawRecItem);
                        log.info("RawRecItem({}) rewrite colOrder({}) to ({})", rawRecItem.getId(), colOrder,
                                nColOrder);
                        log.info("RawRecItem({}) rewrite shardBy({}) to ({})", rawRecItem.getId(), shardBy, nShardBy);
                        log.info("RawRecItem({}) rewrite sortBy({}) to ({})", rawRecItem.getId(), sortBy, nSortBy);
                        log.info("RawRecItem({}) rewrite partitionBy({}) to ({})", rawRecItem.getId(), partitionBy,
                                nPartitionBy);
                    }
                }
                updateHandler.complete();
                layoutIds.addAll(updateHandler.getAddedLayouts());
            });
            logFinishRewrite("augment IndexPlan");

            return layoutIds.stream().filter(approvedLayouts::containsKey)
                    .collect(Collectors.toMap(Function.identity(), approvedLayouts::get));
        }

        private boolean isAbnormal(RawRecItem rawRecItem) {
            return Arrays.stream(rawRecItem.getDependIDs()).anyMatch(id -> abnormalRecIds.contains(-id))
                    || abnormalRecIds.contains(rawRecItem.getId());
        }

        private boolean isInvalidColId(List<Integer> cols, NDataModel model) {
            for (Integer colId : cols) {
                if (!model.getEffectiveCols().containsKey(colId) && !model.getEffectiveMeasures().containsKey(colId)) {
                    return true;
                }
            }
            return false;
        }

        private List<Integer> translateToRealIds(List<Integer> virtualIds, String layoutPropType) {
            List<Integer> realIds = Lists.newArrayList();
            virtualIds.forEach(virtualId -> {
                int realId;
                if (recommendation.getDimensionRefs().containsKey(virtualId)) {
                    int refId = recommendation.getDimensionRefs().get(virtualId).getId();
                    realId = dimensions.get(refId).getId();
                } else if (recommendation.getMeasureRefs().containsKey(virtualId)) {
                    int refId = recommendation.getMeasureRefs().get(virtualId).getId();
                    realId = measures.get(refId).getId();
                } else if (recommendation.getColumnRefs().containsKey(virtualId)) {
                    realId = recommendation.getColumnRefs().get(virtualId).getId();
                } else {
                    String translateErrorMsg = String.format(Locale.ROOT,
                            "virtual id(%s) in %s(%s) cannot map to real id in model(%s/%s)", //
                            virtualId, layoutPropType, virtualIds, recommendation.getProject(),
                            recommendation.getUuid());
                    throw new IllegalStateException(translateErrorMsg);
                }
                realIds.add(realId);
            });
            return realIds;
        }

        private List<Long> reduceIndexPlan(List<RawRecItem> recItems) {
            if (CollectionUtils.isEmpty(recItems)) {
                return Lists.newArrayList();
            }
            logBeginRewrite("reduce IndexPlan");
            List<Long> removedLayoutIds = Lists.newArrayList();
            NIndexPlanManager indexMgr = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            indexMgr.updateIndexPlan(recommendation.getUuid(), copyForWrite -> {
                IndexPlan.IndexPlanUpdateHandler updateHandler = copyForWrite.createUpdateHandler();
                for (RawRecItem rawRecItem : recItems) {
                    if (!rawRecItem.isRemoveLayoutRec()) {
                        continue;
                    }
                    LayoutEntity layout = RawRecUtil.getLayout(rawRecItem);
                    updateHandler.remove(layout, rawRecItem.isAgg(), layout.isManual());
                    removedLayoutIds.add(layout.getId());
                }
                updateHandler.complete();
            });
            logFinishRewrite("reduce IndexPlan");
            return removedLayoutIds;
        }

        private void logBeginRewrite(String rewriteInfo) {
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.SMART_CATEGORY)) {
                log.info("Start to rewrite RawRecItems to {}({}/{})", rewriteInfo, recommendation.getProject(),
                        recommendation.getUuid());
            }
        }

        private void logFinishRewrite(String rewrite) {
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.SMART_CATEGORY)) {
                log.info("Rewrite RawRecItems to {}({}/{}) successfully", rewrite, recommendation.getProject(),
                        recommendation.getUuid());

            }
        }

        private void logWriteProperty(RawRecItem recItem, Object obj) {
            if (obj instanceof NDataModel.NamedColumn) {
                NDataModel.NamedColumn column = (NDataModel.NamedColumn) obj;
                try (SetLogCategory ignored = new SetLogCategory(LogConstant.SMART_CATEGORY)) {
                    log.info("Write RawRecItem({}) to model as Column with id({}), name({}), isDimension({})", //
                            recItem.getId(), column.getId(), column.getName(), column.isDimension());
                }
            } else if (obj instanceof NDataModel.Measure) {
                NDataModel.Measure measure = (NDataModel.Measure) obj;
                try (SetLogCategory ignored = new SetLogCategory(LogConstant.SMART_CATEGORY)) {
                    log.info("Write RawRecItem({}) to model as Measure with id({}), name({}) ", //
                            recItem.getId(), measure.getId(), measure.getName());
                }
            }
        }
    }

    private static class RecNameChecker {

        /**
         * check whether computed column conflict with column of fact table
         */
        private static void checkCCRecConflictWithColumn(OptRecV2 recommendation, String project,
                Map<Integer, String> userDefinedRecNameMap) {
            Map<String, Set<Integer>> checkedTableColumnMap = Maps.newHashMap();

            userDefinedRecNameMap.forEach((id, name) -> {
                if (id >= 0) {
                    return;
                }
                RawRecItem rawRecItem = recommendation.getRawRecItemMap().get(-id);
                if (rawRecItem.getType() == RawRecItem.RawRecType.COMPUTED_COLUMN) {
                    checkedTableColumnMap.putIfAbsent(name.toUpperCase(Locale.ROOT), Sets.newHashSet());
                    checkedTableColumnMap.get(name.toUpperCase(Locale.ROOT)).add(id);
                }
            });

            NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.readSystemKylinConfig(),
                    project);
            NDataModel model = modelManager.getDataModelDesc(recommendation.getUuid());

            String factTableName = model.getRootFactTableName().split("\\.").length < 2 //
                    ? model.getRootFactTableName() //
                    : model.getRootFactTableName().split("\\.")[1];

            model.getAllNamedColumns().forEach(column -> {
                if (!column.isExist()) {
                    return;
                }

                String[] tableAndColumn = column.getAliasDotColumn().split("\\.");
                if (!tableAndColumn[0].equalsIgnoreCase(factTableName)) {
                    return;
                }
                if (checkedTableColumnMap.containsKey(tableAndColumn[1].toUpperCase(Locale.ROOT))) {
                    checkedTableColumnMap.get(tableAndColumn[1]).add(column.getId());
                }

            });

            checkedTableColumnMap.entrySet().removeIf(entry -> entry.getValue().size() < 2);
            if (!checkedTableColumnMap.isEmpty()) {
                throw new KylinException(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION,
                        MsgPicker.getMsg().getAliasConflictOfApprovingRecommendation() + "\n"
                                + JsonUtil.writeValueAsStringQuietly(checkedTableColumnMap));
            }
        }

        /**
         * check user defined name conflict with existing dimension & measure
         */
        private static void checkUserDefinedRecNames(OptRecV2 recommendation, String project,
                Map<Integer, String> userDefinedRecNameMap) {
            Map<String, Set<Integer>> checkedDimensionMap = Maps.newHashMap();
            Map<String, Set<Integer>> checkedMeasureMap = Maps.newHashMap();
            Map<String, Set<Integer>> checkedCCMap = Maps.newHashMap();

            NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.readSystemKylinConfig(),
                    project);
            NDataModel model = modelManager.getDataModelDesc(recommendation.getUuid());
            model.getAllNamedColumns().forEach(column -> {
                if (column.isDimension()) {
                    checkedDimensionMap.putIfAbsent(column.getName(), Sets.newHashSet());
                    checkedDimensionMap.get(column.getName()).add(column.getId());
                }
            });
            model.getAllMeasures().forEach(measure -> {
                if (measure.isTomb()) {
                    return;
                }
                checkedMeasureMap.putIfAbsent(measure.getName(), Sets.newHashSet());
                checkedMeasureMap.get(measure.getName()).add(measure.getId());
            });

            userDefinedRecNameMap.forEach((id, name) -> {
                if (id >= 0) {
                    return;
                }
                RawRecItem rawRecItem = recommendation.getRawRecItemMap().get(-id);
                if (rawRecItem.getType() == RawRecItem.RawRecType.DIMENSION) {
                    checkedDimensionMap.putIfAbsent(name, Sets.newHashSet());
                    Set<Integer> idSet = checkedDimensionMap.get(name);
                    idSet.remove(rawRecItem.getDependIDs()[0]);
                    idSet.add(id);
                }
                if (rawRecItem.getType() == RawRecItem.RawRecType.COMPUTED_COLUMN) {
                    checkedCCMap.putIfAbsent(name, Sets.newHashSet());
                    Set<Integer> idSet = checkedCCMap.get(name);
                    idSet.add(id);
                }
                if (rawRecItem.getType() == RawRecItem.RawRecType.MEASURE) {
                    checkedMeasureMap.putIfAbsent(name, Sets.newHashSet());
                    checkedMeasureMap.get(name).add(id);
                }
            });
            checkedDimensionMap.entrySet().removeIf(entry -> entry.getValue().size() < 2);
            checkedMeasureMap.entrySet().removeIf(entry -> entry.getValue().size() < 2);
            checkedCCMap.entrySet().removeIf(entry -> entry.getValue().size() < 2);
            Map<String, Set<Integer>> conflictMap = Maps.newHashMap();
            checkedDimensionMap.forEach((name, idSet) -> {
                conflictMap.putIfAbsent(name, Sets.newHashSet());
                conflictMap.get(name).addAll(idSet);
            });
            checkedMeasureMap.forEach((name, idSet) -> {
                conflictMap.putIfAbsent(name, Sets.newHashSet());
                conflictMap.get(name).addAll(idSet);
            });
            checkedCCMap.forEach((name, idSet) -> {
                conflictMap.putIfAbsent(name, Sets.newHashSet());
                conflictMap.get(name).addAll(idSet);
            });
            conflictMap.entrySet().removeIf(entry -> entry.getValue().stream().allMatch(id -> id >= 0));
            if (!conflictMap.isEmpty()) {
                throw new KylinException(ServerErrorCode.FAILED_APPROVE_RECOMMENDATION,
                        MsgPicker.getMsg().getAliasConflictOfApprovingRecommendation() + "\n"
                                + JsonUtil.writeValueAsStringQuietly(conflictMap));
            }
        }
    }

    public OptRecResponse approve(String project, OptRecRequest request) {
        if (request.getJobId() != null && request.getToken() != null) {
            JobTokenManager tokenManager = JobTokenManager.getInstance(getConfig());
            JobTokenItem tokenItem = tokenManager.query(request.getJobId());
            String encrypt = tokenManager.encrypt(request.getToken());
            if (tokenItem != null && !encrypt.equalsIgnoreCase(tokenItem.getToken())) {
                OptRecResponse optRecResponse = new OptRecResponse();
                optRecResponse.setProject(project);
                optRecResponse.setModelId(request.getModelId());
                optRecResponse.setAddedLayouts(Lists.newArrayList());
                optRecResponse.setRemovedLayouts(Lists.newArrayList());
                return optRecResponse;
            }
            OptRecResponse optRecResponse = approveInternal(project, request);
            tokenManager.delete(request.getJobId());
            return optRecResponse;
        } else {
            aclEvaluate.checkProjectOperationDesignPermission(project);
            return approveInternal(project, request);
        }
    }

    private OptRecResponse approveInternal(String project, OptRecRequest request) {
        String modelId = request.getModelId();
        if (!FusionIndexService.checkUpdateIndexEnabled(project, modelId)) {
            throw new KylinException(STREAMING_INDEX_UPDATE_DISABLE, MsgPicker.getMsg().getStreamingIndexesApprove());
        }

        OptRecResponse response = new OptRecResponse();
        BaseIndexUpdateHelper baseIndexUpdater = new BaseIndexUpdateHelper(
                getManager(NDataModelManager.class, project).getDataModelDesc(modelId), false);
        Map<Integer, String> userDefinedRecNameMap = request.getNames();
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            RecApproveContext approveContext = new RecApproveContext(project, modelId, userDefinedRecNameMap);
            approveRecItemsToRemoveLayout(request, approveContext);
            approveRecItemsToAddLayout(request, approveContext);
            updateRecommendationCount(project, modelId);

            response.setProject(request.getProject());
            response.setModelId(request.getModelId());
            response.setAddedLayouts(approveContext.addedLayoutIdList);
            response.setRemovedLayouts(approveContext.removedLayoutIdList);
            return null;
        }, project);

        response.setBaseIndexInfo(baseIndexUpdater.update(indexPlanService, false));

        return response;
    }

    /**
     * approve all recommendations in specified models
     */
    public List<OpenRecApproveResponse.RecToIndexResponse> batchApprove(String project, List<String> modelIds,
            String recActionType, boolean filterByModels, boolean discardTableIndex) {
        aclEvaluate.checkProjectOperationDesignPermission(project);
        List<String> allModels = Lists.newArrayList();
        if (filterByModels && CollectionUtils.isNotEmpty(modelIds)) {
            allModels.addAll(modelIds);
        } else if (!filterByModels) {
            allModels.addAll(getManager(NDataModelManager.class, project).listAllModelIds());
        }

        allModels.forEach(modelId -> modelService.checkModelPermission(project, modelId));
        List<OpenRecApproveResponse.RecToIndexResponse> responseList = Lists.newArrayList();
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager systemDfMgr = NDataflowManager.getInstance(KylinConfig.readSystemKylinConfig(), project);
            for (String modelId : allModels) {
                NDataflow df = systemDfMgr.getDataflow(modelId);
                if (df.getStatus() != RealizationStatusEnum.ONLINE || df.getModel().isBroken()) {
                    continue;
                }
                NDataModel model = df.getModel();
                if (allModels.contains(model.getUuid())) {
                    OpenRecApproveResponse.RecToIndexResponse response = approveAllRecItems(project, model.getUuid(),
                            model.getAlias(), recActionType, discardTableIndex);
                    responseList.add(response);
                }
            }
            return null;
        }, project);
        return responseList;
    }

    private OpenRecApproveResponse.RecToIndexResponse approveAllRecItems(String project, String modelId,
            String modelAlias, String recActionType, boolean discardTableIndex) {
        RecApproveContext approveContext = new RecApproveContext(project, modelId, Maps.newHashMap());
        OptRecRequest request = new OptRecRequest();
        request.setProject(project);
        request.setModelId(modelId);
        OptRecV2 recommendation = approveContext.getRecommendation();

        List<Integer> recItemsToAddLayout = filterOutTableIndex(recommendation, discardTableIndex);
        List<Integer> recItemsToRemoveLayout = Lists.newArrayList();

        recommendation.getRemovalLayoutRefs().forEach((key, value) -> recItemsToRemoveLayout.add(-value.getId()));
        if (recActionType.equalsIgnoreCase(OptRecService.RecActionType.ALL.name())) {
            request.setRecItemsToAddLayout(recItemsToAddLayout);
            request.setRecItemsToRemoveLayout(recItemsToRemoveLayout);
        } else if (recActionType.equalsIgnoreCase(OptRecService.RecActionType.ADD_INDEX.name())) {
            request.setRecItemsToAddLayout(recItemsToAddLayout);
            request.setRecItemsToRemoveLayout(Lists.newArrayList());
        } else if (recActionType.equalsIgnoreCase(OptRecService.RecActionType.REMOVE_INDEX.name())) {
            request.setRecItemsToAddLayout(Lists.newArrayList());
            request.setRecItemsToRemoveLayout(recItemsToRemoveLayout);
        } else {
            throw new KylinException(UNSUPPORTED_REC_OPERATION_TYPE, OptRecService.OPERATION_ERROR_MSG);
        }
        NDataModel model = getManager(NDataModelManager.class, project).getDataModelDesc(modelId);
        BaseIndexUpdateHelper baseIndexUpdater = new BaseIndexUpdateHelper(model, false);
        approveRecItemsToRemoveLayout(request, approveContext);
        approveRecItemsToAddLayout(request, approveContext);
        updateRecommendationCount(project, modelId);

        OpenRecApproveResponse.RecToIndexResponse response = new OpenRecApproveResponse.RecToIndexResponse();
        response.setModelId(modelId);
        response.setModelAlias(modelAlias);
        response.setAddedIndexes(approveContext.addedLayoutIdList);
        response.setRemovedIndexes(approveContext.removedLayoutIdList);
        response.setBaseIndexInfo(baseIndexUpdater.update(indexPlanService));

        return response;
    }

    private List<Integer> filterOutTableIndex(OptRecV2 recommendation, boolean discardTableIndex) {
        List<Integer> recItemsToAddLayout = Lists.newArrayList();
        recommendation.getAdditionalLayoutRefs().forEach((key, value) -> {
            if (!discardTableIndex || value.isAgg()) {
                recItemsToAddLayout.add(-value.getId());
            }
        });
        return recItemsToAddLayout;
    }

    private void approveRecItemsToRemoveLayout(OptRecRequest request, RecApproveContext approveContext) {
        List<Integer> recItemsToRemoveLayout = request.getRecItemsToRemoveLayout();
        List<RawRecItem> recItems = approveContext.approveRawRecItems(recItemsToRemoveLayout, false);
        updateStatesOfApprovedRecItems(request.getProject(), recItems);
    }

    private void approveRecItemsToAddLayout(OptRecRequest request, RecApproveContext approveContext) {
        List<Integer> recItemsToAddLayout = request.getRecItemsToAddLayout();
        List<RawRecItem> recItems = approveContext.approveRawRecItems(recItemsToAddLayout, true);
        updateStatesOfApprovedRecItems(request.getProject(), recItems);
    }

    private void updateStatesOfApprovedRecItems(String project, List<RawRecItem> recItems) {
        List<Integer> nonAppliedItemIds = Lists.newArrayList();
        recItems.forEach(recItem -> {
            if (recItem.getState() == RawRecItem.RawRecState.APPLIED) {
                return;
            }
            nonAppliedItemIds.add(recItem.getId());
        });
        RawRecManager rawManager = RawRecManager.getInstance(project);
        rawManager.applyByIds(nonAppliedItemIds);
    }

    private void updateRecommendationCount(String project, String modelId) {
        int size = optRecService.getOptRecLayoutsResponse(project, modelId, OptRecService.ALL).getSize();
        modelService.updateRecommendationsCount(project, modelId, size);
    }

    @Subscribe
    public void approveRawRecItemsByHitCount(ApproveRecsEvent event) {
        String project = event.getProject();
        Map<NDataflow, Map<Long, GarbageLayoutType>> needOptAggressivelyModels = event.getNeedOptAggressivelyModels();

        needOptAggressivelyModels.forEach((dataflow, garbageLayouts) -> {
            IndexPlan indexPlan = dataflow.getIndexPlan();
            List<IndexEntity> indexesBeforeApprove = indexPlan.getIndexes();

            int approveIndexSize = getNeedToBeApprovedIndexSize(indexPlan, garbageLayouts);
            if (approveIndexSize <= 0) {
                return;
            }

            approve(project, dataflow.getUuid(), approveIndexSize);

            IndexPlan newestIndexPlan = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .getIndexPlan(indexPlan.getUuid());
            mergeSameDimLayout(project, newestIndexPlan, indexesBeforeApprove);
        });
    }

    private void approve(String project, String modelId, int approveRecsAmount) {
        OptRecLayoutsResponse optRecs = optRecService.getOptRecLayoutsResponseInner(project, modelId,
                OptRecService.RecActionType.ADD_INDEX.toString());
        List<Integer> approve = optRecs.getLayouts().stream().filter(layout -> layout.getUsage() != 0)
                .sorted((rec1, rec2) -> rec2.getUsage() - rec1.getUsage()).limit(approveRecsAmount)
                .map(OptRecLayoutResponse::getId).collect(Collectors.toList());

        if (approve.isEmpty()) {
            log.info("no RawRecItems need to be approved");
            return;
        }

        aclEvaluate.checkProjectOperationDesignPermission(project);
        OptRecRequest optRecRequest = new OptRecRequest();
        optRecRequest.setProject(project);
        optRecRequest.setModelId(modelId);
        optRecRequest.setRecItemsToAddLayout(approve);
        OptRecResponse approveRes = approveInternal(project, optRecRequest);
        log.info("approve added layouts: {} for model: {} under project: {}", approveRes.getAddedLayouts(), modelId,
                project);
    }

    private int getNeedToBeApprovedIndexSize(IndexPlan indexPlan, Map<Long, GarbageLayoutType> garbageLayouts) {
        int indexSizeBeforeApprove = indexPlan.getIndexes().size();
        int expectIndexSize = indexPlan.getConfig().getExpectedIndexSizeOptimized();
        List<Set<LayoutEntity>> mergedLayouts = getMergedLayouts(indexPlan, garbageLayouts);
        int indexSizeAfterClean = indexSizeBeforeApprove - garbageLayouts.size() + mergedLayouts.size();
        int approveIndexSize = expectIndexSize - indexSizeAfterClean;
        log.info("all index is: {}, expected index size after optimization is: {}. Prepare to approve {} index",
                indexSizeAfterClean, expectIndexSize, approveIndexSize);
        return approveIndexSize;
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

    private void mergeSameDimLayout(String project, IndexPlan indexPlan, List<IndexEntity> indexesBeforeApprove) {
        List<IndexEntity> newestIndexes = indexPlan.getIndexes();
        List<IndexEntity> approvedIndexes = ListUtils.subtract(newestIndexes, indexesBeforeApprove);
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

        List<LayoutEntity> approvedLayouts = Lists.newArrayList();
        approvedIndexes.stream().map(IndexEntity::getLayouts).forEach(approvedLayouts::addAll);
        List<Set<LayoutEntity>> sameDimAggLayouts = IndexPlanReduceUtil.collectSameDimAggLayouts(approvedLayouts);
        indexPlanManager.updateIndexPlan(indexPlan.getId(), copyForWrite -> {
            IndexPlan.IndexPlanUpdateHandler updateHandler = copyForWrite.createUpdateHandler();
            sameDimAggLayouts.stream().flatMap(Collection::stream).forEachOrdered(
                    layout -> updateHandler.remove(layout, IndexEntity.isAggIndex(layout.getId()), false, false));
            updateHandler.complete();
            IndexPlan indexPlanMerged = IndexPlanReduceUtil.mergeSameDimLayout(copyForWrite, sameDimAggLayouts);
            copyForWrite.setIndexes(indexPlanMerged.getIndexes());
            copyForWrite.setLastModified(System.currentTimeMillis());
        });
    }
}
