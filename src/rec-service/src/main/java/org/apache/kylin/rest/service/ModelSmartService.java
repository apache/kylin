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

import static org.apache.kylin.common.exception.ServerErrorCode.INDEXES_NOT_IN_WHITELIST;
import static org.apache.kylin.common.exception.ServerErrorCode.SQL_NUMBER_EXCEEDS_LIMIT;
import static org.apache.kylin.common.exception.ServerErrorCode.WHITELIST_NOT_IN_INDEXES;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;
import static org.apache.kylin.metadata.favorite.FavoriteRule.AUTO_INDEX_PLAN_RULE_NAMES;
import static org.apache.kylin.metadata.favorite.FavoriteRule.INDEX_PLANNER_ENABLE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableBiMap;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.metadata.favorite.ModelFavoriteRuleManager;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.ModelCreateContext;
import org.apache.kylin.rec.ModelReuseContext;
import org.apache.kylin.rec.ModelSelectContext;
import org.apache.kylin.rec.ProposerJob;
import org.apache.kylin.rec.SmartMaster;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.common.SmartConfig;
import org.apache.kylin.rec.model.AbstractJoinRule;
import org.apache.kylin.rec.runner.InMemoryJobRunner;
import org.apache.kylin.rest.request.AutoIndexPlanRuleUpdateRequest;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.request.OpenSqlAccelerateRequest;
import org.apache.kylin.rest.response.LayoutRecDetailResponse;
import org.apache.kylin.rest.response.SuggestAndOptimizedResponse;
import org.apache.kylin.rest.response.SuggestionResponse;
import org.apache.kylin.rest.response.SuggestionResponse.ModelRecResponse;
import org.apache.kylin.rest.service.util.AutoIndexPlanRuleUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("modelSmartService")
public class ModelSmartService extends AbstractModelService implements ModelSmartServiceSupporter {

    private static final Integer AUTO_INDEX_PLAN_OPTION_ALWAYS_ON = 1;
    private static final Integer AUTO_INDEX_PLAN_OPTION_ALWAYS_OFF = 2;
    @Autowired
    private RawRecService rawRecService;

    @Autowired
    private OptRecService optRecService;

    @Autowired
    private ModelService modelService;

    @Autowired
    private IndexPlanService indexPlanService;

    @Autowired
    private ProjectSmartService projectSmartService;

    public SuggestAndOptimizedResponse generateSuggestion(OpenSqlAccelerateRequest request, boolean createNewModel) {
        AbstractContext proposeContext = suggestModel(request.getProject(), request.getSqls(),
                !request.getForce2CreateNewModel(), createNewModel, request.getModelName());
        SuggestionResponse innerResponse = buildModelSuggestionResponse(proposeContext);
        val discardedLayoutRec = handleExtraOpt(innerResponse, request, proposeContext);
        List<ModelRequest> modelRequests = convertToModelRequest(innerResponse.getNewModels(), request);
        Set<String> modelIds = proposeContext.getModelContexts().stream() //
                .map(AbstractContext.ModelContext::getTargetModel) //
                .filter(Objects::nonNull).map(NDataModel::getId) //
                .collect(Collectors.toSet());
        recoverLayoutIfNeeded(innerResponse, discardedLayoutRec);
        if (request.isWithOptimalModel()) {
            fillOptimalModels(proposeContext, innerResponse);
        }
        if (!request.isAcceptRecommendation()) {
            rawRecService.transferAndSaveRecommendations(proposeContext);
        }
        return new SuggestAndOptimizedResponse(innerResponse, modelRequests, proposeContext.isCanCreateNewModel(),
                proposeContext.getProject(), modelIds, discardedLayoutRec);
    }

    private SuggestionResponse recoverLayoutIfNeeded(SuggestionResponse innerResponse,
            Map<String, List<LayoutRecDetailResponse>> discardedLayoutRec) {
        if (discardedLayoutRec.isEmpty()) {
            return innerResponse;
        }

        innerResponse.getReusedModels().forEach(model -> {
            if (discardedLayoutRec.containsKey(model.getUuid())) {
                model.getIndexes().addAll(discardedLayoutRec.get(model.getUuid()));
            }
        });

        return innerResponse;
    }

    private Map<String, List<LayoutRecDetailResponse>> handleExtraOpt(SuggestionResponse innerResponse,
            OpenSqlAccelerateRequest request, AbstractContext proposeContext) {
        Map<String, List<LayoutRecDetailResponse>> result = Maps.newHashMap();
        if (!request.isDiscardTableIndex() || innerResponse.getReusedModels().isEmpty()) {
            return result;
        }

        for (ModelRecResponse model : innerResponse.getReusedModels()) {
            val toBeRemovedLayouts = model.getIndexPlan().getIndexes().stream().map(IndexEntity::getLayouts)
                    .flatMap(Collection::stream)
                    .filter(t -> IndexEntity.isTableIndex(t.getId()) && !t.isBase() && !t.isManual() && t.isAuto())
                    .map(LayoutEntity::getId).collect(Collectors.toSet());
            List<LayoutRecDetailResponse> discardedLayouts = Lists.newArrayList();
            Set<Integer> toBeRecoveredColumns = Sets.newHashSet();
            model.getIndexPlan().getIndexes().forEach(
                    index -> index.getLayouts().removeIf(layout -> toBeRemovedLayouts.contains(layout.getId())));
            model.getIndexes().removeIf(layout -> {
                val dimensions = layout.getDimensions().stream().filter(LayoutRecDetailResponse.RecDimension::isNew)
                        .map(LayoutRecDetailResponse.RecDimension::getDimension).map(NDataModel.NamedColumn::getId)
                        .collect(Collectors.toSet());
                if (toBeRemovedLayouts.contains(layout.getIndexId())) {
                    val layoutCopy = JsonUtil.deepCopyQuietly(layout, LayoutRecDetailResponse.class);
                    layoutCopy.setDiscarded(true);
                    discardedLayouts.add(layoutCopy);
                    toBeRecoveredColumns.addAll(dimensions);
                    return true;
                }

                toBeRecoveredColumns.removeIf(dimensions::contains);
                return false;
            });

            if (!discardedLayouts.isEmpty()) {
                model.getAllNamedColumns().forEach(namedColumn -> {
                    if (toBeRecoveredColumns.contains(namedColumn.getId())) {
                        namedColumn.setStatus(NDataModel.ColumnStatus.EXIST);
                    }
                });
                model.getIndexPlan().getIndexes().removeIf(t -> t.getLayouts().isEmpty());
                model.getIndexPlan().getIndexes()
                        .forEach(t -> t.getDimensions().removeIf(toBeRecoveredColumns::contains));
                result.put(model.getUuid(), discardedLayouts);
                val layoutMap = discardedLayouts.stream()
                        .collect(Collectors.toMap(LayoutRecDetailResponse::getIndexId, Function.identity()));
                String layoutStr = layoutMap.keySet().stream().map(Object::toString).collect(Collectors.joining(","));
                log.info(String.format(Locale.ROOT, "Discard table index %s in model [%s] via api control.", layoutStr,
                        model.getAlias()));
                List<AbstractContext.ModelContext> contextList = proposeContext.getModelContexts().stream()
                        .filter(modelContext -> modelContext.getTargetModel() != null
                                && modelContext.getTargetModel().getUuid().equals(model.getUuid()))
                        .collect(Collectors.toList());
                contextList.forEach(modelContext -> modelContext.getIndexRexItemMap().entrySet()
                        .removeIf(entry -> layoutMap.containsKey(entry.getValue().getLayout().getId())));
            }
        }

        return result;
    }

    private void fillOptimalModels(AbstractContext proposeContext, SuggestionResponse suggestionResponse) {
        List<ModelRecResponse> responseOfOptimalModels = Lists.newArrayList();
        suggestionResponse.setOptimalModels(responseOfOptimalModels);
        Map<String, AccelerateInfo> accelerateInfoMap = proposeContext.getAccelerateInfoMap();
        if (MapUtils.isEmpty(accelerateInfoMap)) {
            return;
        }
        Set<String> reusedOrNewModelSqlSets = Sets.newHashSet();
        suggestionResponse.getReusedModels().stream().map(ModelRecResponse::getIndexes).flatMap(List::stream)
                .map(LayoutRecDetailResponse::getSqlList).forEach(reusedOrNewModelSqlSets::addAll);
        suggestionResponse.getNewModels().stream().map(ModelRecResponse::getIndexes).flatMap(List::stream)
                .map(LayoutRecDetailResponse::getSqlList).forEach(reusedOrNewModelSqlSets::addAll);

        Set<String> constantSqlSet = Sets.newHashSet();
        Map<String, AccelerateInfo> errorOrOptimalAccelerateInfoMap = Maps.newHashMap();
        accelerateInfoMap.forEach((key, value) -> {
            if (reusedOrNewModelSqlSets.contains(key)) {
                return;
            }
            if (!value.isNotSucceed() && CollectionUtils.isEmpty(value.getRelatedLayouts())) {
                constantSqlSet.add(key);
            } else {
                errorOrOptimalAccelerateInfoMap.put(key, value);
            }
        });

        if (CollectionUtils.isNotEmpty(constantSqlSet)) {
            responseOfOptimalModels.add(buildConstantSqlRecResponse(Lists.newArrayList(constantSqlSet)));
        }
        if (MapUtils.isEmpty(errorOrOptimalAccelerateInfoMap)) {
            return;
        }

        Set<String> finishedModelSets = Sets.newHashSet();
        for (AbstractContext.ModelContext modelContext : proposeContext.getModelContexts()) {
            if (modelContext.isTargetModelMissing() || modelContext.getOriginModel() == null
                    || modelContext.getOriginModel().isStreaming()
                    || finishedModelSets.contains(modelContext.getOriginModel().getUuid())) {
                continue;
            }

            try {
                collectResponseOfOptimalModels(modelContext, errorOrOptimalAccelerateInfoMap, responseOfOptimalModels);
                finishedModelSets.add(modelContext.getOriginModel().getUuid());
            } catch (Exception e) {
                log.error("Error occurs when collecting optimal models ", e);
            }
        }
    }

    private ModelRecResponse buildConstantSqlRecResponse(List<String> constantSqlSet) {
        List<LayoutRecDetailResponse> indexRecItems = Lists.newArrayList();
        LayoutRecDetailResponse recDetailResponse = new LayoutRecDetailResponse();
        recDetailResponse.setSqlList(Lists.newArrayList(constantSqlSet));
        recDetailResponse.setIndexId(-1L);
        indexRecItems.add(recDetailResponse);
        ModelRecResponse modelRecResponse = new ModelRecResponse();
        modelRecResponse.setIndexes(indexRecItems);
        modelRecResponse.setAlias("CONSTANT");
        return modelRecResponse;
    }

    private void collectResponseOfOptimalModels(AbstractContext.ModelContext modelContext,
            Map<String, AccelerateInfo> errorOrOptimalAccelerateInfoMap,
            List<SuggestionResponse.ModelRecResponse> responseOfOptimalModels) {
        Map<Long, Set<String>> layoutIdToSqlSetMap = mapLayoutToErrorOrOptimalSqlSet(modelContext,
                errorOrOptimalAccelerateInfoMap);
        if (MapUtils.isEmpty(layoutIdToSqlSetMap)) {
            return;
        }

        NDataModel originModel = modelContext.getOriginModel();
        Map<String, ComputedColumnDesc> oriComputedColumnMap = originModel.getComputedColumnDescs().stream()
                .collect(Collectors.toMap(ComputedColumnDesc::getFullName, Function.identity()));
        Map<Integer, NDataModel.NamedColumn> colsOfOriginModelMap = originModel.getAllNamedColumns().stream()
                .collect(Collectors.toMap(NDataModel.NamedColumn::getId, Function.identity()));
        IndexPlan indexPlan = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), originModel.getProject())
                .getIndexPlan(originModel.getUuid());
        List<LayoutRecDetailResponse> indexRecItems = Lists.newArrayList();
        layoutIdToSqlSetMap.forEach(((layoutId, optimalSqlSet) -> {
            LayoutEntity layoutEntity = indexPlan.getLayoutEntity(layoutId);
            LayoutRecDetailResponse response = new LayoutRecDetailResponse();
            ImmutableList<Integer> colOrder = layoutEntity.getColOrder();
            Map<String, ComputedColumnDesc> computedColumnsMap = Maps.newHashMap();
            colOrder.forEach(idx -> {
                if (idx < NDataModel.MEASURE_ID_BASE) {
                    ImmutableBiMap<Integer, TblColRef> effectiveDimensions = originModel.getEffectiveDimensions();
                    NDataModel.NamedColumn col = colsOfOriginModelMap.get(idx);
                    TblColRef tblColRef = originModel.getEffectiveCols().get(idx);
                    if (!effectiveDimensions.containsKey(idx) || null == col || null == tblColRef) {
                        return;
                    }
                    String dataType = effectiveDimensions.get(idx).getDatatype();
                    response.getDimensions().add(new LayoutRecDetailResponse.RecDimension(col, false, dataType));
                    if (tblColRef.getColumnDesc().isComputedColumn()
                            && oriComputedColumnMap.containsKey(tblColRef.getAliasDotName())) {
                        computedColumnsMap.put(tblColRef.getAliasDotName(),
                                oriComputedColumnMap.get(tblColRef.getAliasDotName()));
                    }
                } else if (originModel.getEffectiveMeasures().containsKey(idx)) {
                    NDataModel.Measure measure = originModel.getEffectiveMeasures().get(idx);
                    response.getMeasures().add(new LayoutRecDetailResponse.RecMeasure(measure, false));
                    List<TblColRef> colRefs = measure.getFunction().getColRefs();
                    colRefs.forEach(colRef -> {
                        if (colRef.getColumnDesc().isComputedColumn()
                                && oriComputedColumnMap.containsKey(colRef.getAliasDotName())) {
                            computedColumnsMap.put(colRef.getAliasDotName(),
                                    oriComputedColumnMap.get(colRef.getAliasDotName()));
                        }
                    });
                }
            });
            List<LayoutRecDetailResponse.RecComputedColumn> computedColumnDescList = computedColumnsMap.values()
                    .stream().map(e -> new LayoutRecDetailResponse.RecComputedColumn(e, false))
                    .collect(Collectors.toList());
            response.setComputedColumns(computedColumnDescList);
            response.setIndexId(layoutEntity.getId());
            response.setSqlList(Lists.newArrayList(optimalSqlSet));
            indexRecItems.add(response);
        }));

        ModelRecResponse response = new ModelRecResponse(originModel);
        response.setIndexPlan(indexPlan);
        response.setIndexes(indexRecItems);
        responseOfOptimalModels.add(response);
    }

    private Map<Long, Set<String>> mapLayoutToErrorOrOptimalSqlSet(AbstractContext.ModelContext modelContext,
            Map<String, AccelerateInfo> errorOrOptimalAccelerateInfoMap) {
        if (modelContext == null || MapUtils.isEmpty(errorOrOptimalAccelerateInfoMap)) {
            return Maps.newHashMap();
        }
        Map<Long, Set<String>> layoutToSqlSetMap = Maps.newHashMap();
        errorOrOptimalAccelerateInfoMap.forEach((sql, info) -> {
            for (AccelerateInfo.QueryLayoutRelation relation : info.getRelatedLayouts()) {
                if (!StringUtils.equalsIgnoreCase(relation.getModelId(), modelContext.getOriginModel().getUuid())) {
                    continue;
                }
                layoutToSqlSetMap.putIfAbsent(relation.getLayoutId(), Sets.newHashSet());
                layoutToSqlSetMap.get(relation.getLayoutId()).add(relation.getSql());
            }
        });
        return layoutToSqlSetMap;
    }

    private List<ModelRequest> convertToModelRequest(List<ModelRecResponse> newModels,
            OpenSqlAccelerateRequest request) {
        return newModels.stream().map(modelResponse -> {
            ModelRequest modelRequest = new ModelRequest(modelResponse);
            modelRequest.setIndexPlan(modelResponse.getIndexPlan());
            modelRequest.setWithEmptySegment(request.isWithEmptySegment());
            modelRequest.setWithModelOnline(request.isWithModelOnline());
            modelRequest.setWithBaseIndex(request.isWithBaseIndex());
            return modelRequest;
        }).collect(Collectors.toList());
    }

    public AbstractContext probeRecommendation(String project, List<String> sqls) {
        if (modelService.isProjectNotExist(project)) {
            throw new KylinException(PROJECT_NOT_EXIST, project);
        }
        AbstractContext proposeContext = new ModelSelectContext(NProjectManager.getProjectConfig(project), project,
                sqls.toArray(new String[0]));

        // don't use proposerJob
        fillExtraMeta(proposeContext);
        new SmartMaster(proposeContext).runWithContext(null);
        return proposeContext;
    }

    /**
     * fill some meta for recommendation without using ProposerJob.
     */
    private void fillExtraMeta(AbstractContext proposeContext) {
        KylinConfig config = proposeContext.getSmartConfig().getKylinConfig();
        Set<String> allModelIdSet = proposeContext.getRelatedModels().stream().map(NDataModel::getUuid)
                .collect(Collectors.toSet());
        String project = proposeContext.getProject();
        Set<String> onlineModelIdSet = Sets.newHashSet();
        Set<String> allModelNames = Sets.newHashSet();
        NDataflowManager.getInstance(config, project).listAllDataflows(true).forEach(df -> {
            NDataModel model = df.getModel();
            allModelNames.add(model.getAlias().toLowerCase(Locale.ROOT));
            if (model.isBroken() || !allModelIdSet.contains(model.getUuid()) || model.isFusionModel()) {
                return;
            }
            if (df.getStatus() == RealizationStatusEnum.ONLINE) {
                onlineModelIdSet.add(model.getUuid());
            }
        });

        String modelOptRule = proposeContext.getSmartConfig().getModelOptRule();
        proposeContext.getExtraMeta().setOnlineModelIds(onlineModelIdSet);
        proposeContext.getExtraMeta().setAllModels(allModelNames);
        proposeContext.getExtraMeta().setModelOptRule(modelOptRule);
    }

    public boolean couldAnsweredByExistedModel(String project, List<String> sqls) {
        aclEvaluate.checkProjectWritePermission(project);
        if (CollectionUtils.isEmpty(sqls)) {
            return true;
        }

        AbstractContext proposeContext = probeRecommendation(project, sqls);
        List<NDataModel> models = proposeContext.getProposedModels().stream().filter(model -> !model.isStreaming())
                .collect(Collectors.toList());
        return CollectionUtils.isNotEmpty(models);
    }

    public AbstractContext suggestModel(String project, List<String> sqls, boolean reuseExistedModel,
            boolean createNewModel) {
        return suggestModel(project, sqls, reuseExistedModel, createNewModel, null);
    }

    public AbstractContext suggestModel(String project, List<String> sqls, boolean reuseExistedModel,
            boolean createNewModel, String modelName) {
        aclEvaluate.checkProjectWritePermission(project);
        if (CollectionUtils.isEmpty(sqls)) {
            return null;
        }
        KylinConfig kylinConfig = NProjectManager.getProjectConfig(project);
        checkBatchSqlSize(kylinConfig, sqls);
        AbstractContext proposeContext;
        String[] sqlArray = sqls.toArray(new String[0]);
        if (SmartConfig.wrap(kylinConfig).getModelOptRule().equalsIgnoreCase(AbstractJoinRule.APPEND)) {
            if (!reuseExistedModel && createNewModel) {
                proposeContext = new ModelCreateContext(kylinConfig, project, sqlArray);
            } else {
                proposeContext = new ModelReuseContext(kylinConfig, project, sqlArray, true);
            }
        } else if (reuseExistedModel) {
            proposeContext = new ModelReuseContext(kylinConfig, project, sqlArray, createNewModel);
        } else {
            proposeContext = new ModelCreateContext(kylinConfig, project, sqlArray);
        }
        proposeContext.setModelName(modelName);
        return ProposerJob.propose(proposeContext,
                (config, runnerType, projectName, resources) -> new InMemoryJobRunner(config, projectName, resources));
    }

    public SuggestionResponse buildModelSuggestionResponse(AbstractContext context) {
        List<ModelRecResponse> responseOfNewModels = Lists.newArrayList();
        List<ModelRecResponse> responseOfReusedModels = Lists.newArrayList();

        for (AbstractContext.ModelContext modelContext : context.getModelContexts()) {
            if (modelContext.isTargetModelMissing()) {
                continue;
            }

            if (modelContext.getOriginModel() != null) {
                collectResponseOfReusedModels(modelContext, responseOfReusedModels);
            } else {
                collectResponseOfNewModels(context, modelContext, responseOfNewModels);
            }
        }
        responseOfReusedModels.removeIf(ModelRecResponse::isStreaming);
        return new SuggestionResponse(responseOfReusedModels, responseOfNewModels);
    }

    private void checkBatchSqlSize(KylinConfig kylinConfig, List<String> sqls) {
        val msg = MsgPicker.getMsg();
        int limit = kylinConfig.getSuggestModelSqlLimit();
        if (sqls.size() > limit) {
            throw new KylinException(SQL_NUMBER_EXCEEDS_LIMIT,
                    String.format(Locale.ROOT, msg.getSqlNumberExceedsLimit(), limit));
        }
    }

    private void collectResponseOfReusedModels(AbstractContext.ModelContext modelContext,
            List<ModelRecResponse> responseOfReusedModels) {
        if (modelContext.isSnapshotSelected()) {
            return;
        }
        collectResponseOfReusedModelsInner(modelContext, responseOfReusedModels);
    }

    private void collectResponseOfReusedModelsInner(AbstractContext.ModelContext modelContext,
            List<ModelRecResponse> responseOfReusedModels) {
        Map<Long, Set<String>> layoutToSqlSet = mapLayoutToSqlSet(modelContext);
        Map<String, ComputedColumnDesc> oriCCMap = Maps.newHashMap();
        List<ComputedColumnDesc> oriCCList = modelContext.getOriginModel().getComputedColumnDescs();
        oriCCList.forEach(cc -> oriCCMap.put(cc.getFullName(), cc));
        Map<String, ComputedColumnDesc> ccMap = Maps.newHashMap();
        List<ComputedColumnDesc> ccList = modelContext.getTargetModel().getComputedColumnDescs();
        ccList.forEach(cc -> ccMap.put(cc.getFullName(), cc));
        NDataModel targetModel = modelContext.getTargetModel();
        NDataModel originModel = modelContext.getOriginModel();
        List<LayoutRecDetailResponse> indexRecItems = Lists.newArrayList();
        modelContext.getIndexRexItemMap().forEach((key, layoutRecItemV2) -> {
            LayoutRecDetailResponse response = new LayoutRecDetailResponse();
            LayoutEntity layout = layoutRecItemV2.getLayout();
            ImmutableList<Integer> colOrder = layout.getColOrder();
            Map<ComputedColumnDesc, Boolean> ccStateMap = Maps.newHashMap();
            Map<Integer, NDataModel.NamedColumn> colsOfTargetModelMap = Maps.newHashMap();
            targetModel.getAllNamedColumns().forEach(col -> colsOfTargetModelMap.put(col.getId(), col));
            colOrder.forEach(idx -> {
                if (idx < NDataModel.MEASURE_ID_BASE && originModel.getEffectiveDimensions().containsKey(idx)) {
                    NDataModel.NamedColumn col = colsOfTargetModelMap.get(idx);
                    String dataType = originModel.getEffectiveDimensions().get(idx).getDatatype();
                    response.getDimensions().add(new LayoutRecDetailResponse.RecDimension(col, false, dataType));
                } else if (idx < NDataModel.MEASURE_ID_BASE) {
                    NDataModel.NamedColumn col = colsOfTargetModelMap.get(idx);
                    TblColRef tblColRef = targetModel.getEffectiveCols().get(idx);
                    String colRefAliasDotName = tblColRef.getAliasDotName();
                    if (tblColRef.getColumnDesc().isComputedColumn() && !oriCCMap.containsKey(colRefAliasDotName)) {
                        ccStateMap.putIfAbsent(ccMap.get(colRefAliasDotName), true);
                    }
                    String dataType = tblColRef.getDatatype();
                    response.getDimensions().add(new LayoutRecDetailResponse.RecDimension(col, true, dataType));
                } else if (originModel.getEffectiveMeasures().containsKey(idx)) {
                    NDataModel.Measure measure = targetModel.getEffectiveMeasures().get(idx);
                    response.getMeasures().add(new LayoutRecDetailResponse.RecMeasure(measure, false));
                } else {
                    NDataModel.Measure measure = targetModel.getEffectiveMeasures().get(idx);
                    List<TblColRef> colRefs = measure.getFunction().getColRefs();
                    colRefs.forEach(colRef -> {
                        String colRefAliasDotName = colRef.getAliasDotName();
                        if (colRef.getColumnDesc().isComputedColumn() && !oriCCMap.containsKey(colRefAliasDotName)) {
                            ccStateMap.putIfAbsent(ccMap.get(colRefAliasDotName), true);
                        }
                    });
                    response.getMeasures().add(new LayoutRecDetailResponse.RecMeasure(measure, true));
                }
            });
            List<LayoutRecDetailResponse.RecComputedColumn> newCCList = Lists.newArrayList();
            ccStateMap.forEach((k, v) -> newCCList.add(new LayoutRecDetailResponse.RecComputedColumn(k, v)));
            response.setComputedColumns(newCCList);
            response.setIndexId(layout.getId());
            Set<String> sqlSet = layoutToSqlSet.get(layout.getId());
            if (CollectionUtils.isNotEmpty(sqlSet)) {
                response.setSqlList(Lists.newArrayList(sqlSet));
            }
            indexRecItems.add(response);
        });

        ModelRecResponse response = new ModelRecResponse(targetModel);
        response.setIndexPlan(modelContext.getTargetIndexPlan());
        response.setIndexes(indexRecItems);
        responseOfReusedModels.add(response);
    }

    private Map<Long, Set<String>> mapLayoutToSqlSet(AbstractContext.ModelContext modelContext) {
        if (modelContext == null) {
            return Maps.newHashMap();
        }
        Map<String, AccelerateInfo> accelerateInfoMap = modelContext.getProposeContext().getAccelerateInfoMap();
        Map<Long, Set<String>> layoutToSqlSet = Maps.newHashMap();
        accelerateInfoMap.forEach((sql, info) -> {
            for (AccelerateInfo.QueryLayoutRelation relation : info.getRelatedLayouts()) {
                if (!StringUtils.equalsIgnoreCase(relation.getModelId(), modelContext.getTargetModel().getUuid())) {
                    continue;
                }
                layoutToSqlSet.putIfAbsent(relation.getLayoutId(), Sets.newHashSet());
                layoutToSqlSet.get(relation.getLayoutId()).add(relation.getSql());
            }
        });
        return layoutToSqlSet;
    }

    private void collectResponseOfNewModels(AbstractContext context, AbstractContext.ModelContext modelContext,
            List<ModelRecResponse> responseOfNewModels) {
        val sqlList = context.getAccelerateInfoMap().entrySet().stream()//
                .filter(entry -> entry.getValue().getRelatedLayouts().stream()//
                        .anyMatch(relation -> relation.getModelId().equals(modelContext.getTargetModel().getId())))
                .map(Map.Entry::getKey).collect(Collectors.toList());
        NDataModel model = modelContext.getTargetModel();
        IndexPlan indexPlan = modelContext.getTargetIndexPlan();
        ImmutableBiMap<Integer, TblColRef> effectiveDimensions = model.getEffectiveDimensions();
        List<LayoutRecDetailResponse.RecDimension> recDims = model.getAllNamedColumns().stream() //
                .filter(NDataModel.NamedColumn::isDimension) //
                .map(c -> {
                    String datatype = effectiveDimensions.get(c.getId()).getDatatype();
                    return new LayoutRecDetailResponse.RecDimension(c, true, datatype);
                }) //
                .collect(Collectors.toList());
        List<LayoutRecDetailResponse.RecMeasure> recMeasures = model.getAllMeasures().stream() //
                .map(measure -> new LayoutRecDetailResponse.RecMeasure(measure, true)) //
                .collect(Collectors.toList());
        List<LayoutRecDetailResponse.RecComputedColumn> recCCList = model.getComputedColumnDescs().stream() //
                .map(cc -> new LayoutRecDetailResponse.RecComputedColumn(cc, true)) //
                .collect(Collectors.toList());
        LayoutRecDetailResponse virtualResponse = new LayoutRecDetailResponse();
        virtualResponse.setIndexId(-1L);
        virtualResponse.setDimensions(recDims);
        virtualResponse.setMeasures(recMeasures);
        virtualResponse.setComputedColumns(recCCList);
        virtualResponse.setSqlList(sqlList);

        ModelRecResponse response = new ModelRecResponse(model);
        response.setIndexPlan(indexPlan);
        response.setIndexes(Lists.newArrayList(virtualResponse));
        responseOfNewModels.add(response);
    }

    @Override
    public Map<String, Object> getAutoIndexPlanRule(String modelId, String project) {
        Map<String, Object> projectRules = projectSmartService.getAutoIndexPlanRule(project);
        // use project config as default
        Map<String, Object> result = new HashMap<>(projectRules);
        ModelFavoriteRuleManager manager = ModelFavoriteRuleManager.getInstance(project, modelId);
        FavoriteRule.AUTO_INDEX_PLAN_RULE_NAMES.stream()
                .filter(ruleName -> !ruleName.equals(FavoriteRule.INDEX_PLANNER_ENABLE)).map(manager::getByName)
                .filter(Objects::nonNull)
                .forEach(rule -> result.put(rule.getName(), AutoIndexPlanRuleUtil.getRuleValue(rule)));
        Integer option = (Integer) AutoIndexPlanRuleUtil
                .getRuleValue(manager.getByName(FavoriteRule.AUTO_INDEX_PLAN_OPTION));
        result.put(FavoriteRule.AUTO_INDEX_PLAN_OPTION, option == null ? 0 : option);
        if (AUTO_INDEX_PLAN_OPTION_ALWAYS_ON.equals(option) && projectSmartService.isEnableAutoSemi(project)) {
            result.put(FavoriteRule.INDEX_PLANNER_ENABLE, true);
        } else if (AUTO_INDEX_PLAN_OPTION_ALWAYS_OFF.equals(option)) {
            result.put(FavoriteRule.INDEX_PLANNER_ENABLE, false);
        }
        return result;
    }

    @Override
    public Map<String, Object> getIndexPlannerRule(String modelId, String project) {
        Map<String, Object> projectRules = projectSmartService.getAutoIndexPlanRule(project);
        // use project config as default
        Map<String, Object> result = new HashMap<>(projectRules);
        ModelFavoriteRuleManager manager = ModelFavoriteRuleManager.getInstance(project, modelId);
        FavoriteRule.INDEX_PLANNER_RULE_NAMES.stream().map(manager::getByName).filter(Objects::nonNull)
                .forEach(rule -> result.put(rule.getName(), AutoIndexPlanRuleUtil.getRuleValue(rule)));
        if (!projectSmartService.isEnableAutoSemi(project)) {
            result.put(FavoriteRule.INDEX_PLANNER_ENABLE, false);
        }
        return result;
    }

    @Override
    public boolean isAutoIndexPlanEnabled(String modelId, String project) {
        ModelFavoriteRuleManager modelManager = ModelFavoriteRuleManager.getInstance(project, modelId);
        FavoriteRule modelRule = modelManager.getByName(FavoriteRule.AUTO_INDEX_PLAN_OPTION);
        Object modelOption = AutoIndexPlanRuleUtil.getRuleValue(modelRule);
        if (!projectSmartService.isEnableAutoSemi(project)) {
            return false;
        }
        if (modelOption != null) {
            Integer option = (Integer) modelOption;
            if (option.equals(AUTO_INDEX_PLAN_OPTION_ALWAYS_ON)) {
                return true;
            } else if (option.equals(AUTO_INDEX_PLAN_OPTION_ALWAYS_OFF)) {
                return false;
            }
        }
        FavoriteRuleManager projectManager = FavoriteRuleManager.getInstance(project);
        FavoriteRule projectRule = projectManager.getByName(FavoriteRule.INDEX_PLANNER_ENABLE);
        Object projectSwitch = AutoIndexPlanRuleUtil.getRuleValue(projectRule);
        return Objects.equals(projectSwitch, true);
    }

    public void updateAutoIndexPlanRule(String modelId, AutoIndexPlanRuleUpdateRequest request) {
        aclEvaluate.checkProjectWritePermission(request.getProject());
        ModelFavoriteRuleManager manager = ModelFavoriteRuleManager.getInstance(request.getProject(), modelId);
        JdbcUtil.withTxAndRetry(manager.getTransactionManager(), () -> {
            AUTO_INDEX_PLAN_RULE_NAMES.forEach(ruleName -> {
                if (INDEX_PLANNER_ENABLE.equals(ruleName)) {
                    return;
                }
                List<FavoriteRule.AbstractCondition> conds = AutoIndexPlanRuleUtil
                        .getConditionsFromUpdateRequest(ruleName, request);
                manager.updateRule(conds, true, ruleName);
            });
            return null;
        });
    }

    public List<Long> getAutoIndexPlanWhiteList(String modelId, String project) {
        IndexPlan indexPlan = getIndexPlan(modelId, project);
        return indexPlan.getPlannerWhiteList();
    }

    public void addToAutoIndexPlanWhiteList(String modelId, String project, List<Long> toAddList) {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelId);
        Set<Long> existingLayoutIds = indexPlan.getAllLayoutIds(false);
        if (!existingLayoutIds.containsAll(toAddList)) {
            Set<Long> diffSet = new HashSet<>(toAddList);
            diffSet.removeAll(existingLayoutIds);
            throw new KylinException(WHITELIST_NOT_IN_INDEXES, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getWhiteListNotInExistingIndex(), StringUtils.join(diffSet, ",")));
        }
        List<Long> existingWhiteList = new ArrayList<>(indexPlan.getPlannerWhiteList());
        if (existingWhiteList.containsAll(toAddList)) {
            return;
        }
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NIndexPlanManager idpMgr = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            idpMgr.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
                Set<Long> allWhiteList = new LinkedHashSet<>(copyForWrite.getPlannerWhiteList());
                allWhiteList.addAll(toAddList);
                copyForWrite.setPlannerWhiteList(new ArrayList<>(allWhiteList));
            });
            return null;
        }, project);
    }

    public void deleteFromAutoIndexPlanWhiteList(String modelId, String project, List<Long> toRemoveList) {
        NIndexPlanManager indexPlanManager = getManager(NIndexPlanManager.class, project);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelId);
        List<Long> existingWhiteList = new ArrayList<>(indexPlan.getPlannerWhiteList());
        if (!existingWhiteList.containsAll(toRemoveList)) {
            Set<Long> diffSet = new HashSet<>(toRemoveList);
            diffSet.removeAll(existingWhiteList);
            throw new KylinException(INDEXES_NOT_IN_WHITELIST, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getIndexesNotInWhiteList(), StringUtils.join(diffSet, ",")));
        }
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NIndexPlanManager idpMgr = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            idpMgr.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
                List<Long> allWhiteList = new ArrayList<>(copyForWrite.getPlannerWhiteList());
                allWhiteList.removeAll(toRemoveList);
                copyForWrite.setPlannerWhiteList(new ArrayList<>(allWhiteList));
            });
            return null;
        }, project);
    }

}
