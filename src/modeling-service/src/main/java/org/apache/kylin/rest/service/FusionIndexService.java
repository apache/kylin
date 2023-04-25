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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.INTEGER_POSITIVE_CHECK;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.exception.code.ErrorCodeServer;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.model.SelectRule;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cube.cuboid.NAggregationGroup;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexEntity.Range;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.metadata.model.FusionModel;
import org.apache.kylin.metadata.model.FusionModelManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.rest.aspect.Transaction;
import org.apache.kylin.rest.request.AggShardByColumnsRequest;
import org.apache.kylin.rest.request.CreateTableIndexRequest;
import org.apache.kylin.rest.request.OpenUpdateRuleBasedCuboidRequest;
import org.apache.kylin.rest.request.UpdateRuleBasedCuboidRequest;
import org.apache.kylin.rest.response.AggIndexResponse;
import org.apache.kylin.rest.response.BuildIndexResponse;
import org.apache.kylin.rest.response.DiffRuleBasedIndexResponse;
import org.apache.kylin.rest.response.IndexResponse;
import org.apache.kylin.rest.service.params.IndexPlanParams;
import org.apache.kylin.rest.service.params.PaginationParams;
import org.apache.kylin.streaming.manager.StreamingJobManager;
import org.apache.kylin.streaming.metadata.StreamingJobMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.kyligence.kap.secondstorage.SecondStorageUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("fusionIndexService")
public class FusionIndexService extends BasicService {
    private static final List<JobStatusEnum> runningStatus = Arrays.asList(JobStatusEnum.STARTING,
            JobStatusEnum.RUNNING, JobStatusEnum.STOPPING);

    private static final String COUNT_ALL_MEASURE = "COUNT_ALL";

    @Autowired
    private IndexPlanService indexPlanService;

    @Transaction(project = 0)
    public Pair<IndexPlan, BuildIndexResponse> updateRuleBasedCuboid(String project,
            final UpdateRuleBasedCuboidRequest request) {
        val model = getManager(NDataModelManager.class, project).getDataModelDesc(request.getModelId());
        if (model.fusionModelStreamingPart()) {
            UpdateRuleBasedCuboidRequest batchRequest = convertBatchUpdateRuleReq(request);
            UpdateRuleBasedCuboidRequest streamingRequest = convertStreamUpdateRuleReq(request);
            indexPlanService.updateRuleBasedCuboid(project, batchRequest);
            return indexPlanService.updateRuleBasedCuboid(project, streamingRequest);
        }
        return indexPlanService.updateRuleBasedCuboid(project, request);
    }

    public RuleBasedIndex getRule(String project, String modelId) {
        val model = getManager(NDataModelManager.class, project).getDataModelDesc(modelId);
        val modelRule = indexPlanService.getRule(project, modelId);
        val newRuleBasedIndex = new RuleBasedIndex();
        if (!checkUpdateIndexEnabled(project, modelId)) {
            newRuleBasedIndex.setIndexUpdateEnabled(false);
        }

        if (modelRule != null) {
            newRuleBasedIndex.getAggregationGroups().addAll(modelRule.getAggregationGroups());
        }

        if (model.fusionModelStreamingPart()) {
            FusionModel fusionModel = getManager(FusionModelManager.class, project).getFusionModel(modelId);
            String batchId = fusionModel.getBatchModel().getUuid();
            val batchRule = indexPlanService.getRule(project, batchId);
            if (batchRule != null) {
                newRuleBasedIndex.getAggregationGroups().addAll(batchRule.getAggregationGroups().stream()
                        .filter(agg -> agg.getIndexRange() == IndexEntity.Range.BATCH).collect(Collectors.toList()));
            }
        }
        return newRuleBasedIndex;
    }

    @Transaction(project = 0)
    public BuildIndexResponse createTableIndex(String project, CreateTableIndexRequest request) {
        NDataModel model = getManager(NDataModelManager.class, project).getDataModelDesc(request.getModelId());
        checkStreamingIndexEnabled(project, model);

        if (model.fusionModelStreamingPart()) {
            if (!indexChangeEnable(project, request.getModelId(), request.getIndexRange(),
                    Lists.newArrayList(IndexEntity.Range.HYBRID, Range.STREAMING))) {
                throw new KylinException(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getStreamingIndexesAdd()));
            }
            FusionModel fusionModel = getManager(FusionModelManager.class, project)
                    .getFusionModel(request.getModelId());
            String batchId = fusionModel.getBatchModel().getUuid();
            CreateTableIndexRequest copy = convertTableIndexRequest(request, model, batchId);
            if (IndexEntity.Range.BATCH == request.getIndexRange()) {
                return indexPlanService.createTableIndex(project, copy);
            } else if (IndexEntity.Range.HYBRID == request.getIndexRange()) {
                val indexPlanManager = getManager(NIndexPlanManager.class, project);
                val streamingIndexPlan = indexPlanManager.getIndexPlan(request.getModelId());
                val batchIndexPlan = indexPlanManager.getIndexPlan(batchId);
                long maxId = Math.max(streamingIndexPlan.getNextTableIndexId(), batchIndexPlan.getNextTableIndexId());
                indexPlanService.createTableIndex(project, copy, maxId + 1);
                return indexPlanService.createTableIndex(project, request, maxId + 1);
            }
        }
        return indexPlanService.createTableIndex(project, request);
    }

    private CreateTableIndexRequest convertTableIndexRequest(CreateTableIndexRequest request, NDataModel model,
            String batchId) {
        CreateTableIndexRequest copy = JsonUtil.deepCopyQuietly(request, CreateTableIndexRequest.class);
        copy.setModelId(batchId);
        String tableAlias = getManager(NDataModelManager.class, model.getProject()).getDataModelDesc(batchId)
                .getRootFactTableRef().getTableName();
        String oldAliasName = model.getRootFactTableRef().getTableName();
        convertTableIndex(copy, tableAlias, oldAliasName);
        return copy;
    }

    private void convertTableIndex(CreateTableIndexRequest copy, String tableName, String oldAliasName) {
        copy.setColOrder(copy.getColOrder().stream().map(x -> changeTableAlias(x, oldAliasName, tableName))
                .collect(Collectors.toList()));
        copy.setShardByColumns(copy.getShardByColumns().stream().map(x -> changeTableAlias(x, oldAliasName, tableName))
                .collect(Collectors.toList()));
        copy.setSortByColumns(copy.getSortByColumns().stream().map(x -> changeTableAlias(x, oldAliasName, tableName))
                .collect(Collectors.toList()));
    }

    private String changeTableAlias(String col, String oldAlias, String newAlias) {
        String table = col.split("\\.")[0];
        String column = col.split("\\.")[1];
        if (table.equalsIgnoreCase(oldAlias)) {
            return newAlias + "." + column;
        }
        return col;
    }

    @Transaction(project = 0)
    public BuildIndexResponse updateTableIndex(String project, CreateTableIndexRequest request) {
        NDataModel model = getManager(NDataModelManager.class, project).getDataModelDesc(request.getModelId());
        checkStreamingIndexEnabled(project, model);

        if (model.fusionModelStreamingPart()) {
            if (!indexChangeEnable(project, request.getModelId(), request.getIndexRange(),
                    Lists.newArrayList(IndexEntity.Range.HYBRID, Range.STREAMING))) {
                throw new KylinException(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getStreamingIndexesEdit()));
            }
            FusionModel fusionModel = getManager(FusionModelManager.class, project)
                    .getFusionModel(request.getModelId());
            String batchId = fusionModel.getBatchModel().getUuid();
            CreateTableIndexRequest copy = convertTableIndexRequest(request, model, batchId);
            if (IndexEntity.Range.BATCH == request.getIndexRange()) {
                copy.setModelId(batchId);
                return indexPlanService.updateTableIndex(project, copy);
            } else if (IndexEntity.Range.HYBRID == request.getIndexRange()) {
                indexPlanService.updateTableIndex(project, copy);
            }
        }
        return indexPlanService.updateTableIndex(project, request);
    }

    public List<IndexResponse> getIndexes(String project, String modelId, String key, List<IndexEntity.Status> status,
                                          String orderBy, Boolean desc, List<IndexEntity.Source> sources, List<Long> ids,
                                          List<IndexEntity.Range> range) {
        return getIndexes(new IndexPlanParams(project, modelId, null, ids, sources, status, range),
                new PaginationParams(null, null, orderBy, desc),
                key);
    }

    public List<IndexResponse> getIndexes(IndexPlanParams indexPlanParams, PaginationParams paginationParams, String key) {
        String project = indexPlanParams.getProject();
        String modelId = indexPlanParams.getModelId();
        List<Long> ids = indexPlanParams.getIds();
        List<IndexEntity.Source> sources = indexPlanParams.getSources();
        List<IndexEntity.Status> status = indexPlanParams.getStatus();
        List<IndexEntity.Range> range = indexPlanParams.getRange();
        String orderBy = paginationParams.getOrderBy();
        Boolean desc = paginationParams.getReverse();

        List<IndexResponse> indexes = indexPlanService.getIndexes(indexPlanParams, paginationParams, key);
        NDataModel model = getManager(NDataModelManager.class, project).getDataModelDesc(modelId);
        if (model.isFusionModel()) {
            FusionModel fusionModel = getManager(FusionModelManager.class, project).getFusionModel(modelId);
            if (fusionModel != null) {
                String batchId = fusionModel.getBatchModel().getUuid();
                String oldAliasName = getManager(NDataModelManager.class, project).getDataModelDesc(batchId)
                        .getRootFactTableRef().getTableName();
                String tableAlias = model.getRootFactTableRef().getTableName();
                indexes.addAll(indexPlanService.getIndexes(project, batchId, key, status, orderBy, desc, sources)
                        .stream().filter(index -> IndexEntity.Range.BATCH == index.getIndexRange())
                        .map(index -> convertTableIndex(index, tableAlias, oldAliasName)).collect(Collectors.toList()));
            } else {
                val streamingModel = getManager(NDataModelManager.class, project).getDataModelDesc(model.getFusionId());
                String oldAliasName = model.getRootFactTableRef().getTableName();
                String tableAlias = streamingModel.getRootFactTableRef().getTableName();
                indexes.stream().forEach(index -> convertTableIndex(index, tableAlias, oldAliasName));
            }
        }

        if (!CollectionUtils.isEmpty(ids)) {
            indexes = indexes.stream().filter(index -> (ids.contains(index.getId()))).collect(Collectors.toList());
        }

        if (!CollectionUtils.isEmpty(range)) {
            indexes = indexes.stream()
                    .filter(index -> (index.getIndexRange() == null || range.contains(index.getIndexRange())))
                    .collect(Collectors.toList());
        }
        return indexes;
    }

    private IndexResponse convertTableIndex(IndexResponse copy, String tableName, String oldAliasName) {
        copy.getColOrder().stream().forEach(x -> x.changeTableAlias(oldAliasName, tableName));
        copy.setShardByColumns(copy.getShardByColumns().stream().map(x -> changeTableAlias(x, oldAliasName, tableName))
                .collect(Collectors.toList()));
        copy.setSortByColumns(copy.getSortByColumns().stream().map(x -> changeTableAlias(x, oldAliasName, tableName))
                .collect(Collectors.toList()));
        return copy;
    }

    @Transaction(project = 0)
    public void removeIndex(String project, String model, final long id, IndexEntity.Range indexRange) {
        NDataModel modelDesc = getManager(NDataModelManager.class, project).getDataModelDesc(model);
        checkStreamingIndexEnabled(project, modelDesc);

        if (modelDesc.fusionModelStreamingPart()) {
            checkStreamingIndexDeleteEnabledWithIndexRange(project, model, indexRange);
            FusionModel fusionModel = getManager(FusionModelManager.class, project).getFusionModel(model);
            String batchId = fusionModel.getBatchModel().getUuid();
            if (IndexEntity.Range.BATCH == indexRange) {
                indexPlanService.removeIndex(project, batchId, id);
                return;
            } else if (IndexEntity.Range.HYBRID == indexRange) {
                removeHybridIndex(project, batchId, Sets.newHashSet(id));
            }
        }
        indexPlanService.removeIndex(project, model, id);
    }

    @Transaction(project = 0)
    public void removeIndexes(String project, String modelId, Set<Long> ids) {
        NDataModel modelDesc = getManager(NDataModelManager.class, project).getDataModelDesc(modelId);
        if (modelDesc.isStreaming() && checkStreamingJobAndSegments(project, modelId)) {
            throw new KylinException(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getStreamingIndexesDelete()));

        }
        indexPlanService.removeIndexes(project, modelId, ids);
    }

    @Transaction(project = 0)
    public void batchRemoveIndex(String project, String modelId, Set<Long> ids, IndexEntity.Range indexRange) {
        NDataModel modelDesc = getManager(NDataModelManager.class, project).getDataModelDesc(modelId);
        checkSecondStorageBaseTableIndexEnabled(project, modelDesc, ids);
        checkStreamingIndexEnabled(project, modelDesc);
        if (!modelDesc.fusionModelStreamingPart()) {
            indexPlanService.removeIndexes(project, modelId, ids);
            return;
        }
        checkStreamingIndexDeleteEnabledWithIndexRange(project, modelId, indexRange);
        FusionModel fusionModel = getManager(FusionModelManager.class, project).getFusionModel(modelId);
        String batchId = fusionModel.getBatchModel().getUuid();
        if (IndexEntity.Range.BATCH == indexRange) {
            indexPlanService.removeIndexes(project, batchId, ids);
            return;
        }

        indexPlanService.removeIndexes(project, modelId, ids);
        if (IndexEntity.Range.HYBRID == indexRange) {
            removeHybridIndex(project, batchId, ids);
        }
    }

    private void checkStreamingIndexDeleteEnabledWithIndexRange(String project, String modelId,
            IndexEntity.Range indexRange) {
        if (!indexChangeEnable(project, modelId, indexRange,
                Lists.newArrayList(IndexEntity.Range.HYBRID, Range.STREAMING, Range.EMPTY))) {
            throw new KylinException(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getStreamingIndexesDelete()));
        }
    }

    private void removeHybridIndex(String project, String model, final Set<Long> ids) {
        val indexPlan = getManager(NIndexPlanManager.class, project).getIndexPlan(model);
        ids.stream().filter(id -> indexPlan.getLayoutEntity(id) != null)
                .forEach(id -> indexPlanService.removeIndex(project, model, id));
    }

    public AggIndexResponse calculateAggIndexCount(UpdateRuleBasedCuboidRequest request) {
        if (isFusionModel(request.getProject(), request.getModelId())) {

            UpdateRuleBasedCuboidRequest batchRequest = convertBatchUpdateRuleReq(request);
            UpdateRuleBasedCuboidRequest streamRequest = convertStreamUpdateRuleReq(request);

            val batchResponse = isEmptyAggregationGroups(batchRequest) ? AggIndexResponse.empty()
                    : indexPlanService.calculateAggIndexCount(batchRequest);
            val streamResponse = isEmptyAggregationGroups(streamRequest) ? AggIndexResponse.empty()
                    : indexPlanService.calculateAggIndexCount(streamRequest);

            return AggIndexResponse.combine(batchResponse, streamResponse, request.getAggregationGroups().stream()
                    .map(NAggregationGroup::getIndexRange).collect(Collectors.toList()));
        }

        return indexPlanService.calculateAggIndexCount(request);
    }

    public DiffRuleBasedIndexResponse calculateDiffRuleBasedIndex(UpdateRuleBasedCuboidRequest request) {
        if (isFusionModel(request.getProject(), request.getModelId())) {
            UpdateRuleBasedCuboidRequest batchRequest = convertBatchUpdateRuleReq(request);
            UpdateRuleBasedCuboidRequest streamRequest = convertStreamUpdateRuleReq(request);

            val batchResponse = isEmptyAggregationGroups(batchRequest) ? DiffRuleBasedIndexResponse.empty()
                    : indexPlanService.calculateDiffRuleBasedIndex(batchRequest);
            val streamResponse = isEmptyAggregationGroups(streamRequest) ? DiffRuleBasedIndexResponse.empty()
                    : indexPlanService.calculateDiffRuleBasedIndex(streamRequest);
            checkStreamingAggEnabled(streamResponse, request.getProject(), request.getModelId());
            return DiffRuleBasedIndexResponse.combine(batchResponse, streamResponse);
        }

        DiffRuleBasedIndexResponse response = indexPlanService.calculateDiffRuleBasedIndex(request);

        val model = getManager(NDataModelManager.class, request.getProject()).getDataModelDesc(request.getModelId());
        if (NDataModel.ModelType.STREAMING == model.getModelType()) {
            checkStreamingAggEnabled(response, request.getProject(), request.getModelId());
        }

        return response;
    }

    private boolean isEmptyAggregationGroups(UpdateRuleBasedCuboidRequest request) {
        return CollectionUtils.isEmpty(request.getAggregationGroups());
    }

    private UpdateRuleBasedCuboidRequest convertStreamUpdateRuleReq(UpdateRuleBasedCuboidRequest request) {
        UpdateRuleBasedCuboidRequest streamRequest = JsonUtil.deepCopyQuietly(request,
                UpdateRuleBasedCuboidRequest.class);
        streamRequest.setAggregationGroups(getStreamingAggGroup(streamRequest.getAggregationGroups()));
        return streamRequest;
    }

    private UpdateRuleBasedCuboidRequest convertBatchUpdateRuleReq(UpdateRuleBasedCuboidRequest request) {
        val batchRequest = JsonUtil.deepCopyQuietly(request, UpdateRuleBasedCuboidRequest.class);
        batchRequest.setAggregationGroups(getBatchAggGroup(batchRequest.getAggregationGroups()));
        FusionModel fusionModel = getManager(FusionModelManager.class, request.getProject())
                .getFusionModel(request.getModelId());
        batchRequest.setModelId(fusionModel.getBatchModel().getUuid());
        return batchRequest;
    }

    private boolean isFusionModel(String project, String modelId) {
        return getManager(NDataModelManager.class, project).getDataModelDesc(modelId).fusionModelStreamingPart();
    }

    private List<NAggregationGroup> getStreamingAggGroup(List<NAggregationGroup> aggregationGroups) {
        return aggregationGroups.stream().filter(agg -> (agg.getIndexRange() == IndexEntity.Range.STREAMING
                || agg.getIndexRange() == IndexEntity.Range.HYBRID)).collect(Collectors.toList());
    }

    private List<NAggregationGroup> getBatchAggGroup(List<NAggregationGroup> aggregationGroups) {
        return aggregationGroups.stream()
                .filter(agg -> (agg.getIndexRange() == Range.BATCH || agg.getIndexRange() == IndexEntity.Range.HYBRID))
                .collect(Collectors.toList());
    }

    @Transaction(project = 0)
    public void updateShardByColumns(String project, AggShardByColumnsRequest aggShardByColumnsRequest) {
        if (isFusionModel(project, aggShardByColumnsRequest.getModelId())) {
            val batchId = getBatchModel(project, aggShardByColumnsRequest.getModelId());
            val batchRequest = JsonUtil.deepCopyQuietly(aggShardByColumnsRequest, AggShardByColumnsRequest.class);
            batchRequest.setModelId(batchId);
            indexPlanService.updateShardByColumns(project, batchRequest);
        }
        indexPlanService.updateShardByColumns(project, aggShardByColumnsRequest);
    }

    public List<IndexResponse> getAllIndexes(String project, String modelId, String key,
            List<IndexEntity.Status> status, String orderBy, Boolean desc, List<IndexEntity.Source> sources) {
        if (isFusionModel(project, modelId)) {
            val batchId = getBatchModel(project, modelId);
            List<IndexResponse> response = new ArrayList<>();
            List<IndexResponse> batchResponse = indexPlanService.getIndexes(project, batchId, key, status, orderBy,
                    desc, sources);
            batchResponse.stream().forEach(index -> index.setIndexRange(Range.BATCH));
            response.addAll(batchResponse);

            List<IndexResponse> streamingResponse = indexPlanService.getIndexes(project, modelId, key, status, orderBy,
                    desc, sources);
            streamingResponse.stream().forEach(index -> index.setIndexRange(Range.STREAMING));
            response.addAll(streamingResponse);
            return response;
        }
        return indexPlanService.getIndexes(project, modelId, key, status, orderBy, desc, sources);
    }

    public List<IndexResponse> getIndexesWithRelatedTables(String project, String modelId, String key,
            List<IndexEntity.Status> status, String orderBy, Boolean desc, List<IndexEntity.Source> sources,
            List<Long> batchIndexIds) {
        if (isFusionModel(project, modelId)) {
            val batchId = getBatchModel(project, modelId);
            List<IndexResponse> response = new ArrayList<>();
            List<IndexResponse> batchResponse = indexPlanService.getIndexesWithRelatedTables(project, batchId, key,
                    status, orderBy, desc, sources, batchIndexIds);
            batchResponse.forEach(index -> index.setIndexRange(Range.BATCH));
            response.addAll(batchResponse);

            List<IndexResponse> streamingResponse = indexPlanService.getIndexes(project, modelId, key, status, orderBy,
                    desc, sources);
            streamingResponse.forEach(index -> index.setIndexRange(Range.STREAMING));
            response.addAll(streamingResponse);
            return response;
        }
        return indexPlanService.getIndexesWithRelatedTables(project, modelId, key, status, orderBy, desc, sources,
                batchIndexIds);
    }

    public UpdateRuleBasedCuboidRequest convertOpenToInternal(OpenUpdateRuleBasedCuboidRequest request,
            NDataModel model) {
        checkParamPositive(request.getGlobalDimCap());
        val dimMap = model.getEffectiveDimensions().entrySet().stream().collect(Collectors
                .toMap(e -> e.getValue().getAliasDotName(), Map.Entry::getKey, (u, v) -> v, LinkedHashMap::new));
        val meaMap = model.getEffectiveMeasures().entrySet().stream().collect(
                Collectors.toMap(e -> e.getValue().getName(), Map.Entry::getKey, (u, v) -> v, LinkedHashMap::new));

        List<NAggregationGroup> newAdded = request.getAggregationGroups().stream().map(aggGroup -> {
            NAggregationGroup group = new NAggregationGroup();
            Preconditions.checkNotNull(aggGroup.getDimensions(), "dimension should not null");
            checkParamPositive(aggGroup.getDimCap());
            val selectedDimMap = extractIds(aggGroup.getDimensions(), dimMap, AggGroupParams.DIMENSION);
            group.setIncludes(selectedDimMap.values().toArray(new Integer[0]));
            String[] measures = extractMeasures(aggGroup.getMeasures());
            val selectedMeaMap = extractIds(measures, meaMap, AggGroupParams.MEASURE);
            group.setMeasures(selectedMeaMap.values().toArray(new Integer[0]));
            SelectRule selectRule = new SelectRule();
            val mandatoryDimMap = extractIds(aggGroup.getMandatoryDims(), selectedDimMap, AggGroupParams.MANDATORY);
            Set<String> allDims = new HashSet<>(mandatoryDimMap.keySet());
            selectRule.setMandatoryDims(mandatoryDimMap.values().toArray(new Integer[0]));
            selectRule.setHierarchyDims(extractJointOrHierarchyIds(aggGroup.getHierarchyDims(), selectedDimMap, allDims,
                    AggGroupParams.HIERARCHY));
            selectRule.setJointDims(
                    extractJointOrHierarchyIds(aggGroup.getJointDims(), selectedDimMap, allDims, AggGroupParams.JOINT));
            selectRule.setDimCap(aggGroup.getDimCap() != null ? aggGroup.getDimCap() : request.getGlobalDimCap());
            group.setSelectRule(selectRule);
            return group;
        }).collect(Collectors.toList());

        RuleBasedIndex ruleBasedIndex = getRule(request.getProject(), model.getUuid());
        List<NAggregationGroup> groups = ruleBasedIndex.getAggregationGroups();
        groups.addAll(newAdded);

        UpdateRuleBasedCuboidRequest result = new UpdateRuleBasedCuboidRequest();
        result.setModelId(model.getUuid());
        result.setProject(request.getProject());
        result.setLoadData(false);
        result.setRestoreDeletedIndex(request.isRestoreDeletedIndex());
        result.setAggregationGroups(groups);
        return result;
    }

    private String[] extractMeasures(String[] measures) {
        if (ArrayUtils.isEmpty(measures)) {
            return new String[] { COUNT_ALL_MEASURE };
        } else {
            List<String> list = Arrays.stream(measures).filter(m -> !m.equals(COUNT_ALL_MEASURE))
                    .collect(Collectors.toList());
            list.add(0, COUNT_ALL_MEASURE);
            return list.toArray(new String[0]);
        }
    }

    private void checkParamPositive(Integer dimCap) {
        if (dimCap != null && dimCap <= 0) {
            throw new KylinException(INTEGER_POSITIVE_CHECK);
        }
    }

    private Map<String, Integer> extractIds(String[] dimOrMeaNames, Map<String, Integer> nameIdMap,
            AggGroupParams aggGroupParams) {
        if (dimOrMeaNames == null || dimOrMeaNames.length == 0) {
            return new HashMap<>();
        }
        Set<String> set = Arrays.stream(dimOrMeaNames)
                .map(str -> aggGroupParams == AggGroupParams.MEASURE ? str : StringUtils.upperCase(str, Locale.ROOT))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        if (set.size() < dimOrMeaNames.length) {
            throw new IllegalStateException(
                    "Dimension or measure in agg group must not contain duplication: " + Arrays.asList(dimOrMeaNames));
        }

        Map<String, Integer> upperCaseMap = nameIdMap.entrySet().stream()
                .collect(Collectors.toMap(entry -> aggGroupParams == AggGroupParams.MEASURE ? entry.getKey()
                        : StringUtils.upperCase(entry.getKey(), Locale.ROOT), Map.Entry::getValue));
        if (!upperCaseMap.keySet().containsAll(set)) {
            switch (aggGroupParams) {
            case DIMENSION:
                throw new KylinException(ErrorCodeServer.DIMENSION_NOT_IN_MODEL);
            case MEASURE:
                throw new KylinException(ErrorCodeServer.MEASURE_NOT_IN_MODEL);
            case MANDATORY:
                throw new KylinException(ErrorCodeServer.MANDATORY_NOT_IN_DIMENSION);
            case HIERARCHY:
                throw new KylinException(ErrorCodeServer.HIERARCHY_NOT_IN_DIMENSION);
            case JOINT:
                throw new KylinException(ErrorCodeServer.JOINT_NOT_IN_DIMENSION);
            default:
                throw new IllegalStateException("this should not happen");
            }
        }
        return set.stream()
                .collect(Collectors.toMap(Function.identity(), upperCaseMap::get, (v1, v2) -> v1, LinkedHashMap::new));
    }

    private Integer[][] extractJointOrHierarchyIds(String[][] origins, Map<String, Integer> selectedDimMap, Set<String> allDims,
            AggGroupParams aggGroupParams) {
        if (origins == null || origins.length == 0) {
            return new Integer[0][];
        }
        Integer[][] result = new Integer[origins.length][];
        for (int i = 0; i < origins.length; i++) {
            if (ArrayUtils.isEmpty(origins[i])) {
                continue;
            }
            Map<String, Integer> tmp = extractIds(origins[i], selectedDimMap, aggGroupParams);
            if (Sets.intersection(tmp.keySet(), allDims).isEmpty()) {
                allDims.addAll(tmp.keySet());
                result[i] = tmp.values().toArray(new Integer[0]);
            } else {
                throw new KylinException(ErrorCodeServer.DIMENSION_ONLY_SET_ONCE);
            }
        }
        return result;
    }

    enum AggGroupParams {
        DIMENSION, MEASURE, MANDATORY, HIERARCHY, JOINT
    }

    private String getBatchModel(String project, String modelId) {
        FusionModel fusionModel = getManager(FusionModelManager.class, project).getFusionModel(modelId);
        return fusionModel.getBatchModel().getId();
    }

    private static void checkStreamingAggEnabled(DiffRuleBasedIndexResponse streamResponse, String project,
            String modelId) {
        if ((streamResponse.getDecreaseLayouts() > 0 || streamResponse.getIncreaseLayouts() > 0)
                && checkStreamingJobAndSegments(project, modelId)) {
            throw new KylinException(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE,
                    MsgPicker.getMsg().getStreamingIndexesEdit());
        }
    }

    public static boolean checkUpdateIndexEnabled(String project, String modelId) {
        val model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataModelDesc(modelId);
        if (model == null) {
            log.warn("model {} is not existed in project:{}", modelId, project);
            return false;
        }
        return (NDataModel.ModelType.STREAMING != model.getModelType()
                && NDataModel.ModelType.HYBRID != model.getModelType())
                || !checkStreamingJobAndSegments(project, model.getUuid());
    }

    private static void checkStreamingIndexEnabled(String project, NDataModel model) throws KylinException {
        if (NDataModel.ModelType.STREAMING == model.getModelType()
                && checkStreamingJobAndSegments(project, model.getUuid())) {
            throw new KylinException(ServerErrorCode.STREAMING_INDEX_UPDATE_DISABLE,
                    MsgPicker.getMsg().getStreamingIndexesDelete());
        }
    }

    private static void checkSecondStorageBaseTableIndexEnabled(String project, NDataModel model, Set<Long> ids)
            throws KylinException {
        IndexPlan indexPlan = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getIndexPlan(model.getUuid());
        boolean checkCannotDeleteEnabled = SecondStorageUtil.isModelEnable(project, model.getUuid())
                && ids.stream().map(indexPlan::getLayoutEntity).filter(Objects::nonNull)
                        .anyMatch(layout -> layout.isBase() && layout.getIndex().isTableIndex());
        if (checkCannotDeleteEnabled) {
            throw new KylinException(ErrorCodeServer.BASE_TABLE_INDEX_DELETE_DISABLE);
        }
    }

    private static boolean indexChangeEnable(String project, String modelId, IndexEntity.Range range,
            List<IndexEntity.Range> ranges) {
        if (!ranges.contains(range)) {
            return true;
        }
        return !checkStreamingJobAndSegments(project, modelId);
    }

    public static boolean checkStreamingJobAndSegments(String project, String modelId) {
        String jobId = StreamingUtils.getJobId(modelId, JobTypeEnum.STREAMING_BUILD.name());
        val config = KylinConfig.getInstanceFromEnv();
        StreamingJobManager mgr = StreamingJobManager.getInstance(config, project);
        StreamingJobMeta meta = mgr.getStreamingJobByUuid(jobId);

        NDataflowManager dataflowManager = NDataflowManager.getInstance(config, project);
        NDataflow df = dataflowManager.getDataflow(modelId);
        return runningStatus.contains(meta.getCurrentStatus()) || !df.getSegments().isEmpty();
    }
}
