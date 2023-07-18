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

import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.INDEX_DUPLICATE;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.LAYOUT_LIST_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.LAYOUT_NOT_EXISTS;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SHARD_BY_COLUMN_NOT_IN_INDEX;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.SegmentOnlineMode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.OutOfMaxCombinationException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.spark.smarter.IndexDependencyParser;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.common.SegmentUtil;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.cuboid.NAggregationGroup;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexEntity.Source;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.IndexPlan.UpdateRuleImpact;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.util.ExpandableMeasureUtil;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.sourceusage.SourceUsageManager;
import org.apache.kylin.rest.aspect.Transaction;
import org.apache.kylin.rest.model.FuzzyKeySearcher;
import org.apache.kylin.rest.request.AggShardByColumnsRequest;
import org.apache.kylin.rest.request.CreateBaseIndexRequest;
import org.apache.kylin.rest.request.CreateBaseIndexRequest.LayoutProperty;
import org.apache.kylin.rest.request.CreateTableIndexRequest;
import org.apache.kylin.rest.request.UpdateRuleBasedCuboidRequest;
import org.apache.kylin.rest.response.AggIndexCombResult;
import org.apache.kylin.rest.response.AggIndexResponse;
import org.apache.kylin.rest.response.AggShardByColumnsResponse;
import org.apache.kylin.rest.response.BuildBaseIndexResponse;
import org.apache.kylin.rest.response.BuildIndexResponse;
import org.apache.kylin.rest.response.DiffRuleBasedIndexResponse;
import org.apache.kylin.rest.response.IndexGraphResponse;
import org.apache.kylin.rest.response.IndexResponse;
import org.apache.kylin.rest.response.IndexStatResponse;
import org.apache.kylin.rest.response.TableIndexResponse;
import org.apache.kylin.rest.service.params.IndexPlanParams;
import org.apache.kylin.rest.service.params.PaginationParams;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.SpringContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.kyligence.kap.secondstorage.SecondStorageUpdater;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import lombok.Setter;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("indexPlanService")
public class IndexPlanService extends BasicService implements TableIndexPlanSupporter {

    public static final String DATA_SIZE = "data_size";
    private static final Logger logger = LoggerFactory.getLogger(IndexPlanService.class);

    @Setter
    @Autowired
    private ModelSemanticHelper semanticUpater;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired(required = false)
    private final List<ModelChangeSupporter> modelChangeSupporters = Lists.newArrayList();

    /**
     * expand expand EXPANDABLE measures in index plan request's indexes
     * @param plan
     */
    public void expandIndexPlanRequest(IndexPlan plan, NDataModel model) {
        ExpandableMeasureUtil.expandIndexPlanIndexes(plan, model);
    }

    /**
     * convert update rule based index req to ruble based index
     * expand EXPANDABLE measures if any
     * @param request
     * @return
     */
    private RuleBasedIndex convertRequestToRuleBasedIndex(UpdateRuleBasedCuboidRequest request) {
        val model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject())
                .getDataModelDesc(request.getModelId());
        val newRuleBasedCuboid = new RuleBasedIndex();
        BeanUtils.copyProperties(request, newRuleBasedCuboid);

        ExpandableMeasureUtil.expandRuleBasedIndex(newRuleBasedCuboid, model);
        newRuleBasedCuboid.setGlobalDimCap(request.getGlobalDimCap());

        return newRuleBasedCuboid;
    }

    @Transaction(project = 0)
    public Pair<IndexPlan, BuildIndexResponse> updateRuleBasedCuboid(String project,
            final UpdateRuleBasedCuboidRequest request) {
        aclEvaluate.checkProjectOperationDesignPermission(project);
        try {
            val kylinConfig = KylinConfig.getInstanceFromEnv();
            val indexPlanManager = getManager(NIndexPlanManager.class, project);
            val modelManager = NDataModelManager.getInstance(kylinConfig, request.getProject());
            IndexPlan originIndexPlan = getIndexPlan(request.getProject(), request.getModelId());
            val model = modelManager.getDataModelDesc(request.getModelId());

            Preconditions.checkNotNull(model);

            val indexPlan = indexPlanManager.updateIndexPlan(originIndexPlan.getUuid(), copyForWrite -> {
                RuleBasedIndex ruleBasedIndex = convertRequestToRuleBasedIndex(request);
                ruleBasedIndex.setLastModifiedTime(System.currentTimeMillis());
                copyForWrite.setRuleBasedIndex(ruleBasedIndex, Sets.newHashSet(), false, true,
                        request.isRestoreDeletedIndex());
            });
            BuildIndexResponse response = new BuildIndexResponse();
            if (request.isLoadData()) {
                response = semanticUpater.handleIndexPlanUpdateRule(request.getProject(), model.getUuid(),
                        originIndexPlan.getRuleBasedIndex(), indexPlan.getRuleBasedIndex(), false);
            }
            modelChangeSupporters.forEach(listener -> listener.onUpdate(project, request.getModelId()));
            return new Pair<>(indexPlanManager.getIndexPlan(originIndexPlan.getUuid()), response);
        } catch (Exception e) {
            logger.error("Update agg index failed...", e);
            throw e;
        }
    }

    @Transaction(project = 0)
    public BuildIndexResponse updateTableIndex(String project, CreateTableIndexRequest request) {
        aclEvaluate.checkProjectOperationDesignPermission(project);
        try {
            val indexPlan = getIndexPlan(request.getProject(), request.getModelId());
            val layout = parseToLayout(project, request, indexPlan.getNextTableIndexId() + 1);
            for (LayoutEntity cuboidLayout : indexPlan.getAllLayouts()) {
                if (cuboidLayout.equals(layout) && cuboidLayout.isManual()) {
                    throw new KylinException(ServerErrorCode.DUPLICATE_INDEX, MsgPicker.getMsg().getDuplicateLayout());
                }
            }
            if (indexPlan.getLayoutEntity(request.getId()) != null && IndexEntity.isTableIndex(request.getId())) {
                deleteOrMarkTobeDelete(project, request.getModelId(), Sets.newHashSet(request.getId()));
            }
            return createTableIndex(project, request);
        } catch (Exception e) {
            logger.error("Update table index failed...", e);
            throw e;
        }
    }

    private LayoutEntity parseToLayout(String project, CreateTableIndexRequest request, long layoutId) {
        val indexPlan = getIndexPlan(request.getProject(), request.getModelId());
        NDataModel model = indexPlan.getModel();

        val newLayout = new LayoutEntity();
        newLayout.setId(layoutId);

        // handle remove the latest table index
        if (Objects.equals(newLayout.getId(), request.getId())) {
            newLayout.setId(newLayout.getId() + IndexEntity.INDEX_ID_STEP);
        }
        newLayout.setColOrder(convertColumn(request.getColOrder(), model));
        newLayout.setStorageType(request.getStorageType());
        newLayout.setShardByColumns(convertColumn(request.getShardByColumns(), model));
        newLayout.setUpdateTime(System.currentTimeMillis());
        newLayout.setOwner(BasicService.getUsername());
        newLayout.setManual(true);
        newLayout.setIndexRange(request.getIndexRange());
        if (!newLayout.getColOrder().containsAll(newLayout.getShardByColumns())) {
            throw new KylinException(SHARD_BY_COLUMN_NOT_IN_INDEX);
        }

        Map<Integer, String> layoutOverride = Maps.newHashMap();
        if (request.getLayoutOverrideIndexes() != null) {
            for (Map.Entry<String, String> entry : request.getLayoutOverrideIndexes().entrySet()) {
                layoutOverride.put(model.getColumnIdByColumnName(entry.getKey()), entry.getValue());
            }
        }
        newLayout.setLayoutOverrideIndexes(layoutOverride);
        return newLayout;
    }

    @Transaction(project = 0)
    public BuildIndexResponse createTableIndex(String project, CreateTableIndexRequest request) {
        aclEvaluate.checkProjectOperationDesignPermission(project);
        val indexPlan = getIndexPlan(request.getProject(), request.getModelId());
        return createTableIndex(project, request, indexPlan.getNextTableIndexId() + 1);
    }

    @Transaction(project = 0)
    public BuildIndexResponse createTableIndex(String project, CreateTableIndexRequest request, long layoutId) {
        aclEvaluate.checkProjectOperationDesignPermission(project);
        val newLayout = parseToLayout(project, request, layoutId);
        return createTableIndex(project, request.getModelId(), newLayout, request.isLoadData());
    }

    private BuildIndexResponse createTableIndex(String project, String modelId, LayoutEntity newLayout,
            boolean loadData) {
        NIndexPlanManager indexPlanManager = getManager(NIndexPlanManager.class, project);
        val jobManager = getManager(JobManager.class, project);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(modelId);
        for (LayoutEntity cuboidLayout : indexPlan.getAllLayouts()) {
            if (cuboidLayout.equals(newLayout) && cuboidLayout.isManual()) {
                throw new KylinException(INDEX_DUPLICATE);
            }
        }
        int layoutIndex = indexPlan.getWhitelistLayouts().indexOf(newLayout);
        if (layoutIndex != -1) {
            // find the target layout in this index plan
            indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
                val oldLayout = copyForWrite.getWhitelistLayouts().get(layoutIndex);
                oldLayout.setManual(true);
                oldLayout.setOwner(BasicService.getUsername());
                oldLayout.setUpdateTime(System.currentTimeMillis());
            });
            modelChangeSupporters.forEach(listener -> listener.onUpdate(project, modelId));
            return new BuildIndexResponse(BuildIndexResponse.BuildIndexType.NO_LAYOUT);
        } else {
            // create a new index for this layout
            // update this index plan with the new layout
            indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
                val newCuboid = new IndexEntity();
                newCuboid.setId(newLayout.getId() - 1);
                newCuboid.setDimensions(Lists.newArrayList(newLayout.getColOrder()));
                newCuboid.setLayouts(Arrays.asList(newLayout));
                newCuboid.setIndexPlan(copyForWrite);
                IndexEntity lookForIndex = copyForWrite.getWhiteListIndexesMap().get(newCuboid.createIndexIdentifier());
                if (lookForIndex == null) {
                    copyForWrite.getIndexes().add(newCuboid);
                } else {
                    IndexEntity realIndex = copyForWrite.getIndexes()
                            .get(copyForWrite.getIndexes().indexOf(lookForIndex));
                    newLayout.setId(realIndex.getId() + realIndex.getNextLayoutOffset());
                    realIndex.setNextLayoutOffset((realIndex.getNextLayoutOffset() + 1) % IndexEntity.INDEX_ID_STEP);
                    realIndex.getLayouts().add(newLayout);
                }
            });
            modelChangeSupporters.forEach(listener -> listener.onUpdate(project, modelId));
            if (loadData) {
                val df = getManager(NDataflowManager.class, project).getDataflow(modelId);
                val readySegs = df.getSegments();
                if (readySegs.isEmpty()) {
                    return new BuildIndexResponse(BuildIndexResponse.BuildIndexType.NO_SEGMENT);
                }
                getManager(SourceUsageManager.class).licenseCheckWrap(project,
                        () -> jobManager.addIndexJob(new JobParam(indexPlan.getUuid(), BasicService.getUsername())));
                return new BuildIndexResponse(BuildIndexResponse.BuildIndexType.NORM_BUILD);
            }
        }
        return new BuildIndexResponse();
    }

    @Deprecated
    @Transaction(project = 0)
    public void removeTableIndex(String project, String model, final long id) {
        aclEvaluate.checkProjectOperationDesignPermission(project);
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, project);

        val indexPlan = getIndexPlan(project, model);
        Preconditions.checkState(indexPlan != null);
        if (id < IndexEntity.TABLE_INDEX_START_ID) {
            throw new IllegalStateException("Table Index Id should large than " + IndexEntity.TABLE_INDEX_START_ID);
        }
        val layout = indexPlan.getLayoutEntity(id);
        Preconditions.checkNotNull(layout);
        Preconditions.checkState(layout.isManual());

        indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(id), false, true);
        });
    }

    @Transaction(project = 0)
    public void removeIndex(String project, String model, final long id) {
        removeIndexes(project, model, Collections.singleton(id));
    }

    /**
     * Remove index by ids.
     * @param project project name
     * @param modelId model uuid
     * @param ids layout ids to remove
     */
    @Transaction(project = 0)
    public void removeIndexes(String project, String modelId, Set<Long> ids) {
        removeIndexes(project, modelId, ids, Sets.newHashSet(), Sets.newHashSet());
    }

    /**
     * Remove index by ids, also support remove invalid dimensions and measures.
     * If the dimensions and measures on model has been deleted, these dimensions and measures
     * must delete
     * @param project project name
     * @param modelId model uuid
     * @param ids layout ids to remove
     * @param invalidDimensions invalid dimension ids
     * @param invalidMeasures  invalid measure ids
     */
    @Transaction(project = 0)
    public void removeIndexes(String project, String modelId, Set<Long> ids, Set<Integer> invalidDimensions,
            Set<Integer> invalidMeasures) {
        aclEvaluate.checkProjectOperationDesignPermission(project);
        if (CollectionUtils.isEmpty(ids)) {
            throw new KylinException(LAYOUT_LIST_EMPTY);
        }

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, project);

        IndexPlan indexPlan = getIndexPlan(project, modelId);
        Preconditions.checkNotNull(indexPlan);

        String notExistsLayoutIds = ids.stream().filter(id -> Objects.isNull(indexPlan.getLayoutEntity(id))).sorted()
                .map(String::valueOf).collect(Collectors.joining(","));

        if (StringUtils.isNotEmpty(notExistsLayoutIds)) {
            throw new KylinException(LAYOUT_NOT_EXISTS, notExistsLayoutIds);
        }

        indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
            val ruleLayoutIds = ids.stream().filter(id -> {
                if (IndexEntity.isAggIndex(id) && indexPlan.getLayoutEntity(id).isManual()) {
                    return true;
                }
                return false;
            }).collect(Collectors.toList());
            copyForWrite.addRuleBasedBlackList(ruleLayoutIds);
            copyForWrite.removeLayouts(ids, true, true);
            removeAggGroup(invalidDimensions, invalidMeasures, copyForWrite);
        });

        modelChangeSupporters.forEach(listener -> listener.onUpdate(project, modelId));

        removeSecondStorageIndex(project, modelId, ids);
    }

    private void removeAggGroup(Set<Integer> invalidDimensions, Set<Integer> invalidMeasures, IndexPlan indexPlan) {
        RuleBasedIndex oldRuleBasedIndex = indexPlan.getRuleBasedIndex();
        if ((invalidDimensions.isEmpty() && invalidMeasures.isEmpty()) || oldRuleBasedIndex == null) {
            return;
        }
        List<NAggregationGroup> reservedAggGroups = oldRuleBasedIndex.getAggregationGroups().stream()
                .filter(aggGroup -> Arrays.stream(aggGroup.getIncludes()).noneMatch(invalidDimensions::contains)
                        && Arrays.stream(aggGroup.getMeasures()).noneMatch(invalidMeasures::contains))
                .collect(Collectors.toList());
        RuleBasedIndex newRuleBasedIndex = RuleBasedIndex.copyAndResetAggGroups(oldRuleBasedIndex, reservedAggGroups);
        indexPlan.setRuleBasedIndex(newRuleBasedIndex, Sets.newHashSet(), false, true, false);
    }

    private boolean addIndexToBeDeleted(String project, String modelId, Set<Long> layoutIds) {
        aclEvaluate.checkProjectOperationDesignPermission(project);
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, project);

        val indexPlan = getIndexPlan(project, modelId);
        Preconditions.checkNotNull(indexPlan);
        for (Long id : layoutIds) {
            val layout = indexPlan.getLayoutEntity(id);
            Preconditions.checkNotNull(layout);
        }

        Map<Long, Boolean> secondStorageLayoutStatus = getSecondStorageLayoutStatus(project, layoutIds, indexPlan);
        indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> copyForWrite
                .markWhiteIndexToBeDelete(indexPlan.getUuid(), Sets.newHashSet(layoutIds), secondStorageLayoutStatus));

        return true;
    }

    public Map<Long, Boolean> getSecondStorageLayoutStatus(String project, Set<Long> layoutIds, IndexPlan indexPlan) {
        if (!SecondStorageUtil.isModelEnable(project, indexPlan.getUuid())) {
            return Collections.emptyMap();
        }
        Map<Long, Boolean> secondStorageLayoutStatus = new HashMap<>(layoutIds.size());
        for (long layoutId : layoutIds) {
            if (indexPlan.getLayoutEntity(layoutId) != null && indexPlan.getLayoutEntity(layoutId).isBaseIndex()
                    && indexPlan.getLayoutEntity(layoutId).getIndex().isTableIndex()) {
                secondStorageLayoutStatus.put(layoutId,
                        isSecondStorageLayoutReady(project, indexPlan.getUuid(), layoutId));
            }
        }
        return secondStorageLayoutStatus;
    }

    public DiffRuleBasedIndexResponse calculateDiffRuleBasedIndex(UpdateRuleBasedCuboidRequest request) {
        aclEvaluate.checkProjectOperationDesignPermission(request.getProject());
        UpdateRuleImpact diff = getIndexPlan(request.getProject(), request.getModelId())
                .diffRuleBasedIndex(convertRequestToRuleBasedIndex(request));

        return DiffRuleBasedIndexResponse.from(request.getModelId(), diff);
    }

    public AggIndexResponse calculateAggIndexCount(UpdateRuleBasedCuboidRequest request) {
        aclEvaluate.checkProjectOperationDesignPermission(request.getProject());
        val maxCount = getConfig().getCubeAggrGroupMaxCombination();
        // The agg group for updates which includes all agg group for the index
        List<NAggregationGroup> aggregationGroups = request.getAggregationGroups();
        val indexPlan = getIndexPlan(request.getProject(), request.getModelId()).copy();
        AggIndexCombResult totalResult;
        AggIndexCombResult aggIndexResult;

        // Filter the invalid agg group
        val aggregationGroupsCopy = aggregationGroups.stream()
                .filter(aggGroup -> aggGroup.getIncludes() != null && aggGroup.getIncludes().length != 0)
                .collect(Collectors.toList());
        request.setAggregationGroups(aggregationGroupsCopy);

        boolean invalid = false;
        try {
            RuleBasedIndex ruleBasedIndex = convertRequestToRuleBasedIndex(request);
            NDataModel model = NDataModelManager.getInstance(getConfig(), request.getProject())
                    .getDataModelDesc(indexPlan.getUuid());

            if (CollectionUtils.isNotEmpty(ruleBasedIndex.getDimensions())) {
                List<Integer> notExistCols = ruleBasedIndex.getDimensions().stream()
                        .filter(col -> null == model.getEffectiveDimensions()
                                || null == model.getEffectiveDimensions().get(col))
                        .collect(Collectors.toList());

                if (CollectionUtils.isNotEmpty(notExistCols)) {
                    throw new KylinException(ServerErrorCode.EFFECTIVE_DIMENSION_NOT_FIND,
                            String.format(Locale.ROOT, MsgPicker.getMsg().getEffectiveDimensionNotFind(),
                                    StringUtils.join(notExistCols.iterator(), ",")));
                }
            }
            indexPlan.setRuleBasedIndex(ruleBasedIndex);
        } catch (OutOfMaxCombinationException oe) {
            invalid = true;
            log.error("The number of cuboid for the cube exceeds the limit, ", oe);
        } catch (IllegalStateException e) {
            log.error(e.getMessage());
        }

        List<AggIndexCombResult> aggIndexCounts = Lists.newArrayList();
        for (NAggregationGroup group : aggregationGroups) {
            if (group.getIncludes() != null && group.getIncludes().length == 0) {
                aggIndexResult = AggIndexCombResult.successResult(0L);
                aggIndexCounts.add(aggIndexResult);
                continue;
            }
            long count = group.calculateCuboidCombination();
            if (count > maxCount) {
                aggIndexResult = AggIndexCombResult.errorResult();
                invalid = true;
            } else {
                aggIndexResult = AggIndexCombResult.successResult(count);
            }
            aggIndexCounts.add(aggIndexResult);
        }

        if (invalid) {
            totalResult = AggIndexCombResult.errorResult();
        } else {
            try {
                long cuboidCount = indexPlan.getRuleBasedIndex().getCuboidScheduler().getCuboidCount();
                totalResult = AggIndexCombResult.successResult(cuboidCount);
            } catch (OutOfMaxCombinationException outOfMaxCombinationException) {
                totalResult = AggIndexCombResult.errorResult();
            }
        }
        return new AggIndexResponse(aggIndexCounts, totalResult, getConfig().getCubeAggrGroupMaxCombination());
    }

    public void checkIndexCountWithinLimit(UpdateRuleBasedCuboidRequest request) {
        val maxCount = getConfig().getCubeAggrGroupMaxCombination();
        List<NAggregationGroup> aggGroups = request.getAggregationGroups();

        val indexPlan = getIndexPlan(request.getProject(), request.getModelId()).copy();

        val aggregationGroupsCopy = aggGroups.stream()
                .filter(aggGroup -> aggGroup.getIncludes() != null && aggGroup.getIncludes().length != 0)
                .collect(Collectors.toList());
        request.setAggregationGroups(aggregationGroupsCopy);

        try {
            indexPlan.setRuleBasedIndex(convertRequestToRuleBasedIndex(request));
        } catch (OutOfMaxCombinationException oe) {
            log.error("The number of cuboid for the cube exceeds the limit, ", oe);
        } catch (IllegalStateException e) {
            log.error(e.getMessage());
        }

        for (NAggregationGroup aggGroup : aggGroups) {
            long count = aggGroup.calculateCuboidCombination();
            if (count > maxCount) {
                throw new IllegalArgumentException(
                        "The aggregate amount exceeds its limit per aggregate group, please optimize the group setting or reduce dimension amount.");
            }
        }
    }

    @Transaction(project = 0)
    public void updateShardByColumns(String project, AggShardByColumnsRequest request) {
        aclEvaluate.checkProjectOperationDesignPermission(project);

        val modelId = request.getModelId();
        val indexPlanManager = getManager(NIndexPlanManager.class, project);
        val indexPlan = indexPlanManager.getIndexPlan(modelId);
        val model = indexPlan.getModel();

        val dimensions = model.getDimensionNameIdMap();
        for (String shardByColumn : request.getShardByColumns()) {
            if (!dimensions.containsKey(shardByColumn)) {
                throw new KylinException(PERMISSION_DENIED,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getColumuIsNotDimension(), shardByColumn));
            }
        }

        indexPlanManager.updateIndexPlan(modelId, copyForWrite -> {
            copyForWrite.setAggShardByColumns(request.getShardByColumns().stream().map(model::getColumnIdByColumnName)
                    .collect(Collectors.toList()));
        });
        if (request.isLoadData()) {
            val jobManager = getManager(JobManager.class, project);
            getManager(SourceUsageManager.class).licenseCheckWrap(project,
                    () -> jobManager.addIndexJob(new JobParam(modelId, BasicService.getUsername())));
        }
    }

    public AggShardByColumnsResponse getShardByColumns(String project, String modelId) {
        aclEvaluate.checkProjectOperationDesignPermission(project);
        val indexPlanManager = getManager(NIndexPlanManager.class, project);
        val indexPlan = indexPlanManager.getIndexPlan(modelId);
        val model = indexPlan.getModel();
        val result = new AggShardByColumnsResponse();
        result.setModelId(modelId);
        result.setProject(project);
        result.setShardByColumns(indexPlan.getAggShardByColumns().stream().map(model::getColumnNameByColumnId)
                .collect(Collectors.toList()));

        val df = getManager(NDataflowManager.class, project).getDataflow(modelId);
        Segments<NDataSegment> segments = df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING,
                SegmentStatusEnum.NEW);

        val executableManager = getManager(NExecutableManager.class, project);
        List<AbstractExecutable> executables = executableManager.listExecByModelAndStatus(modelId,
                ExecutableState::isProgressing, JobTypeEnum.INDEX_BUILD, JobTypeEnum.INC_BUILD,
                JobTypeEnum.INDEX_REFRESH, JobTypeEnum.INDEX_MERGE);

        if (segments.isEmpty() && executables.isEmpty()) {
            result.setShowLoadData(false);
        }

        return result;
    }

    public List<TableIndexResponse> getTableIndexs(String project, String model) {
        aclEvaluate.checkProjectReadPermission(project);
        val indexPlan = getIndexPlan(project, model);
        Preconditions.checkState(indexPlan != null);
        List<TableIndexResponse> result = Lists.newArrayList();
        for (LayoutEntity cuboidLayout : indexPlan.getAllLayouts()) {
            if (IndexEntity.isTableIndex(cuboidLayout.getId())) {
                result.add(convertToTableIndexResponse(cuboidLayout, indexPlan.getModel()));
            }
        }
        return result;
    }

    public List<IndexResponse> getIndexesWithRelatedTables(String project, String modelId, String key,
            List<IndexEntity.Status> status, String orderBy, Boolean desc, List<IndexEntity.Source> sources,
            List<Long> ids) {
        List<IndexResponse> indexes = getIndexes(project, modelId, key, status, orderBy, desc, sources, ids);
        IndexPlan indexPlan = getIndexPlan(project, modelId);
        NDataModel model = indexPlan.getModel();
        IndexDependencyParser parser = new IndexDependencyParser(model);
        indexes.forEach(indexResponse -> {
            LayoutEntity layout = indexPlan.getLayoutEntity(indexResponse.getId());
            indexResponse.setRelatedTables(parser.getRelatedTables(layout));
        });
        return indexes;
    }

    public List<IndexResponse> getIndexes(String project, String modelId, String key, List<IndexEntity.Status> status,
            String orderBy, Boolean desc, List<IndexEntity.Source> sources, List<Long> ids) {
        List<IndexResponse> indexes = getIndexes(project, modelId, key, status, orderBy, desc, sources);
        if (CollectionUtils.isEmpty(ids)) {
            return indexes;
        }
        return indexes.stream().filter(index -> ids.contains(index.getId())).collect(Collectors.toList());
    }

    public List<IndexResponse> getIndexes(String project, String modelId, String key, List<IndexEntity.Status> status,
                                          String orderBy, Boolean desc, List<IndexEntity.Source> sources) {
        return getIndexes(new IndexPlanParams(project, modelId, null, null, sources, status, null),
                new PaginationParams(null, null, orderBy, desc),
                key);
    }

    public List<IndexResponse> getIndexes(IndexPlanParams indexPlanParams, PaginationParams paginationParams, String key) {
        String project = indexPlanParams.getProject();
        String modelId = indexPlanParams.getModelId();
        String segmentId = indexPlanParams.getSegmentId();
        List<IndexEntity.Source> sources = indexPlanParams.getSources();
        List<IndexEntity.Status> status = indexPlanParams.getStatus();
        String orderBy = paginationParams.getOrderBy();
        Boolean desc = paginationParams.getReverse();

        aclEvaluate.checkProjectReadPermission(project);
        Set<IndexEntity.Status> statusSet = Sets.newHashSet(status);

        val indexPlan = getIndexPlan(project, modelId);
        Preconditions.checkState(indexPlan != null);
        val model = indexPlan.getModel();
        val layouts = indexPlan.getAllLayouts();
        Set<Long> layoutsByRunningJobs = getLayoutsByRunningJobs(project, modelId);
        if (StringUtils.isBlank(key)) {
            return sortAndFilterLayouts(layouts.stream()
                    .map(layoutEntity -> convertToResponse(layoutEntity, indexPlan.getModel(), layoutsByRunningJobs, segmentId))
                    .filter(indexResponse -> statusSet.isEmpty() || statusSet.contains(indexResponse.getStatus())),
                    orderBy, desc, sources);
        }

        Set<String> ccFullNameSet = FuzzyKeySearcher.searchComputedColumns(model, key.trim());
        Set<Integer> matchDimensions = FuzzyKeySearcher.searchDimensions(model, ccFullNameSet, key.trim());
        Set<Integer> matchMeasures = FuzzyKeySearcher.searchMeasures(model, ccFullNameSet, key.trim());

        return sortAndFilterLayouts(layouts.stream().filter(index -> {
            Set<Integer> colOrderSet = Sets.newHashSet(index.getColOrder());
            return String.valueOf(index.getId()).equals(key.trim())
                    || !Sets.intersection(matchDimensions, colOrderSet).isEmpty()
                    || !Sets.intersection(matchMeasures, colOrderSet).isEmpty();
        }).map(layoutEntity -> convertToResponse(layoutEntity, indexPlan.getModel(), layoutsByRunningJobs, segmentId))
                .filter(indexResponse -> statusSet.isEmpty() || statusSet.contains(indexResponse.getStatus())), orderBy,
                desc, sources);
    }

    public IndexGraphResponse getIndexGraph(String project, String modelId, int maxSize) {
        aclEvaluate.checkProjectReadPermission(project);
        val indexPlan = getIndexPlan(project, modelId);
        Preconditions.checkNotNull(indexPlan);
        val indexes = indexPlan.getAllLayouts();
        val indexResponses = sortAndFilterLayouts(
                indexes.stream().map(layoutEntity -> convertToResponse(layoutEntity, indexPlan.getModel())), DATA_SIZE,
                true, Lists.newArrayList());
        val indexGraphResponse = new IndexGraphResponse();

        indexGraphResponse.setProject(project);
        indexGraphResponse.setModel(modelId);
        indexGraphResponse.setTotalIndexes(indexResponses.size());
        indexGraphResponse.setEmptyIndexes(
                indexResponses.stream().filter(r -> IndexEntity.Status.NO_BUILD == r.getStatus()).count());

        Function<IndexResponse, IndexGraphResponse.Index> responseConverter = res -> {
            val index = new IndexGraphResponse.Index();
            BeanUtils.copyProperties(res, index);
            return index;
        };
        indexGraphResponse.setAutoAggIndexes(convertToIndexList(indexResponses.stream().limit(maxSize)
                .filter(r -> IndexEntity.Source.RECOMMENDED_AGG_INDEX == r.getSource()).map(responseConverter)));
        indexGraphResponse.setManualAggIndexes(convertToIndexList(indexResponses.stream().limit(maxSize)
                .filter(r -> IndexEntity.Source.CUSTOM_AGG_INDEX == r.getSource()).map(responseConverter)));
        indexGraphResponse.setAutoTableIndexes(convertToIndexList(indexResponses.stream().limit(maxSize)
                .filter(r -> IndexEntity.Source.RECOMMENDED_TABLE_INDEX == r.getSource()).map(responseConverter)));
        indexGraphResponse.setManualTableIndexes(convertToIndexList(indexResponses.stream().limit(maxSize)
                .filter(r -> IndexEntity.Source.CUSTOM_TABLE_INDEX == r.getSource()).map(responseConverter)));

        val dataflow = NDataflowManager.getInstance(indexPlan.getConfig(), indexPlan.getProject())
                .getDataflow(indexPlan.getId());
        // include all segs (except refreshing, merging) for data range count
        val readySegments = SegmentUtil.getSegmentsExcludeRefreshingAndMerging(dataflow.getSegments()).stream()
                .collect(Collectors.toCollection(Segments::new));
        long startTime = 0;
        long endTime = 0;
        if (!readySegments.isEmpty()) {
            startTime = Long.MAX_VALUE;
            for (NDataSegment seg : readySegments) {
                long start = Long.parseLong(seg.getSegRange().getStart().toString());
                long end = Long.parseLong(seg.getSegRange().getEnd().toString());
                startTime = Math.min(startTime, start);
                endTime = Math.max(endTime, end);
            }
        }
        indexGraphResponse.setStartTime(startTime);
        indexGraphResponse.setEndTime(endTime);

        long segmentToComplementCount = 0;
        long allIndexCountWithoutTobeDel = indexPlan.getAllLayoutsSize(false);
        for (NDataSegment seg : readySegments) {
            val lockedIndexCountInSeg = seg.getLayoutsMap().values().stream()
                    .filter(nDataLayout -> nDataLayout.getLayout().isToBeDeleted()).count();
            if ((seg.getSegDetails().getAllLayouts().size() - lockedIndexCountInSeg) != allIndexCountWithoutTobeDel) {
                segmentToComplementCount += 1;
            }
        }
        indexGraphResponse.setSegmentToComplementCount(segmentToComplementCount);

        return indexGraphResponse;
    }

    private List<IndexResponse> sortAndFilterLayouts(Stream<IndexResponse> layouts, String orderBy, boolean reverse,
            List<IndexEntity.Source> sources) {
        if (CollectionUtils.isNotEmpty(sources)) {
            layouts = layouts.filter(r -> sources.contains(r.getSource()));
        }
        Comparator<IndexResponse> comparator;
        // default EMPTY layout at first then order by data_size ascending;
        if (StringUtils.isEmpty(orderBy)) {
            comparator = Comparator.<IndexResponse> comparingInt(res -> res.getStatus().ordinal())
                    .thenComparing(BasicService.propertyComparator(DATA_SIZE, true));
        } else {
            comparator = BasicService.propertyComparator(orderBy, !reverse);
        }
        return layouts.sorted(comparator).collect(Collectors.toList());
    }

    private IndexGraphResponse.IndexList convertToIndexList(Stream<IndexGraphResponse.Index> stream) {
        val indexList = new IndexGraphResponse.IndexList();
        indexList.setIndexes(stream.collect(Collectors.toList()));
        indexList.setTotalSize(indexList.getIndexes().stream().mapToLong(IndexGraphResponse.Index::getDataSize).sum());
        return indexList;

    }

    public RuleBasedIndex getRule(String project, String model) {
        aclEvaluate.checkProjectOperationDesignPermission(project);
        val indexPlan = getIndexPlan(project, model);
        Preconditions.checkState(indexPlan != null);

        val index = indexPlan.getRuleBasedIndex();
        if (index == null) {
            return null;
        }
        val newRuleBasedIndex = new RuleBasedIndex();

        newRuleBasedIndex.setIndexUpdateEnabled(index.getIndexUpdateEnabled());
        newRuleBasedIndex.setAggregationGroups(new LinkedList<>());

        for (NAggregationGroup aggGrp : index.getAggregationGroups()) {
            val aggGrpCopy = new NAggregationGroup();
            aggGrpCopy.setIncludes(aggGrp.getIncludes());
            aggGrpCopy.setSelectRule(aggGrp.getSelectRule());
            aggGrpCopy.setIndexRange(aggGrp.getIndexRange());

            List<Integer> filteredMeasure = Lists.newArrayList(aggGrp.getMeasures());
            filteredMeasure.removeIf(indexPlan.getModel().getEffectiveInternalMeasureIds()::contains);
            aggGrpCopy.setMeasures(filteredMeasure.toArray(new Integer[0]));
            newRuleBasedIndex.getAggregationGroups().add(aggGrpCopy);
        }

        return newRuleBasedIndex;
    }

    private TableIndexResponse convertToTableIndexResponse(LayoutEntity cuboidLayout, NDataModel model) {
        val response = new TableIndexResponse();
        BeanUtils.copyProperties(cuboidLayout, response);
        response.setColOrder(convertColumnIdName(cuboidLayout.getColOrder(), model));
        response.setShardByColumns(convertColumnIdName(cuboidLayout.getShardByColumns(), model));
        response.setSortByColumns(convertColumnIdName(cuboidLayout.getSortByColumns(), model));
        response.setProject(model.getProject());
        response.setModel(model.getUuid());

        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val dataflow = dfMgr.getDataflow(cuboidLayout.getIndex().getIndexPlan().getUuid());
        TableIndexResponse.Status status = TableIndexResponse.Status.AVAILABLE;
        int readyCount = 0;
        for (NDataSegment segment : dataflow.getSegments()) {
            val dataCuboid = segment.getLayout(cuboidLayout.getId());
            if (dataCuboid == null) {
                continue;
            }
            readyCount++;
        }
        if (readyCount != dataflow.getSegments().size() || CollectionUtils.isEmpty(dataflow.getSegments())) {
            status = TableIndexResponse.Status.EMPTY;
        }
        response.setStatus(status);
        response.setUpdateTime(cuboidLayout.getUpdateTime());
        return response;
    }

    @VisibleForTesting
    public Set<Long> getLayoutsByRunningJobs(String project, String modelId) {
        List<AbstractExecutable> runningJobList = NExecutableManager
                .getInstance(KylinConfig.getInstanceFromEnv(), project) //
                .getPartialExecutablesByStatusList(
                        Sets.newHashSet(ExecutableState.READY, ExecutableState.RUNNING, ExecutableState.PAUSED,
                                ExecutableState.ERROR), //
                        path -> StringUtils.endsWith(path, modelId));

        return runningJobList.stream()
                .filter(abstractExecutable -> Objects.equals(modelId, abstractExecutable.getTargetSubject()))
                .map(AbstractExecutable::getToBeDeletedLayoutIds).flatMap(Set::stream).collect(Collectors.toSet());
    }

    private IndexResponse convertToResponse(LayoutEntity layoutEntity, NDataModel model) {
        return convertToResponse(layoutEntity, model, Sets.newHashSet(), null);
    }

    private IndexResponse convertToResponse(LayoutEntity layoutEntity, NDataModel model,
            Set<Long> layoutIdsOfRunningJobs, String segmentId) {

        // remove all internal measures
        val colOrders = Lists.newArrayList(layoutEntity.getColOrder());
        colOrders.removeIf(model.getEffectiveInternalMeasureIds()::contains);

        val response = new IndexResponse();
        BeanUtils.copyProperties(layoutEntity, response);
        response.setColOrder(convertColumnOrMeasureIdName(colOrders, model));
        response.setShardByColumns(convertColumnIdName(layoutEntity.getShardByColumns(), model));
        response.setSortByColumns(convertColumnIdName(layoutEntity.getSortByColumns(), model));
        response.setProject(model.getProject());
        response.setModel(model.getUuid());

        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val dataflow = dfMgr.getDataflow(layoutEntity.getIndex().getIndexPlan().getUuid());
        long dataSize = 0L;
        int readyCount = 0;
        boolean hasDataInconsistent = false;

        List<NDataSegment> segments = StringUtils.isBlank(segmentId) ? dataflow.getSegments()
                : dataflow.getSegments().stream().filter(seg -> seg.getId().equals(segmentId)).collect(Collectors.toList());
        for (NDataSegment segment : segments) {
            NDataLayout layout = segment.getLayout(layoutEntity.getId(), true);
            if (layout == null) {
                continue;
            }
            if (layout.getAbnormalType() == NDataLayout.AbnormalType.DATA_INCONSISTENT) {
                hasDataInconsistent = true;
                continue;
            }
            readyCount++;
            dataSize += layout.getByteSize();
        }

        IndexEntity.Status status;
        if (readyCount <= 0) {
            if (layoutIdsOfRunningJobs.contains(layoutEntity.getId())) {
                status = IndexEntity.Status.BUILDING;
            } else {
                status = IndexEntity.Status.NO_BUILD;
                if (hasDataInconsistent) {
                    response.setAbnormalType(NDataLayout.AbnormalType.DATA_INCONSISTENT);
                }
            }
        } else {
            status = IndexEntity.Status.ONLINE;
        }

        if (layoutEntity.isToBeDeleted()) {
            status = IndexEntity.Status.LOCKED;
        }

        response.setNeedUpdate(needUpdateLayout(model, layoutEntity));

        response.setStatus(status);
        response.setDataSize(dataSize);
        response.setLastModified(layoutEntity.getUpdateTime());
        if (dataflow.getLayoutHitCount().get(layoutEntity.getId()) != null) {
            response.setUsage(dataflow.getLayoutHitCount().get(layoutEntity.getId()).getDateFrequency().values()
                    .stream().mapToInt(Integer::intValue).sum());
        }
        return response;
    }

    private boolean needUpdateLayout(NDataModel model, LayoutEntity layoutEntity) {
        IndexPlan indexPlan = getIndexPlan(model.getProject(), model.getId());
        if (layoutEntity.isBase()) {
            if (IndexEntity.isAggIndex(layoutEntity.getId())) {
                return indexPlan.needUpdateBaseAggLayout(indexPlan.createBaseAggIndex(model), true);
            } else {
                return indexPlan.needUpdateBaseTableLayout(indexPlan.createBaseTableIndex(model), true);
            }
        }
        return false;
    }

    private IndexPlan getIndexPlan(String project, String model) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val indexPlanManager = NIndexPlanManager.getInstance(kylinConfig, project);
        return indexPlanManager.getIndexPlan(model);
    }

    private List<String> convertColumnIdName(List<Integer> ids, NDataModel model) {
        if (CollectionUtils.isEmpty(ids)) {
            return Lists.newArrayList();
        }
        val result = Lists.<String> newArrayList();
        for (Integer columnId : ids) {
            val name = (columnId < NDataModel.MEASURE_ID_BASE) ? model.getColumnNameByColumnId(columnId)
                    : model.getMeasureNameByMeasureId(columnId);
            result.add(name);
        }
        return result;

    }

    private List<IndexResponse.ColOrderPair> convertColumnOrMeasureIdName(List<Integer> ids, NDataModel model) {
        if (CollectionUtils.isEmpty(ids)) {
            return Lists.newArrayList();
        }
        NTableMetadataManager tableMetadata = NTableMetadataManager.getInstance(getConfig(), model.getProject());
        val result = Lists.<IndexResponse.ColOrderPair> newArrayList();
        for (Integer id : ids) {
            if (id < NDataModel.MEASURE_ID_BASE) {
                String columnName = model.getColumnNameByColumnId(id);
                if (columnName == null) {
                    result.add(new IndexResponse.ColOrderPair(id + "", "column", null));
                    continue;
                }
                TblColRef colRef = model.findColumnByAlias(columnName);
                TableExtDesc tableExt = tableMetadata.getTableExtIfExists(colRef.getTableRef().getTableDesc());
                TableExtDesc.ColumnStats columnStats = Objects.isNull(tableExt) ? null
                        : tableExt.getColumnStatsByName(colRef.getName());
                result.add(new IndexResponse.ColOrderPair(columnName, "column",
                        Objects.isNull(columnStats) ? null : columnStats.getCardinality()));
            } else {
                result.add(new IndexResponse.ColOrderPair(model.getMeasureNameByMeasureId(id), "measure"));
            }
        }
        return result;

    }

    private List<Integer> convertColumn(List<String> columns, NDataModel model) {
        if (CollectionUtils.isEmpty(columns)) {
            return Lists.newArrayList();
        }
        val result = Lists.<Integer> newArrayList();
        for (String column : columns) {
            val id = model.getColumnIdByColumnName(column);
            result.add(id);
        }
        return result;
    }

    public void reloadLayouts(String project, String modelId, Set<Long> changedLayouts) {
        getManager(NIndexPlanManager.class, project).updateIndexPlan(modelId, copy -> {
            val indexes = changedLayouts.stream()
                    .map(layout -> (layout / IndexEntity.INDEX_ID_STEP) * IndexEntity.INDEX_ID_STEP)
                    .collect(Collectors.toSet());
            val originAgg = JsonUtil.deepCopyQuietly(copy.getRuleBasedIndex(), RuleBasedIndex.class);

            val reloadLayouts = copy.getRuleBaseLayouts().stream().filter(l -> changedLayouts.contains(l.getId()))
                    .collect(Collectors.toSet());

            copy.addRuleBasedBlackList(reloadLayouts.stream().map(LayoutEntity::getId).collect(Collectors.toList()));
            val changedIndexes = copy.getIndexes().stream().filter(i -> indexes.contains(i.getId()))
                    .collect(Collectors.toList());
            copy.setIndexes(
                    copy.getIndexes().stream().filter(i -> !indexes.contains(i.getId())).collect(Collectors.toList()));
            var nextAggIndexId = copy.getNextAggregationIndexId();
            var nextTableIndexId = copy.getNextTableIndexId();
            for (IndexEntity indexEntity : changedIndexes) {
                var nextLayoutOffset = 1L;
                var nextIndexId = indexEntity.isTableIndex() ? nextTableIndexId : nextAggIndexId;
                indexEntity.setId(nextIndexId);
                if (indexEntity.isTableIndex()) {
                    nextTableIndexId = nextIndexId + IndexEntity.INDEX_ID_STEP;
                } else {
                    nextAggIndexId = nextIndexId + IndexEntity.INDEX_ID_STEP;
                }
                for (LayoutEntity layoutEntity : indexEntity.getLayouts()) {
                    layoutEntity.setId(indexEntity.getId() + (nextLayoutOffset++));
                }
                indexEntity.setNextLayoutOffset(nextLayoutOffset);
            }
            val indexList = copy.getIndexes();
            indexList.addAll(changedIndexes);
            copy.setIndexes(indexList);
            if (originAgg != null) {
                val updatedAgg = new RuleBasedIndex();
                updatedAgg.setAggregationGroups(originAgg.getAggregationGroups());
                updatedAgg.setGlobalDimCap(originAgg.getGlobalDimCap());
                updatedAgg.setDimensions(originAgg.getDimensions());
                updatedAgg.setLastModifiedTime(System.currentTimeMillis());
                copy.setRuleBasedIndex(updatedAgg, reloadLayouts, false, false, false);
            }
            // cleanup to_be_deleted layouts
            copy.removeLayouts(changedLayouts, true, true);
        });
    }

    public void updateForMeasureChange(String project, String modelId, Set<Integer> invalidMeasures,
            Map<Integer, Integer> replacedMeasure) {
        if (getManager(NIndexPlanManager.class, project).getIndexPlan(modelId).getRuleBasedIndex() == null)
            return;

        getManager(NIndexPlanManager.class, project).updateIndexPlan(modelId, copy -> {
            val copyRuleBaseIndex = JsonUtil.deepCopyQuietly(copy.getRuleBasedIndex(), RuleBasedIndex.class);

            for (ListIterator<Integer> measureItr = copyRuleBaseIndex.getMeasures().listIterator(); measureItr
                    .hasNext();) {
                Integer measureId = measureItr.next();
                if (invalidMeasures.contains(measureId)) {
                    measureItr.remove();
                } else if (replacedMeasure.get(measureId) != null) {
                    measureItr.set(replacedMeasure.get(measureId));
                }
            }

            for (ListIterator<NAggregationGroup> aggGroupItr = copyRuleBaseIndex.getAggregationGroups()
                    .listIterator(); aggGroupItr.hasNext();) {
                NAggregationGroup aggGroup = aggGroupItr.next();
                Integer[] aggGroupMeasures = aggGroup.getMeasures();
                for (int i = 0; i < aggGroupMeasures.length; i++) {
                    if (replacedMeasure.get(aggGroupMeasures[i]) != null) {
                        aggGroupMeasures[i] = replacedMeasure.get(aggGroupMeasures[i]);
                    } else if (invalidMeasures.contains(aggGroupMeasures[i])) {
                        aggGroupItr.remove();
                    }
                }
            }

            // prevent genCuboidLayouts() from creating layout
            if (invalidMeasures.size() > 0 && copyRuleBaseIndex.getAggregationGroups().size() == 0) {
                copyRuleBaseIndex.setDimensions(new ArrayList<>());
            }

            copy.setRuleBasedIndex(copyRuleBaseIndex);
        });
    }

    @Transaction(project = 0)
    public void clearShardColIfNotDim(String project, String modelId) {
        aclEvaluate.checkProjectWritePermission(project);

        val indexPlanManager = getManager(NIndexPlanManager.class, project);
        val indexPlan = indexPlanManager.getIndexPlan(modelId);
        val dimensions = indexPlan.getModel().getEffectiveDimensions();

        val oldShardCols = indexPlan.getAggShardByColumns();
        val effectiveShardCol = oldShardCols.stream().filter(dimensions::containsKey).collect(Collectors.toList());

        if (effectiveShardCol.size() < oldShardCols.size()) {
            indexPlanManager.updateIndexPlan(modelId, copyForWrite -> {
                copyForWrite.setAggShardByColumns(effectiveShardCol);
            });
        }
    }

    @Transaction(project = 0)
    public BuildBaseIndexResponse updateBaseIndex(String project, CreateBaseIndexRequest request,
            boolean createIfNotExist) {
        return updateBaseIndex(project, request, createIfNotExist, createIfNotExist, false);
    }

    @Transaction(project = 0)
    public BuildBaseIndexResponse updateBaseIndex(String project, CreateBaseIndexRequest request,
            boolean createIfNotExistTableLayout, boolean createIfNotExistAggLayout, boolean isAuto) {
        aclEvaluate.checkProjectOperationDesignPermission(project);
        // update = delete + create
        Set<Long> needDelete = checkNeedUpdateBaseIndex(project, request, isAuto);
        List<LayoutEntity> needRetainAggLayout = getNeedRetainAggLayout(project, request, needDelete);
        deleteOrMarkTobeDelete(project, request.getModelId(), needDelete);
        removeFromBlackList(project, request, needDelete, needRetainAggLayout);

        if (createIfNotExistAggLayout) {
            request.getSourceTypes().add(Source.BASE_AGG_INDEX);
        }
        if (createIfNotExistTableLayout) {
            request.getSourceTypes().add(Source.BASE_TABLE_INDEX);
        }
        if (request.getSourceTypes().isEmpty()) {
            return BuildBaseIndexResponse.EMPTY;
        }

        BuildBaseIndexResponse response = createBaseIndex(project, request);
        response.setIndexUpdateType(needDelete);
        return response;
    }

    private List<LayoutEntity> getNeedRetainAggLayout(String project, CreateBaseIndexRequest request,
            Set<Long> needDelete) {
        if (CollectionUtils.isEmpty(needDelete)) {
            return Collections.emptyList();
        }
        String modelId = request.getModelId();
        IndexPlan indexPlan = getIndexPlan(project, modelId);
        LayoutEntity baseAggLayout = indexPlan.getBaseAggLayout();
        if (baseAggLayout != null) {
            long baseLaoutId = baseAggLayout.getId();
            if (indexPlan.getRuleBaseLayouts().contains(indexPlan.getBaseAggLayout())
                    && needDelete.contains(baseLaoutId)) {
                return indexPlan.getRuleBaseLayouts().stream().filter(r -> r.getId() == baseLaoutId)
                        .collect(Collectors.toList());
            }
        }
        return Collections.emptyList();
    }

    @Transaction(project = 0)
    private void removeFromBlackList(String project, CreateBaseIndexRequest request, Set<Long> needDelete,
            List<LayoutEntity> needRetainAggLayout) {
        if (CollectionUtils.isEmpty(needRetainAggLayout)) {
            return;
        }

        String modelId = request.getModelId();
        IndexPlan indexPlan = getIndexPlan(project, modelId);
        Set<Long> layoutIdMapping = new HashSet<>(indexPlan.getRuleBasedIndex().getLayoutIdMapping());

        List<LayoutEntity> retainLayout = needRetainAggLayout.stream()
                .filter(l -> layoutIdMapping.contains(l.getId()) && needDelete.contains(l.getId()))
                .collect(Collectors.toList());
        Set<Long> retainLayoutIds = retainLayout.stream().map(LayoutEntity::getId).collect(Collectors.toSet());
        if (CollectionUtils.isNotEmpty(retainLayout)) {
            NIndexPlanManager indexPlanManager = getManager(NIndexPlanManager.class, project);
            indexPlanManager.updateIndexPlan(indexPlan.getUuid(), copyForWrite -> {
                copyForWrite.getRuleBasedIndex().getLayoutBlackList().removeAll(retainLayoutIds);
                copyForWrite.getRuleBaseLayouts().addAll(retainLayout);
            });
        }
    }

    private Set<Long> checkNeedUpdateBaseIndex(String project, CreateBaseIndexRequest request, boolean isAuto) {
        String modelId = request.getModelId();
        IndexPlan indexPlan = getIndexPlan(project, modelId);
        NDataModel model = getManager(NDataModelManager.class, project).getDataModelDesc(request.getModelId());
        Set<Long> needDelete = Sets.newHashSet();
        Set<IndexEntity.Source> updateTypes = Sets.newHashSet();

        LayoutEntity baseAggLayout = indexPlan.createBaseAggIndex(model);
        overrideLayout(baseAggLayout, request.getBaseAggIndexProperty(), model);
        if (request.needHandleBaseAggIndex() && indexPlan.needUpdateBaseAggLayout(baseAggLayout, isAuto)) {
            needDelete.add(indexPlan.getBaseAggLayoutId());
            updateTypes.add(Source.BASE_AGG_INDEX);
        }

        LayoutEntity baseTablelayout = indexPlan.createBaseTableIndex(model);
        if (baseTablelayout != null) {
            overrideLayout(baseTablelayout, request.getBaseTableIndexProperty(), model);
            if (request.needHandleBaseTableIndex() && indexPlan.needUpdateBaseTableLayout(baseTablelayout, isAuto)) {
                needDelete.add(indexPlan.getBaseTableLayoutId());
                updateTypes.add(Source.BASE_TABLE_INDEX);
            }
        } else {
            if (indexPlan.containBaseTableLayout()) {
                needDelete.add(indexPlan.getBaseTableLayoutId());
            }
        }
        request.setSourceTypes(updateTypes);
        return needDelete;
    }

    private void deleteOrMarkTobeDelete(String project, String modelId, Set<Long> needDelete) {
        if (CollectionUtils.isEmpty(needDelete)) {
            return;
        }
        NDataflow df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataflow(modelId);

        val readySegs = df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        if (readySegs.isEmpty()) {
            removeIndexes(project, modelId, needDelete);
        } else {
            Set<Long> effectiveLayouts = readySegs.stream().flatMap(seg -> seg.getLayoutIds().stream())
                    .collect(Collectors.toSet());

            for (long layoutId : needDelete) {
                if (!effectiveLayouts.contains(layoutId) && !isSecondStorageLayoutReady(project, modelId, layoutId)) {
                    removeIndex(project, modelId, layoutId);
                } else {
                    addIndexToBeDeleted(project, modelId, Sets.newHashSet(layoutId));
                }
            }
        }
    }

    public boolean isSecondStorageLayoutReady(String project, String modelId, long layoutId) {
        if (!SecondStorageUtil.isModelEnable(project, modelId)) {
            return false;
        }
        if (!SegmentOnlineMode.ANY.toString().equalsIgnoreCase(NProjectManager.getInstance(getConfig())
                .getProject(project).getConfig().getKylinEngineSegmentOnlineMode())) {
            return false;
        }
        Optional<Manager<TableFlow>> tableFlowManager = SecondStorageUtil.tableFlowManager(getConfig(), project);
        if (!tableFlowManager.isPresent()) {
            return false;
        }
        Optional<TableFlow> tableFlow = tableFlowManager.get().get(modelId);
        return tableFlow.map(flow -> flow.containsLayout(layoutId)).orElse(false);
    }

    @Transaction(project = 0)
    public BuildBaseIndexResponse createBaseIndex(String project, CreateBaseIndexRequest request) {
        aclEvaluate.checkProjectOperationDesignPermission(project);
        NDataModel model = getManager(NDataModelManager.class, project).getDataModelDesc(request.getModelId());
        NIndexPlanManager indexPlanManager = getManager(NIndexPlanManager.class, project);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(request.getModelId());

        List<LayoutEntity> needCreateBaseLayouts = getNotCreateBaseLayout(model, indexPlan, request);
        indexPlanManager.updateIndexPlan(indexPlan.getUuid(),
                copyForWrite -> copyForWrite.createAndAddBaseIndex(needCreateBaseLayouts));

        BuildBaseIndexResponse response = new BuildBaseIndexResponse();
        for (LayoutEntity layoutEntity : needCreateBaseLayouts) {
            response.addLayout(layoutEntity);
        }
        modelChangeSupporters.forEach(listener -> listener.onUpdate(project, request.getModelId()));
        return response;
    }

    private List<LayoutEntity> getNotCreateBaseLayout(NDataModel model, IndexPlan indexPlan,
            CreateBaseIndexRequest request) {
        List<LayoutEntity> needCreateBaseLayouts = Lists.newArrayList();

        if (request.needHandleBaseAggIndex() && !indexPlan.containBaseAggLayout()) {
            LayoutEntity baseAggLayout = indexPlan.createBaseAggIndex(model);
            overrideLayout(baseAggLayout, request.getBaseAggIndexProperty(), model);
            needCreateBaseLayouts.add(baseAggLayout);
        }

        if (request.needHandleBaseTableIndex() && !indexPlan.containBaseTableLayout()) {
            LayoutEntity baseTableLayout = indexPlan.createBaseTableIndex(model);
            if (baseTableLayout != null) {
                overrideLayout(baseTableLayout, request.getBaseTableIndexProperty(), model);
                needCreateBaseLayouts.add(baseTableLayout);
            }
        }
        return needCreateBaseLayouts;
    }

    private void overrideLayout(LayoutEntity layout, LayoutProperty layoutProperty, NDataModel model) {
        layout.setOwner(BasicService.getUsername());
        if (layoutProperty == null) {
            return;
        }
        if (!layoutProperty.getColOrder().isEmpty()) {
            List<Integer> overrideColOrder = convertColumn(layoutProperty.getColOrder(), model);
            List<Integer> dimsIds = layout.getDimsIds();
            if (checkColsMatch(dimsIds, overrideColOrder)) {
                layout.setColOrder(ImmutableList.<Integer> builder().addAll(overrideColOrder)
                        .addAll(layout.getMeasureIds()).build());
            } else {
                throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "col order %s doesn't match current base layout %s ",
                                layoutProperty.getColOrder(), layout.getColOrder()));
            }
        }
        if (!CollectionUtils.isEmpty(layoutProperty.getShardByColumns())) {
            layout.setShardByColumns(convertColumn(layoutProperty.getShardByColumns(), model));
        }

    }

    private boolean checkColsMatch(List<Integer> colOrder, List<Integer> overrideColOrder) {
        if (colOrder.size() == overrideColOrder.size()) {
            List<Integer> colOrder1 = Lists.newArrayList(colOrder);
            List<Integer> colOrder2 = Lists.newArrayList(overrideColOrder);
            colOrder1.sort(Integer::compareTo);
            colOrder2.sort(Integer::compareTo);
            return colOrder1.equals(colOrder2);
        }
        return false;
    }

    public IndexStatResponse getStat(String project, String modelId) {
        List<IndexResponse> results = getIndexes(project, modelId, "", Lists.newArrayList(), null, false,
                Lists.newArrayList(), Lists.newArrayList());
        IndexPlan indexPlan = getIndexPlan(project, modelId);
        IndexStatResponse response = IndexStatResponse.from(results);
        if (!indexPlan.containBaseAggLayout()) {
            response.setNeedCreateBaseAggIndex(true);
        }
        if (!indexPlan.containBaseTableLayout() && indexPlan.createBaseTableIndex() != null) {
            response.setNeedCreateBaseTableIndex(true);
        }
        return response;
    }

    @Transaction(project = 0)
    private void removeSecondStorageIndex(String project, String modelId, Set<Long> layoutIds) {
        if (SecondStorageUtil.isModelEnable(project, modelId)) {
            SecondStorageUpdater updater = SpringContext.getBean(SecondStorageUpdater.class);
            updater.removeIndexByLayoutId(project, modelId, layoutIds);
        }
    }

    @Override
    public void onReloadLayouts(String project, String modelId, Set<Long> changedLayouts) {
        reloadLayouts(project, modelId, changedLayouts);
    }

    @Override
    public void onUpdateBaseIndex(Object indexUpdateHelper) {
        val baseIndexUpdater = (BaseIndexUpdateHelper) indexUpdateHelper;
        baseIndexUpdater.update(this);
    }

    @Override
    public Object getIndexUpdateHelper(NDataModel model, boolean createIfNotExist) {
        val baseIndexUpdater = new BaseIndexUpdateHelper(model, false);
        return baseIndexUpdater;
    }
}
