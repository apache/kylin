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

package org.apache.kylin.metadata.cube.model;

import static java.util.stream.Collectors.groupingBy;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_MERGE_CHECK_INDEX_ILLEGAL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_MERGE_CHECK_PARTITION_ILLEGAL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_MERGE_CONTAINS_GAPS;
import static org.apache.kylin.metadata.realization.RealizationStatusEnum.ONLINE;
import static org.apache.kylin.common.util.SegmentMergeStorageChecker.checkMergeSegmentThreshold;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TimeRange;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationProvider;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.util.scd2.SCD2CondChecker;
import org.apache.kylin.metadata.project.NProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import lombok.val;
import lombok.var;

/**
 * TODO
 * Since Version 4.x.x, model-dataflow relationship depends on model type,
 * refer to [NDataModel.ModelType]
 * Batch model still mapping to ONE dataflow
 * While Streaming model will be mapping to TWO dataflows
 */
public class NDataflowManager implements IRealizationProvider {
    private static final Logger logger = LoggerFactory.getLogger(NDataflowManager.class);

    public static NDataflowManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NDataflowManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static NDataflowManager newInstance(KylinConfig config, String project) {
        return new NDataflowManager(config, project);
    }

    // ============================================================================

    private KylinConfig config;
    private String project;

    private CachedCrudAssist<NDataflow> crud;

    private NDataflowManager(KylinConfig cfg, final String project) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing NDataflowManager with KylinConfig Id: {} for project {}",
                    System.identityHashCode(cfg), project);
        this.config = cfg;
        this.project = project;
        String resourceRootPath = "/" + project + NDataflow.DATAFLOW_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<NDataflow>(getStore(), resourceRootPath, NDataflow.class) {
            @Override
            protected NDataflow initEntityAfterReload(NDataflow df, String resourceName) {
                IndexPlan plan = NIndexPlanManager.getInstance(config, project).getIndexPlan(df.getUuid());
                df.initAfterReload((KylinConfigExt) plan.getConfig(), project);
                return df;
            }

            @Override
            protected NDataflow initBrokenEntity(NDataflow entity, String resourceName) {
                val dataflow = super.initBrokenEntity(entity, resourceName);
                IndexPlan plan = NIndexPlanManager.getInstance(config, project).getIndexPlan(resourceName);
                if (plan != null) {
                    dataflow.setConfig((KylinConfigExt) plan.getConfig());
                } else {
                    dataflow.setConfig((KylinConfigExt) KylinConfig.getInstanceFromEnv());
                }
                dataflow.setProject(project);
                dataflow.setDependencies(dataflow.calcDependencies());
                return dataflow;
            }
        };
        this.crud.setCheckCopyOnWrite(true);
    }

    public NDataflow removeLayouts(NDataflow df, Collection<Long> tobeRemoveCuboidLayoutIds) {
        List<NDataLayout> tobeRemoveCuboidLayout = Lists.newArrayList();
        Segments<NDataSegment> segments = df.getSegments();
        for (NDataSegment segment : segments) {
            for (Long tobeRemoveCuboidLayoutId : tobeRemoveCuboidLayoutIds) {
                NDataLayout dataCuboid = segment.getLayout(tobeRemoveCuboidLayoutId);
                if (dataCuboid == null) {
                    continue;
                }
                tobeRemoveCuboidLayout.add(dataCuboid);
            }
        }

        if (CollectionUtils.isNotEmpty(tobeRemoveCuboidLayout)) {
            NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
            update.setToRemoveLayouts(tobeRemoveCuboidLayout.toArray(new NDataLayout[0]));
            return updateDataflow(update);
        }
        return df;
    }

    @Override
    public String getRealizationType() {
        return NDataflow.REALIZATION_TYPE;
    }

    @Override
    public IRealization getRealization(String id) {
        val df = getDataflow(id);
        if (df == null || df.checkBrokenWithRelatedInfo()) {
            return null;
        }
        return df;
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    // listAllDataflows only get the healthy dataflows,
    // the broken ones need to be invisible in the auto-suggestion process,
    // anyone in dataflow, indexPlan and dataModel is broken, the dataflow is considered to be broken
    public List<NDataflow> listAllDataflows() {
        return listAllDataflows(false);
    }

    // get all dataflows include/exclude broken ones
    public List<NDataflow> listAllDataflows(boolean includeBroken) {
        return crud.listAll().stream().filter(df -> includeBroken || !df.checkBrokenWithRelatedInfo())
                .collect(Collectors.toList());
    }

    // listUnderliningDataModels only get the healthy models,
    // the broken ones need to be invisible in the auto-suggestion process,
    // anyone in dataflow, indexPlan and dataModel is broken, the model is considered to be broken
    public List<NDataModel> listUnderliningDataModels() {
        return listUnderliningDataModels(false);
    }

    public List<NDataModel> listDataModelsByStatus(RealizationStatusEnum status) {
        List<NDataflow> dataflows = listAllDataflows();
        List<NDataModel> onlineModels = Lists.newArrayList();
        for (NDataflow dataflow : dataflows) {
            if (status == dataflow.getStatus()) {
                onlineModels.add(dataflow.getModel());
            }
        }
        return onlineModels;
    }

    public NDataflow updateDataflowStatus(String uuid, RealizationStatusEnum status) {
        return updateDataflow(uuid, copyForWrite -> copyForWrite.setStatus(status));
    }

    // get all models include broken ones
    public List<NDataModel> listUnderliningDataModels(boolean includeBroken) {
        if (KylinConfig.getInstanceFromEnv().checkModelDependencyHealthy()) {
            val dataflows = listAllDataflows(includeBroken);
            return dataflows.stream().map(NDataflow::getModel).collect(Collectors.toList());
        }
        val models = NDataModelManager.getInstance(config, project).listAllModels();
        return includeBroken ? models
                : models.stream().filter(dataModel -> !dataModel.isBroken()).collect(Collectors.toList());
    }

    public List<NDataModel> listOnlineDataModels() {
        return listAllDataflows(false).stream().filter(d -> d.getStatus() == ONLINE).map(NDataflow::getModel)
                .collect(Collectors.toList());
    }

    public Map<String, List<NDataModel>> getModelsGroupbyTable() {
        return listUnderliningDataModels().stream().collect(groupingBy(NDataModel::getRootFactTableName));
    }

    // within a project, find models that use the specified table
    public List<NDataModel> getModelsUsingTable(TableDesc table) {
        List<NDataModel> models = new ArrayList<>();
        for (NDataModel modelDesc : listUnderliningDataModels()) {
            if (modelDesc.containsTable(table))
                models.add(modelDesc);
        }
        return models;
    }

    // within a project, find models that use the specified table as root table
    public List<NDataModel> getModelsUsingRootTable(TableDesc table) {
        List<NDataModel> models = new ArrayList<>();
        for (NDataModel modelDesc : listUnderliningDataModels()) {
            if (modelDesc.isRootFactTable(table)) {
                models.add(modelDesc);
            }
        }
        return models;
    }

    public List<NDataModel> getTableOrientedModelsUsingRootTable(TableDesc table) {
        List<NDataModel> models = new ArrayList<>();
        for (NDataModel modelDesc : listUnderliningDataModels()) {
            if (modelDesc.isRootFactTable(table) && modelDesc.getManagementType() == ManagementType.TABLE_ORIENTED) {
                models.add(modelDesc);
            }
        }
        return models;
    }

    public NDataflow getDataflow(String id) {
        if (StringUtils.isEmpty(id)) {
            return null;
        }
        return crud.get(id);
    }

    public NDataflow getDataflowByModelAlias(String name) {
        return listAllDataflows(true).stream().filter(dataflow -> Objects.equals(dataflow.getModelAlias(), name))
                .findFirst().orElse(null);
    }

    public void reloadAll() {
        crud.reloadAll();
    }

    public NDataflow createDataflow(IndexPlan plan, String owner) {
        return createDataflow(plan, owner, RealizationStatusEnum.OFFLINE);
    }

    public NDataflow createDataflow(IndexPlan plan, String owner, RealizationStatusEnum realizationStatusEnum) {
        NDataflow df = NDataflow.create(plan, realizationStatusEnum);
        df.initAfterReload((KylinConfigExt) plan.getConfig(), project);

        // save dataflow
        df.getSegments().validate();
        crud.save(df);

        fillDf(df);

        return df;
    }

    public void fillDf(NDataflow df) {
        // if it's table oriented, create segments at once
        if (df.getModel().getManagementType() != ManagementType.TABLE_ORIENTED) {
            return;
        }
        val dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(config, project);
        String tableName = df.getModel().getRootFactTable().getTableIdentity();
        NDataLoadingRange dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(tableName);
        val segmentRanges = dataLoadingRangeManager.getSegRangesToBuildForNewDataflow(dataLoadingRange);
        if (CollectionUtils.isNotEmpty(segmentRanges)) {
            fillDfWithNewRanges(df, segmentRanges);
        }

    }

    public void fillDfWithNewRanges(NDataflow df, List<SegmentRange> segmentRanges) {
        Segments<NDataSegment> segs = new Segments<>();

        segmentRanges.forEach(segRange -> {
            NDataSegment newSegment = newSegment(df, segRange);
            newSegment.setStatus(SegmentStatusEnum.READY);
            segs.add(newSegment);
        });
        val update = new NDataflowUpdate(df.getUuid());
        update.setToAddSegs(segs.toArray(new NDataSegment[0]));
        updateDataflow(update);
    }

    public NDataSegment appendSegment(NDataflow df, SegmentRange segRange) {
        return appendSegment(df, segRange, SegmentStatusEnum.NEW);
    }

    public NDataSegment appendSegment(NDataflow df, SegmentRange segRange, SegmentStatusEnum status) {
        return appendSegment(df, segRange, status, null);
    }

    public NDataSegment appendSegment(NDataflow df, SegmentRange segRange, SegmentStatusEnum status,
            List<String[]> multiPartitionValues) {
        NDataSegment newSegment = newSegment(df, segRange);
        newSegment.setStatus(status);
        validateNewSegments(df, newSegment);

        NDataflowUpdate upd = new NDataflowUpdate(df.getUuid());
        upd.setToAddSegs(newSegment);
        updateDataflow(upd);
        if (CollectionUtils.isNotEmpty(multiPartitionValues)) {
            newSegment = appendPartitions(df.getId(), newSegment.getId(), multiPartitionValues);
        }
        return newSegment;
    }

    public NDataSegment appendPartitions(String dfId, String segId, List<String[]> partitionValues) {
        val copy = copy(getDataflow(dfId));
        val segmentCopy = copy.getSegment(segId);
        partitionValues.forEach(partitionValue -> {
            if (copy.getSegment(segmentCopy.getId()).isPartitionOverlap(partitionValue)) {
                throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "Duplicate partition value [%s] found in segment [%s]",
                                Arrays.toString(partitionValue), segmentCopy.getId()));
            }
        });
        val modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        Set<Long> addPartitions = modelManager.addPartitionsIfAbsent(copy.getModel(), partitionValues);
        addPartitions.forEach(partition -> {
            segmentCopy.getMultiPartitions().add(new SegmentPartition(partition));
        });
        crud.save(copy);
        return segmentCopy;
    }

    public NDataSegment appendSegmentForStreaming(NDataflow df, SegmentRange segRange) {
        return appendSegmentForStreaming(df, segRange, null);
    }

    public NDataSegment appendSegmentForStreaming(NDataflow df, SegmentRange segRange, String newSegId) {
        if (!StringUtils.isEmpty(newSegId) && df.getSegment(newSegId) != null) {
            return df.getSegment(newSegId);
        }
        val removeSegs = new ArrayList<NDataSegment>();
        val segments = df.getSegments().stream().filter(item -> !item.getAdditionalInfo().containsKey("file_layer"))
                .collect(Collectors.toList());
        Collections.sort(segments);
        if (!segments.isEmpty()) {
            val lastL0Seg = segments.get(segments.size() - 1);
            val lastL0SegRange = (SegmentRange.KafkaOffsetPartitionedSegmentRange) lastL0Seg.getSegRange();
            val newSegRange = (SegmentRange.KafkaOffsetPartitionedSegmentRange) segRange;
            if (lastL0SegRange.equals(segRange)
                    || lastL0SegRange.comparePartitionOffset(lastL0SegRange.getSourcePartitionOffsetStart(),
                            newSegRange.getSourcePartitionOffsetEnd()) >= 0) {
                NDataSegment emptySeg = NDataSegment.empty();
                emptySeg.setId(StringUtils.EMPTY);
                return emptySeg;
            } else if (newSegRange.contains(lastL0SegRange) || lastL0SegRange.contains(newSegRange)) {
                removeSegs.add(lastL0Seg);
            }
        }

        NDataSegment newSegment = new NDataSegment(df, segRange, newSegId);

        //        validateNewSegments(df, newSegment);
        NDataflowUpdate upd = new NDataflowUpdate(df.getUuid());
        upd.setToAddSegs(newSegment);
        upd.setToRemoveSegsWithArray(removeSegs.toArray(new NDataSegment[0]));
        updateDataflow(upd);
        return newSegment;
    }

    public NDataSegment refreshSegment(NDataflow df, SegmentRange segRange) {

        NDataSegment newSegment = newSegment(df, segRange);

        NDataSegment toRefreshSeg = null;
        for (NDataSegment NDataSegment : df.getSegments()) {
            if (NDataSegment.getSegRange().equals(segRange)) {
                toRefreshSeg = NDataSegment;
                break;
            }
        }

        if (toRefreshSeg == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT,
                    "no ready segment with range %s exists on model %s", segRange.toString(), df.getModelAlias()));
        }

        newSegment.setSegmentRange(toRefreshSeg.getSegRange());
        newSegment.setMultiPartitions(toRefreshSeg.getMultiPartitions().stream() //
                .map(partition -> new SegmentPartition(partition.getPartitionId())) //
                .collect(Collectors.toList()));

        NDataflowUpdate upd = new NDataflowUpdate(df.getUuid());
        upd.setToAddSegs(newSegment);
        updateDataflow(upd);

        return newSegment;
    }

    public NDataSegment mergeSegments(NDataflow dataflow, SegmentRange segRange, boolean force) {
        return mergeSegments(dataflow, segRange, force, null, null);
    }

    public NDataSegment mergeSegments(NDataflow dataflow, SegmentRange segRange, boolean force, Integer fileLayer,
            String newSegId) {
        NDataflow dataflowCopy = dataflow.copy();
        if (dataflowCopy.getSegments().isEmpty())
            throw new IllegalArgumentException(dataflow + " has no segments");
        Preconditions.checkArgument(segRange != null);

        checkCubeIsPartitioned(dataflowCopy);

        NDataSegment newSegment = newSegment(dataflowCopy, segRange);
        NDataflowUpdate update = new NDataflowUpdate(dataflowCopy.getUuid());

        //  for streaming merge
        if (fileLayer != null) {
            if (!StringUtils.isEmpty(newSegId)) {
                newSegment.setId(newSegId);
                if (dataflowCopy.getSegment(newSegId) != null) {
                    return dataflowCopy.getSegment(newSegment.getId());
                }
            }
            val segments = dataflow.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING).stream()
                    .filter(item -> item.getAdditionalInfo().containsKey("file_layer")).collect(Collectors.toList());
            for (int i = 0; i < segments.size(); i++) {
                val seg = segments.get(i);
                if (seg.getSegRange().equals(segRange)) {
                    update.setToRemoveSegs(seg);
                    break;
                }
            }
        }

        Segments<NDataSegment> mergingSegments = dataflowCopy.getMergingSegments(newSegment);
        if (mergingSegments.size() <= 1)
            throw new IllegalArgumentException("Range " + newSegment.getSegRange()
                    + " must contain at least 2 segments, but there is " + mergingSegments.size());

        NDataSegment first = mergingSegments.get(0);
        NDataSegDetails firstSegDetails = first.getSegDetails();
        for (int i = 1; i < mergingSegments.size(); i++) {
            NDataSegment dataSegment = mergingSegments.get(i);
            NDataSegDetails details = dataSegment.getSegDetails();
            if (!firstSegDetails.checkLayoutsBeforeMerge(details))
                throw new KylinException(SEGMENT_MERGE_CHECK_INDEX_ILLEGAL);
        }

        if (!force) {
            for (int i = 0; i < mergingSegments.size() - 1; i++) {
                if (!mergingSegments.get(i).getSegRange().connects(mergingSegments.get(i + 1).getSegRange()))
                    throw new KylinException(SEGMENT_MERGE_CONTAINS_GAPS);
            }

            List<String> emptySegment = Lists.newArrayList();
            for (NDataSegment seg : mergingSegments) {
                if (seg.getSegDetails().getTotalRowCount() == 0) {
                    emptySegment.add(seg.getName());
                }
            }
            if (emptySegment.size() > 0) {
                throw new IllegalArgumentException(
                        "Empty cube segment found, couldn't merge unless 'forceMergeEmptySegment' set to true: "
                                + emptySegment);
            }
        }

        NDataSegment last = mergingSegments.get(mergingSegments.size() - 1);
        newSegment.setSegmentRange(first.getSegRange().coverWith(last.getSegRange()));

        if (first.isOffsetCube()) {
            newSegment.setSegmentRange(segRange);
        } else {
            newSegment.setTimeRange(new TimeRange(first.getTSRange().getStart(), last.getTSRange().getEnd()));
        }
        // for streaming merge
        if (fileLayer != null) {
            newSegment.getAdditionalInfo().put("file_layer", String.valueOf(fileLayer));
        } else {
            validateNewSegments(dataflowCopy, newSegment);
        }
        checkMergeSegmentThreshold(config, config.getHdfsWorkingDirectory(),
                mergingSegments.stream().mapToLong(NDataSegment::getStorageBytesSize).sum());

        checkAndMergeMultiPartitions(dataflow, newSegment, mergingSegments);

        update.setToAddSegs(newSegment);
        updateDataflow(update);
        return newSegment;
    }

    private void checkAndMergeMultiPartitions(NDataflow dataflow, NDataSegment newSegment,
            Segments<NDataSegment> mergingSegments) {
        if (!dataflow.getModel().isMultiPartitionModel()) {
            return;
        }
        Set<Long> partitions = mergingSegments.get(0).getMultiPartitions().stream()
                .map(SegmentPartition::getPartitionId).collect(Collectors.toSet());
        mergingSegments.forEach(segment -> {
            if (MapUtils.isEmpty(segment.getLayoutsMap())) {
                throw new KylinException(SEGMENT_MERGE_CHECK_INDEX_ILLEGAL);
            }
            segment.getLayoutsMap().values().forEach(layout -> {
                Set<Long> partitionsInLayout = layout.getMultiPartition().stream().map(LayoutPartition::getPartitionId)
                        .collect(Collectors.toSet());
                if (!partitionsInLayout.equals(partitions)) {
                    throw new KylinException(SEGMENT_MERGE_CHECK_PARTITION_ILLEGAL);
                }
            });
        });
        partitions.forEach(partition -> newSegment.getMultiPartitions().add(new SegmentPartition(partition)));
    }

    private void checkCubeIsPartitioned(NDataflow dataflow) {
        if (!dataflow.getModel().getPartitionDesc().isPartitioned()) {
            throw new IllegalStateException(
                    "there is no partition date column specified, only full build is supported");
        }
    }

    @VisibleForTesting
    NDataSegment newSegment(NDataflow df, SegmentRange segRange) {
        // BREAKING CHANGE: remove legacy caring as in org.apache.kylin.cube.CubeManager.SegmentAssist.newSegment()
        Preconditions.checkNotNull(segRange);
        return new NDataSegment(df, segRange);
    }

    private void validateNewSegments(NDataflow df, NDataSegment newSegments) {
        List<NDataSegment> tobe = df.calculateToBeSegments(newSegments);
        List<NDataSegment> newList = Arrays.asList(newSegments);
        if (!tobe.containsAll(newList)) {
            throw new IllegalStateException("For NDataflow " + df + ", the new segments " + newList
                    + " do not fit in its current " + df.getSegments() + "; the resulted tobe is " + tobe);
        }
    }

    public List<NDataSegment> getToRemoveSegs(NDataflow dataflow, NDataSegment segment) {
        Segments tobe = dataflow.calculateToBeSegments(segment);

        if (!tobe.contains(segment))
            throw new IllegalStateException(
                    "For NDataflow " + dataflow + ", segment " + segment + " is expected but not in the tobe " + tobe);

        if (segment.getStatus() == SegmentStatusEnum.NEW)
            segment.setStatus(SegmentStatusEnum.READY);

        List<NDataSegment> toRemoveSegs = Lists.newArrayList();
        for (NDataSegment s : dataflow.getSegments()) {
            if (!tobe.contains(s))
                toRemoveSegs.add(s);
        }

        logger.info("promoting new ready segment {} in dataflow {}, segments to removed: {}", segment, dataflow,
                toRemoveSegs);

        return toRemoveSegs;
    }

    public NDataflow copy(NDataflow df) {
        return crud.copyBySerialization(df);
    }

    public List<NDataflow> getDataflowsByTableAndStatus(String tableName, RealizationStatusEnum status) {
        val tableManager = NTableMetadataManager.getInstance(config, project);
        val table = tableManager.getTableDesc(tableName);
        val models = getTableOrientedModelsUsingRootTable(table);
        List<NDataflow> dataflows = Lists.newArrayList();
        for (val model : models) {
            dataflows.add(getDataflow(model.getUuid()));
        }
        return dataflows.stream().filter(dataflow -> dataflow.getStatus() == status).collect(Collectors.toList());

    }

    public void fillDfManually(NDataflow df, List<SegmentRange> ranges) {
        Preconditions.checkState(df.getModel().getManagementType() == ManagementType.MODEL_BASED);
        if (CollectionUtils.isEmpty(ranges)) {
            return;
        }
        fillDfWithNewRanges(df, ranges);
    }

    public NDataflow handleRetention(NDataflow df) {
        Segments<NDataSegment> segsToRemove = df.getSegmentsToRemoveByRetention();
        if (CollectionUtils.isEmpty(segsToRemove)) {
            return df;
        }
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(segsToRemove.toArray(new NDataSegment[segsToRemove.size()]));
        val loadingRangeManager = NDataLoadingRangeManager.getInstance(config, project);
        val model = df.getModel();
        loadingRangeManager.updateCoveredRangeAfterRetention(model, segsToRemove.getLastSegment());
        return updateDataflow(update);
    }

    public interface NDataflowUpdater {
        void modify(NDataflow copyForWrite);
    }

    /**
     * update the dataflow from the restore by lambda function updater.
     * sometimes, dataflow's segments is removed, but do not from the restore, need to remove again.
     *
     * @param dfId
     * @param updater
     * @return
     */
    public NDataflow updateDataflow(String dfId, NDataflowUpdater updater) {
        NDataflow cached = getDataflow(dfId);
        NDataflow copy = copy(cached);
        updater.modify(copy);

        Set<String> copySegIdSet = copy.getSegments().stream().map(NDataSegment::getId).collect(Collectors.toSet());
        val nDataSegDetailsManager = NDataSegDetailsManager.getInstance(cached.getConfig(), project);
        for (NDataSegment segment : cached.getSegments()) {
            if (!copySegIdSet.contains(segment.getId())) {
                nDataSegDetailsManager.removeForSegment(copy, segment.getId());
            }
        }
        return crud.save(copy);
    }

    public long getDataflowUsage(String dataflowId) {
        return getDataflow(dataflowId).getQueryHitCount();
    }

    public long getDataflowStorageSize(String dataflowId) {
        return getDataflow(dataflowId).getStorageBytesSize();
    }

    public long getDataflowSourceSize(String modelId) {
        return getDataflow(modelId).getSourceBytesSize();
    }

    public long getDataflowLastBuildTime(String modelId) {
        return getDataflow(modelId).getLastBuildTime();
    }

    public void updateDataflowDetailsLayouts(final NDataSegment seg, final List<NDataLayout> layouts) {
        NDataSegDetailsManager segDetailsManager = NDataSegDetailsManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);
        NDataSegDetails details = segDetailsManager.getForSegment(seg);
        details.setLayouts(layouts);
        NDataSegDetailsManager.getInstance(KylinConfig.getInstanceFromEnv(), project).upsertForSegment(details);
        updateDataflow(seg.getDataflow().getId(),
                copyForWrite -> updateSegmentStatus(copyForWrite.getSegment(seg.getId())));

    }

    public NDataflow updateDataflow(final NDataflowUpdate update) {
        updateDataflow(update.getDataflowId(), df -> {
            Segments<NDataSegment> newSegs = (Segments<NDataSegment>) df.getSegments().clone();

            Arrays.stream(Optional.ofNullable(update.getToAddSegs()).orElse(new NDataSegment[0])).forEach(seg -> {
                seg.setDataflow(df);
                newSegs.add(seg);
            });

            Arrays.stream(Optional.ofNullable(update.getToUpdateSegs()).orElse(new NDataSegment[0])).forEach(seg -> {
                seg.setDataflow(df);
                newSegs.replace(Comparator.comparing(NDataSegment::getId), seg);
            });

            if (update.getToRemoveSegs() != null) {
                Iterator<NDataSegment> iterator = newSegs.iterator();
                val toRemoveIds = Arrays.stream(update.getToRemoveSegs()).map(NDataSegment::getId)
                        .collect(Collectors.toSet());
                while (iterator.hasNext()) {
                    NDataSegment currentSeg = iterator.next();
                    if (toRemoveIds.contains(currentSeg.getId())) {
                        logger.info("Remove segment {}", currentSeg);
                        iterator.remove();
                    }
                }
            }

            Arrays.stream(Optional.ofNullable(update.getToRemoveLayouts()).orElse(new NDataLayout[0]))
                    .forEach(removeLayout -> df.getLayoutHitCount().remove(removeLayout.getLayoutId()));

            df.setSegments(newSegs);

            val newStatus = Optional.ofNullable(update.getStatus()).orElse(df.getStatus());
            df.setStatus(newStatus);

            df.setCost(update.getCost() > 0 ? update.getCost() : df.getCost());

            NDataSegDetailsManager.getInstance(df.getConfig(), project).updateDataflow(df, update);
            newSegs.forEach(this::updateSegmentStatus);
        });
        if (ArrayUtils.isNotEmpty(update.getToRemoveSegs())) {
            NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(),
                    project);
            IndexPlan indexPlan = indexPlanManager.getIndexPlan(update.getDataflowId());
            if (!indexPlan.isBroken() && !indexPlan.getAllToBeDeleteLayoutId().isEmpty()) {
                indexPlanManager.updateIndexPlan(update.getDataflowId(), IndexPlan::removeTobeDeleteIndexIfNecessary);
            }
        }
        return getDataflow(update.getDataflowId());
    }

    private void updateSegmentStatus(NDataSegment seg) {
        NDataSegDetails segDetails = NDataSegDetailsManager.getInstance(seg.getConfig(), project).getForSegment(seg);
        if (seg.getStatus() == SegmentStatusEnum.WARNING && segDetails != null && segDetails.getLayouts().isEmpty()) {
            seg.setStatus(SegmentStatusEnum.READY);
        }
    }

    private boolean needUpdateSourceUsage(final NDataflowUpdate update) {
        return ArrayUtils.isNotEmpty(update.getToRemoveSegs()) || ArrayUtils.isNotEmpty(update.getToRemoveLayouts());
    }

    public NDataflow dropDataflow(String dfId) {
        NDataflow df = getDataflow(dfId);
        var dfInfo = dfId;
        if (df != null) {
            dfInfo = df.toString();
        } else {
            logger.warn("Dropping NDataflow '{}' does not exist", dfInfo);
            return null;
        }
        logger.info("Dropping NDataflow '{}'", dfInfo);

        // delete NDataSegDetails first
        NDataSegDetailsManager segDetailsManager = NDataSegDetailsManager.getInstance(config, project);
        segDetailsManager.removeDetails(df);

        // remove NDataflow and update cache
        crud.delete(df);

        return df;
    }

    List<NDataSegment> calculateHoles(String dfId) {
        final NDataflow df = getDataflow(dfId);
        Preconditions.checkNotNull(df);
        return calculateHoles(dfId, df.getSegments());
    }

    public List<NDataSegment> calculateHoles(String dfId, List<NDataSegment> segments) {
        List<NDataSegment> holes = Lists.newArrayList();
        final NDataflow df = getDataflow(dfId);
        Preconditions.checkNotNull(df);

        Collections.sort(segments);
        for (int i = 0; i < segments.size() - 1; ++i) {
            NDataSegment first = segments.get(i);
            NDataSegment second = segments.get(i + 1);
            if (first.getSegRange().connects(second.getSegRange()))
                continue;

            if (first.getSegRange().apartBefore(second.getSegRange())) {
                NDataSegment hole = new NDataSegment(df, first.getSegRange().gapTill(second.getSegRange()));
                hole.setTimeRange(new TimeRange(first.getTSRange().getEnd(), second.getTSRange().getStart()));

                // TODO: fix segment
                holes.add(hole);
            }
        }
        return holes;
    }

    public List<SegmentRange> calculateSegHoles(String dfId) {
        return calculateHoles(dfId).stream().map(NDataSegment::getSegRange).collect(Collectors.toList());
    }

    public List<NDataSegment> checkHoleIfNewSegBuild(String dfId, SegmentRange toBuildSegment) {
        final NDataflow df = getDataflow(dfId);
        List<NDataSegment> segments = Lists.newArrayList(df.getSegments());
        if (toBuildSegment != null) {
            segments.add(new NDataSegment(df, toBuildSegment));
        }

        return calculateHoles(dfId, segments);
    }

    public void removeSegmentPartition(String dfId, Set<Long> toBeDeletedPartIds, Set<String> segments) {
        val dfCopy = copy(getDataflow(dfId));
        val updateSegments = new Segments<NDataSegment>();
        if (CollectionUtils.isEmpty(segments)) {
            updateSegments.addAll(dfCopy.getSegments());
        } else {
            dfCopy.getSegments().forEach(segment -> {
                if (segments.contains(segment.getId())) {
                    updateSegments.add(segment);
                }
            });
        }
        updateSegments.forEach(segment -> segment.getMultiPartitions()
                .removeIf(partition -> toBeDeletedPartIds.contains(partition.getPartitionId())));
        crud.save(dfCopy);
    }

    public void removeLayoutPartition(String dfId, Set<Long> toBeDeletedPartIds, Set<String> segments) {
        val dataflow = copy(getDataflow(dfId));
        List<NDataSegment> updateSegments = Lists.newArrayList();
        if (segments == null) {
            updateSegments.addAll(dataflow.getSegments());
        } else {
            updateSegments.addAll(dataflow.getSegments(segments));
        }
        val affectedLayouts = Lists.newArrayList();
        for (NDataSegment segment : updateSegments) {
            val layouts = segment.getSegDetails().getLayouts();
            layouts.forEach(dataLayout -> {
                if (dataLayout.removeMultiPartition(toBeDeletedPartIds)) {
                    affectedLayouts.add(dataLayout);
                }
            });
        }
        val dfUpdate = new NDataflowUpdate(dfId);
        dfUpdate.setToAddOrUpdateLayouts(affectedLayouts.toArray(new NDataLayout[0]));
        val detailsManager = NDataSegDetailsManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        detailsManager.updateDataflow(dataflow, dfUpdate);
    }

    public void appendLayoutPartitions(NDataSegment segment, long layoutId, List<LayoutPartition> addPartitions) {
        NDataSegDetailsManager detailsManager = NDataSegDetailsManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);

        NDataSegDetails segmentDetailCopy = detailsManager.getForSegment(segment);
        NDataflow df = segmentDetailCopy.getDataflow();
        NDataLayout layout = segmentDetailCopy.getLayoutById(layoutId);
        List<Long> partitionIds = addPartitions.stream().map(LayoutPartition::getPartitionId)
                .collect(Collectors.toList());
        Preconditions.checkState(layout.getPartitionsByIds(partitionIds).size() == 0);
        layout.getMultiPartition().addAll(addPartitions);

        NDataflowUpdate update = new NDataflowUpdate(df.getId());
        update.setToAddOrUpdateLayouts(layout);
        detailsManager.updateDataflow(df, update);
    }

    public boolean isOfflineModel(NDataflow df) {
        val prjManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(df.getProject());
        KylinConfigExt config = prjManager.getConfig();
        boolean offlineManually = df.getIndexPlan().isOfflineManually();
        boolean isOfflineMultiPartitionModel = df.getModel().isMultiPartitionModel()
                && !config.isMultiPartitionEnabled();
        boolean isOfflineScdModel = SCD2CondChecker.INSTANCE.isScd2Model(df.getModel())
                && !config.isQueryNonEquiJoinModelEnabled();
        return offlineManually || isOfflineMultiPartitionModel || isOfflineScdModel;
    }

}
