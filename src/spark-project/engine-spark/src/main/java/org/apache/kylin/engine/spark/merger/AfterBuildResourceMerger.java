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

package org.apache.kylin.engine.spark.merger;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.engine.spark.ExecutableUtils;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.MergerInfo;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.PartitionStatusEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AfterBuildResourceMerger extends MetadataMerger {

    public AfterBuildResourceMerger(KylinConfig config, String project) {
        super(config, project);
    }

    public NDataLayout[] merge(String dataflowId, Set<String> segmentId, Set<Long> layoutIds,
            ResourceStore remoteResourceStore, JobTypeEnum jobType, Set<Long> partitions) {
        switch (jobType) {
        case INDEX_BUILD:
        case SUB_PARTITION_BUILD:
            return mergeAfterCatchup(dataflowId, segmentId, layoutIds, remoteResourceStore, partitions);
        case INC_BUILD:
            Preconditions.checkArgument(segmentId.size() == 1);
            return mergeAfterIncrement(dataflowId, segmentId.iterator().next(), layoutIds, remoteResourceStore);
        default:
            throw new UnsupportedOperationException("Error job type: " + jobType);
        }
    }

    @Override
    public NDataLayout[] merge(MergerInfo.TaskMergeInfo info) {
        try (val buildResourceStore = ExecutableUtils.getRemoteStore(this.getConfig(), info.getOutputMetaUrl())) {
            val dataFlowId = info.getDataFlowId();
            val segmentIds = info.getSegmentIds();
            val layoutIds = info.getLayoutIds();
            val partitionIds = info.getPartitionIds();
            val jobType = info.getJobType();
            val needBuildSnapshots = info.isNeedBuildSnapshots();
            NDataLayout[] nDataLayouts = merge(dataFlowId, segmentIds, layoutIds, buildResourceStore, jobType,
                    partitionIds);
            NDataflow dataflow = NDataflowManager.getInstance(getConfig(), getProject()).getDataflow(dataFlowId);
            if (needBuildSnapshots) {
                mergeSnapshotMeta(dataflow, buildResourceStore);
            }
            mergeTableExtMeta(dataflow, buildResourceStore);
            return nDataLayouts;
        }
    }

    public NDataLayout[] mergeAfterIncrement(String dataflowId, String segmentId, Set<Long> layoutIds,
            ResourceStore remoteStore) {
        val localDataflowManager = NDataflowManager.getInstance(getConfig(), getProject());
        val remoteDataflowManager = NDataflowManager.getInstance(remoteStore.getConfig(), getProject());
        val remoteDataflow = remoteDataflowManager.getDataflow(dataflowId);

        val dfUpdate = new NDataflowUpdate(dataflowId);
        val theSeg = remoteDataflow.getSegment(segmentId).copy();
        val toRemoveSegments = remoteDataflowManager.getToRemoveSegs(remoteDataflow, theSeg);

        if (theSeg.getModel().isMultiPartitionModel()) {
            final long lastBuildTime = System.currentTimeMillis();
            theSeg.getMultiPartitions().forEach(partition -> {
                partition.setStatus(PartitionStatusEnum.READY);
                partition.setLastBuildTime(lastBuildTime);
            });
            theSeg.setLastBuildTime(lastBuildTime);
        } else {
            theSeg.setLastBuildTime(theSeg.getSegDetails().getLastModified());
        }
        theSeg.setMvcc(localDataflowManager.getDataflow(dataflowId).getSegment(segmentId).getMvcc());

        resetBreakpoints(theSeg);

        theSeg.setStatus(SegmentStatusEnum.READY);
        dfUpdate.setToUpdateSegs(theSeg);
        dfUpdate.setToRemoveSegs(toRemoveSegments.toArray(new NDataSegment[toRemoveSegments.size()]));
        dfUpdate.setToAddOrUpdateLayouts(theSeg.getSegDetails().getEffectiveLayouts().toArray(new NDataLayout[0]));
        if (remoteDataflow.getModel().getStorageType().isV3Storage()) {
            mergeLayoutDetails(getProject(), remoteDataflow.getModel().getId(), layoutIds, theSeg, toRemoveSegments,
                    remoteStore.getConfig());
        }

        localDataflowManager.updateDataflow(dfUpdate);
        updateIndexPlan(dataflowId, remoteStore);
        return dfUpdate.getToAddOrUpdateLayouts();
    }

    public NDataLayout[] mergeAfterCatchup(String flowName, Set<String> segmentIds, Set<Long> layoutIds,
            ResourceStore remoteStore, Set<Long> partitionIds) {
        if (CollectionUtils.isNotEmpty(partitionIds)) {
            return mergeMultiPartitionModelAfterCatchUp(flowName, segmentIds, layoutIds, remoteStore, partitionIds);
        } else {
            return mergeNormalModelAfterCatchUp(flowName, segmentIds, layoutIds, remoteStore);
        }
    }

    public NDataLayout[] mergeNormalModelAfterCatchUp(String flowName, Set<String> segmentIds, Set<Long> layoutIds,
            ResourceStore remoteStore) {
        val localDataflowManager = NDataflowManager.getInstance(getConfig(), getProject());
        val dataflow = localDataflowManager.getDataflow(flowName);
        val remoteDataflowManager = NDataflowManager.getInstance(remoteStore.getConfig(), getProject());
        val remoteDataflow = remoteDataflowManager.getDataflow(flowName);

        val dfUpdate = new NDataflowUpdate(flowName);
        val addCuboids = Lists.<NDataLayout> newArrayList();
        val availableLayoutIds = getAvailableLayoutIds(dataflow, layoutIds);

        List<NDataSegment> segsToUpdate = Lists.newArrayList();
        for (String segId : segmentIds) {
            val localSeg = dataflow.getSegment(segId);
            val remoteSeg = remoteDataflow.getSegment(segId).copy();
            // ignore if local segment is not ready
            if (isUnavailableSegment(localSeg)) {
                continue;
            }
            remoteSeg.setLastBuildTime(remoteSeg.getSegDetails().getLastModified());
            for (long layoutId : availableLayoutIds) {
                NDataLayout dataCuboid = remoteSeg.getLayout(layoutId, true);
                Preconditions.checkNotNull(dataCuboid);
                addCuboids.add(dataCuboid);
            }
            remoteSeg.setMvcc(localSeg.getMvcc());

            if (remoteDataflow.getModel().getStorageType().isV3Storage()) {
                mergeLayoutDetails(getProject(), remoteDataflow.getModel().getId(), layoutIds, remoteSeg, segsToUpdate,
                        remoteStore.getConfig());
            }

            resetBreakpoints(remoteSeg);

            segsToUpdate.add(remoteSeg);
        }

        dfUpdate.setToUpdateSegs(segsToUpdate.toArray(new NDataSegment[0]));
        dfUpdate.setToAddOrUpdateLayouts(addCuboids.toArray(new NDataLayout[0]));

        localDataflowManager.updateDataflow(dfUpdate);
        updateIndexPlan(flowName, remoteStore);
        return dfUpdate.getToAddOrUpdateLayouts();
    }

    private boolean isUnavailableSegment(NDataSegment localSeg) {
        if (localSeg == null) {
            return true;
        }
        return localSeg.getStatus() != SegmentStatusEnum.READY && localSeg.getStatus() != SegmentStatusEnum.WARNING;
    }

    /**
     * MultiPartition model job:
     * New layoutIds mean index build job which should add new layouts.
     * Old layoutIds mean partition build job which should update partitions in layouts.
     */
    public NDataLayout[] mergeMultiPartitionModelAfterCatchUp(String flowName, Set<String> segmentIds,
            Set<Long> layoutIds, ResourceStore remoteStore, Set<Long> partitionIds) {

        val localDataflowManager = NDataflowManager.getInstance(getConfig(), getProject());
        val localDataflow = localDataflowManager.getDataflow(flowName);
        val remoteDataflowManager = NDataflowManager.getInstance(remoteStore.getConfig(), getProject());
        val remoteDataflow = remoteDataflowManager.getDataflow(flowName);
        val dataflow = localDataflowManager.getDataflow(flowName);
        val dfUpdate = new NDataflowUpdate(flowName);
        val upsertCuboids = Lists.<NDataLayout> newArrayList();
        val availableLayoutIds = getAvailableLayoutIds(dataflow, layoutIds);
        List<NDataSegment> segsToUpdate = Lists.newArrayList();

        for (String segId : segmentIds) {
            val localSeg = localDataflow.getSegment(segId).copy();
            val remoteSeg = remoteDataflow.getSegment(segId).copy();
            // ignore if local segment is not ready
            if (isUnavailableSegment(localSeg)) {
                continue;
            }
            NDataSegment updateSegment = upsertSegmentPartition(localSeg, remoteSeg, partitionIds);

            for (long layoutId : availableLayoutIds) {
                NDataLayout remoteLayout = remoteSeg.getLayout(layoutId);
                NDataLayout localLayout = localSeg.getLayout(layoutId);
                NDataLayout upsertLayout = upsertLayoutPartition(localLayout, remoteLayout, partitionIds);
                if (upsertLayout == null) {
                    log.warn("Layout {} is null in segment {}. Segment have layouts {} ", layoutId, segId,
                            remoteSeg.getLayoutIds());
                }
                upsertCuboids.add(upsertLayout);
            }
            segsToUpdate.add(updateSegment);
        }
        dfUpdate.setToUpdateSegs(segsToUpdate.toArray(new NDataSegment[0]));
        dfUpdate.setToAddOrUpdateLayouts(upsertCuboids.toArray(new NDataLayout[0]));

        localDataflowManager.updateDataflow(dfUpdate);
        updateIndexPlan(flowName, remoteStore);
        return dfUpdate.getToAddOrUpdateLayouts();
    }

    private void resetBreakpoints(NDataSegment dataSegment) {
        // Reset breakpoints.
        dataSegment.setFactViewReady(false);
        dataSegment.setDictReady(false);
        if (!getConfig().isPersistFlatTableEnabled()) {
            dataSegment.setFlatTableReady(false);
        }

        // Multi level partition FLAT-TABLE is not reusable.
        if (Objects.nonNull(dataSegment.getModel()) //
                && Objects.nonNull(dataSegment.getModel().getMultiPartitionDesc())) {
            dataSegment.setFlatTableReady(false);
            // By design, multi level partition shouldn't refresh snapshots frequently.
        } else {
            dataSegment.setSnapshotReady(false);
        }
    }
}
