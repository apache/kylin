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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.LayoutPartition;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.model.PartitionStatusEnum;
import org.apache.kylin.metadata.cube.model.SegmentPartition;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import lombok.Getter;
import lombok.val;

public abstract class SparkJobMetadataMerger extends MetadataMerger {
    private static final Logger log = LoggerFactory.getLogger(SparkJobMetadataMerger.class);
    @Getter
    private final String project;

    protected SparkJobMetadataMerger(KylinConfig config, String project) {
        super(config);
        this.project = project;
    }

    public KylinConfig getProjectConfig(ResourceStore remoteStore) throws IOException {
        val globalConfig = KylinConfig.createKylinConfig(
                KylinConfig.streamToProps(remoteStore.getResource("/kylin.properties").getByteSource().openStream()));
        val projectConfig = JsonUtil
                .readValue(remoteStore.getResource("/_global/project/" + project + ".json").getByteSource().read(),
                        ProjectInstance.class)
                .getLegalOverrideKylinProps();
        return KylinConfigExt.createInstance(globalConfig, projectConfig);
    }

    @Override
    public NDataLayout[] merge(String dataflowId, Set<String> segmentIds, Set<Long> layoutIds, //
            ResourceStore remoteResourceStore, JobTypeEnum jobType, Set<Long> partitions) {
        return new NDataLayout[0];
    }

    public void recordDownJobStats(AbstractExecutable buildTask, NDataLayout[] addOrUpdateCuboids) {
        // make sure call this method in the last step, if 4th step is added, please modify the logic accordingly
        String model = buildTask.getTargetSubject();
        // get end time from current task instead of parent job，since parent job is in running state at this time
        long buildEndTime = buildTask.getEndTime();
        long duration = buildTask.getParent().getDuration();
        long byteSize = 0;
        for (NDataLayout dataCuboid : addOrUpdateCuboids) {
            byteSize += dataCuboid.getByteSize();
        }
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        long startOfDay = TimeUtil.getDayStart(buildEndTime);
        // update
        NExecutableManager executableManager = NExecutableManager.getInstance(kylinConfig, project);
        executableManager.updateJobOutput(buildTask.getParentId(), null, null, null, null, byteSize);
        JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(kylinConfig,
                buildTask.getProject());
        jobStatisticsManager.updateStatistics(startOfDay, model, duration, byteSize, 0);
    }

    protected void mergeSnapshotMeta(NDataflow dataflow, ResourceStore remoteResourceStore) {
        if (!isSnapshotManualManagementEnabled(remoteResourceStore)) {
            val remoteTblMgr = NTableMetadataManager.getInstance(remoteResourceStore.getConfig(), getProject());
            val localTblMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
            dataflow.getModel().getLookupTables().forEach(remoteTableRef -> {
                val tableName = remoteTableRef.getTableIdentity();
                val localTbDesc = localTblMgr.getTableDesc(tableName);
                val remoteTbDesc = remoteTblMgr.getTableDesc(tableName);
                if (remoteTbDesc == null) {
                    return;
                }

                val copy = localTblMgr.copyForWrite(localTbDesc);
                copy.setLastSnapshotPath(remoteTbDesc.getLastSnapshotPath());
                copy.setLastSnapshotSize(remoteTbDesc.getLastSnapshotSize());
                copy.setSnapshotLastModified(remoteTbDesc.getSnapshotLastModified());
                copy.setSnapshotTotalRows(remoteTbDesc.getSnapshotTotalRows());
                localTblMgr.updateTableDesc(copy);
            });
        }
    }

    protected void mergeTableExtMeta(NDataflow dataflow, ResourceStore remoteResourceStore) {
        val remoteTblMgr = NTableMetadataManager.getInstance(remoteResourceStore.getConfig(), getProject());
        val localTblMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        dataflow.getModel().getLookupTables().forEach(remoteTableRef -> {
            val tableName = remoteTableRef.getTableIdentity();
            val localTbDesc = localTblMgr.getTableDesc(tableName);
            val remoteTbDesc = remoteTblMgr.getTableDesc(tableName);
            if (remoteTbDesc == null) {
                return;
            }

            val remoteTblExtDesc = remoteTblMgr.getOrCreateTableExt(remoteTbDesc);
            val copyExt = localTblMgr.copyForWrite(localTblMgr.getOrCreateTableExt(localTbDesc));
            if (remoteTblExtDesc.getOriginalSize() != -1) {
                copyExt.setOriginalSize(remoteTblExtDesc.getOriginalSize());
            }
            copyExt.setTotalRows(remoteTblExtDesc.getTotalRows());
            localTblMgr.saveTableExt(copyExt);
        });
    }

    protected boolean isSnapshotManualManagementEnabled(ResourceStore configStore) {
        try {
            val projectConfig = getProjectConfig(configStore);
            if (!projectConfig.isSnapshotManualManagementEnabled()) {
                return false;
            }
        } catch (IOException e) {
            log.error("Fail to get project config.");
        }
        return true;
    }

    // Note: DO NOT copy max bucketId in segment.
    public NDataSegment upsertSegmentPartition(NDataSegment localSegment, NDataSegment newSegment, //
            Set<Long> partitionIds) {
        localSegment.getMultiPartitions().removeIf(partition -> partitionIds.contains(partition.getPartitionId()));
        List<SegmentPartition> upsertPartitions = newSegment.getMultiPartitions().stream() //
                .filter(partition -> partitionIds.contains(partition.getPartitionId())).collect(Collectors.toList());
        final long lastBuildTime = System.currentTimeMillis();
        upsertPartitions.forEach(partition -> {
            partition.setStatus(PartitionStatusEnum.READY);
            partition.setLastBuildTime(lastBuildTime);
        });
        localSegment.getMultiPartitions().addAll(upsertPartitions);
        List<SegmentPartition> partitions = localSegment.getMultiPartitions();
        localSegment.setSourceCount(partitions.stream() //
                .mapToLong(SegmentPartition::getSourceCount).sum());
        final Map<String, Long> merged = Maps.newHashMap();
        partitions.stream().map(SegmentPartition::getColumnSourceBytes) //
                .forEach(item -> //
                item.forEach((k, v) -> //
                merged.put(k, v + merged.getOrDefault(k, 0L))));
        localSegment.setColumnSourceBytes(merged);
        // KE-18417 snapshot management.
        localSegment.setLastBuildTime(newSegment.getLastBuildTime());
        localSegment.setSourceBytesSize(newSegment.getSourceBytesSize());
        localSegment.setLastBuildTime(lastBuildTime);
        return localSegment;
    }

    public NDataLayout upsertLayoutPartition(NDataLayout localLayout, NDataLayout newLayout, Set<Long> partitionIds) {
        if (localLayout == null) {
            return newLayout;
        }
        localLayout.getMultiPartition().removeIf(partition -> partitionIds.contains(partition.getPartitionId()));
        List<LayoutPartition> upsertLayouts = newLayout.getMultiPartition().stream() //
                .filter(partition -> partitionIds.contains(partition.getPartitionId())).collect(Collectors.toList());
        localLayout.getMultiPartition().addAll(upsertLayouts);
        List<LayoutPartition> partitions = localLayout.getMultiPartition();
        localLayout.setRows(partitions.stream() //
                .mapToLong(LayoutPartition::getRows).sum());
        localLayout.setSourceRows(partitions.stream() //
                .mapToLong(LayoutPartition::getSourceRows).sum());
        localLayout.setFileCount(partitions.stream() //
                .mapToLong(LayoutPartition::getFileCount).sum());
        localLayout.setByteSize(partitions.stream() //
                .mapToLong(LayoutPartition::getByteSize).sum());
        return localLayout;
    }

    public Set<Long> getAvailableLayoutIds(NDataflow dataflow, Set<Long> layoutIds) {
        val layoutInCubeIds = dataflow.getIndexPlan().getAllLayouts().stream().map(LayoutEntity::getId)
                .collect(Collectors.toList());
        return layoutIds.stream().filter(layoutInCubeIds::contains).collect(Collectors.toSet());
    }

    public void updateIndexPlan(String dfId, ResourceStore remoteStore) {
        val remoteDataflowManager = NDataflowManager.getInstance(remoteStore.getConfig(), getProject());
        IndexPlan remoteIndexPlan = remoteDataflowManager.getDataflow(dfId).getIndexPlan();
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getConfig(), getProject());
        indexPlanManager.updateIndexPlan(dfId, copyForWrite -> {
            copyForWrite.setLayoutBucketNumMapping(remoteIndexPlan.getLayoutBucketNumMapping());
        });
    }
}
