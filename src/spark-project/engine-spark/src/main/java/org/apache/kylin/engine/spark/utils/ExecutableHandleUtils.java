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

package org.apache.kylin.engine.spark.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.engine.spark.job.NSparkCubingJob;
import org.apache.kylin.engine.spark.job.NSparkExecutable;
import org.apache.kylin.engine.spark.merger.MetadataMerger;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.common.IndexBuildJobUtil;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultExecutableOnModel;
import org.apache.kylin.job.execution.ExecutableHandler;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.MergerInfo;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExecutableHandleUtils {

    public static void recordDownJobStats(AbstractExecutable buildTask, NDataLayout[] addOrUpdateCuboids,
            String project) {
        // make sure call this method in the last step, if 4th step is added, please modify the logic accordingly
        String model = buildTask.getTargetSubject();
        // get end time from current task instead of parent job，since parent job is in running state at this time
        long buildEndTime = buildTask.getEndTime();
        long duration = buildTask.getParent().getDuration();
        long byteSize = Arrays.stream(addOrUpdateCuboids).mapToLong(NDataLayout::getByteSize).sum();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        long startOfDay = TimeUtil.getDayStart(buildEndTime);
        JobContextUtil.withTxAndRetry(() -> {
            // update
            ExecutableManager executableManager = ExecutableManager.getInstance(kylinConfig, project);
            executableManager.updateJobOutput(buildTask.getParentId(), null, null, null, null, byteSize);

            return true;
        });
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            JobStatisticsManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateStatistics(startOfDay,
                    model, duration, byteSize, 0);
            return true;
        }, project);
    }

    public static List<AbstractExecutable> getNeedMergeTasks(DefaultExecutableOnModel parent) {
        return parent.getTasks().stream().filter(task -> task instanceof NSparkExecutable)
                .filter(task -> ((NSparkExecutable) task).needMergeMetadata()).collect(Collectors.toList());
    }


    // merge metadata for table based jobs, include Sampling，Snapshot and InternalTable loading.
    public static void mergeMetadataForTable(String project, MergerInfo mergerInfo) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            MetadataMerger merger = MetadataMerger.createMetadataMerger(project, mergerInfo.getHandlerType());

            List<MergerInfo.TaskMergeInfo> infoList = mergerInfo.getTaskMergeInfoList();
            Preconditions.checkArgument(infoList.size() == 1);

            return merger.merge(infoList.get(0));
        }, project);
    }

    public static List<NDataLayout[]> mergeMetadata(String project, MergerInfo mergerInfo) {
        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            MetadataMerger merger = MetadataMerger.createMetadataMerger(project, mergerInfo.getHandlerType());

            List<NDataLayout[]> mergedLayouts = new ArrayList<>();
            mergerInfo.getTaskMergeInfoList().forEach(info -> mergedLayouts.add(merger.merge(info)));

            if (mergerInfo.getHandlerType() == ExecutableHandler.HandlerType.ADD_CUBOID) {
                tryRemoveToBeDeletedLayouts(project, mergerInfo);
            }
            markDFStatus(project, mergerInfo.getModelId());
            return mergedLayouts;
        }, project);
    }

    private static void tryRemoveToBeDeletedLayouts(String project, MergerInfo mergerInfo) {
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        AbstractExecutable executable = ExecutableManager.getInstance(conf, project)
                .getJob(mergerInfo.getJobId());
        if (!(executable instanceof NSparkCubingJob)) {
            return;
        }
        NSparkCubingJob job = (NSparkCubingJob) executable;
        if (job.getSparkCubingStep().getStatus() != ExecutableState.SUCCEED) {
            return;
        }
        boolean layoutsDeletableAfterBuild = Boolean
                .parseBoolean(job.getParam(NBatchConstants.P_LAYOUTS_DELETABLE_AFTER_BUILD));
        if (!layoutsDeletableAfterBuild) {
            return;
        }

        // Notice: The following df & indexPlan have been updated in transaction
        NDataflow df = NDataflowManager.getInstance(conf, project).getDataflow(job.getTargetModelId());
        IndexPlan indexPlan = NIndexPlanManager.getInstance(conf, project).getIndexPlan(job.getTargetModelId());
        Set<Long> toBeDeletedLayoutIds = indexPlan.getAllToBeDeleteLayoutId();

        if (!toBeDeletedLayoutIds.isEmpty()) {
            Set<Long> processedLayouts = mergerInfo.getTaskMergeInfoList().stream()
                    .flatMap(taskMergeInfo -> taskMergeInfo.getLayoutIds().stream())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            List<NDataSegment> targetSegments = df.getSegments(Sets.newHashSet(job.getTargetSegments()));

            // Almost the final layouts which will be deleted for sure
            Set<Long> prunedToBeDeletedLayoutIds = IndexBuildJobUtil
                    .pruneTBDelLayouts(
                            toBeDeletedLayoutIds.stream().map(indexPlan::getLayoutEntity)
                                    .collect(Collectors.toCollection(LinkedHashSet::new)),
                            processedLayouts.stream().map(indexPlan::getLayoutEntity).collect(
                                    Collectors.toCollection(LinkedHashSet::new)),
                            df, indexPlan, targetSegments)
                    .stream().map(LayoutEntity::getId).collect(Collectors.toSet());

            log.info("The final toBeDeletedLayouts: {}", prunedToBeDeletedLayoutIds);
            if (!prunedToBeDeletedLayoutIds.isEmpty()) {
                NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateIndexPlan(
                        mergerInfo.getModelId(),
                        copyForWrite -> copyForWrite.removeLayouts(prunedToBeDeletedLayoutIds, true, true));
            }
        }
    }

    static void markDFStatus(String project, String modelId) {
        NDataflowManager dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataflow df = dfManager.getDataflow(modelId);
        boolean isOffline = dfManager.isOfflineModel(df);
        RealizationStatusEnum status = df.getStatus();
        if (RealizationStatusEnum.ONLINE == status && isOffline) {
            dfManager.updateDataflowStatus(df.getId(), RealizationStatusEnum.OFFLINE);
        } else if (RealizationStatusEnum.OFFLINE == status && !isOffline) {
            NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateDataflowStatus(df.getId(),
                    RealizationStatusEnum.ONLINE);
        }
    }

    public static void makeSegmentReady(String project, String modelId, String segmentId) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();

        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);
        NDataflow df = dfMgr.getDataflow(modelId);

        //update target seg's status
        val dfUpdate = new NDataflowUpdate(modelId);
        val seg = df.getSegment(segmentId).copy();
        seg.setStatus(SegmentStatusEnum.READY);
        dfUpdate.setToUpdateSegs(seg);
        dfMgr.updateDataflow(dfUpdate);
        markDFStatus(project, modelId);
    }
}
