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

package org.apache.kylin.engine.spark.job;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.common.SegmentUtil;
import org.apache.kylin.job.execution.DefaultChainedExecutableOnModel;
import org.apache.kylin.job.execution.ExecutableHandler;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.engine.spark.merger.AfterBuildResourceMerger;
import org.apache.kylin.metadata.cube.model.NDataLoadingRangeManager;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.ManagementType;
import org.apache.kylin.metadata.model.NDataModel;

import com.google.common.base.Preconditions;

import lombok.val;

public class ExecutableAddSegmentHandler extends ExecutableHandler {

    public ExecutableAddSegmentHandler(String project, String modelId, String owner, String segmentId, String jobId) {
        super(project, modelId, owner, segmentId, jobId);
    }

    @Override
    public void handleFinished() {
        String project = getProject();
        val executable = getExecutable();
        val jobId = executable.getId();
        Preconditions.checkState(executable.getTasks().size() > 1, "job " + jobId + " steps is not enough");
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val merger = new AfterBuildResourceMerger(kylinConfig, project);
        executable.getTasks().stream().filter(task -> task instanceof NSparkExecutable)
                .filter(task -> ((NSparkExecutable) task).needMergeMetadata())
                .forEach(task -> ((NSparkExecutable) task).mergerMetadata(merger));
        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, project);
        markDFStatus(dfMgr);
    }

    @Override
    public void handleDiscardOrSuicidal() {
        if (((DefaultChainedExecutableOnModel) getExecutable()).checkAnyLayoutExists()) {
            return;
        }
        makeSegmentReady();
    }

    private void makeSegmentReady() {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val segmentId = getSegmentId();

        NDataflowManager dfMgr = NDataflowManager.getInstance(kylinConfig, getProject());
        NDataflow df = dfMgr.getDataflow(getModelId());

        //update target seg's status
        val dfUpdate = new NDataflowUpdate(getModelId());
        val seg = df.copy().getSegment(segmentId);
        seg.setStatus(SegmentStatusEnum.READY);
        dfUpdate.setToUpdateSegs(seg);
        dfMgr.updateDataflow(dfUpdate);
        markDFStatus(dfMgr);
    }

    private void markDFStatus(NDataflowManager dfManager) {
        super.markDFStatus();
        val df = dfManager.getDataflow(getModelId());
        RealizationStatusEnum status = df.getStatus();
        if (RealizationStatusEnum.LAG_BEHIND == status) {
            val model = df.getModel();
            Preconditions.checkState(ManagementType.TABLE_ORIENTED == model.getManagementType());
            if (checkOnline(model) && !df.getIndexPlan().isOfflineManually()) {
                dfManager.updateDataflowStatus(df.getId(), RealizationStatusEnum.ONLINE);
            }
        }
    }

    private boolean checkOnline(NDataModel model) {
        // 1. check the job status of the model
        val executableManager = getExecutableManager(model.getProject(), KylinConfig.getInstanceFromEnv());
        val count = executableManager
                .listExecByModelAndStatus(model.getId(), ExecutableState::isNotProgressing, JobTypeEnum.INC_BUILD)
                .size();
        if (count > 0) {
            return false;
        }
        // 2. check the model aligned with data loading range
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val df = dfManager.getDataflow(model.getId());
        val dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(KylinConfig.getInstanceFromEnv(),
                model.getProject());
        val dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(model.getRootFactTableName());
        // In theory, dataLoadingRange can not be null, because full load table related model will build with INDEX_BUILD job or INDEX_REFRESH job.
        Preconditions.checkState(dataLoadingRange != null);
        val querableSegmentRange = dataLoadingRangeManager.getQuerableSegmentRange(dataLoadingRange);
        Preconditions.checkState(querableSegmentRange != null);
        val segments = SegmentUtil
                .getSegmentsExcludeRefreshingAndMerging(df.getSegments().getSegmentsByRange(querableSegmentRange));
        for (NDataSegment segment : segments) {
            if (SegmentStatusEnum.NEW == segment.getStatus()) {
                return false;
            }
        }
        return true;
    }

}
