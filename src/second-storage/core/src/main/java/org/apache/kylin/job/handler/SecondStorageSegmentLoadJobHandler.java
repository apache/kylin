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

package org.apache.kylin.job.handler;

import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;

import static org.apache.kylin.common.exception.ServerErrorCode.BASE_TABLE_INDEX_NOT_AVAILABLE;
import static org.apache.kylin.common.exception.ServerErrorCode.SECOND_STORAGE_ADD_JOB_FAILED;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.exception.JobSubmissionException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.model.JobParam;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_JOB_EXPORT_TO_TIERED_STORAGE_WITHOUT_BASE_INDEX;
import static org.apache.kylin.job.factory.JobFactoryConstant.STORAGE_JOB_FACTORY;

public class SecondStorageSegmentLoadJobHandler extends AbstractJobHandler {

    @Override
    protected AbstractExecutable createJob(JobParam jobParam) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        NDataflow dataflow = NDataflowManager.getInstance(kylinConfig, jobParam.getProject())
                .getDataflow(jobParam.getModel());
        List<String> hasBaseIndexSegmentIds = jobParam.getTargetSegments().stream().map(dataflow::getSegment)
                .filter(segment -> segment.getLayoutsMap().values()
                        .stream().map(NDataLayout::getLayout).anyMatch(SecondStorageUtil::isBaseTableIndex))
                .map(NDataSegment::getId)
                .collect(Collectors.toList());

        if (hasBaseIndexSegmentIds.isEmpty()) {
            throw new KylinException(BASE_TABLE_INDEX_NOT_AVAILABLE,
                    MsgPicker.getMsg().getSecondStorageSegmentWithoutBaseIndex());
        }

        return JobFactory.createJob(STORAGE_JOB_FACTORY,
                new JobFactory.JobBuildParams(
                        jobParam.getTargetSegments().stream().map(dataflow::getSegment).collect(Collectors.toSet()),
                        jobParam.getProcessLayouts(),
                        jobParam.getOwner(),
                        JobTypeEnum.EXPORT_TO_SECOND_STORAGE,
                        jobParam.getJobId(),
                        null,
                        jobParam.getIgnoredSnapshotTables(),
                        null,
                        null));
    }

    @Override
    protected boolean needComputeJobBucket() {
        return false;
    }

    @Override
    protected void checkBeforeHandle(JobParam jobParam) {
        String model = jobParam.getModel();
        String project = jobParam.getProject();
        checkNotNull(project);
        checkNotNull(model);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NExecutableManager execManager = NExecutableManager.getInstance(kylinConfig, project);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(kylinConfig, project);
        List<AbstractExecutable> executables = execManager.listExecByModelAndStatus(model, ExecutableState::isRunning, JobTypeEnum.EXPORT_TO_SECOND_STORAGE);
        NDataflow dataflow = dataflowManager.getDataflow(jobParam.getModel());

        Set<String> targetSegs = new HashSet<>(jobParam.getTargetSegments());
        List<String> failedSegs = executables.stream().flatMap(executable -> executable.getTargetSegments().stream())
                .filter(targetSegs::contains).collect(Collectors.toList());

        // segment doesn't have base table index
        List<String> noBaseIndexSegs = targetSegs.stream().filter(seg -> {
            NDataSegment segment = dataflow.getSegment(seg);
            return segment.getIndexPlan().getAllLayouts().stream().anyMatch(SecondStorageUtil::isBaseTableIndex);
        }).collect(Collectors.toList());
        if (failedSegs.isEmpty()) {
            return;
        }
        JobSubmissionException jobSubmissionException = new JobSubmissionException(
                MsgPicker.getMsg().getAddJobCheckFail());
        for (String failedSeg : failedSegs) {
            jobSubmissionException.addJobFailInfo(failedSeg,
                    new KylinException(SECOND_STORAGE_ADD_JOB_FAILED, MsgPicker.getMsg().getAddExportJobFail()));
        }
        noBaseIndexSegs.forEach(seg -> jobSubmissionException.addJobFailInfo(seg,
                new KylinException(FAILED_CREATE_JOB_EXPORT_TO_TIERED_STORAGE_WITHOUT_BASE_INDEX, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getAddJobCheckFailWithoutBaseIndex(), seg))));
        throw jobSubmissionException;
    }
}
