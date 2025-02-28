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

import static org.apache.kylin.engine.spark.utils.ExecutableHandleUtils.mergeMetadata;

import java.util.List;

import org.apache.kylin.engine.spark.utils.ExecutableHandleUtils;
import org.apache.kylin.engine.spark.utils.SparkJobFactoryUtils;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultExecutableOnModel;
import org.apache.kylin.job.execution.ExecutableHandler;
import org.apache.kylin.job.execution.MergerInfo;
import org.apache.kylin.job.util.ExecutableParaUtil;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class ExecutableAddCuboidHandler extends ExecutableHandler {
    private static final Logger logger = LoggerFactory.getLogger(ExecutableAddCuboidHandler.class);

    public ExecutableAddCuboidHandler(DefaultExecutableOnModel job) {
        this(job.getProject(), job.getTargetSubject(), job.getSubmitter(), null, job.getId());
    }

    public ExecutableAddCuboidHandler(String project, String modelId, String owner, String segmentId, String jobId) {
        super(project, modelId, owner, segmentId, jobId);
    }

    @Override
    public void handleFinished() {
        val project = getProject();
        val jobId = getJobId();
        val modelId = getModelId();
        val executable = getExecutable();
        Preconditions.checkState(executable.getTasks().size() > 1, "job " + jobId + " steps is not enough");

        val errorOrPausedJobCount = getErrorOrPausedJobCount();
        String toBeDeletedLayoutIdsStr = ExecutableParaUtil.getToBeDeletedLayoutIdsStr(executable);
        MergerInfo mergerInfo = new MergerInfo(project, toBeDeletedLayoutIdsStr, modelId, jobId, errorOrPausedJobCount,
                HandlerType.ADD_CUBOID);
        ExecutableHandleUtils.getNeedMergeTasks(executable)
                .forEach(task -> mergerInfo.addTaskMergeInfo(task, SparkJobFactoryUtils.needBuildSnapshots(task)));

        List<NDataLayout[]> mergedLayout = mergeMetadata(project, mergerInfo);
        List<AbstractExecutable> tasks = ExecutableHandleUtils.getNeedMergeTasks(executable);
        // Preconditions.checkArgument(mergedLayout.size() == tasks.size());
        for (int idx = 0; idx < mergedLayout.size(); idx++) {
            AbstractExecutable task = tasks.get(idx);
            NDataLayout[] layouts = mergedLayout.get(idx);
            ExecutableHandleUtils.recordDownJobStats(task, layouts, project);
            task.notifyUserIfNecessary(layouts);
        }
    }

    @Override
    public void handleDiscardOrSuicidal() {
        val job = getExecutable();
        // anyTargetSegmentExists && checkCuttingInJobByModel need restart job
        if (!(job.checkCuttingInJobByModel() && job.checkAnyTargetSegmentAndPartitionExists())) {
            return;
        }
    }

}
