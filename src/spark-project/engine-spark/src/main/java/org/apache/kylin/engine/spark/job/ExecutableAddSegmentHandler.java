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

import static org.apache.kylin.engine.spark.utils.ExecutableHandleUtils.makeSegmentReady;
import static org.apache.kylin.engine.spark.utils.ExecutableHandleUtils.mergeMetadata;

import java.util.List;

import org.apache.kylin.engine.spark.utils.ExecutableHandleUtils;
import org.apache.kylin.engine.spark.utils.SparkJobFactoryUtils;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultExecutableOnModel;
import org.apache.kylin.job.execution.ExecutableHandler;
import org.apache.kylin.job.execution.MergerInfo;
import org.apache.kylin.metadata.cube.model.NDataLayout;

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
        val modelId = getModelId();
        Preconditions.checkState(executable.getTasks().size() > 1, "job " + jobId + " steps is not enough");
        val errorOrPausedJobCount = getErrorOrPausedJobCount();
        MergerInfo mergerInfo = new MergerInfo(project, modelId, jobId, errorOrPausedJobCount, HandlerType.ADD_SEGMENT);
        ExecutableHandleUtils.getNeedMergeTasks(executable)
                .forEach(task -> mergerInfo.addTaskMergeInfo(task, SparkJobFactoryUtils.needBuildSnapshots(task)));

        List<NDataLayout[]> mergedLayout = mergeMetadata(project, mergerInfo);
        List<AbstractExecutable> tasks = ExecutableHandleUtils.getNeedMergeTasks(executable);
        Preconditions.checkArgument(mergedLayout.size() == tasks.size());
        for (int idx = 0; idx < tasks.size(); idx++) {
            AbstractExecutable task = tasks.get(idx);
            NDataLayout[] layouts = mergedLayout.get(idx);
            ExecutableHandleUtils.recordDownJobStats(task, layouts, project);
            task.notifyUserIfNecessary(layouts);
        }
    }

    @Override
    public void handleDiscardOrSuicidal() {
        if (((DefaultExecutableOnModel) getExecutable()).checkAnyLayoutExists()) {
            return;
        }
        makeSegmentReady(getProject(), getModelId(), getSegmentId());
    }
}
