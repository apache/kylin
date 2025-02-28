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

package org.apache.kylin.job.execution;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.NDataSegment;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor
@AllArgsConstructor
public abstract class ExecutableHandler {

    protected static final String SUBJECT_NOT_EXIST_COMMENT = "subject does not exist or is broken, roll back to to-be-accelerated status";

    @Getter
    @Setter
    private String project;
    @Getter
    @Setter
    private String modelId;
    @Getter
    @Setter
    private String owner;
    @Getter
    @Setter
    private String segmentId;
    @Getter
    @Setter
    private String jobId;

    public abstract void handleFinished();

    public abstract void handleDiscardOrSuicidal();

    protected ExecutableManager getExecutableManager(String project, KylinConfig config) {
        return ExecutableManager.getInstance(config, project);
    }

    protected void addJob(String segmentId, JobTypeEnum jobTypeEnum) {
        JobManager manager = JobManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        NDataSegment segment = NDataSegment.empty();
        if (segmentId != null) {
            segment.setId(segmentId);
        }
        manager.addJob(new JobParam(segment, modelId, owner).withJobTypeEnum(jobTypeEnum));
    }

    protected DefaultExecutableOnModel getExecutable() {
        val executable = getExecutableManager(project, KylinConfig.getInstanceFromEnv()).getJob(jobId);
        Preconditions.checkNotNull(executable);
        Preconditions.checkArgument(executable instanceof DefaultExecutableOnModel);
        return (DefaultExecutableOnModel) executable;
    }

    public int getErrorOrPausedJobCount() {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val executableManager = getExecutableManager(project, kylinConfig);
        return executableManager
                .listExecByModelAndStatus(modelId, ExecutableState::isNotProgressing, JobTypeEnum.INC_BUILD).size();
    }

    public enum HandlerType {
        ADD_CUBOID, ADD_SEGMENT, MERGE_OR_REFRESH, SAMPLING, SNAPSHOT, LOAD_INTERNAL_TABLE
    }
}
