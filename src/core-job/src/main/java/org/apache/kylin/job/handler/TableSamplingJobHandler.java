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

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.factory.JobFactoryConstant;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.NBatchConstants;

import lombok.Getter;

public class TableSamplingJobHandler extends AbstractJobHandler {

    @Getter
    public static class TableSamplingJobBuildParam extends JobFactory.JobBuildParams {
        private final String table;
        private final String project;
        private final int row;

        public TableSamplingJobBuildParam(JobParam jobParam) {
            super(null, jobParam.getDeleteLayouts(), jobParam.getOwner(), jobParam.getJobTypeEnum(),
                    jobParam.getJobId(), jobParam.getDeleteLayouts(), jobParam.getIgnoredSnapshotTables(),
                    jobParam.getTargetPartitions(), jobParam.getTargetBuckets(), jobParam.getExtParams());
            this.table = jobParam.getTable();
            this.project = jobParam.getProject();
            this.row = Integer.parseInt(jobParam.getExtParams().get(NBatchConstants.P_SAMPLING_ROWS));
        }
    }

    @Override
    protected AbstractExecutable createJob(JobParam jobParam) {
        return JobFactory.createJob(JobFactoryConstant.TABLE_SAMPLING_JOB_FACTORY,
                new TableSamplingJobBuildParam(jobParam));
    }

    @Override
    protected void checkBeforeHandle(JobParam jobParam) {
        String project = jobParam.getProject();
        String table = jobParam.getTable();
        List<JobInfo> existingJobs = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .fetchNotFinalJobsByTypes(project, Lists.newArrayList(JobTypeEnum.TABLE_SAMPLING.name()),
                        Lists.newArrayList(table));
        ExecutableManager execMgr = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        if (CollectionUtils.isNotEmpty(existingJobs)) {
            existingJobs.forEach(jobInfo -> execMgr.discardJob(jobInfo.getJobId()));
        }
        JobManager.checkStorageQuota(project);
    }
}
