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

import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.factory.JobFactoryConstant;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.NBatchConstants;

import lombok.Getter;

public class InternalTableJobHandler extends AbstractJobHandler {
    @Getter
    public static class InternalTableJobBuildParam extends JobFactory.JobBuildParams {

        private String project;
        private String table;
        private String incrementalBuild;
        private String isRefresh;
        private String startDate;
        private String endDate;
        private String deletePartitionValues;
        private String deletePartition; // true or false

        public InternalTableJobBuildParam(JobParam jobParam) {
            super(null, null, jobParam.getOwner(), jobParam.getJobTypeEnum(), jobParam.getJobId(), null, null, null,
                    null, jobParam.getExtParams());
            this.project = jobParam.getProject();
            this.table = jobParam.getTable();
            this.incrementalBuild = jobParam.getExtParams().get(NBatchConstants.P_INCREMENTAL_BUILD);
            this.isRefresh = jobParam.getExtParams().get(NBatchConstants.P_OUTPUT_MODE);
            this.startDate = jobParam.getExtParams().get(NBatchConstants.P_START_DATE);
            this.endDate = jobParam.getExtParams().get(NBatchConstants.P_END_DATE);
            this.deletePartitionValues = jobParam.getExtParams().get(NBatchConstants.P_DELETE_PARTITION_VALUES);
            this.deletePartition = jobParam.getExtParams().get(NBatchConstants.P_DELETE_PARTITION);
        }
    }

    @Override
    protected AbstractExecutable createJob(JobParam jobParam) {
        return JobFactory.createJob(JobFactoryConstant.INTERNAL_TABLE_JOB_FACTORY,
                new InternalTableJobBuildParam(jobParam));
    }

    @Override
    protected void checkBeforeHandle(JobParam jobParam) {
        String project = jobParam.getProject();
        JobManager.checkStorageQuota(project);
    }

}
