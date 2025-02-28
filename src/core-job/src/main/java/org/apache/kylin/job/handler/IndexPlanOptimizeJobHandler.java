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

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.factory.JobFactoryConstant;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.NBatchConstants;

import lombok.Getter;

public class IndexPlanOptimizeJobHandler extends AbstractJobHandler {

    @Getter
    public static class IndexPlanOptimizeJobParam extends JobFactory.JobBuildParams {

        private final String project;
        private final String dataflowId;

        private final Integer initializeCuboidCount;
        private final Integer maxCuboidCount;
        private final Integer maxCuboidChangeCount;
        private final Long dataRangeStart;
        private final Long dataRangeEnd;

        private final boolean isAutoApproveEnabled;
        private final String operationToken;



        public IndexPlanOptimizeJobParam(JobParam jobParam) {
            super(null, jobParam.getDeleteLayouts(), jobParam.getOwner(), jobParam.getJobTypeEnum(),
                    jobParam.getJobId(), jobParam.getDeleteLayouts(), jobParam.getIgnoredSnapshotTables(),
                    jobParam.getTargetPartitions(), jobParam.getTargetBuckets(), jobParam.getExtParams());

            this.project = jobParam.getProject();
            this.dataflowId = jobParam.getExtParams().get(NBatchConstants.P_DATAFLOW_ID);

            /** planner parameters */
            this.initializeCuboidCount = Integer.parseInt(
                    jobParam.getExtParams().getOrDefault(NBatchConstants.P_PLANNER_INITIALIZE_CUBOID_COUNT, "0"));
            this.maxCuboidCount = Integer
                    .parseInt(jobParam.getExtParams().get(NBatchConstants.P_PLANNER_MAX_CUBOID_COUNT));
            this.maxCuboidChangeCount = Integer.parseInt(jobParam.getExtParams()
                    .getOrDefault(NBatchConstants.P_PLANNER_MAX_CUBOID_CHANGE_COUNT, String.valueOf(0L)));
            this.dataRangeStart = Long
                    .parseLong(jobParam.getExtParams().get(NBatchConstants.P_PLANNER_DATA_RANGE_START));
            this.dataRangeEnd = Long
                    .parseLong(jobParam.getExtParams().getOrDefault(NBatchConstants.P_PLANNER_DATA_RANGE_END,
                            String.valueOf(LocalDate.now(ZoneId.systemDefault()).atStartOfDay(ZoneOffset.UTC)
                                    .toInstant().toEpochMilli())));

            this.isAutoApproveEnabled = Boolean.parseBoolean(jobParam.getExtParams()
                    .getOrDefault(NBatchConstants.P_PLANNER_AUTO_APPROVE_ENABLED, Boolean.FALSE.toString()));
            this.operationToken = jobParam.getExtParams().get(NBatchConstants.P_PLANNER_OPERATION_TOKEN);
        }
    }

    @Override
    protected AbstractExecutable createJob(JobParam jobParam) {
        return JobFactory.createJob(JobFactoryConstant.INDEX_PLAN_OPTIMIZE_JOB_FACTORY,
                new IndexPlanOptimizeJobHandler.IndexPlanOptimizeJobParam(jobParam));
    }

    @Override
    protected void checkBeforeHandle(JobParam jobParam) {
        // No need to check.
    }
}
