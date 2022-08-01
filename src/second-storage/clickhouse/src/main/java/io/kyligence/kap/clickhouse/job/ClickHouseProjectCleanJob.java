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

package io.kyligence.kap.clickhouse.job;

import org.apache.kylin.metadata.cube.model.NBatchConstants;
import lombok.val;
import org.apache.kylin.job.ProjectJob;
import org.apache.kylin.job.SecondStorageCleanJobBuildParams;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.factory.JobFactory;

import java.util.Collections;
import java.util.Set;

public class ClickHouseProjectCleanJob extends DefaultChainedExecutable implements ProjectJob {

    public ClickHouseProjectCleanJob() {
    }

    public ClickHouseProjectCleanJob(Object notSetId) {
        super(notSetId);
    }

    public ClickHouseProjectCleanJob(ClickHouseCleanJobParam builder) {
        setId(builder.getJobId());
        setName(JobTypeEnum.SECOND_STORAGE_NODE_CLEAN.toString());
        setJobType(JobTypeEnum.SECOND_STORAGE_NODE_CLEAN);
        setProject(builder.getProject());
        long startTime = 0L;
        long endTime = Long.MAX_VALUE - 1;
        setParam(NBatchConstants.P_DATA_RANGE_START, String.valueOf(startTime));
        setParam(NBatchConstants.P_DATA_RANGE_END, String.valueOf(endTime));
        setParam(NBatchConstants.P_JOB_ID, getId());
        setParam(NBatchConstants.P_PROJECT_NAME, builder.project);

        AbstractClickHouseClean step = new ClickHouseDatabaseClean();
        step.setProject(getProject());
        step.setJobType(getJobType());
        step.setParams(getParams());
        step.init();
        addTask(step);
    }

    @Override
    public Set<String> getSegmentIds() {
        return Collections.emptySet();
    }

    public static class ProjectCleanJobFactory extends JobFactory {
        @Override
        protected AbstractExecutable create(JobBuildParams jobBuildParams) {
            SecondStorageCleanJobBuildParams params = (SecondStorageCleanJobBuildParams) jobBuildParams;
            val param = ClickHouseCleanJobParam.builder()
                    .jobId(params.getJobId())
                    .submitter(params.getSubmitter())
                    .project(params.getProject())
                    .build();
            return new ClickHouseProjectCleanJob(param);
        }
    }
}
