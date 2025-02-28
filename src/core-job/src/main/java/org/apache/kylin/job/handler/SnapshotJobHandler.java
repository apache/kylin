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
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.NBatchConstants;

import lombok.Getter;

public class SnapshotJobHandler extends AbstractJobHandler {

    @Getter
    public static class SnapshotJobBuildParam extends JobFactory.JobBuildParams {

        private String project;
        private String table;
        private String incrementalBuild;
        private String partitionCol;
        private String partitionsToBuild;

        public SnapshotJobBuildParam(JobParam jobParam) {
            super(null, jobParam.getDeleteLayouts(), jobParam.getOwner(), jobParam.getJobTypeEnum(),
                    jobParam.getJobId(), jobParam.getDeleteLayouts(), jobParam.getIgnoredSnapshotTables(),
                    jobParam.getTargetPartitions(), jobParam.getTargetBuckets(), jobParam.getExtParams());
            this.project = jobParam.getProject();
            this.table = jobParam.getTable();
            this.incrementalBuild = jobParam.getExtParams().get(NBatchConstants.P_INCREMENTAL_BUILD);
            this.partitionCol = jobParam.getExtParams().get(NBatchConstants.P_SELECTED_PARTITION_COL);
            this.partitionsToBuild = jobParam.getExtParams().get(NBatchConstants.P_SELECTED_PARTITION_VALUE);
        }
    }

    @Override
    protected AbstractExecutable createJob(JobParam jobParam) {
        return JobFactory.createJob(JobFactoryConstant.SNAPSHOT_JOB_FACTORY, new SnapshotJobBuildParam(jobParam));
    }

    @Override
    protected void checkBeforeHandle(JobParam jobParam) {
    }
}
