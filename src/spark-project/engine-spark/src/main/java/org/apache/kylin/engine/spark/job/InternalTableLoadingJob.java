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
import org.apache.kylin.job.execution.DefaultExecutableOnTable;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.factory.JobFactoryConstant;
import org.apache.kylin.job.handler.InternalTableJobHandler.InternalTableJobBuildParam;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.table.InternalTableDesc;
import org.apache.kylin.metadata.table.InternalTableManager;
import org.sparkproject.guava.base.Preconditions;

public class InternalTableLoadingJob extends DefaultExecutableOnTable {

    static {
        JobFactory.register(JobFactoryConstant.INTERNAL_TABLE_JOB_FACTORY, new InternalTableJobFactory());
    }

    public InternalTableLoadingJob() {
        super();
    }

    public InternalTableLoadingJob(Object notSetId) {
        super(notSetId);
    }

    public static InternalTableLoadingJob create(InternalTableJobBuildParam param) {
        KylinConfig projectConfig = NProjectManager.getProjectConfig(param.getProject());
        InternalTableManager tableManager = InternalTableManager.getInstance(projectConfig, param.getProject());
        InternalTableDesc internalTable = tableManager.getInternalTableDesc(param.getTable());
        Preconditions.checkArgument(param.getSubmitter() != null);
        InternalTableLoadingJob job = new InternalTableLoadingJob();
        job.setSubmitter(param.getSubmitter());
        job.setName(param.getJobType().toString());
        job.setJobType(param.getJobType());
        job.setId(param.getJobId());
        job.setTargetSubject(internalTable.getIdentity());
        job.setProject(internalTable.getProject());

        job.setParam(NBatchConstants.P_PROJECT_NAME, internalTable.getProject());
        job.setParam(NBatchConstants.P_JOB_ID, param.getJobId());
        job.setParam(NBatchConstants.P_TABLE_NAME, internalTable.getIdentity());

        job.setParam(NBatchConstants.P_INCREMENTAL_BUILD, param.getIncrementalBuild());
        job.setParam(NBatchConstants.P_OUTPUT_MODE, param.getIsRefresh());
        job.setParam(NBatchConstants.P_START_DATE, param.getStartDate());
        job.setParam(NBatchConstants.P_END_DATE, param.getEndDate());
        job.setParam(NBatchConstants.P_DELETE_PARTITION_VALUES, param.getDeletePartitionValues());
        job.setParam(NBatchConstants.P_DELETE_PARTITION, param.getDeletePartition());
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        StepEnum.BUILD_INTERNAL.create(job, config);
        if (isPreloadedCacheEnable(internalTable, projectConfig)) {
            StepEnum.LOAD_GLUTEN_CACHE.create(job, config);
        }
        return job;
    }

    private static boolean isPreloadedCacheEnable(InternalTableDesc internalTable, KylinConfig projectConfig) {
        if (internalTable.isPreloadedCacheEnable()) {
            return true;
        }
        return projectConfig.isInternalTablePreloadCacheEnabled();
    }

    public static class InternalTableJobFactory extends JobFactory {

        protected InternalTableJobFactory() {
        }

        @Override
        protected InternalTableLoadingJob create(JobBuildParams jobBuildParams) {
            return InternalTableLoadingJob.create((InternalTableJobBuildParam) jobBuildParams);
        }
    }
}
