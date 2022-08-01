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

import static org.apache.kylin.engine.spark.stats.utils.HiveTableRefChecker.isNeedCleanUpTransactionalTableJob;

import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.execution.DefaultChainedExecutableOnTable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.sparkproject.guava.base.Preconditions;

import lombok.SneakyThrows;

/**
 *
 */
public class NSparkSnapshotJob extends DefaultChainedExecutableOnTable {
    public NSparkSnapshotJob() {
        super();
    }

    public NSparkSnapshotJob(Object notSetId) {
        super(notSetId);
    }

    public static NSparkSnapshotJob create(TableDesc tableDesc, String submitter, String partitionCol,
            boolean incrementBuild, Set<String> partitionToBuild, boolean isRefresh, String yarnQueue, Object tag) {
        JobTypeEnum jobType = isRefresh ? JobTypeEnum.SNAPSHOT_REFRESH : JobTypeEnum.SNAPSHOT_BUILD;
        return create(tableDesc, submitter, jobType, RandomUtil.randomUUIDStr(), partitionCol, incrementBuild,
                partitionToBuild, yarnQueue, tag);
    }

    public static NSparkSnapshotJob create(TableDesc tableDesc, String submitter, boolean isRefresh, String yarnQueue) {
        JobTypeEnum jobType = isRefresh ? JobTypeEnum.SNAPSHOT_REFRESH : JobTypeEnum.SNAPSHOT_BUILD;
        return create(tableDesc, submitter, jobType, RandomUtil.randomUUIDStr(), null, false, null, yarnQueue, null);
    }

    @SneakyThrows
    public static NSparkSnapshotJob create(TableDesc tableDesc, String submitter, JobTypeEnum jobType, String jobId,
            String partitionCol, boolean incrementalBuild, Set<String> partitionToBuild, String yarnQueue, Object tag) {
        Preconditions.checkArgument(submitter != null);
        NSparkSnapshotJob job = new NSparkSnapshotJob();
        String project = tableDesc.getProject();
        job.setId(jobId);
        job.setProject(project);
        job.setName(jobType.toString());
        job.setJobType(jobType);
        job.setSubmitter(submitter);
        job.setTargetSubject(tableDesc.getIdentity());

        job.setParam(NBatchConstants.P_PROJECT_NAME, project);
        job.setParam(NBatchConstants.P_JOB_ID, jobId);
        job.setParam(NBatchConstants.P_TABLE_NAME, tableDesc.getIdentity());

        job.setParam(NBatchConstants.P_INCREMENTAL_BUILD, incrementalBuild + "");
        job.setParam(NBatchConstants.P_SELECTED_PARTITION_COL, partitionCol);
        if (partitionToBuild != null) {
            job.setParam(NBatchConstants.P_SELECTED_PARTITION_VALUE, JsonUtil.writeValueAsString(partitionToBuild));
        }

        job.setSparkYarnQueueIfEnabled(project, yarnQueue);
        job.setTag(tag);

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        JobStepType.BUILD_SNAPSHOT.createStep(job, config);
        if (isNeedCleanUpTransactionalTableJob(tableDesc.isTransactional(), tableDesc.isRangePartition(),
                config.isReadTransactionalTableEnabled())) {
            JobStepType.CLEAN_UP_TRANSACTIONAL_TABLE.createStep(job, config);
        }
        return job;
    }

    public NSparkSnapshotBuildingStep getSnapshotBuildingStep() {
        return getTask(NSparkSnapshotBuildingStep.class);
    }

}
