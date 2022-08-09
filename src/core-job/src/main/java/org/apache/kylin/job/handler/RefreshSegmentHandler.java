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

import static org.apache.kylin.job.factory.JobFactoryConstant.CUBE_JOB_FACTORY;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.model.PartitionStatusEnum;

import com.google.common.collect.Sets;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public class RefreshSegmentHandler extends AbstractJobHandler {
    @Override
    protected AbstractExecutable createJob(JobParam jobParam) {

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataflowManager dfm = NDataflowManager.getInstance(kylinConfig, jobParam.getProject());
        NDataflow dataflow = dfm.getDataflow(jobParam.getModel()).copy();

        if (jobParam.isMultiPartitionJob()) {
            // update partition status to refresh
            val segment = dataflow.getSegment(jobParam.getSegment());
            segment.getMultiPartitions().forEach(partition -> {
                if (jobParam.getTargetPartitions().contains(partition.getPartitionId())) {
                    partition.setStatus(PartitionStatusEnum.REFRESH);
                }
            });
            val dfUpdate = new NDataflowUpdate(dataflow.getId());
            dfUpdate.setToUpdateSegs(segment);
            dfm.updateDataflow(dfUpdate);
        }

        return JobFactory.createJob(CUBE_JOB_FACTORY,
                new JobFactory.JobBuildParams(Sets.newHashSet(dataflow.getSegment(jobParam.getSegment())),
                        jobParam.getProcessLayouts(), jobParam.getOwner(), jobParam.getJobTypeEnum(),
                        jobParam.getJobId(), null, jobParam.getIgnoredSnapshotTables(), jobParam.getTargetPartitions(),
                        jobParam.getTargetBuckets(), jobParam.getExtParams()));
    }
}
