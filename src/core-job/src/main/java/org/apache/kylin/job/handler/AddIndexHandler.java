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

import java.util.HashSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;

import com.google.common.collect.Sets;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public class AddIndexHandler extends AbstractJobHandler {

    @Override
    protected AbstractExecutable createJob(JobParam jobParam) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        String modelId = jobParam.getModel();
        String project = jobParam.getProject();
        NDataflow df = NDataflowManager.getInstance(kylinConfig, project).getDataflow(modelId);

        var readySegs = df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        val targetSegments = new HashSet<>(jobParam.getTargetSegments());
        final Segments<NDataSegment> toDealSeg = new Segments<>();
        readySegs.stream().filter(segment -> targetSegments.contains(segment.getId() + "")).forEach(toDealSeg::add);
        readySegs = toDealSeg;

        if (CollectionUtils.isEmpty(jobParam.getProcessLayouts())
                && CollectionUtils.isEmpty(jobParam.getDeleteLayouts())) {
            log.info("Event {} is no longer valid because no layout awaits process", jobParam);
            return null;
        }
        if (readySegs.isEmpty()) {
            throw new IllegalArgumentException("No segment is ready in this job.");
        }
        return JobFactory.createJob(CUBE_JOB_FACTORY,
                new JobFactory.JobBuildParams(Sets.newLinkedHashSet(readySegs), jobParam.getProcessLayouts(),
                        jobParam.getOwner(), jobParam.getJobTypeEnum(), jobParam.getJobId(),
                        jobParam.getDeleteLayouts(), jobParam.getIgnoredSnapshotTables(),
                        jobParam.getTargetPartitions(), jobParam.getTargetBuckets(), jobParam.getExtParams()));
    }

}
