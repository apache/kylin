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
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.SecondStorageCleanJobBuildParams;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.metadata.model.SegmentRange;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ClickHouseSegmentCleanJob extends DefaultExecutable {

    public ClickHouseSegmentCleanJob() {}

    public ClickHouseSegmentCleanJob(Object notSetId) {
        super(notSetId);
    }

    public ClickHouseSegmentCleanJob(ClickHouseCleanJobParam builder) {
        setId(builder.getJobId());
        setName(JobTypeEnum.SECOND_STORAGE_SEGMENT_CLEAN.toString());
        setJobType(JobTypeEnum.SECOND_STORAGE_SEGMENT_CLEAN);
        setTargetSubject(builder.getModelId());
        setTargetSegments(builder.segments.stream().map(x -> String.valueOf(x.getId())).collect(Collectors.toList()));
        setProject(builder.df.getProject());
        long startTime = Long.MAX_VALUE - 1;
        long endTime = 0L;
        for (NDataSegment segment : builder.segments) {
            startTime = Math.min(startTime, Long.parseLong(segment.getSegRange().getStart().toString()));
            endTime = endTime > Long.parseLong(segment.getSegRange().getStart().toString()) ? endTime
                    : Long.parseLong(segment.getSegRange().getEnd().toString());
        }
        setParam(NBatchConstants.P_DATA_RANGE_START, String.valueOf(startTime));
        setParam(NBatchConstants.P_DATA_RANGE_END, String.valueOf(endTime));
        setParam(NBatchConstants.P_JOB_ID, getId());
        setParam(NBatchConstants.P_PROJECT_NAME, builder.project);
        setParam(NBatchConstants.P_TARGET_MODEL, getTargetSubject());
        setParam(NBatchConstants.P_DATAFLOW_ID, builder.df.getId());

        ClickHousePartitionClean step = new ClickHousePartitionClean();
        step.setProject(getProject());
        step.setJobType(getJobType());
        step.setTargetSegments(getTargetSegments());
        step.setTargetSubject(getTargetSubject());
        step.setProject(getProject());
        step.setParams(getParams());
        Map<String, SegmentRange<Long>> segmentRangeMap = new HashMap<>(builder.segments.size());
        builder.getSegments().forEach(seg -> segmentRangeMap.put(seg.getId(), seg.getSegRange()));
        val model = builder.getDf().getModel();
        if (model.isIncrementBuildOnExpertMode()) {
            step.setDateFormat(model.getPartitionDesc().getPartitionDateFormat());
        }
        step.setSegmentRangeMap(segmentRangeMap);
        step.init();
        addTask(step);
    }

    public static class SegmentCleanJobFactory extends JobFactory {
        @Override
        protected AbstractExecutable create(JobBuildParams jobBuildParams) {
            SecondStorageCleanJobBuildParams params = (SecondStorageCleanJobBuildParams) jobBuildParams;
            val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), params.getProject());
            val param = ClickHouseCleanJobParam.builder().jobId(params.getJobId()).submitter(params.getSubmitter())
                    .project(params.getProject()).modelId(params.getModelId())
                    .df(dfManager.getDataflow(params.getDataflowId())).segments(params.getSegments()).build();
            return new ClickHouseSegmentCleanJob(param);
        }
    }
}
