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

import com.google.common.base.Preconditions;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.SecondStorageCleanJobBuildParams;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.metadata.model.SegmentRange;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ClickHouseIndexCleanJob extends DefaultChainedExecutable {

    public ClickHouseIndexCleanJob(Object notSetId) {
        super(notSetId);
    }

    public ClickHouseIndexCleanJob(ClickHouseCleanJobParam builder) {
        setId(builder.getJobId());
        setName(JobTypeEnum.SECOND_STORAGE_INDEX_CLEAN.toString());
        setJobType(JobTypeEnum.SECOND_STORAGE_INDEX_CLEAN);
        setTargetSubject(builder.getModelId());
        setProject(builder.df.getProject());

        Optional<Manager<TableFlow>> manager = SecondStorageUtil.tableFlowManager(builder.df);
        Preconditions.checkState(manager.isPresent());
        Optional<TableFlow> tableFlow = manager.get().get(builder.df.getId());
        Preconditions.checkState(tableFlow.isPresent());

        Set<String> segSet;
        String dateFormat = null;
        Map<String, SegmentRange<Long>> segmentRangeMap = new HashMap<>();

        if (builder.segments != null && !builder.segments.isEmpty()) {
            segSet = builder.segments.stream().map(NDataSegment::getId).collect(Collectors.toSet());

            builder.getSegments().forEach(seg -> segmentRangeMap.put(seg.getId(), seg.getSegRange()));
            val model = builder.getDf().getModel();
            if (model.isIncrementBuildOnExpertMode()) {
                dateFormat = model.getPartitionDesc().getPartitionDateFormat();
            }
        } else {
            segSet = tableFlow.get().getTableDataList().stream().flatMap(tableData ->
                    tableData.getPartitions().stream().map(TablePartition::getSegmentId)
            ).collect(Collectors.toSet());
        }

        setRange(builder, segSet);
        setBaseParam(builder);

        ClickHouseIndexClean step = new ClickHouseIndexClean();
        step.setProject(getProject());
        step.setTargetSubject(getTargetSubject());
        step.setJobType(getJobType());
        step.setParams(getParams());
        step.setSegmentRangeMap(segmentRangeMap);
        step.setDateFormat(dateFormat);
        step.setNeedDeleteLayoutIds(builder.getNeedDeleteLayoutIds());

        step.init();
        addTask(step);
    }

    @Override
    public Set<String> getSegmentIds() {
        return Collections.emptySet();
    }

    public static class IndexCleanJobFactory extends JobFactory {
        @Override
        protected AbstractExecutable create(JobBuildParams jobBuildParams) {
            SecondStorageCleanJobBuildParams params = (SecondStorageCleanJobBuildParams) jobBuildParams;
            val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), params.getProject());
            val param = ClickHouseCleanJobParam.builder()
                    .modelId(params.getModelId())
                    .jobId(params.getJobId())
                    .submitter(params.getSubmitter())
                    .df(dfManager.getDataflow(params.getDataflowId()))
                    .project(params.getProject())
                    .needDeleteLayoutIds(params.getSecondStorageDeleteLayoutIds())
                    .segments(jobBuildParams.getSegments())
                    .build();
            return new ClickHouseIndexCleanJob(param);
        }
    }

    private void setRange(ClickHouseCleanJobParam builder, Set<String> segSet) {
        long startTime = Long.MAX_VALUE - 1;
        long endTime = 0L;
        for (NDataSegment segment : builder.df.getSegments().stream()
                .filter(seg -> segSet.contains(seg.getId()))
                .collect(Collectors.toList())) {
            startTime = Math.min(startTime, Long.parseLong(segment.getSegRange().getStart().toString()));
            endTime = endTime > Long.parseLong(segment.getSegRange().getStart().toString()) ? endTime
                    : Long.parseLong(segment.getSegRange().getEnd().toString());
        }
        if (startTime > endTime) {
            // when doesn't have segment swap start and end time
            long tmp = endTime;
            endTime = startTime;
            startTime = tmp;
        }

        setParam(NBatchConstants.P_DATA_RANGE_START, String.valueOf(startTime));
        setParam(NBatchConstants.P_DATA_RANGE_END, String.valueOf(endTime));
    }

    private void setBaseParam(ClickHouseCleanJobParam builder) {
        setParam(NBatchConstants.P_JOB_ID, getId());
        setParam(NBatchConstants.P_PROJECT_NAME, builder.project);
        setParam(NBatchConstants.P_TARGET_MODEL, getTargetSubject());
        setParam(NBatchConstants.P_DATAFLOW_ID, builder.df.getId());
    }
}
