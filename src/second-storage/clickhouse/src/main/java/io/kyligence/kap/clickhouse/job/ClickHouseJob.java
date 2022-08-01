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
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.factory.JobFactory;

import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

public class ClickHouseJob extends DefaultChainedExecutable {

    private static void wrapWithKylinException(final Runnable lambda) {
        try {
            lambda.run();
        } catch (IllegalArgumentException e) {
            throw new KylinException(INVALID_PARAMETER, e);
        }
    }

    /** This constructor is needed by reflection, since a private constructor is already defined for Builder,
     *  we have to manually define it. Please check {@link org.apache.kylin.job.execution.NExecutableManager#fromPO}
     */
    public ClickHouseJob() {
    }

    public ClickHouseJob(Object notSetId) {
        super(notSetId);
    }

    private ClickHouseJob(Builder builder) {
        // check parameters

        setId(builder.jobId);
        setName(builder.jobType.toString());
        setJobType(builder.jobType);
        setTargetSubject(builder.segments.iterator().next().getModel().getUuid());
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
        setParam(NBatchConstants.P_PROJECT_NAME, builder.df.getProject());
        setParam(NBatchConstants.P_TARGET_MODEL, getTargetSubject());
        setParam(NBatchConstants.P_DATAFLOW_ID, builder.df.getId());
        setParam(NBatchConstants.P_LAYOUT_IDS,
                builder.layouts.stream()
                        .map(LayoutEntity::getId)
                        .map(String::valueOf)
                        .collect(Collectors.joining(",")));
        setParam(NBatchConstants.P_SEGMENT_IDS, String.join(",", getTargetSegments()));

        AbstractExecutable step = new ClickHouseLoad();
        step.setProject(getProject());
        step.setTargetSubject(getTargetSubject());
        step.setJobType(getJobType());
        step.setParams(getParams());
        addTask(step);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class StorageJobFactory extends JobFactory {

        @Override
        protected ClickHouseJob create(JobBuildParams jobBuildParams) {
            wrapWithKylinException(() -> Preconditions.checkNotNull(jobBuildParams));

            Builder builder = ClickHouseJob.builder();
            Set<NDataSegment> segments = jobBuildParams.getSegments();
            NDataflow df = segments.iterator().next().getDataflow();
            builder.setDf(df)
                    .setJobId(jobBuildParams.getJobId())
                    .setSubmitter(jobBuildParams.getSubmitter())
                    .setSegments(segments)
                    .setLayouts(jobBuildParams.getLayouts());
            return builder.build();
        }
    }

    public static final class Builder {
        String jobId;
        JobTypeEnum jobType = JobTypeEnum.EXPORT_TO_SECOND_STORAGE;
        String submitter;
        NDataflow df;
        Set<NDataSegment> segments;
        Set<LayoutEntity> layouts;

        public ClickHouseJob build() {
            return new ClickHouseJob(this);
        }

        public Builder setJobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder setJobType(JobTypeEnum jobType) {
            this.jobType = jobType;
            return this;
        }

        public Builder setSubmitter(String submitter) {
            this.submitter = submitter;
            return this;
        }

        public Builder setDf(NDataflow df) {
            this.df = df;
            return this;
        }

        public Builder setSegments(Set<NDataSegment> segments) {
            this.segments = segments;
            return this;
        }

        public Builder setLayouts(Set<LayoutEntity> layouts) {
            this.layouts = layouts;
            return this;
        }
    }
}
