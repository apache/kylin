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

import static io.kyligence.kap.clickhouse.job.ClickhouseRefreshSecondaryIndex.CLICKHOUSE_ADD_SECONDARY_INDEX;
import static io.kyligence.kap.clickhouse.job.ClickhouseRefreshSecondaryIndex.CLICKHOUSE_LAYOUT_ID;
import static io.kyligence.kap.clickhouse.job.ClickhouseRefreshSecondaryIndex.CLICKHOUSE_REMOVE_SECONDARY_INDEX;

import java.util.Collections;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.SecondStorageRefreshSecondaryIndexJobParams;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataflowManager;

import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import lombok.val;

public class ClickHouseRefreshSecondaryIndexJob extends DefaultExecutable {

    public ClickHouseRefreshSecondaryIndexJob(Object notSetId) {
        super(notSetId);
    }

    public ClickHouseRefreshSecondaryIndexJob(ClickHouseRefreshSecondaryIndexParam builder) {
        setId(builder.getJobId());
        setName(JobTypeEnum.SECOND_STORAGE_REFRESH_SECONDARY_INDEXES.toString());
        setJobType(JobTypeEnum.SECOND_STORAGE_REFRESH_SECONDARY_INDEXES);
        setTargetSubject(builder.getModelId());
        setProject(builder.getProject());

        setParam(NBatchConstants.P_JOB_ID, getId());
        setParam(NBatchConstants.P_DATA_RANGE_START, String.valueOf(0));
        setParam(NBatchConstants.P_DATA_RANGE_END, String.valueOf(Long.MAX_VALUE));

        ClickhouseRefreshSecondaryIndex step = new ClickhouseRefreshSecondaryIndex();
        step.setProject(getProject());
        step.setTargetSubject(getTargetSubject());
        step.setJobType(getJobType());
        step.setParams(getParams());
        step.setParam(CLICKHOUSE_ADD_SECONDARY_INDEX, JsonUtil.writeValueAsStringQuietly(builder.getNewIndexes()));
        step.setParam(CLICKHOUSE_REMOVE_SECONDARY_INDEX,
                JsonUtil.writeValueAsStringQuietly(builder.getToBeDeleteIndexed()));
        step.setParam(CLICKHOUSE_LAYOUT_ID, String.valueOf(builder.getLayout().getId()));

        addTask(step);
    }

    @Override
    public Set<String> getSegmentIds() {
        return Collections.emptySet();
    }

    public static class RefreshSecondaryIndexJobFactory extends JobFactory {
        @Override
        protected AbstractExecutable create(JobBuildParams jobBuildParams) {
            SecondStorageRefreshSecondaryIndexJobParams params = (SecondStorageRefreshSecondaryIndexJobParams) jobBuildParams;
            val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), params.getProject());
            val param = ClickHouseRefreshSecondaryIndexParam.builder().modelId(params.getModelId())
                    .jobId(params.getJobId()).submitter(params.getSubmitter())
                    .df(dfManager.getDataflow(params.getModelId())).project(params.getProject())
                    .newIndexes(params.getNewIndexes()).toBeDeleteIndexed(params.getToBeDeleteIndexed())
                    .layout(Lists.newArrayList(jobBuildParams.getLayouts()).get(0)).build();
            return new ClickHouseRefreshSecondaryIndexJob(param);
        }
    }
}
