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

import static org.apache.kylin.job.factory.JobFactoryConstant.LAYOUT_DATA_OPTIMIZE_JOB_FACTORY;

import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.model.JobParam;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LayoutDataOptimizeJobHandler extends AbstractJobHandler {

    @Getter
    public static class LayoutDataOptimizeJobParam extends JobFactory.JobBuildParams {
        private final JobParam optimizeJobParam;
        private final String project;
        private final String modelId;

        public LayoutDataOptimizeJobParam(JobParam jobParam) {
            super(null, jobParam.getProcessLayouts(), jobParam.getOwner(), jobParam.getJobTypeEnum(),
                    jobParam.getJobId(), jobParam.getDeleteLayouts(), jobParam.getIgnoredSnapshotTables(),
                    jobParam.getTargetPartitions(), jobParam.getTargetBuckets(), jobParam.getExtParams());
            this.modelId = jobParam.getModel();
            this.project = jobParam.getProject();
            this.optimizeJobParam = jobParam;
        }
    }

    @Override
    protected AbstractExecutable createJob(JobParam jobParam) {
        return JobFactory.createJob(LAYOUT_DATA_OPTIMIZE_JOB_FACTORY, new LayoutDataOptimizeJobParam(jobParam));
    }
}
