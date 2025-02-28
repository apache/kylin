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

import static org.apache.kylin.job.factory.JobFactoryConstant.LAYOUT_DATA_OPTIMIZE_JOB_FACTORY;
import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_DATAFLOW_ID;
import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_LAYOUT_IDS;
import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_PROJECT_NAME;

import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.DefaultExecutableOnModel;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.handler.LayoutDataOptimizeJobHandler;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NSparkLayoutDataOptimizeJob extends DefaultExecutableOnModel {

    static {
        JobFactory.register(LAYOUT_DATA_OPTIMIZE_JOB_FACTORY, new LayoutDataOptimizeJobFactory());
    }

    static class LayoutDataOptimizeJobFactory extends JobFactory {

        private LayoutDataOptimizeJobFactory() {
        }

        @Override
        protected NSparkLayoutDataOptimizeJob create(JobBuildParams jobBuildParams) {
            return NSparkLayoutDataOptimizeJob
                    .create((LayoutDataOptimizeJobHandler.LayoutDataOptimizeJobParam) jobBuildParams);
        }
    }

    public NSparkLayoutDataOptimizeJob() {
        super();
    }

    public NSparkLayoutDataOptimizeJob(Object notSetId) {
        super(notSetId);
    }

    @SneakyThrows
    public static NSparkLayoutDataOptimizeJob create(
            LayoutDataOptimizeJobHandler.LayoutDataOptimizeJobParam jobBuildParams) {
        NSparkLayoutDataOptimizeJob job = new NSparkLayoutDataOptimizeJob();
        String project = jobBuildParams.getProject();
        String modelId = jobBuildParams.getModelId();
        String jobId = jobBuildParams.getJobId();
        job.setProject(project);
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getDataflow(modelId);
        job.setId(jobBuildParams.getJobId());
        job.setName(JobTypeEnum.LAYOUT_DATA_OPTIMIZE.toString());
        job.setJobType(JobTypeEnum.LAYOUT_DATA_OPTIMIZE);
        job.setTargetSubject(modelId);
        job.setId(jobId);
        job.setParam(P_LAYOUT_IDS, NSparkCubingUtil.ids2Str(NSparkCubingUtil.toLayoutIds(jobBuildParams.getLayouts())));
        job.setParam(P_DATAFLOW_ID, modelId);
        job.setParam(P_PROJECT_NAME, project);
        job.setParam(NBatchConstants.P_JOB_ID, jobId);
        log.info("Create index data optimize job for {}.", modelId);
        StepEnum.RESOURCE_DETECT.create(job, dataflow.getConfig());
        StepEnum.LAYOUT_DATA_OPTIMIZE.create(job, dataflow.getConfig());
        return job;
    }

    @Override
    public boolean checkSuicide() {
        return null == NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .getDataflow(getParam(P_DATAFLOW_ID));
    }

    @Override
    public Set<String> getMetadataDumpList(KylinConfig config) {
        final String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        return NDataflowManager.getInstance(config, getProject()) //
                .getDataflow(dataflowId) //
                .collectPrecalculationResource();
    }

}
