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

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.kylin.job.factory.JobFactoryConstant;
import org.apache.kylin.job.handler.IndexPlanOptimizeJobHandler;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.sparkproject.guava.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NSparkIndexPlanOptimizeJob extends DefaultExecutable {

    static {
        JobFactory.register(JobFactoryConstant.INDEX_PLAN_OPTIMIZE_JOB_FACTORY,
                new NSparkIndexPlanOptimizeJob.IndexPlanOptimizeJobFactory());
    }

    public NSparkIndexPlanOptimizeJob() {
        super();
    }

    public NSparkIndexPlanOptimizeJob(Object notSetId) {
        super(notSetId);
    }

    public static NSparkIndexPlanOptimizeJob create(
            IndexPlanOptimizeJobHandler.IndexPlanOptimizeJobParam indexOptJobParam) {
        return internalCreate(indexOptJobParam);
    }

    public static NSparkIndexPlanOptimizeJob internalCreate(
            IndexPlanOptimizeJobHandler.IndexPlanOptimizeJobParam indexOptJobParam) {
        String project = indexOptJobParam.getProject();
        String dataflowId = indexOptJobParam.getDataflowId();
        NDataflowManager dataflowMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataflow dataflow = dataflowMgr.getDataflow(dataflowId);

        Preconditions.checkArgument(indexOptJobParam.getSubmitter() != null);
        NSparkIndexPlanOptimizeJob job = new NSparkIndexPlanOptimizeJob();
        job.setId(indexOptJobParam.getJobId());
        job.setProject(project);
        job.setName(indexOptJobParam.getJobType().toString());
        job.setJobType(indexOptJobParam.getJobType());
        job.setSubmitter(indexOptJobParam.getSubmitter());
        job.setTargetSubject(dataflow.getModel().getUuid());

        // need P_DATAFLOW_ID, P_DIST_META_URL,P_TABLE_NAME, P_TOKEN
        job.setParam(NBatchConstants.P_JOB_ID, indexOptJobParam.getJobId());
        job.setParam(NBatchConstants.P_PROJECT_NAME, project);
        job.setParam(NBatchConstants.P_DATAFLOW_ID, dataflowId);
        job.setParam(NBatchConstants.P_PLANNER_INITIALIZE_CUBOID_COUNT,
                String.valueOf(indexOptJobParam.getInitializeCuboidCount()));
        job.setParam(NBatchConstants.P_PLANNER_MAX_CUBOID_COUNT, String.valueOf(indexOptJobParam.getMaxCuboidCount()));
        job.setParam(NBatchConstants.P_PLANNER_MAX_CUBOID_CHANGE_COUNT,
                String.valueOf(indexOptJobParam.getMaxCuboidChangeCount()));
        job.setParam(NBatchConstants.P_DATA_RANGE_START, String.valueOf(indexOptJobParam.getDataRangeStart()));
        job.setParam(NBatchConstants.P_DATA_RANGE_END, String.valueOf(indexOptJobParam.getDataRangeEnd()));
        job.setParam(NBatchConstants.P_PLANNER_AUTO_APPROVE_ENABLED,
                String.valueOf(indexOptJobParam.isAutoApproveEnabled()));
        job.setParam(NBatchConstants.P_PLANNER_OPERATION_TOKEN, String.valueOf(indexOptJobParam.getOperationToken()));

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        StepEnum.OPTIMIZE_INDEX_PLAN.create(job, config);
        return job;
    }

    public static class IndexPlanOptimizeStep extends NSparkExecutable {

        // called by reflection
        public IndexPlanOptimizeStep() {
        }

        public IndexPlanOptimizeStep(Object notSetId) {
            super(notSetId);
        }

        // Ensure metadata compatibility
        public IndexPlanOptimizeStep(String sparkSubmitClassName) {
            this.setSparkSubmitClassName(sparkSubmitClassName);
            this.setName(ExecutableConstants.STEP_NAME_INDEX_PLAN_OPT);
        }

        @Override
        protected ExecuteResult doWork(JobContext context) throws ExecuteException {
            ExecuteResult result = super.doWork(context);
            if (!result.succeed()) {
                return result;
            }
            if (checkSuicide()) {
                log.info("This Index Plan Optimize job seems meaningless now, skipped");
                return null;
            }
            return result;
        }

        @Override
        protected Set<String> getMetadataDumpList(KylinConfig config) {
            Set<String> dumpList = new LinkedHashSet<>();
            NDataflow df = NDataflowManager.getInstance(config, getProject()).getDataflow(getDataflowId());
            dumpList.addAll(df.collectPrecalculationResource());
            dumpList.addAll(getLogicalViewMetaDumpList(config));
            return dumpList;
        }
    }

    public static class IndexPlanOptimizeJobFactory extends JobFactory {

        protected IndexPlanOptimizeJobFactory() {
        }

        @Override
        protected NSparkIndexPlanOptimizeJob create(JobBuildParams indexOptJobParam) {
            return NSparkIndexPlanOptimizeJob
                    .create((IndexPlanOptimizeJobHandler.IndexPlanOptimizeJobParam) indexOptJobParam);
        }
    }
}
