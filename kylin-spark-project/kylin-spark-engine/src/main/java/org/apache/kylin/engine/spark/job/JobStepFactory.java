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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.DefaultChainedExecutable;

public class JobStepFactory {

    private JobStepFactory() {
    }

    public static NSparkExecutable addStep(DefaultChainedExecutable parent, JobStepType type,
            CubeInstance cube) {
        NSparkExecutable step;
        KylinConfig config = cube.getConfig();
        switch (type) {
        case RESOURCE_DETECT:
            step = new NResourceDetectStep(parent);
            break;
        case CUBING:
            step = new NSparkCubingStep(config.getSparkBuildClassName());
            break;
        case MERGING:
            step = new NSparkMergingStep(config.getSparkMergeClassName());
            break;
        case OPTIMIZING:
            step = new NSparkOptimizingStep(OptimizeBuildJob.class.getName());
            break;
        case MERGE_STATISTICS:
            step = new NSparkMergeStatisticsStep();
            break;
        case CLEAN_UP_AFTER_MERGE:
            step = new NSparkUpdateMetaAndCleanupAfterMergeStep();
            break;
        case FILTER_RECOMMEND_CUBOID:
            step = new NSparkLocalStep();
            step.setSparkSubmitClassName(FilterRecommendCuboidJob.class.getName());
            step.setName(ExecutableConstants.STEP_NAME_FILTER_RECOMMEND_CUBOID_DATA_FOR_OPTIMIZATION);
            break;
        default:
            throw new IllegalArgumentException();
        }
        step.setParams(parent.getParams());
        step.setProject(parent.getProject());
        step.setTargetSubject(parent.getTargetSubject());
        parent.addTask(step);
        //after addTask, step's id is changed
        step.setDistMetaUrl(config.getJobTmpMetaStoreUrl(parent.getProject(), step.getId()));
        return step;
    }
}
