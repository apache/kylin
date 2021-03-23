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

import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.DefaultChainedExecutable;

public class NResourceDetectStep extends NSparkLocalStep {

    // called by reflection
    public NResourceDetectStep() {

    }

    public NResourceDetectStep(DefaultChainedExecutable parent) {
        if (parent instanceof NSparkCubingJob) {
            this.setSparkSubmitClassName(ResourceDetectBeforeCubingJob.class.getName());
        } else if (parent instanceof NSparkMergingJob) {
            this.setSparkSubmitClassName(ResourceDetectBeforeMergingJob.class.getName());
        /*} else if (parent instanceof NTableSamplingJob) {
            this.setSparkSubmitClassName(ResourceDetectBeforeSampling.class.getName());
        */
        } else if (parent instanceof NSparkOptimizingJob) {
            this.setSparkSubmitClassName(ResourceDetectBeforeOptimizingJob.class.getName());
        } else {
            throw new IllegalArgumentException("Unsupported resource detect for " + parent.getName() + " job");
        }
        this.setName(ExecutableConstants.STEP_NAME_DETECT_RESOURCE);
    }
}
