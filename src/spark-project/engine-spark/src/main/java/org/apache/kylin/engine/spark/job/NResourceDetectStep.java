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

import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NResourceDetectStep extends NSparkExecutable {

    // called by reflection
    public NResourceDetectStep() {

    }

    public NResourceDetectStep(Object notSetId) {
        super(notSetId);
    }

    @Override
    protected void initHandler() {
        sparkJobHandler = new DefaultSparkBuildJobHandler();
    }

    public NResourceDetectStep(DefaultChainedExecutable parent) {
        if (parent instanceof NSparkCubingJob) {
            this.setSparkSubmitClassName(RDSegmentBuildJob.class.getName());
        } else if (parent instanceof NSparkMergingJob) {
            this.setSparkSubmitClassName(ResourceDetectBeforeMergingJob.class.getName());
        } else if (parent instanceof NTableSamplingJob) {
            this.setSparkSubmitClassName(ResourceDetectBeforeSampling.class.getName());
        } else {
            throw new IllegalArgumentException("Unsupported resource detect for " + parent.getName() + " job");
        }
        this.setName(ExecutableConstants.STEP_NAME_DETECT_RESOURCE);
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        final AbstractExecutable parent = getParent();
        if (parent instanceof DefaultChainedExecutable) {
            return ((DefaultChainedExecutable) parent).getMetadataDumpList(config);
        }
        throw new IllegalStateException("Unsupported resource detect for non chained executable!");
    }

    @Override
    protected Map<String, String> getSparkConfigOverride(KylinConfig config) {
        Map<String, String> sparkConfigOverride = super.getSparkConfigOverride(config);
        log.info("spark.master override " + sparkConfigOverride.get(SPARK_MASTER));
        if (!CLUSTER_MODE.equals(sparkConfigOverride.get(DEPLOY_MODE))) {
            sparkConfigOverride.put("spark.master", "local");
        }
        log.info("spark.master already " + sparkConfigOverride.get(SPARK_MASTER));
        sparkConfigOverride.put("spark.sql.autoBroadcastJoinThreshold", "-1");
        sparkConfigOverride.put("spark.sql.adaptive.enabled", "false");
        return sparkConfigOverride;
    }
}
