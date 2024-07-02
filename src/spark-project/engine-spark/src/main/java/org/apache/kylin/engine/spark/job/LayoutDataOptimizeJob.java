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

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.engine.spark.job.exec.OptimizeExec;
import org.apache.kylin.engine.spark.job.stage.optimize.LayoutDataCompactionOptimize;
import org.apache.kylin.engine.spark.job.stage.optimize.LayoutDataRepartitionOptimize;
import org.apache.kylin.engine.spark.job.stage.optimize.LayoutDataVacuumOptimize;
import org.apache.kylin.engine.spark.job.stage.optimize.LayoutDataZorderOptimize;

import lombok.val;

public class LayoutDataOptimizeJob extends AbstractLayoutDataOptimizeJob {

    public static void main(String[] args) {
        LayoutDataOptimizeJob layoutDataOptimizeJob = new LayoutDataOptimizeJob();
        layoutDataOptimizeJob.execute(args);
    }

    @Override
    protected void doExecute() throws Exception {
        val jobStepId = StringUtils.replace(infos.getJobStepId(), JOB_NAME_PREFIX, "");
        val step = new OptimizeExec(jobStepId);

        step.addStage(new LayoutDataVacuumOptimize(this));
        step.addStage(new LayoutDataRepartitionOptimize(this));
        step.addStage(new LayoutDataZorderOptimize(this));
        step.addStage(new LayoutDataCompactionOptimize(this));

        step.optimize();
    }
}
