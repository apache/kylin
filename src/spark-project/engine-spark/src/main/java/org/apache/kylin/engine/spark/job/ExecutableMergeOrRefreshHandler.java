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
import org.apache.kylin.job.execution.ExecutableHandler;
import org.apache.kylin.engine.spark.merger.AfterMergeOrRefreshResourceMerger;

import lombok.val;

public class ExecutableMergeOrRefreshHandler extends ExecutableHandler {

    public ExecutableMergeOrRefreshHandler(String project, String modelId, String owner, String segmentId,
            String jobId) {
        super(project, modelId, owner, segmentId, jobId);
    }

    @Override
    public void handleFinished() {
        String project = getProject();
        val executable = getExecutable();
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val merger = new AfterMergeOrRefreshResourceMerger(kylinConfig, project);
        executable.getTasks().stream().filter(task -> task instanceof NSparkExecutable)
                .filter(task -> ((NSparkExecutable) task).needMergeMetadata())
                .forEach(task -> ((NSparkExecutable) task).mergerMetadata(merger));
        markDFStatus();
    }

    @Override
    public void handleDiscardOrSuicidal() {
        // Do nothing
    }
}
