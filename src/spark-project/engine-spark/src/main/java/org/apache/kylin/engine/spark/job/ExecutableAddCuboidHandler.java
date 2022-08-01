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
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.DefaultChainedExecutableOnModel;
import org.apache.kylin.job.execution.ExecutableHandler;
import org.apache.kylin.engine.spark.merger.AfterBuildResourceMerger;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import lombok.val;

public class ExecutableAddCuboidHandler extends ExecutableHandler {
    private static final Logger logger = LoggerFactory.getLogger(ExecutableAddCuboidHandler.class);

    public ExecutableAddCuboidHandler(DefaultChainedExecutableOnModel job) {
        this(job.getProject(), job.getTargetSubject(), job.getSubmitter(), null, job.getId());
    }

    public ExecutableAddCuboidHandler(String project, String modelId, String owner, String segmentId, String jobId) {
        super(project, modelId, owner, segmentId, jobId);
    }

    @Override
    public void handleFinished() {
        val project = getProject();
        val jobId = getJobId();
        val modelId = getModelId();
        val executable = getExecutable();
        Preconditions.checkState(executable.getTasks().size() > 1, "job " + jobId + " steps is not enough");
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val merger = new AfterBuildResourceMerger(kylinConfig, project);
        executable.getTasks().stream() //
                .filter(task -> task instanceof NSparkExecutable) //
                .filter(task -> ((NSparkExecutable) task).needMergeMetadata())
                .forEach(task -> ((NSparkExecutable) task).mergerMetadata(merger));

        Optional.ofNullable(executable.getParams()).ifPresent(params -> {
            String toBeDeletedLayoutIdsStr = params.get(NBatchConstants.P_TO_BE_DELETED_LAYOUT_IDS);
            if (StringUtils.isNotBlank(toBeDeletedLayoutIdsStr)) {
                logger.info("Try to delete the toBeDeletedLayoutIdsStr: {}, jobId: {}", toBeDeletedLayoutIdsStr, jobId);
                Set<Long> toBeDeletedLayoutIds = new LinkedHashSet<>();
                for (String id : toBeDeletedLayoutIdsStr.split(",")) {
                    toBeDeletedLayoutIds.add(Long.parseLong(id));
                }

                NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateIndexPlan(modelId,
                        copyForWrite -> copyForWrite.removeLayouts(toBeDeletedLayoutIds, true, true));
            }
        });
        markDFStatus();
    }

    @Override
    public void handleDiscardOrSuicidal() {
        val job = getExecutable();
        // anyTargetSegmentExists && checkCuttingInJobByModel need restart job
        if (!(job.checkCuttingInJobByModel() && job.checkAnyTargetSegmentAndPartitionExists())) {
            return;
        }
    }

}
