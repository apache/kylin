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

package org.apache.kylin.job.execution;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class CheckpointExecutable extends DefaultChainedExecutable {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointExecutable.class);

    public static final Integer DEFAULT_PRIORITY = 30;

    private static final String DEPLOY_ENV_NAME = "envName";

    private final List<AbstractExecutable> subTasksForCheck = Lists.newArrayList();

    public void addTaskForCheck(AbstractExecutable executable) {
        this.subTasksForCheck.add(executable);
    }

    public void addTaskListForCheck(List<AbstractExecutable> executableList) {
        this.subTasksForCheck.addAll(executableList);
    }

    public List<AbstractExecutable> getSubTasksForCheck() {
        return subTasksForCheck;
    }

    @Override
    public boolean isReady() {
        if (!super.isReady()) {
            return false;
        }
        for (Executable task : subTasksForCheck) {
            final Output output = getManager().getOutput(task.getId());
            if (output.getState() != ExecutableState.SUCCEED) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected void onExecuteFinished(ExecuteResult result, ExecutableContext executableContext) {
        super.onExecuteFinished(result, executableContext);
        if (!isDiscarded() && result.succeed()) {
            List<? extends Executable> jobs = getTasks();
            boolean allSucceed = true;
            for (Executable task : jobs) {
                final ExecutableState status = task.getStatus();
                if (status != ExecutableState.SUCCEED) {
                    allSucceed = false;
                }
            }
            if (allSucceed) {
                // Add last optimization time
                CubeManager cubeManager = CubeManager.getInstance(executableContext.getConfig());
                CubeInstance cube = cubeManager.getCube(getCubeName());
                CubeInstance copyForWrite = cube.latestCopyForWrite();
                try {
                    copyForWrite.setCuboidLastOptimized(getEndTime());
                    CubeUpdate cubeUpdate = new CubeUpdate(copyForWrite);
                    cubeManager.updateCube(cubeUpdate);
                } catch (IOException e) {
                    logger.error("Failed to update last optimized for " + getCubeName(), e);
                }
            }
        }
    }

    public String getDeployEnvName() {
        return getParam(DEPLOY_ENV_NAME);
    }

    public void setDeployEnvName(String name) {
        setParam(DEPLOY_ENV_NAME, name);
    }

    @Override
    public int getDefaultPriority() {
        return DEFAULT_PRIORITY;
    }

}
