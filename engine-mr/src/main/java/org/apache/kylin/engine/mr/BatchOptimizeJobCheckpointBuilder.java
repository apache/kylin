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

package org.apache.kylin.engine.mr;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.mr.steps.UpdateCubeInfoAfterCheckpointStep;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.CheckpointExecutable;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;

import com.google.common.base.Preconditions;

public class BatchOptimizeJobCheckpointBuilder {

    protected SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss", Locale.ROOT);

    final protected CubeInstance cube;
    final protected String submitter;

    private final IMROutput2.IMRBatchOptimizeOutputSide2 outputSide;

    public BatchOptimizeJobCheckpointBuilder(CubeInstance cube, String submitter) {
        this.cube = cube;
        this.submitter = submitter;

        Preconditions.checkNotNull(cube.getFirstSegment(), "Cube " + cube + " is empty!!!");
        this.outputSide = MRUtil.getBatchOptimizeOutputSide2(cube.getFirstSegment());
    }

    public CheckpointExecutable build() {
        KylinConfig kylinConfig = cube.getConfig();
        List<ProjectInstance> projList = ProjectManager.getInstance(kylinConfig).findProjects(cube.getType(),
                cube.getName());
        if (projList == null || projList.size() == 0) {
            throw new RuntimeException("Cannot find the project containing the cube " + cube.getName() + "!!!");
        } else if (projList.size() >= 2) {
            throw new RuntimeException("Find more than one project containing the cube " + cube.getName()
                    + ". It does't meet the uniqueness requirement!!! ");
        }

        CheckpointExecutable checkpointJob = new CheckpointExecutable();
        checkpointJob.setSubmitter(submitter);
        CubingExecutableUtil.setCubeName(cube.getName(), checkpointJob.getParams());
        checkpointJob.setName(
                cube.getName() + " - OPTIMIZE CHECKPOINT - " + format.format(new Date(System.currentTimeMillis())));
        checkpointJob.setDeployEnvName(kylinConfig.getDeployEnv());
        checkpointJob.setProjectName(projList.get(0).getName());

        // Phase 1: Update cube information
        checkpointJob.addTask(createUpdateCubeInfoAfterCheckpointStep());

        // Phase 2: Garbage collection
        outputSide.addStepPhase5_Cleanup(checkpointJob);

        return checkpointJob;
    }

    private UpdateCubeInfoAfterCheckpointStep createUpdateCubeInfoAfterCheckpointStep() {
        UpdateCubeInfoAfterCheckpointStep result = new UpdateCubeInfoAfterCheckpointStep();
        result.setName(ExecutableConstants.STEP_NAME_UPDATE_CUBE_INFO);

        CubingExecutableUtil.setCubeName(cube.getName(), result.getParams());
        return result;
    }

}
