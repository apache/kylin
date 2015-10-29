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

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.IMRInput.IMRBatchCubingInputSide;
import org.apache.kylin.engine.mr.IMROutput2.IMRBatchCubingOutputSide2;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.InMemCuboidJob;
import org.apache.kylin.engine.mr.steps.SaveStatisticsStep;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchCubingJobBuilder2 extends JobBuilderSupport {
    private static final Logger logger = LoggerFactory.getLogger(BatchCubingJobBuilder2.class);

    private final IMRBatchCubingInputSide inputSide;
    private final IMRBatchCubingOutputSide2 outputSide;

    public BatchCubingJobBuilder2(CubeSegment newSegment, String submitter) {
        super(newSegment, submitter);
        this.inputSide = MRUtil.getBatchCubingInputSide(seg);
        this.outputSide = MRUtil.getBatchCubingOutputSide2((CubeSegment)seg);
    }

    public CubingJob build() {
        logger.info("MR_V2 new job to BUILD segment " + seg);
        
        final CubingJob result = CubingJob.createBuildJob((CubeSegment)seg, submitter, config);
        final String jobId = result.getId();

        // Phase 1: Create Flat Table
        inputSide.addStepPhase1_CreateFlatTable(result);

        // Phase 2: Build Dictionary
        result.addTask(createFactDistinctColumnsStepWithStats(jobId));
        result.addTask(createBuildDictionaryStep(jobId));
        result.addTask(createSaveStatisticsStep(jobId));
        outputSide.addStepPhase2_BuildDictionary(result);

        // Phase 3: Build Cube
        result.addTask(createInMemCubingStep(jobId));
        outputSide.addStepPhase3_BuildCube(result);

        // Phase 4: Update Metadata & Cleanup
        result.addTask(createUpdateCubeInfoAfterBuildStep(jobId));
        inputSide.addStepPhase4_Cleanup(result);
        outputSide.addStepPhase4_Cleanup(result);

        return result;
    }

    private SaveStatisticsStep createSaveStatisticsStep(String jobId) {
        SaveStatisticsStep result = new SaveStatisticsStep();
        result.setName(ExecutableConstants.STEP_NAME_SAVE_STATISTICS);
        result.setCubeName(seg.getRealization().getName());
        result.setSegmentId(seg.getUuid());
        result.setStatisticsPath(getStatisticsPath(jobId));
        return result;
    }

    private MapReduceExecutable createInMemCubingStep(String jobId) {
        // base cuboid job
        MapReduceExecutable cubeStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, ((CubeSegment)seg).getCubeDesc().getModel());

        cubeStep.setName(ExecutableConstants.STEP_NAME_BUILD_IN_MEM_CUBE);

        appendExecCmdParameters(cmd, "cubename", seg.getRealization().getName());
        appendExecCmdParameters(cmd, "segmentname", seg.getName());
        appendExecCmdParameters(cmd, "jobname", "Kylin_Cube_Builder_" + seg.getRealization().getName());
        appendExecCmdParameters(cmd, "jobflowid", jobId);

        cubeStep.setMapReduceParams(cmd.toString());
        cubeStep.setMapReduceJobClass(InMemCuboidJob.class);
        cubeStep.setCounterSaveAs(CubingJob.SOURCE_RECORD_COUNT + "," + CubingJob.SOURCE_SIZE_BYTES + "," + CubingJob.CUBE_SIZE_BYTES);
        return cubeStep;
    }

}
