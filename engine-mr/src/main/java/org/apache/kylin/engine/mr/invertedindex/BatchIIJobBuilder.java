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

package org.apache.kylin.engine.mr.invertedindex;

import org.apache.kylin.engine.mr.IMRInput.IMRBatchCubingInputSide;
import org.apache.kylin.engine.mr.IMROutput;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchIIJobBuilder extends JobBuilderSupport {

    private static final Logger logger = LoggerFactory.getLogger(BatchIIJobBuilder.class);

    private final IMRBatchCubingInputSide inputSide;
    private final IMROutput.IMRBatchInvertedIndexingOutputSide outputSide;

    public BatchIIJobBuilder(IISegment newSegment, String submitter) {
        super(newSegment, submitter);
        this.inputSide = MRUtil.getBatchCubingInputSide(newSegment);
        this.outputSide = MRUtil.getBatchInvertedIndexingOutputSide(newSegment);
    }

    public IIJob build() {
        logger.info("MR new job to BUILD segment " + seg);

        final IIJob result = IIJob.createBuildJob((IISegment) seg, submitter, config);
        final String jobId = result.getId();

        final String iiRootPath = getRealizationRootPath(jobId) + "/";
        // Phase 1: Create Flat Table
        inputSide.addStepPhase1_CreateFlatTable(result);

        // Phase 2: Build Inverted Index
        result.addTask(createInvertedIndexStep((IISegment) seg, iiRootPath));
        outputSide.addStepPhase3_BuildII(result, iiRootPath);

        // Phase 3: Update Metadata & Cleanup
        result.addTask(createUpdateIIInfoAfterBuildStep(jobId));
        inputSide.addStepPhase4_Cleanup(result);
        outputSide.addStepPhase4_Cleanup(result);

        return result;
    }

    private MapReduceExecutable createInvertedIndexStep(IISegment seg, String iiOutputTempPath) {
        MapReduceExecutable buildIIStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd);

        buildIIStep.setName(ExecutableConstants.STEP_NAME_BUILD_II);

        appendExecCmdParameters(cmd, BatchConstants.ARG_II_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, iiOutputTempPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, ExecutableConstants.STEP_NAME_BUILD_II);

        buildIIStep.setMapReduceParams(cmd.toString());
        buildIIStep.setMapReduceJobClass(InvertedIndexJob.class);
        return buildIIStep;
    }

}
