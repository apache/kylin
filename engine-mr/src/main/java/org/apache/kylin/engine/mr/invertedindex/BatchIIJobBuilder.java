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

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.IMRInput.IMRBatchCubingInputSide;
import org.apache.kylin.engine.mr.IMROutput;
import org.apache.kylin.engine.mr.IMROutput.IMRBatchCubingOutputSide;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.HadoopShellExecutable;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.BaseCuboidJob;
import org.apache.kylin.engine.mr.steps.NDCuboidJob;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.model.IIJoinedFlatTableDesc;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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

        final IIJob result = IIJob.createBuildJob((IISegment)seg, submitter, config);
        final String jobId = result.getId();
        
        final String iiRootPath = getRealizationRootPath(jobId) + "/";
        // Phase 1: Create Flat Table
        inputSide.addStepPhase1_CreateFlatTable(result);

        final String intermediateTableIdentity = seg.getJoinedFlatTableDesc().getTableName();
        // Phase 2: Build Dictionary
        result.addTask(createIIFactDistinctColumnsStep(seg, intermediateTableIdentity, getFactDistinctColumnsPath(jobId)));
        result.addTask(createIIBuildDictionaryStep(seg, getFactDistinctColumnsPath(jobId)));

        // Phase 3: Build Cube
        result.addTask(createInvertedIndexStep((IISegment)seg, intermediateTableIdentity, iiRootPath));
        outputSide.addStepPhase3_BuildII(result, iiRootPath);

        // Phase 4: Update Metadata & Cleanup
        result.addTask(createUpdateInvertedIndexInfoAfterBuildStep(jobId));
        inputSide.addStepPhase4_Cleanup(result);
        outputSide.addStepPhase4_Cleanup(result);

        return result;
    }

    private MapReduceExecutable createIIFactDistinctColumnsStep(IRealizationSegment seg, String factTableName, String output) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName(ExecutableConstants.STEP_NAME_FACT_DISTINCT_COLUMNS);
        result.setMapReduceJobClass(IIDistinctColumnsJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, seg.getRealization().getDataModelDesc());
        appendExecCmdParameters(cmd, "tablename", factTableName);
        appendExecCmdParameters(cmd, "iiname", seg.getRealization().getName());
        appendExecCmdParameters(cmd, "output", output);
        appendExecCmdParameters(cmd, "jobname", "Kylin_Fact_Distinct_Columns_" + seg.getRealization().getName() + "_Step");

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    private HadoopShellExecutable createIIBuildDictionaryStep(IRealizationSegment seg, String factDistinctColumnsPath) {
        // base cuboid job
        HadoopShellExecutable buildDictionaryStep = new HadoopShellExecutable();
        buildDictionaryStep.setName(ExecutableConstants.STEP_NAME_BUILD_DICTIONARY);
        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "iiname", seg.getRealization().getName());
        appendExecCmdParameters(cmd, "input", factDistinctColumnsPath);

        buildDictionaryStep.setJobParams(cmd.toString());
        buildDictionaryStep.setJobClass(CreateInvertedIndexDictionaryJob.class);
        return buildDictionaryStep;
    }

    private MapReduceExecutable createInvertedIndexStep(IISegment seg, String intermediateHiveTable, String iiOutputTempPath) {
        // base cuboid job
        MapReduceExecutable buildIIStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, seg.getRealization().getDataModelDesc());

        buildIIStep.setName(ExecutableConstants.STEP_NAME_BUILD_II);

        appendExecCmdParameters(cmd, "iiname", seg.getRealization().getName());
        appendExecCmdParameters(cmd, "tablename", intermediateHiveTable);
        appendExecCmdParameters(cmd, "output", iiOutputTempPath);
        appendExecCmdParameters(cmd, "jobname", ExecutableConstants.STEP_NAME_BUILD_II);

        buildIIStep.setMapReduceParams(cmd.toString());
        buildIIStep.setMapReduceJobClass(InvertedIndexJob.class);
        return buildIIStep;
    }
    
}
