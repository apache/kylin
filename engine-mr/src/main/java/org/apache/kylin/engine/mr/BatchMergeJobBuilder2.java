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

import java.util.List;

import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.MergeDictionaryJob;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class BatchMergeJobBuilder2 extends JobBuilderSupport {
    private static final Logger logger = LoggerFactory.getLogger(BatchMergeJobBuilder2.class);

    private final IMROutput2.IMRBatchMergeOutputSide2 outputSide;
    private final IMRInput.IMRBatchMergeInputSide inputSide;

    public BatchMergeJobBuilder2(CubeSegment mergeSegment, String submitter) {
        super(mergeSegment, submitter);
        this.outputSide = MRUtil.getBatchMergeOutputSide2(seg);
        this.inputSide = MRUtil.getBatchMergeInputSide(seg);
    }

    public CubingJob build() {
        logger.info("MR_V2 new job to MERGE segment " + seg);

        final CubeSegment cubeSegment = seg;
        final CubingJob result = CubingJob.createMergeJob(cubeSegment, submitter, config);
        final String jobId = result.getId();

        final List<CubeSegment> mergingSegments = cubeSegment.getCubeInstance().getMergingSegments(cubeSegment);
        Preconditions.checkState(mergingSegments.size() > 1, "there should be more than 2 segments to merge, target segment " + cubeSegment);
        final List<String> mergingSegmentIds = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingSegmentIds.add(merging.getUuid());
        }

        // Phase 1: Merge Dictionary
        inputSide.addStepPhase1_MergeDictionary(result);
        result.addTask(createMergeDictionaryStep(cubeSegment, jobId, mergingSegmentIds));
        result.addTask(createUpdateDictionaryStep(cubeSegment, jobId, mergingSegmentIds));
        outputSide.addStepPhase1_MergeDictionary(result);

        // Phase 2: Merge Cube Files
        outputSide.addStepPhase2_BuildCube(seg, mergingSegments, result);

        // Phase 3: Update Metadata & Cleanup
        result.addTask(createUpdateCubeInfoAfterMergeStep(mergingSegmentIds, jobId));
        outputSide.addStepPhase3_Cleanup(result);

        return result;
    }

    public MapReduceExecutable createMergeDictionaryStep(CubeSegment seg, String jobID, List<String> mergingSegmentIds) {
        MapReduceExecutable mergeDictionaryStep = new MapReduceExecutable();
        mergeDictionaryStep.setName(ExecutableConstants.STEP_NAME_MERGE_DICTIONARY);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, JobEngineConfig.CUBE_MERGE_JOB_CONF_SUFFIX);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_META_URL, getSegmentMetadataUrl(seg.getConfig(), jobID));
        appendExecCmdParameters(cmd, MergeDictionaryJob.OPTION_MERGE_SEGMENT_IDS.getOpt(), StringUtil.join(mergingSegmentIds, ","));
        appendExecCmdParameters(cmd, MergeDictionaryJob.OPTION_OUTPUT_PATH_DICT.getOpt(), getDictInfoPath(jobID));
        appendExecCmdParameters(cmd, MergeDictionaryJob.OPTION_OUTPUT_PATH_STAT.getOpt(), getStatisticsPath(jobID));
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Merge_Dictionary_" + seg.getCubeInstance().getName() + "_Step");

        mergeDictionaryStep.setMapReduceParams(cmd.toString());
        mergeDictionaryStep.setMapReduceJobClass(MergeDictionaryJob.class);

        return mergeDictionaryStep;
    }

}
