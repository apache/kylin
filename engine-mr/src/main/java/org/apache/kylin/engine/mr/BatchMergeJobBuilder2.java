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
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.mr.steps.MergeCuboidJob;
import org.apache.kylin.engine.mr.steps.MergeStatisticsStep;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class BatchMergeJobBuilder2 extends JobBuilderSupport {
    private static final Logger logger = LoggerFactory.getLogger(BatchMergeJobBuilder2.class);

    private final IMROutput2.IMRBatchMergeOutputSide2 outputSide;

    public BatchMergeJobBuilder2(CubeSegment mergeSegment, String submitter) {
        super(mergeSegment, submitter);
        this.outputSide = MRUtil.getBatchMergeOutputSide2((CubeSegment) seg);
    }

    public CubingJob build() {
        logger.info("MR_V2 new job to MERGE segment " + seg);

        final CubeSegment cubeSegment = (CubeSegment) seg;
        final CubingJob result = CubingJob.createMergeJob(cubeSegment, submitter, config);
        final String jobId = result.getId();
        final String cuboidRootPath = getCuboidRootPath(jobId);

        final List<CubeSegment> mergingSegments = cubeSegment.getCubeInstance().getMergingSegments(cubeSegment);
        Preconditions.checkState(mergingSegments.size() > 1, "there should be more than 2 segments to merge");
        final List<String> mergingSegmentIds = Lists.newArrayList();
        final List<String> mergingCuboidPaths = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingSegmentIds.add(merging.getUuid());
            mergingCuboidPaths.add(getCuboidRootPath(merging) + "*");
        }

        // Phase 1: Merge Dictionary
        result.addTask(createMergeDictionaryStep(mergingSegmentIds));
        result.addTask(createMergeStatisticsStep(cubeSegment, mergingSegmentIds, getStatisticsPath(jobId)));
        outputSide.addStepPhase1_MergeDictionary(result);

        // Phase 2: Merge Cube Files
        String formattedPath = StringUtil.join(mergingCuboidPaths, ",");
        result.addTask(createMergeCuboidDataStep(cubeSegment, formattedPath, cuboidRootPath));
        outputSide.addStepPhase2_BuildCube(result, cuboidRootPath);

        // Phase 3: Update Metadata & Cleanup
        result.addTask(createUpdateCubeInfoAfterMergeStep(mergingSegmentIds, jobId));
        outputSide.addStepPhase3_Cleanup(result);

        return result;
    }

    private MergeStatisticsStep createMergeStatisticsStep(CubeSegment seg, List<String> mergingSegmentIds, String mergedStatisticsFolder) {
        MergeStatisticsStep result = new MergeStatisticsStep();
        result.setName(ExecutableConstants.STEP_NAME_MERGE_STATISTICS);

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setMergingSegmentIds(mergingSegmentIds, result.getParams());
        CubingExecutableUtil.setMergedStatisticsPath(mergedStatisticsFolder, result.getParams());

        return result;
    }

    private MapReduceExecutable createMergeCuboidDataStep(CubeSegment seg, String inputPath, String outputPath) {
        MapReduceExecutable mergeCuboidDataStep = new MapReduceExecutable();
        mergeCuboidDataStep.setName(ExecutableConstants.STEP_NAME_MERGE_CUBOID);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd, seg.getRealization().getDataModelDesc());
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_NAME, seg.getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, inputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, outputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Merge_Cuboid_" + seg.getCubeInstance().getName() + "_Step");

        mergeCuboidDataStep.setMapReduceParams(cmd.toString());
        mergeCuboidDataStep.setMapReduceJobClass(MergeCuboidJob.class);
        return mergeCuboidDataStep;
    }

}
