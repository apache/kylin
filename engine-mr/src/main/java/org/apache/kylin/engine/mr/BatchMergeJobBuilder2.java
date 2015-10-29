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
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.MergeCuboidFromStorageJob;
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
        this.outputSide = MRUtil.getBatchMergeOutputSide2((CubeSegment)seg);
    }

    public CubingJob build() {
        logger.info("MR_V2 new job to MERGE segment " + seg);
        
        final CubeSegment cubeSegment = (CubeSegment)seg;
        final CubingJob result = CubingJob.createMergeJob(cubeSegment, submitter, config);
        final String jobId = result.getId();

        final List<CubeSegment> mergingSegments = cubeSegment.getCubeInstance().getMergingSegments(cubeSegment);
        Preconditions.checkState(mergingSegments.size() > 1, "there should be more than 2 segments to merge");
        final List<String> mergingSegmentIds = Lists.newArrayList();
        final List<String> mergingHTables = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingSegmentIds.add(merging.getUuid());
            mergingHTables.add(merging.getStorageLocationIdentifier());
        }

        // Phase 1: Merge Dictionary
        result.addTask(createMergeDictionaryStep(mergingSegmentIds));
        result.addTask(createMergeStatisticsStep(cubeSegment, mergingSegmentIds, getStatisticsPath(jobId)));
        outputSide.addStepPhase1_MergeDictionary(result);

        // Phase 2: Merge Cube
        String formattedTables = StringUtil.join(mergingHTables, ",");
        result.addTask(createMergeCuboidDataFromStorageStep(formattedTables, jobId));
        outputSide.addStepPhase2_BuildCube(result);

        // Phase 3: Update Metadata & Cleanup
        result.addTask(createUpdateCubeInfoAfterMergeStep(mergingSegmentIds, jobId));
        outputSide.addStepPhase3_Cleanup(result);

        return result;
    }

    private MergeStatisticsStep createMergeStatisticsStep(CubeSegment seg, List<String> mergingSegmentIds, String mergedStatisticsFolder) {
        MergeStatisticsStep result = new MergeStatisticsStep();
        result.setName(ExecutableConstants.STEP_NAME_MERGE_STATISTICS);
        result.setCubeName(seg.getCubeInstance().getName());
        result.setSegmentId(seg.getUuid());
        result.setMergingSegmentIds(mergingSegmentIds);
        result.setMergedStatisticsPath(mergedStatisticsFolder);
        return result;
    }

    private MapReduceExecutable createMergeCuboidDataFromStorageStep(String inputTableNames, String jobId) {
        MapReduceExecutable mergeCuboidDataStep = new MapReduceExecutable();
        mergeCuboidDataStep.setName(ExecutableConstants.STEP_NAME_MERGE_CUBOID);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd, ((CubeSegment)seg).getCubeDesc().getModel());
        appendExecCmdParameters(cmd, "cubename", seg.getRealization().getName());
        appendExecCmdParameters(cmd, "segmentname", seg.getName());
        appendExecCmdParameters(cmd, "jobname", "Kylin_Merge_Cuboid_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, "jobflowid", jobId);

        mergeCuboidDataStep.setMapReduceParams(cmd.toString());
        mergeCuboidDataStep.setMapReduceJobClass(MergeCuboidFromStorageJob.class);
        mergeCuboidDataStep.setCounterSaveAs(",," + CubingJob.CUBE_SIZE_BYTES);
        return mergeCuboidDataStep;
    }

}
