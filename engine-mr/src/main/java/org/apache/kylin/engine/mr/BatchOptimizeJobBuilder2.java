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
import org.apache.kylin.cube.cuboid.CuboidModeEnum;
import org.apache.kylin.cube.cuboid.CuboidUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.CopyDictionaryStep;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.mr.steps.FilterRecommendCuboidDataJob;
import org.apache.kylin.engine.mr.steps.InMemCuboidFromBaseCuboidJob;
import org.apache.kylin.engine.mr.steps.MergeStatisticsWithOldStep;
import org.apache.kylin.engine.mr.steps.NDCuboidJob;
import org.apache.kylin.engine.mr.steps.UpdateCubeInfoAfterOptimizeStep;
import org.apache.kylin.engine.mr.steps.UpdateOldCuboidShardJob;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;

public class BatchOptimizeJobBuilder2 extends JobBuilderSupport {
    private static final Logger logger = LoggerFactory.getLogger(BatchOptimizeJobBuilder2.class);

    private final IMROutput2.IMRBatchOptimizeOutputSide2 outputSide;

    public BatchOptimizeJobBuilder2(CubeSegment optimizeSegment, String submitter) {
        super(optimizeSegment, submitter);
        this.outputSide = MRUtil.getBatchOptimizeOutputSide2(optimizeSegment);
    }

    public CubingJob build() {
        logger.info("MR_V2 new job to OPTIMIZE a segment " + seg);

        final CubingJob result = CubingJob.createOptimizeJob(seg, submitter, config);
        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());

        final String jobId = result.getId();
        final String cuboidRootPath = getCuboidRootPath(jobId);
        final String optimizeCuboidRootPath = getOptimizationCuboidPath(jobId);

        CubeSegment oldSegment = seg.getCubeInstance().getOriginalSegmentToOptimize(seg);
        Preconditions.checkNotNull(oldSegment, "cannot find the original segment to be optimized by " + seg);

        // Phase 1: Prepare base cuboid data from old segment
        String oldcuboidRootPath = getCuboidRootPath(oldSegment) + "*";
        result.addTask(createFilterRecommendCuboidDataStep(oldcuboidRootPath, optimizeCuboidRootPath));

        // Phase 2: Prepare dictionary and statistics for new segment
        result.addTask(createCopyDictionaryStep());
        String optStatsSourcePath = getBaseCuboidPath(optimizeCuboidRootPath);
        String optStatsDstPath = getOptimizationStatisticsPath(jobId);
        result.addTask(createCalculateStatsFromBaseCuboid(optStatsSourcePath, optStatsDstPath,
                CuboidModeEnum.RECOMMEND_MISSING));
        result.addTask(createMergeStatisticsWithOldStep(jobId, optStatsDstPath, getStatisticsPath(jobId)));
        outputSide.addStepPhase2_CreateHTable(result);

        result.addTask(createUpdateShardForOldCuboidDataStep(optimizeCuboidRootPath + "*", cuboidRootPath));

        // Phase 3: Build Cube for Missing Cuboid Data
        addLayerCubingSteps(result, jobId, CuboidModeEnum.RECOMMEND_MISSING_WITH_BASE, cuboidRootPath); // layer cubing
        result.addTask(createInMemCubingStep(jobId, CuboidModeEnum.RECOMMEND_MISSING_WITH_BASE, cuboidRootPath));

        outputSide.addStepPhase3_BuildCube(result);

        // Phase 4: Update Metadata & Cleanup
        result.addTask(createUpdateCubeInfoAfterOptimizeStep(jobId));
        outputSide.addStepPhase4_Cleanup(result);

        return result;
    }

    public MapReduceExecutable createFilterRecommendCuboidDataStep(String inputPath, String outputPath) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName(ExecutableConstants.STEP_NAME_FILTER_RECOMMEND_CUBOID_DATA_FOR_OPTIMIZATION);

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, inputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, outputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME,
                "Kylin_Filter_Recommend_Cuboid_Data_" + seg.getRealization().getName());

        result.setMapReduceParams(cmd.toString());
        result.setMapReduceJobClass(FilterRecommendCuboidDataJob.class);
        return result;
    }

    public CopyDictionaryStep createCopyDictionaryStep() {
        CopyDictionaryStep result = new CopyDictionaryStep();
        result.setName(ExecutableConstants.STEP_NAME_COPY_DICTIONARY);

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        return result;
    }

    private MapReduceExecutable createUpdateShardForOldCuboidDataStep(String inputPath, String outputPath) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName(ExecutableConstants.STEP_NAME_UPDATE_OLD_CUBOID_SHARD);

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, inputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, outputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME,
                "Kylin_Update_Old_Cuboid_Shard_" + seg.getRealization().getName());

        result.setMapReduceParams(cmd.toString());
        result.setMapReduceJobClass(UpdateOldCuboidShardJob.class);
        return result;
    }

    private MergeStatisticsWithOldStep createMergeStatisticsWithOldStep(final String jobId, final String optStatsPath,
            final String mergedStatisticsFolder) {
        MergeStatisticsWithOldStep result = new MergeStatisticsWithOldStep();
        result.setName(ExecutableConstants.STEP_NAME_MERGE_STATISTICS_WITH_OLD);

        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());
        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setStatisticsPath(optStatsPath, result.getParams());
        CubingExecutableUtil.setMergedStatisticsPath(mergedStatisticsFolder, result.getParams());

        return result;
    }

    private void addLayerCubingSteps(final CubingJob result, final String jobId, final CuboidModeEnum cuboidMode,
            final String cuboidRootPath) {
        // Don't know statistics so that tree cuboid scheduler is not determined. Determine the maxLevel at runtime
        final int maxLevel = CuboidUtil.getLongestDepth(seg.getCubeInstance().getCuboidsByMode(cuboidMode));
        // Don't need to build base cuboid
        // n dim cuboid steps
        for (int i = 1; i <= maxLevel; i++) {
            String parentCuboidPath = i == 1 ? getBaseCuboidPath(cuboidRootPath)
                    : getCuboidOutputPathsByLevel(cuboidRootPath, i - 1);
            result.addTask(createNDimensionCuboidStep(parentCuboidPath, getCuboidOutputPathsByLevel(cuboidRootPath, i),
                    i, jobId, cuboidMode));
        }
    }

    private MapReduceExecutable createNDimensionCuboidStep(String parentPath, String outputPath, int level,
            String jobId, CuboidModeEnum cuboidMode) {
        // ND cuboid job
        MapReduceExecutable ndCuboidStep = new MapReduceExecutable();

        ndCuboidStep.setName(ExecutableConstants.STEP_NAME_BUILD_N_D_CUBOID + " : level " + level);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, parentPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, outputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME,
                "Kylin_ND-Cuboid_Builder_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_LEVEL, "" + level);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBOID_MODE, cuboidMode.toString());

        ndCuboidStep.setMapReduceParams(cmd.toString());
        ndCuboidStep.setMapReduceJobClass(getNDCuboidJob());
        return ndCuboidStep;
    }

    private MapReduceExecutable createInMemCubingStep(String jobId, CuboidModeEnum cuboidMode, String cuboidRootPath) {
        MapReduceExecutable cubeStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, JobEngineConfig.IN_MEM_JOB_CONF_SUFFIX);

        cubeStep.setName(ExecutableConstants.STEP_NAME_BUILD_IN_MEM_CUBE);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, getBaseCuboidPath(cuboidRootPath));
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getInMemCuboidPath(cuboidRootPath));
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME,
                "Kylin_Cube_Builder_" + seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBOID_MODE, cuboidMode.toString());

        cubeStep.setMapReduceParams(cmd.toString());
        cubeStep.setMapReduceJobClass(InMemCuboidFromBaseCuboidJob.class);
        cubeStep.setCounterSaveAs(
                CubingJob.SOURCE_RECORD_COUNT + "," + CubingJob.SOURCE_SIZE_BYTES + "," + CubingJob.CUBE_SIZE_BYTES);
        return cubeStep;
    }

    public UpdateCubeInfoAfterOptimizeStep createUpdateCubeInfoAfterOptimizeStep(String jobId) {
        final UpdateCubeInfoAfterOptimizeStep result = new UpdateCubeInfoAfterOptimizeStep();
        result.setName(ExecutableConstants.STEP_NAME_UPDATE_CUBE_INFO);

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());

        return result;
    }

    protected Class<? extends AbstractHadoopJob> getNDCuboidJob() {
        return NDCuboidJob.class;
    }
}
