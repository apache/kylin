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

package org.apache.kylin.engine.spark;

import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.CuboidModeEnum;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.steps.CopyDictionaryStep;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.mr.steps.MergeStatisticsWithOldStep;
import org.apache.kylin.engine.mr.steps.UpdateCubeInfoAfterOptimizeStep;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkBatchOptimizeJobBuilder2 extends JobBuilderSupport {
    private static final Logger logger = LoggerFactory.getLogger(SparkBatchOptimizeJobBuilder2.class);

    private final ISparkOutput.ISparkBatchOptimizeOutputSide outputSide;

    public SparkBatchOptimizeJobBuilder2(CubeSegment optimizeSegment, String submitter) {
        super(optimizeSegment, submitter);
        this.outputSide = SparkUtil.getBatchOptimizeOutputSide2(seg);
    }

    public CubingJob build() {
        logger.info("Spark new job to Optimize segment " + seg);
        final CubingJob result = CubingJob.createOptimizeJob(seg, submitter, config);
        final String jobId = result.getId();
        final String cuboidRootPath = getCuboidRootPath(jobId);

        final String optimizeCuboidRootPath = getOptimizationCuboidPath(jobId);

        CubeSegment oldSegment = seg.getCubeInstance().getOriginalSegmentToOptimize(seg);
        Preconditions.checkNotNull(oldSegment, "cannot find the original segment to be optimized by " + seg);

        // Phase 1: Prepare base cuboid data from old segment
        String oldcuboidRootPath = getCuboidRootPath(oldSegment) + "*";
        // Filter cuboid to optimizeCuboidRootPath + /base_cuboid and +/old
        result.addTask(createFilterRecommendCuboidDataStep(oldcuboidRootPath, optimizeCuboidRootPath, jobId));

        // Phase 2: Prepare dictionary and statistics for new segment
        result.addTask(createCopyDictionaryStep());
        String optStatsSourcePath = getBaseCuboidPath(optimizeCuboidRootPath);
        String optStatsDstPath = getOptimizationStatisticsPath(jobId);
        // Calculate statistic
        if (seg.getConfig().isUseSparkCalculateStatsEnable()) {
            result.addTask(createCalculateStatsFromBaseCuboidStepWithSpark(optStatsSourcePath, optStatsDstPath,
                    CuboidModeEnum.RECOMMEND_MISSING, jobId));
        } else {
            result.addTask(createCalculateStatsFromBaseCuboid(optStatsSourcePath, optStatsDstPath,
                    CuboidModeEnum.RECOMMEND_MISSING));
        }

        result.addTask(createMergeStatisticsWithOldStep(jobId, optStatsDstPath, getStatisticsPath(jobId)));
        outputSide.addStepPhase2_CreateHTable(result);

        result.addTask(createUpdateShardForOldCuboidDataStep(optimizeCuboidRootPath, cuboidRootPath, jobId));

        // Phase 3: Build Cube for Missing Cuboid Data
        addLayerCubingSteps(result, jobId, CuboidModeEnum.RECOMMEND_MISSING_WITH_BASE,
                SparkUtil.generateFilePath(PathNameCuboidBase, cuboidRootPath), cuboidRootPath); // layer cubing

        outputSide.addStepPhase3_BuildCube(result);

        // Phase 4: Update Metadata & Cleanup
        result.addTask(createUpdateCubeInfoAfterOptimizeStep(jobId));
        outputSide.addStepPhase4_Cleanup(result);

        return result;
    }

    private SparkExecutable createCalculateStatsFromBaseCuboidStepWithSpark(String inputPath, String outputPath,
            CuboidModeEnum recommendMissing, String jobId) {
        final SparkExecutable sparkExecutable = SparkExecutableFactory.instance(seg.getConfig());
        sparkExecutable.setName(ExecutableConstants.STEP_NAME_CALCULATE_STATS_FROM_BASECUBOID_SPARK);

        sparkExecutable.setParam(SparkCalculateStatsFromBaseCuboidJob.OPTION_CUBE_NAME.getOpt(),
                seg.getRealization().getName());
        sparkExecutable.setParam(SparkCalculateStatsFromBaseCuboidJob.OPTION_SEGMENT_ID.getOpt(), seg.getUuid());
        sparkExecutable.setParam(SparkCalculateStatsFromBaseCuboidJob.OPTION_INPUT_PATH.getOpt(), inputPath);
        sparkExecutable.setParam(SparkCalculateStatsFromBaseCuboidJob.OPTION_OUTPUT_PATH.getOpt(), outputPath);
        sparkExecutable.setParam(SparkCalculateStatsFromBaseCuboidJob.OPTION_META_URL.getOpt(),
                getSegmentMetadataUrl(seg.getConfig(), jobId));
        sparkExecutable.setParam(SparkCalculateStatsFromBaseCuboidJob.OPTION_JOB_MODE.getOpt(),
                recommendMissing.toString());
        sparkExecutable.setParam(SparkCalculateStatsFromBaseCuboidJob.OPTION_SAMPLING_PERCENT.getOpt(),
                String.valueOf(config.getConfig().getCubingInMemSamplingPercent()));
        //need JobId in sparkExecutable
        sparkExecutable.setJobId(jobId);

        sparkExecutable.setClassName(SparkCalculateStatsFromBaseCuboidJob.class.getName());

        return sparkExecutable;
    }

    private SparkExecutable createFilterRecommendCuboidDataStep(String inputPath, String outputPath , String jobId) {
        final SparkExecutable sparkExecutable = SparkExecutableFactory.instance(seg.getConfig());
        sparkExecutable.setName(ExecutableConstants.STEP_NAME_FILTER_RECOMMEND_CUBOID_DATA_FOR_OPTIMIZATION_SPARK);

        sparkExecutable.setParam(SparkFilterRecommendCuboidDataJob.OPTION_CUBE_NAME.getOpt(), seg.getRealization().getName());
        sparkExecutable.setParam(SparkFilterRecommendCuboidDataJob.OPTION_SEGMENT_ID.getOpt(), seg.getUuid());
        sparkExecutable.setParam(SparkFilterRecommendCuboidDataJob.OPTION_INPUT_PATH.getOpt(), inputPath);
        sparkExecutable.setParam(SparkFilterRecommendCuboidDataJob.OPTION_OUTPUT_PATH.getOpt(), outputPath);
        sparkExecutable.setParam(SparkFilterRecommendCuboidDataJob.OPTION_META_URL.getOpt(), getSegmentMetadataUrl(seg.getConfig(), jobId));
        sparkExecutable.setClassName(SparkFilterRecommendCuboidDataJob.class.getName());
        //need JobId in sparkExecutable
        sparkExecutable.setJobId(jobId);

        return sparkExecutable;
    }

    private UpdateCubeInfoAfterOptimizeStep createUpdateCubeInfoAfterOptimizeStep(String jobId) {
        final UpdateCubeInfoAfterOptimizeStep result = new UpdateCubeInfoAfterOptimizeStep();
        result.setName(ExecutableConstants.STEP_NAME_UPDATE_CUBE_INFO);

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());

        return result;
    }

    private void addLayerCubingSteps(CubingJob result, String jobId, CuboidModeEnum mode, String input, String output) {
        final SparkExecutable sparkExecutable = SparkExecutableFactory.instance(seg.getConfig());
        sparkExecutable.setClassName(SparkCubingByLayerForOpt.class.getName());
        configureSparkJob(seg, sparkExecutable, jobId, input, output, mode);
        result.addTask(sparkExecutable);
    }

    private SparkExecutable createUpdateShardForOldCuboidDataStep(String inputPath, String outputPath, String jobId) {
        final SparkExecutable sparkExecutable = SparkExecutableFactory.instance(seg.getConfig());
        sparkExecutable.setName(ExecutableConstants.STEP_NAME_UPDATE_OLD_CUBOID_SHARD_SPARK);

        sparkExecutable.setParam(SparkUpdateShardForOldCuboidDataStep.OPTION_CUBE_NAME.getOpt(),
                seg.getRealization().getName());
        sparkExecutable.setParam(SparkUpdateShardForOldCuboidDataStep.OPTION_SEGMENT_ID.getOpt(), seg.getUuid());
        sparkExecutable.setParam(SparkUpdateShardForOldCuboidDataStep.OPTION_INPUT_PATH.getOpt(), inputPath);
        sparkExecutable.setParam(SparkUpdateShardForOldCuboidDataStep.OPTION_OUTPUT_PATH.getOpt(), outputPath);
        sparkExecutable.setParam(SparkUpdateShardForOldCuboidDataStep.OPTION_META_URL.getOpt(),
                getSegmentMetadataUrl(seg.getConfig(), jobId));
        //need JobId in sparkExecutable
        sparkExecutable.setJobId(jobId);
        sparkExecutable.setClassName(SparkUpdateShardForOldCuboidDataStep.class.getName());

        return sparkExecutable;
    }

    private MergeStatisticsWithOldStep createMergeStatisticsWithOldStep(String jobId, String optStatsPath,
             String mergedStatisticsFolder) {
        MergeStatisticsWithOldStep result = new MergeStatisticsWithOldStep();
        result.setName(ExecutableConstants.STEP_NAME_MERGE_STATISTICS_WITH_OLD);

        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());
        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setStatisticsPath(optStatsPath, result.getParams());
        CubingExecutableUtil.setMergedStatisticsPath(mergedStatisticsFolder, result.getParams());

        return result;
    }

    private AbstractExecutable createCopyDictionaryStep() {
        CopyDictionaryStep result = new CopyDictionaryStep();
        result.setName(ExecutableConstants.STEP_NAME_COPY_DICTIONARY);

        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        return result;
    }

    private void configureSparkJob(final CubeSegment seg, final SparkExecutable sparkExecutable, final String jobId,
            final String input, String output, CuboidModeEnum mode) {

        sparkExecutable.setParam(SparkCubingByLayerForOpt.OPTION_CUBE_NAME.getOpt(), seg.getRealization().getName());
        sparkExecutable.setParam(SparkCubingByLayerForOpt.OPTION_SEGMENT_ID.getOpt(), seg.getUuid());
        sparkExecutable.setParam(SparkCubingByLayerForOpt.OPTION_META_URL.getOpt(),
                getSegmentMetadataUrl(seg.getConfig(), jobId));
        sparkExecutable.setParam(SparkCubingByLayerForOpt.OPTION_OUTPUT_PATH.getOpt(), output);
        sparkExecutable.setParam(SparkCubingByLayerForOpt.OPTION_INPUT_PATH.getOpt(), input);
        sparkExecutable.setParam(SparkCubingByLayerForOpt.OPTION_CUBOID_MODE.getOpt(), mode.toString());
        sparkExecutable.setJobId(jobId);

        StringBuilder jars = new StringBuilder();

        StringUtil.appendWithSeparator(jars, seg.getConfig().getSparkAdditionalJars());
        sparkExecutable.setJars(jars.toString());
        sparkExecutable.setName(ExecutableConstants.STEP_NAME_OPTIMIZE_SPARK_CUBE + ":" + seg.toString());
    }
}
