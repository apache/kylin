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
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.mr.steps.InMemCuboidFromBaseCuboidJob;
import org.apache.kylin.engine.mr.steps.NDCuboidJob;
import org.apache.kylin.engine.mr.streaming.ColumnToRowJob;
import org.apache.kylin.engine.mr.streaming.MergeDictJob;
import org.apache.kylin.engine.mr.streaming.SaveDictStep;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.stream.core.util.HDFSUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by gwang3 on 2017/2/23.
 */
public class StreamingCubingJobBuilder extends JobBuilderSupport {
    private static final Logger logger = LoggerFactory.getLogger(StreamingCubingJobBuilder.class);

    private final IMROutput2.IMRBatchCubingOutputSide2 outputSide;

    public StreamingCubingJobBuilder(CubeSegment newSegment, String submitter) {
        super(newSegment, submitter);
        this.outputSide = MRUtil.getBatchCubingOutputSide2(seg);
    }

    public CubingJob build() {
        logger.info("MR_V2 new job to BUILD streaming segment " + seg);

        final CubingJob result = CubingJob.createStreamJob(seg, submitter, config);
        final String jobId = result.getId();
        final String streamingStoragePath = getStreamingIndexPath(jobId);
        final String cuboidRootPath = getCuboidRootPath(jobId);

        // Phase 1: Merge Dictionaries produced by each streaming receiver
        result.addTask(createMergeDictStep(streamingStoragePath, jobId, result));
        result.addTask(createSaveDictStep(jobId, result));

        String tmpBaseCuboidPath = getBaseCuboidPathForStreaming(jobId);
        //Phase 2: Convert columnar records to row records produced by each streaming receiver, out put as base cuboid
        addBuildBaseCuboidStep(result, tmpBaseCuboidPath, streamingStoragePath);

        //Phase 3: Calculate statistics from base cuboid
        //TODO: reuse optimization file path, need define a own path.
        result.addTask(createCalculateStatsFromBaseCuboid(tmpBaseCuboidPath, getStatisticsPath(jobId)));
        result.addTask(createSaveStatisticsStep(jobId));

        //create HTable
        outputSide.addStepPhase2_BuildDictionary(result);

        // Phase 4: Build Cube
        // addLayerCubingStepsOnBaseCuboid(result, jobId, cuboidRootPath); // not add layer cubing for streaming cube
        result.addTask(createInMemCubingStep(jobId, CuboidModeEnum.CURRENT, cuboidRootPath, tmpBaseCuboidPath)); // inmem cubing, only selected algorithm will execute
        outputSide.addStepPhase3_BuildCube(result);

        // Phase 5: Update Metadata & Cleanup
//        result.addTask(createUpdateStreamCubeInfoAfterBuildStep(jobId));
        outputSide.addStepPhase4_Cleanup(result);

        return result;
    }

    private void addBuildBaseCuboidStep(final CubingJob result, final String outputBaseCuboidPath,
                                        final String streamingStoragePath) {
        result.addTask(createBaseCuboidStep(streamingStoragePath, outputBaseCuboidPath));
    }

    private void addLayerCubingStepsOnBaseCuboid(final CubingJob result, final String jobId, final String cuboidRootPath) {
        // Don't know statistics so that tree cuboid scheduler is not determined. Determine the maxLevel at runtime
        final int maxLevel = CuboidUtil.getLongestDepth(seg.getCuboidScheduler().getAllCuboidIds());
        // base cuboid step
        result.addTask(createBaseCuboidStep(getCuboidOutputPathsByLevel(cuboidRootPath, 0), jobId));
        // n dim cuboid steps
        for (int i = 1; i <= maxLevel; i++) {
            result.addTask(createNDimensionCuboidStep(getCuboidOutputPathsByLevel(cuboidRootPath, i - 1),
                    getCuboidOutputPathsByLevel(cuboidRootPath, i), i, jobId));
        }
    }

    private MapReduceExecutable createMergeDictStep(String streamingStoragePath, String jobId, DefaultChainedExecutable jobFlow) {
        MapReduceExecutable mergeDict = new MapReduceExecutable();
        mergeDict.setName(ExecutableConstants.STEP_NAME_STREAMING_CREATE_DICTIONARY);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd, JobEngineConfig.CUBE_MERGE_JOB_CONF_SUFFIX);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME,
                ExecutableConstants.STEP_NAME_STREAMING_CREATE_DICTIONARY);
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, streamingStoragePath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_NAME, seg.getName());
        //Instead of using mr job output, trySaveNewDict api is used, so output path is useless here
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getDictPath(jobId));

        final String cubeName = CubingExecutableUtil.getCubeName(jobFlow.getParams());
        mergeDict.setMapReduceParams(cmd.toString());
        mergeDict.setMapReduceJobClass(MergeDictJob.class);
        mergeDict.setLockPathName(cubeName);
        mergeDict.setIsNeedLock(true);
        mergeDict.setIsNeedReleaseLock(false);
        mergeDict.setJobFlowJobId(jobFlow.getId());

        return mergeDict;

    }

    private MapReduceExecutable createInMemCubingStep(String jobId, CuboidModeEnum cuboidMode, String cuboidRootPath,
                                                      String tmpBaseCuboidPath) {
        MapReduceExecutable cubeStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, JobEngineConfig.IN_MEM_JOB_CONF_SUFFIX);

        cubeStep.setName(ExecutableConstants.STEP_NAME_BUILD_IN_MEM_CUBE);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, tmpBaseCuboidPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, getInMemCuboidPath(cuboidRootPath));
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Cube_Builder_"
                + seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBOID_MODE, cuboidMode.toString());
        appendExecCmdParameters(cmd, BatchConstants.ARG_UPDATE_SHARD, "true");

        cubeStep.setMapReduceParams(cmd.toString());
        cubeStep.setMapReduceJobClass(InMemCuboidFromBaseCuboidJob.class);
        cubeStep.setCounterSaveAs(",,"
                + CubingJob.CUBE_SIZE_BYTES);
        return cubeStep;
    }

    private MapReduceExecutable createNDimensionCuboidStep(String parentPath, String outputPath, int level, String jobId) {
        // ND cuboid job
        MapReduceExecutable ndCuboidStep = new MapReduceExecutable();

        ndCuboidStep.setName(ExecutableConstants.STEP_NAME_BUILD_N_D_CUBOID + " : level " + level);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, parentPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, outputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_ND-Cuboid_Builder_"
                + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_LEVEL, "" + level);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);

        ndCuboidStep.setMapReduceParams(cmd.toString());
        ndCuboidStep.setMapReduceJobClass(getNDCuboidJob());
        return ndCuboidStep;
    }

    protected Class<? extends AbstractHadoopJob> getNDCuboidJob() {
        return NDCuboidJob.class;
    }

    private MapReduceExecutable createBaseCuboidStep(String streamingStoragePath, String basicCuboidOutputPath) {
        // base cuboid job
        MapReduceExecutable baseCuboidStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, JobEngineConfig.IN_MEM_JOB_CONF_SUFFIX);
        baseCuboidStep.setName(ExecutableConstants.STEP_NAME_STREAMING_BUILD_BASE_CUBOID);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_NAME, seg.getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, streamingStoragePath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, basicCuboidOutputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Base_Cuboid_Builder_"
                + seg.getRealization().getName());

        baseCuboidStep.setMapReduceParams(cmd.toString());
        baseCuboidStep.setMapReduceJobClass(ColumnToRowJob.class);
        // TODO need some way to get real source record count from fragment metadata
        baseCuboidStep.setCounterSaveAs(CubingJob.SOURCE_RECORD_COUNT + "," + CubingJob.SOURCE_SIZE_BYTES);
        return baseCuboidStep;
    }

    private SaveDictStep createSaveDictStep(String jobId, DefaultChainedExecutable jobFlow) {
        SaveDictStep result = new SaveDictStep();
        final String cubeName = CubingExecutableUtil.getCubeName(jobFlow.getParams());

        result.setName(ExecutableConstants.STEP_NAME_STREAMING_SAVE_DICTS);
        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setDictsPath(getDictPath(jobId), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());

        result.setIsNeedReleaseLock(true);
        result.setJobFlowJobId(jobFlow.getId());
        result.setLockPathName(cubeName);
        return result;
    }

    public String getStreamingIndexPath(String jobId) {
        return HDFSUtil.getStreamingSegmentFilePath(seg.getRealization().getName(), seg.getName());
    }

    private String getBaseCuboidPathForStreaming(String jobId) {
        return getRealizationRootPath(jobId) + "/stream_temp/" + PathNameCuboidBase;
    }

    private String getDictPath(String jobId) {
        return getRealizationRootPath(jobId) + "/dict";
    }
}
