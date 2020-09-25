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
package org.apache.kylin.engine.flink;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * The batch merge job build implements with Apache Flink.
 */
public class FlinkBatchMergeJobBuilder2 extends JobBuilderSupport {

    private static final Logger logger = LoggerFactory.getLogger(FlinkBatchMergeJobBuilder2.class);

    private final IFlinkOutput.IFlinkBatchMergeOutputSide outputSide;
    private final IFlinkInput.IFlinkBatchMergeInputSide inputSide;

    public FlinkBatchMergeJobBuilder2(CubeSegment mergeSegment, String submitter) {
        super(mergeSegment, submitter);
        this.outputSide = FlinkUtil.getBatchMergeOutputSide2(seg);
        this.inputSide = FlinkUtil.getBatchMergeInputSide(seg);
    }

    public CubingJob build() {
        logger.info("Flink job to MERGE segment " + seg);

        final CubeSegment cubeSegment = seg;
        final CubingJob result = CubingJob.createMergeJob(cubeSegment, submitter, config);
        final String jobId = result.getId();

        final List<CubeSegment> mergingSegments = cubeSegment.getCubeInstance().getMergingSegments(cubeSegment);
        Preconditions.checkState(mergingSegments.size() > 1,
                "there should be more than 2 segments to merge, target segment " + cubeSegment);
        final List<String> mergingSegmentIds = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingSegmentIds.add(merging.getUuid());
        }

        // Phase 1: Merge Dictionary
        inputSide.addStepPhase1_MergeDictionary(result);
        result.addTask(createMergeDictionaryFlinkStep(cubeSegment, jobId, mergingSegmentIds));
        result.addTask(createUpdateDictionaryStep(cubeSegment, jobId, mergingSegmentIds));
        outputSide.addStepPhase1_MergeDictionary(result);

        // merge cube
        result.addTask(createMergeCuboidDataFlinkStep(cubeSegment, mergingSegments, jobId));

        // Phase 2: Merge Cube Files
        outputSide.addStepPhase2_BuildCube(seg, mergingSegments, result);

        // Phase 3: Update Metadata & Cleanup
        result.addTask(createUpdateCubeInfoAfterMergeStep(mergingSegmentIds, jobId));
        outputSide.addStepPhase3_Cleanup(result);

        return result;
    }

    public FlinkExecutable createMergeDictionaryFlinkStep(CubeSegment seg, String jobID, List<String> mergingSegmentIds) {
        final FlinkExecutable flinkExecutable = new FlinkExecutable();
        flinkExecutable.setClassName(FlinkMergingDictionary.class.getName());

        flinkExecutable.setParam(FlinkMergingDictionary.OPTION_CUBE_NAME.getOpt(), seg.getRealization().getName());
        flinkExecutable.setParam(FlinkMergingDictionary.OPTION_SEGMENT_ID.getOpt(), seg.getUuid());
        flinkExecutable.setParam(FlinkMergingDictionary.OPTION_META_URL.getOpt(), getSegmentMetadataUrl(seg.getConfig(), jobID));
        flinkExecutable.setParam(FlinkMergingDictionary.OPTION_MERGE_SEGMENT_IDS.getOpt(), StringUtil.join(mergingSegmentIds, ","));
        flinkExecutable.setParam(FlinkMergingDictionary.OPTION_OUTPUT_PATH_DICT.getOpt(), getDictInfoPath(jobID));
        flinkExecutable.setParam(FlinkMergingDictionary.OPTION_OUTPUT_PATH_STAT.getOpt(), getStatisticsPath(jobID));

        flinkExecutable.setJobId(jobID);
        flinkExecutable.setName(ExecutableConstants.STEP_NAME_MERGE_DICTIONARY);
        flinkExecutable.setFlinkConfigName(ExecutableConstants.FLINK_SPECIFIC_CONFIG_NAME_MERGE_DICTIONARY);

        StringBuilder jars = new StringBuilder();

        StringUtil.appendWithSeparator(jars, seg.getConfig().getFlinkAdditionalJars());
        flinkExecutable.setJars(jars.toString());

        return flinkExecutable;
    }

    public FlinkExecutable createMergeCuboidDataFlinkStep(CubeSegment seg, List<CubeSegment> mergingSegments, String jobID) {
        final List<String> mergingCuboidPaths = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingCuboidPaths.add(getCuboidRootPath(merging));
        }
        String formattedPath = StringUtil.join(mergingCuboidPaths, ",");
        String outputPath = getCuboidRootPath(jobID);

        final FlinkExecutable flinkExecutable = new FlinkExecutable();
        flinkExecutable.setClassName(FlinkCubingMerge.class.getName());
        flinkExecutable.setParam(FlinkCubingMerge.OPTION_CUBE_NAME.getOpt(), seg.getRealization().getName());
        flinkExecutable.setParam(FlinkCubingMerge.OPTION_SEGMENT_ID.getOpt(), seg.getUuid());
        flinkExecutable.setParam(FlinkCubingMerge.OPTION_INPUT_PATH.getOpt(), formattedPath);
        flinkExecutable.setParam(FlinkCubingMerge.OPTION_META_URL.getOpt(), getSegmentMetadataUrl(seg.getConfig(), jobID));
        flinkExecutable.setParam(FlinkCubingMerge.OPTION_OUTPUT_PATH.getOpt(), outputPath);

        flinkExecutable.setJobId(jobID);
        flinkExecutable.setName(ExecutableConstants.STEP_NAME_MERGE_CUBOID);

        StringBuilder jars = new StringBuilder();

        StringUtil.appendWithSeparator(jars, seg.getConfig().getFlinkAdditionalJars());
        flinkExecutable.setJars(jars.toString());

        return flinkExecutable;
    }

}
