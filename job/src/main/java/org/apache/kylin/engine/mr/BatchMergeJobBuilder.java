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
import org.apache.kylin.job.common.MapReduceExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.hadoop.cube.MergeCuboidJob;
import org.apache.kylin.job.hadoop.cubev2.MergeCuboidFromHBaseJob;
import org.apache.kylin.job.hadoop.cubev2.MergeStatisticsStep;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class BatchMergeJobBuilder extends JobBuilderSupport {

    public BatchMergeJobBuilder(CubeSegment mergeSegment, String submitter) {
        super(mergeSegment, submitter);
    }

    public CubingJob build() {
        final CubingJob result = CubingJob.createMergeJob(seg,  submitter,  config);
        final String jobId = result.getId();
        final String cuboidRootPath = getCuboidRootPath(jobId);

        final List<CubeSegment> mergingSegments = seg.getCubeInstance().getMergingSegments(seg);
        Preconditions.checkState(mergingSegments.size() > 1, "there should be more than 2 segments to merge");
        final List<String> mergingSegmentIds = Lists.newArrayList();
        final List<String> mergingCuboidPaths = Lists.newArrayList();
        final List<String> mergingHTables = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingSegmentIds.add(merging.getUuid());
            mergingCuboidPaths.add(getCuboidRootPath(merging) + "*");
            mergingHTables.add(merging.getStorageLocationIdentifier());
        }

        result.addTask(createMergeDictionaryStep(mergingSegmentIds));
        
        if (config.isInMemCubing()) {

            String mergedStatisticsFolder = getStatisticsPath(jobId);
            result.addTask(createMergeStatisticsStep(seg, mergingSegmentIds, mergedStatisticsFolder));

            // create htable step
            result.addTask(createCreateHTableStep(jobId));

            String formattedTables = StringUtil.join(mergingHTables, ",");
            result.addTask(createMergeCuboidDataFromHBaseStep(formattedTables, jobId));

        } else {
            // merge cuboid
            String formattedPath = StringUtil.join(mergingCuboidPaths, ",");
            result.addTask(createMergeCuboidDataStep(seg, formattedPath, cuboidRootPath));
            
            // convert htable
            result.addTask(createRangeRowkeyDistributionStep(cuboidRootPath + "*", jobId));
            // create htable step
            result.addTask(createCreateHTableStep(jobId));
            // generate hfiles step
            result.addTask(createConvertCuboidToHfileStep(cuboidRootPath + "*", jobId));
        }
        
        // bulk load step
        result.addTask(createBulkLoadStep(jobId));

        // update cube info
        result.addTask(createUpdateCubeInfoAfterMergeStep(mergingSegmentIds, jobId));

        result.addTask(createGarbageCollectionStep(mergingHTables, null));

        return result;
    }
    
    private MergeDictionaryStep createMergeDictionaryStep(List<String> mergingSegmentIds) {
        MergeDictionaryStep result = new MergeDictionaryStep();
        result.setName(ExecutableConstants.STEP_NAME_MERGE_DICTIONARY);
        result.setCubeName(seg.getCubeInstance().getName());
        result.setSegmentId(seg.getUuid());
        result.setMergingSegmentIds(mergingSegmentIds);
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

    private MapReduceExecutable createMergeCuboidDataStep(CubeSegment seg, String inputPath, String outputPath) {
        MapReduceExecutable mergeCuboidDataStep = new MapReduceExecutable();
        mergeCuboidDataStep.setName(ExecutableConstants.STEP_NAME_MERGE_CUBOID);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd, seg);
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "segmentname", seg.getName());
        appendExecCmdParameters(cmd, "input", inputPath);
        appendExecCmdParameters(cmd, "output", outputPath);
        appendExecCmdParameters(cmd, "jobname", "Kylin_Merge_Cuboid_" + seg.getCubeInstance().getName() + "_Step");

        mergeCuboidDataStep.setMapReduceParams(cmd.toString());
        mergeCuboidDataStep.setMapReduceJobClass(MergeCuboidJob.class);
        return mergeCuboidDataStep;
    }


    private MapReduceExecutable createMergeCuboidDataFromHBaseStep(String inputTableNames, String jobId) {
        MapReduceExecutable mergeCuboidDataStep = new MapReduceExecutable();
        mergeCuboidDataStep.setName(ExecutableConstants.STEP_NAME_MERGE_CUBOID);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd, seg);
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "segmentname", seg.getName());
        appendExecCmdParameters(cmd, "input", inputTableNames);
        appendExecCmdParameters(cmd, "output", getHFilePath(jobId));
        appendExecCmdParameters(cmd, "htablename", seg.getStorageLocationIdentifier());
        appendExecCmdParameters(cmd, "jobname", "Kylin_Merge_Cuboid_" + seg.getCubeInstance().getName() + "_Step");

        mergeCuboidDataStep.setMapReduceParams(cmd.toString());
        mergeCuboidDataStep.setMapReduceJobClass(MergeCuboidFromHBaseJob.class);
        mergeCuboidDataStep.setCounterSaveAs(",," + CubingJob.CUBE_SIZE_BYTES);
        return mergeCuboidDataStep;
    }

    private UpdateCubeInfoAfterMergeStep createUpdateCubeInfoAfterMergeStep(List<String> mergingSegmentIds, String jobId) {
        UpdateCubeInfoAfterMergeStep result = new UpdateCubeInfoAfterMergeStep();
        result.setName(ExecutableConstants.STEP_NAME_UPDATE_CUBE_INFO);
        result.setCubeName(seg.getCubeInstance().getName());
        result.setSegmentId(seg.getUuid());
        result.setMergingSegmentIds(mergingSegmentIds);
        result.setCubingJobId(jobId);
        return result;
    }

    
}
