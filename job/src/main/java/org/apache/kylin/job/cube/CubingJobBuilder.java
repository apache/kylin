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

package org.apache.kylin.job.cube;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.AbstractJobBuilder;
import org.apache.kylin.job.common.HadoopShellExecutable;
import org.apache.kylin.job.common.MapReduceExecutable;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.hadoop.cube.BaseCuboidJob;
import org.apache.kylin.job.hadoop.cube.CubeHFileJob;
import org.apache.kylin.job.hadoop.cube.FactDistinctColumnsJob;
import org.apache.kylin.job.hadoop.cube.MergeCuboidJob;
import org.apache.kylin.job.hadoop.cube.NDCuboidJob;
import org.apache.kylin.job.hadoop.cube.RangeKeyDistributionJob;
import org.apache.kylin.job.hadoop.cubev2.InMemCuboidJob;
import org.apache.kylin.job.hadoop.dict.CreateDictionaryJob;
import org.apache.kylin.job.hadoop.hbase.BulkLoadJob;
import org.apache.kylin.job.hadoop.hbase.CreateHTableJob;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;

/**
 * Created by qianzhou on 12/25/14.
 */
public final class CubingJobBuilder extends AbstractJobBuilder {

    private boolean useImMemCubing = false;

    public CubingJobBuilder(JobEngineConfig engineConfig) {
        super(engineConfig);
    }

    public CubingJob buildJob(CubeSegment seg) {
        checkPreconditions(seg);

        final CubingJob result = initialJob(seg, "BUILD");
        final String jobId = result.getId();
        final String cuboidRootPath = getJobWorkingDir(jobId) + "/" + seg.getCubeInstance().getName() + "/cuboid/";

        // cubing
        Pair<AbstractExecutable, AbstractExecutable> twoSteps = addCubingSteps(seg, cuboidRootPath, result);
        String intermediateHiveTableStepId = twoSteps.getFirst().getId();
        String baseCuboidStepId = twoSteps.getSecond().getId();

        // convert htable
        AbstractExecutable convertCuboidToHfileStep = addHTableSteps(seg, cuboidRootPath, result);

        // update cube info
        result.addTask(createUpdateCubeInfoAfterBuildStep(seg, intermediateHiveTableStepId, baseCuboidStepId, convertCuboidToHfileStep.getId(), jobId));

        return result;
    }

    public CubingJob buildAndMergeJob(CubeSegment appendSegment, CubeSegment mergeSegment) {
        checkPreconditions(appendSegment, mergeSegment);

        CubingJob result = initialJob(mergeSegment, "BUILD");
        final String jobId = result.getId();
        final String appendRootPath = getJobWorkingDir(jobId) + "/" + appendSegment.getCubeInstance().getName() + "/append_cuboid/";
        final String mergedRootPath = getJobWorkingDir(jobId) + "/" + appendSegment.getCubeInstance().getName() + "/cuboid/";

        // cubing the incremental segment
        Pair<AbstractExecutable, AbstractExecutable> twoSteps = addCubingSteps(appendSegment, appendRootPath, result);
        final String intermediateHiveTableStepId = twoSteps.getFirst().getId();
        final String baseCuboidStepId = twoSteps.getSecond().getId();

        // update the append segment info
        result.addTask(createUpdateCubeInfoAfterBuildStep(appendSegment, intermediateHiveTableStepId, baseCuboidStepId, null, jobId));

        List<CubeSegment> mergingSegments = mergeSegment.getCubeInstance().getMergingSegments(mergeSegment);
        Preconditions.checkState(mergingSegments.size() > 1, "there should be more than 2 segments to merge");
        List<String> mergingSegmentIds = Lists.newArrayList();
        List<String> mergingCuboidPaths = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingSegmentIds.add(merging.getUuid());
            if (merging.equals(appendSegment))
                mergingCuboidPaths.add(appendRootPath + "*");
            else
                mergingCuboidPaths.add(getPathToMerge(merging));
        }

        // merge cuboid
        addMergeSteps(mergeSegment, mergingSegmentIds, mergingCuboidPaths, mergedRootPath, result);

        // convert htable
        AbstractExecutable convertCuboidToHfileStep = addHTableSteps(mergeSegment, mergedRootPath, result);

        // update cube info
        result.addTask(createUpdateCubeInfoAfterMergeStep(mergeSegment, mergingSegmentIds, convertCuboidToHfileStep.getId(), jobId));

        return result;
    }

    public CubingJob mergeJob(CubeSegment seg) {
        checkPreconditions(seg);

        CubingJob result = initialJob(seg, "MERGE");
        final String jobId = result.getId();
        final String mergedCuboidPath = getJobWorkingDir(jobId) + "/" + seg.getCubeInstance().getName() + "/cuboid/";

        List<CubeSegment> mergingSegments = seg.getCubeInstance().getMergingSegments(seg);
        Preconditions.checkState(mergingSegments.size() > 1, "there should be more than 2 segments to merge");
        List<String> mergingSegmentIds = Lists.newArrayList();
        List<String> mergingCuboidPaths = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingSegmentIds.add(merging.getUuid());
            mergingCuboidPaths.add(getPathToMerge(merging));
        }

        // merge cuboid
        addMergeSteps(seg, mergingSegmentIds, mergingCuboidPaths, mergedCuboidPath, result);

        // convert htable
        AbstractExecutable convertCuboidToHfileStep = addHTableSteps(seg, mergedCuboidPath, result);

        // update cube info
        result.addTask(createUpdateCubeInfoAfterMergeStep(seg, mergingSegmentIds, convertCuboidToHfileStep.getId(), jobId));

        return result;
    }

    void addMergeSteps(CubeSegment seg, List<String> mergingSegmentIds, List<String> mergingCuboidPaths, String mergedCuboidPath, CubingJob result) {

        result.addTask(createMergeDictionaryStep(seg, mergingSegmentIds));

        String formattedPath = StringUtils.join(mergingCuboidPaths, ",");
        result.addTask(createMergeCuboidDataStep(seg, formattedPath, mergedCuboidPath));
    }

    Pair<AbstractExecutable, AbstractExecutable> addCubingSteps(CubeSegment seg, String cuboidRootPath, CubingJob result) {
        final int groupRowkeyColumnsCount = seg.getCubeDesc().getRowkey().getNCuboidBuildLevels();
        final int totalRowkeyColumnsCount = seg.getCubeDesc().getRowkey().getRowKeyColumns().length;

        final String jobId = result.getId();
        final CubeJoinedFlatTableDesc intermediateTableDesc = new CubeJoinedFlatTableDesc(seg.getCubeDesc(), seg);
        final String intermediateHiveTableName = getIntermediateHiveTableName(intermediateTableDesc, jobId);
        final String intermediateHiveTableLocation = getIntermediateHiveTableLocation(intermediateTableDesc, jobId);
        final String factDistinctColumnsPath = getFactDistinctColumnsPath(seg, jobId);
        final String[] cuboidOutputTempPath = getCuboidOutputPaths(cuboidRootPath, totalRowkeyColumnsCount, groupRowkeyColumnsCount);

        final AbstractExecutable intermediateHiveTableStep = createIntermediateHiveTableStep(intermediateTableDesc, jobId);
        result.addTask(intermediateHiveTableStep);
        result.addTask(createFactDistinctColumnsStep(seg, intermediateHiveTableName, jobId));
        result.addTask(createBuildDictionaryStep(seg, factDistinctColumnsPath));
        MapReduceExecutable baseCuboidStep = null;
        if(!useImMemCubing) {
            // base cuboid step
            baseCuboidStep = createBaseCuboidStep(seg, intermediateHiveTableLocation, cuboidOutputTempPath);
            result.addTask(baseCuboidStep);

            // n dim cuboid steps
            for (int i = 1; i <= groupRowkeyColumnsCount; i++) {
                int dimNum = totalRowkeyColumnsCount - i;
                result.addTask(createNDimensionCuboidStep(seg, cuboidOutputTempPath, dimNum, totalRowkeyColumnsCount));
            }
        } else {
            baseCuboidStep = createInMemCubingStep(seg, intermediateHiveTableLocation, intermediateHiveTableName, cuboidOutputTempPath);
            result.addTask(baseCuboidStep);
        }

        return new Pair<AbstractExecutable, AbstractExecutable>(intermediateHiveTableStep, baseCuboidStep);
    }

    AbstractExecutable addHTableSteps(CubeSegment seg, String cuboidRootPath, CubingJob result) {
        final String jobId = result.getId();
        final String cuboidPath = cuboidRootPath + "*";

        result.addTask(createRangeRowkeyDistributionStep(seg, cuboidPath));
        // create htable step
        result.addTask(createCreateHTableStep(seg));
        // generate hfiles step
        final MapReduceExecutable convertCuboidToHfileStep = createConvertCuboidToHfileStep(seg, cuboidPath, jobId);
        result.addTask(convertCuboidToHfileStep);
        // bulk load step
        result.addTask(createBulkLoadStep(seg, jobId));

        return convertCuboidToHfileStep;
    }

    private CubingJob initialJob(CubeSegment seg, String type) {
        CubingJob result = new CubingJob();
        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone(engineConfig.getTimeZone()));
        result.setCubeName(seg.getCubeInstance().getName());
        result.setSegmentId(seg.getUuid());
        result.setName(seg.getCubeInstance().getName() + " - " + seg.getName() + " - " + type + " - " + format.format(new Date(System.currentTimeMillis())));
        result.setSubmitter(submitter);
        result.setNotifyList(seg.getCubeInstance().getDescriptor().getNotifyList());
        return result;
    }

    private void checkPreconditions(CubeSegment... segments) {
        for (CubeSegment seg : segments) {
            Preconditions.checkNotNull(seg, "segment cannot be null");
        }
        Preconditions.checkNotNull(engineConfig, "jobEngineConfig cannot be null");
    }

    private String getPathToMerge(CubeSegment seg) {
        return getJobWorkingDir(seg.getLastBuildJobID()) + "/" + seg.getCubeInstance().getName() + "/cuboid/*";
    }

    private String getRowkeyDistributionOutputPath(CubeSegment seg) {
        return engineConfig.getHdfsWorkingDirectory() + "/" + seg.getCubeInstance().getName() + "/rowkey_stats";
    }

    private void appendMapReduceParameters(StringBuilder builder, CubeSegment seg) {
        try {
            String jobConf = engineConfig.getHadoopJobConfFilePath(seg.getCubeDesc().getModel().getCapacity());
            if (jobConf != null && jobConf.length() > 0) {
                builder.append(" -conf ").append(jobConf);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String[] getCuboidOutputPaths(String cuboidRootPath, int totalRowkeyColumnCount, int groupRowkeyColumnsCount) {
        String[] paths = new String[groupRowkeyColumnsCount + 1];
        for (int i = 0; i <= groupRowkeyColumnsCount; i++) {
            int dimNum = totalRowkeyColumnCount - i;
            if (dimNum == totalRowkeyColumnCount) {
                paths[i] = cuboidRootPath + "base_cuboid";
            } else {
                paths[i] = cuboidRootPath + dimNum + "d_cuboid";
            }
        }
        return paths;
    }

    private String getFactDistinctColumnsPath(CubeSegment seg, String jobUuid) {
        return getJobWorkingDir(jobUuid) + "/" + seg.getCubeInstance().getName() + "/fact_distinct_columns";
    }


    private String getStatisticsPath(CubeSegment seg, String jobUuid) {
        return getJobWorkingDir(jobUuid) + "/" + seg.getCubeInstance().getName() + "/statistics";
    }


    private String getHFilePath(CubeSegment seg, String jobId) {
        return getJobWorkingDir(jobId) + "/" + seg.getCubeInstance().getName() + "/hfile/";
    }

    private MapReduceExecutable createFactDistinctColumnsStep(CubeSegment seg, String intermediateHiveTableName, String jobId) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName(ExecutableConstants.STEP_NAME_FACT_DISTINCT_COLUMNS);
        result.setMapReduceJobClass(FactDistinctColumnsJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, seg);
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "output", getFactDistinctColumnsPath(seg, jobId));
        appendExecCmdParameters(cmd, "statisticsenabled",  String.valueOf(useImMemCubing));
        appendExecCmdParameters(cmd, "statisticsoutput", getStatisticsPath(seg, jobId));
        appendExecCmdParameters(cmd, "jobname", "Kylin_Fact_Distinct_Columns_" + seg.getCubeInstance().getName() + "_Step");
        appendExecCmdParameters(cmd, "tablename", intermediateHiveTableName);

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    private HadoopShellExecutable createBuildDictionaryStep(CubeSegment seg, String factDistinctColumnsPath) {
        // base cuboid job
        HadoopShellExecutable buildDictionaryStep = new HadoopShellExecutable();
        buildDictionaryStep.setName(ExecutableConstants.STEP_NAME_BUILD_DICTIONARY);
        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "segmentname", seg.getName());
        appendExecCmdParameters(cmd, "input", factDistinctColumnsPath);

        buildDictionaryStep.setJobParams(cmd.toString());
        buildDictionaryStep.setJobClass(CreateDictionaryJob.class);
        return buildDictionaryStep;
    }

    private MapReduceExecutable createInMemCubingStep(CubeSegment seg, String intermediateHiveTableLocation, String intermediateHiveTableName, String[] cuboidOutputTempPath) {
        // base cuboid job
        MapReduceExecutable baseCuboidStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, seg);

        baseCuboidStep.setName(ExecutableConstants.STEP_NAME_BUILD_IN_MEM_CUBE);

        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "segmentname", seg.getName());
        appendExecCmdParameters(cmd, "input", intermediateHiveTableLocation);
        appendExecCmdParameters(cmd, "output", cuboidOutputTempPath[0]);
        appendExecCmdParameters(cmd, "jobname", "Kylin_Base_Cuboid_Builder_" + seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "level", "0");
        appendExecCmdParameters(cmd, "tablename", intermediateHiveTableName);

        baseCuboidStep.setMapReduceParams(cmd.toString());
        baseCuboidStep.setMapReduceJobClass(InMemCuboidJob.class);
        return baseCuboidStep;
    }

    private MapReduceExecutable createBaseCuboidStep(CubeSegment seg, String intermediateHiveTableLocation, String[] cuboidOutputTempPath) {
        // base cuboid job
        MapReduceExecutable baseCuboidStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, seg);

        baseCuboidStep.setName(ExecutableConstants.STEP_NAME_BUILD_BASE_CUBOID);

        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "segmentname", seg.getName());
        appendExecCmdParameters(cmd, "input", intermediateHiveTableLocation);
        appendExecCmdParameters(cmd, "output", cuboidOutputTempPath[0]);
        appendExecCmdParameters(cmd, "jobname", "Kylin_Base_Cuboid_Builder_" + seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "level", "0");

        baseCuboidStep.setMapReduceParams(cmd.toString());
        baseCuboidStep.setMapReduceJobClass(BaseCuboidJob.class);
        return baseCuboidStep;
    }

    private MapReduceExecutable createNDimensionCuboidStep(CubeSegment seg, String[] cuboidOutputTempPath, int dimNum, int totalRowkeyColumnCount) {
        // ND cuboid job
        MapReduceExecutable ndCuboidStep = new MapReduceExecutable();

        ndCuboidStep.setName(ExecutableConstants.STEP_NAME_BUILD_N_D_CUBOID + " : " + dimNum + "-Dimension");
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd, seg);
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "segmentname", seg.getName());
        appendExecCmdParameters(cmd, "input", cuboidOutputTempPath[totalRowkeyColumnCount - dimNum - 1]);
        appendExecCmdParameters(cmd, "output", cuboidOutputTempPath[totalRowkeyColumnCount - dimNum]);
        appendExecCmdParameters(cmd, "jobname", "Kylin_ND-Cuboid_Builder_" + seg.getCubeInstance().getName() + "_Step");
        appendExecCmdParameters(cmd, "level", "" + (totalRowkeyColumnCount - dimNum));

        ndCuboidStep.setMapReduceParams(cmd.toString());
        ndCuboidStep.setMapReduceJobClass(NDCuboidJob.class);
        return ndCuboidStep;
    }

    private MapReduceExecutable createRangeRowkeyDistributionStep(CubeSegment seg, String inputPath) {
        MapReduceExecutable rowkeyDistributionStep = new MapReduceExecutable();
        rowkeyDistributionStep.setName(ExecutableConstants.STEP_NAME_GET_CUBOID_KEY_DISTRIBUTION);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd, seg);
        appendExecCmdParameters(cmd, "input", inputPath);
        appendExecCmdParameters(cmd, "output", getRowkeyDistributionOutputPath(seg));
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "jobname", "Kylin_Region_Splits_Calculator_" + seg.getCubeInstance().getName() + "_Step");

        rowkeyDistributionStep.setMapReduceParams(cmd.toString());
        rowkeyDistributionStep.setMapReduceJobClass(RangeKeyDistributionJob.class);
        return rowkeyDistributionStep;
    }

    private HadoopShellExecutable createCreateHTableStep(CubeSegment seg) {
        HadoopShellExecutable createHtableStep = new HadoopShellExecutable();
        createHtableStep.setName(ExecutableConstants.STEP_NAME_CREATE_HBASE_TABLE);
        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "input", getRowkeyDistributionOutputPath(seg) + "/part-r-00000");
        appendExecCmdParameters(cmd, "htablename", seg.getStorageLocationIdentifier());

        createHtableStep.setJobParams(cmd.toString());
        createHtableStep.setJobClass(CreateHTableJob.class);

        return createHtableStep;
    }

    private MapReduceExecutable createConvertCuboidToHfileStep(CubeSegment seg, String inputPath, String jobId) {
        MapReduceExecutable createHFilesStep = new MapReduceExecutable();
        createHFilesStep.setName(ExecutableConstants.STEP_NAME_CONVERT_CUBOID_TO_HFILE);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd, seg);
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "input", inputPath);
        appendExecCmdParameters(cmd, "output", getHFilePath(seg, jobId));
        appendExecCmdParameters(cmd, "htablename", seg.getStorageLocationIdentifier());
        appendExecCmdParameters(cmd, "jobname", "Kylin_HFile_Generator_" + seg.getCubeInstance().getName() + "_Step");

        createHFilesStep.setMapReduceParams(cmd.toString());
        createHFilesStep.setMapReduceJobClass(CubeHFileJob.class);

        return createHFilesStep;
    }

    private HadoopShellExecutable createBulkLoadStep(CubeSegment seg, String jobId) {
        HadoopShellExecutable bulkLoadStep = new HadoopShellExecutable();
        bulkLoadStep.setName(ExecutableConstants.STEP_NAME_BULK_LOAD_HFILE);

        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "input", getHFilePath(seg, jobId));
        appendExecCmdParameters(cmd, "htablename", seg.getStorageLocationIdentifier());
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());

        bulkLoadStep.setJobParams(cmd.toString());
        bulkLoadStep.setJobClass(BulkLoadJob.class);

        return bulkLoadStep;

    }

    private UpdateCubeInfoAfterBuildStep createUpdateCubeInfoAfterBuildStep(CubeSegment seg, String createFlatTableStepId, String baseCuboidStepId, String convertToHFileStepId, String jobId) {
        final UpdateCubeInfoAfterBuildStep updateCubeInfoStep = new UpdateCubeInfoAfterBuildStep();
        updateCubeInfoStep.setName(ExecutableConstants.STEP_NAME_UPDATE_CUBE_INFO);
        updateCubeInfoStep.setCubeName(seg.getCubeInstance().getName());
        updateCubeInfoStep.setSegmentId(seg.getUuid());
        updateCubeInfoStep.setCreateFlatTableStepId(createFlatTableStepId);
        updateCubeInfoStep.setBaseCuboidStepId(baseCuboidStepId);
        updateCubeInfoStep.setConvertToHFileStepId(convertToHFileStepId);
        updateCubeInfoStep.setCubingJobId(jobId);
        return updateCubeInfoStep;
    }

    private MergeDictionaryStep createMergeDictionaryStep(CubeSegment seg, List<String> mergingSegmentIds) {
        MergeDictionaryStep result = new MergeDictionaryStep();
        result.setName(ExecutableConstants.STEP_NAME_MERGE_DICTIONARY);
        result.setCubeName(seg.getCubeInstance().getName());
        result.setSegmentId(seg.getUuid());
        result.setMergingSegmentIds(mergingSegmentIds);
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

    private UpdateCubeInfoAfterMergeStep createUpdateCubeInfoAfterMergeStep(CubeSegment seg, List<String> mergingSegmentIds, String convertToHFileStepId, String jobId) {
        UpdateCubeInfoAfterMergeStep result = new UpdateCubeInfoAfterMergeStep();
        result.setName(ExecutableConstants.STEP_NAME_UPDATE_CUBE_INFO);
        result.setCubeName(seg.getCubeInstance().getName());
        result.setSegmentId(seg.getUuid());
        result.setMergingSegmentIds(mergingSegmentIds);
        result.setConvertToHFileStepId(convertToHFileStepId);
        result.setCubingJobId(jobId);
        return result;
    }

}
