package com.kylinolap.job.cube;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.job.AbstractJobBuilder;
import com.kylinolap.job.common.HadoopShellExecutable;
import com.kylinolap.job.common.MapReduceExecutable;
import com.kylinolap.job.constant.ExecutableConstants;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.hadoop.cube.BaseCuboidJob;
import com.kylinolap.job.hadoop.cube.CubeHFileJob;
import com.kylinolap.job.hadoop.cube.FactDistinctColumnsJob;
import com.kylinolap.job.hadoop.cube.MergeCuboidJob;
import com.kylinolap.job.hadoop.cube.NDCuboidJob;
import com.kylinolap.job.hadoop.cube.RangeKeyDistributionJob;
import com.kylinolap.job.hadoop.dict.CreateDictionaryJob;
import com.kylinolap.job.hadoop.hbase.BulkLoadJob;
import com.kylinolap.job.hadoop.hbase.CreateHTableJob;
import com.kylinolap.job.hadoop.hive.CubeJoinedFlatTableDesc;
import com.kylinolap.job.impl.threadpool.AbstractExecutable;

/**
 * Created by qianzhou on 12/25/14.
 */
public final class CubingJobBuilder extends AbstractJobBuilder {

    private CubingJobBuilder() {
    }

    public static CubingJobBuilder newBuilder() {
        return new CubingJobBuilder();
    }

    protected CubeDesc getCubeDesc() {
        return ((CubeSegment) segment).getCubeDesc();
    }

    public CubingJob buildJob() {
        checkPreconditions();
        final int groupRowkeyColumnsCount = getCubeDesc().getRowkey().getNCuboidBuildLevels();
        final int totalRowkeyColumnsCount = getCubeDesc().getRowkey().getRowKeyColumns().length;

        CubingJob result = initialJob("BUILD");
        final String jobId = result.getId();
        final CubeJoinedFlatTableDesc intermediateTableDesc = new CubeJoinedFlatTableDesc(getCubeDesc(), (CubeSegment) this.segment);
        final String intermediateHiveTableName = getIntermediateHiveTableName(intermediateTableDesc, jobId);
        final String intermediateHiveTableLocation = getIntermediateHiveTableLocation(intermediateTableDesc, jobId);
        final String factDistinctColumnsPath = getFactDistinctColumnsPath(jobId);
        final String cuboidRootPath = getJobWorkingDir(jobId) + "/" + getCubeName() + "/cuboid/";
        final String cuboidPath = cuboidRootPath + "*";
        final String[] cuboidOutputTempPath = getCuboidOutputPaths(cuboidRootPath, totalRowkeyColumnsCount, groupRowkeyColumnsCount);

        final AbstractExecutable intermediateHiveTableStep = createIntermediateHiveTableStep(intermediateTableDesc, jobId);
        result.addTask(intermediateHiveTableStep);

        result.addTask(createFactDistinctColumnsStep(intermediateHiveTableName, jobId));

        result.addTask(createBuildDictionaryStep(factDistinctColumnsPath));

        // base cuboid step
        final MapReduceExecutable baseCuboidStep = createBaseCuboidStep(intermediateHiveTableLocation, cuboidOutputTempPath);
        result.addTask(baseCuboidStep);

        // n dim cuboid steps
        for (int i = 1; i <= groupRowkeyColumnsCount; i++) {
            int dimNum = totalRowkeyColumnsCount - i;
            result.addTask(createNDimensionCuboidStep(cuboidOutputTempPath, dimNum, totalRowkeyColumnsCount));
        }

        result.addTask(createRangeRowkeyDistributionStep(cuboidPath));
        // create htable step
        result.addTask(createCreateHTableStep());
        // generate hfiles step
        final MapReduceExecutable convertCuboidToHfileStep = createConvertCuboidToHfileStep(cuboidPath, jobId);
        result.addTask(convertCuboidToHfileStep);
        // bulk load step
        result.addTask(createBulkLoadStep(jobId));

        result.addTask(createUpdateCubeInfoStep(intermediateHiveTableStep.getId(), baseCuboidStep.getId(), convertCuboidToHfileStep.getId(), jobId));

        return result;
    }

    public CubingJob mergeJob() {
        checkPreconditions();
        CubingJob result = initialJob("MERGE");
        final String jobId = result.getId();
        CubeSegment seg = (CubeSegment) segment;
        List<CubeSegment> mergingSegments = seg.getCubeInstance().getMergingSegments(seg);
        Preconditions.checkState(mergingSegments != null && mergingSegments.size() > 1, "there should be more than 2 segments to merge");
        
        String[] cuboidPaths = new String[mergingSegments.size()];
        for (int i = 0; i < mergingSegments.size(); i++) {
            cuboidPaths[i] = getPathToMerge(mergingSegments.get(i));
        }
        final String formattedPath = StringUtils.join(cuboidPaths, ",");
        final String mergedCuboidPath = getJobWorkingDir(jobId) + "/" + getCubeName() + "/cuboid";

        result.addTask(createMergeCuboidDataStep(formattedPath, mergedCuboidPath));

        // get output distribution step
        result.addTask(createRangeRowkeyDistributionStep(mergedCuboidPath));

        // create htable step
        result.addTask(createCreateHTableStep());

        // generate hfiles step
        final MapReduceExecutable convertCuboidToHfileStep = createConvertCuboidToHfileStep(mergedCuboidPath, jobId);
        result.addTask(convertCuboidToHfileStep);

        // bulk load step
        result.addTask(createBulkLoadStep(jobId));

        final List<String> mergingSegmentIds = Lists.transform(mergingSegments, new Function<CubeSegment, String>() {
            @Nullable
            @Override
            public String apply(CubeSegment input) {
                return input.getUuid();
            }
        });
        result.addTask(createUpdateCubeInfoAfterMergeStep(mergingSegmentIds, convertCuboidToHfileStep.getId(), jobId));

        return result;
    }

    private CubingJob initialJob(String type) {
        CubingJob result = new CubingJob();
        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone(jobEngineConfig.getTimeZone()));
        result.setCubeName(getCubeName());
        result.setSegmentId(segment.getUuid());
        result.setName(getCubeName() + " - " + segment.getName() + " - " + type + " - " + format.format(new Date(System.currentTimeMillis())));
        result.setSubmitter(this.submitter);
        return result;
    }

    private void checkPreconditions() {
        Preconditions.checkNotNull(this.segment, "segment cannot be null");
        Preconditions.checkNotNull(this.jobEngineConfig, "jobEngineConfig cannot be null");
    }

    private String getPathToMerge(CubeSegment segment) {
        return getJobWorkingDir(segment.getLastBuildJobID()) + "/" + getCubeName() + "/cuboid/*";
    }

    private String getCubeName() {
        return ((CubeSegment) segment).getCubeInstance().getName();
    }

    private String getRowkeyDistributionOutputPath() {
        return jobEngineConfig.getHdfsWorkingDirectory() + "/" + getCubeName() + "/rowkey_stats";
    }

    private void appendMapReduceParameters(StringBuilder builder, JobEngineConfig engineConfig) {
        try {
            String jobConf = engineConfig.getHadoopJobConfFilePath(getCubeDesc().getCapacity());
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

    private String getFactDistinctColumnsPath(String jobUuid) {
        return getJobWorkingDir(jobUuid) + "/" + getCubeName() + "/fact_distinct_columns";
    }

    private String getHTableName() {
        return ((CubeSegment) segment).getStorageLocationIdentifier();
    }

    private String getHFilePath(String jobId) {
        return getJobWorkingDir(jobId) + "/" + getCubeName() + "/hfile/";
    }

    private MapReduceExecutable createFactDistinctColumnsStep(String intermediateHiveTableName, String jobId) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName(ExecutableConstants.STEP_NAME_FACT_DISTINCT_COLUMNS);
        result.setMapReduceJobClass(FactDistinctColumnsJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, jobEngineConfig);
        appendExecCmdParameters(cmd, "cubename", getCubeName());
        appendExecCmdParameters(cmd, "output", getFactDistinctColumnsPath(jobId));
        appendExecCmdParameters(cmd, "jobname", "Kylin_Fact_Distinct_Columns_" + getCubeName() + "_Step");
        appendExecCmdParameters(cmd, "tablename", intermediateHiveTableName);

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    private HadoopShellExecutable createBuildDictionaryStep(String factDistinctColumnsPath) {
        // base cuboid job
        HadoopShellExecutable buildDictionaryStep = new HadoopShellExecutable();
        buildDictionaryStep.setName(ExecutableConstants.STEP_NAME_BUILD_DICTIONARY);
        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "cubename", getCubeName());
        appendExecCmdParameters(cmd, "segmentname", segment.getName());
        appendExecCmdParameters(cmd, "input", factDistinctColumnsPath);

        buildDictionaryStep.setJobParams(cmd.toString());
        buildDictionaryStep.setJobClass(CreateDictionaryJob.class);
        return buildDictionaryStep;
    }

    private MapReduceExecutable createBaseCuboidStep(String intermediateHiveTableLocation, String[] cuboidOutputTempPath) {
        // base cuboid job
        MapReduceExecutable baseCuboidStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, jobEngineConfig);

        baseCuboidStep.setName(ExecutableConstants.STEP_NAME_BUILD_BASE_CUBOID);

        appendExecCmdParameters(cmd, "cubename", getCubeName());
        appendExecCmdParameters(cmd, "segmentname", segment.getName());
        appendExecCmdParameters(cmd, "input", intermediateHiveTableLocation);
        appendExecCmdParameters(cmd, "output", cuboidOutputTempPath[0]);
        appendExecCmdParameters(cmd, "jobname", "Kylin_Base_Cuboid_Builder_" + getCubeName());
        appendExecCmdParameters(cmd, "level", "0");

        baseCuboidStep.setMapReduceParams(cmd.toString());
        baseCuboidStep.setMapReduceJobClass(BaseCuboidJob.class);
        return baseCuboidStep;
    }

    private MapReduceExecutable createNDimensionCuboidStep(String[] cuboidOutputTempPath, int dimNum, int totalRowkeyColumnCount) {
        // ND cuboid job
        MapReduceExecutable ndCuboidStep = new MapReduceExecutable();

        ndCuboidStep.setName(ExecutableConstants.STEP_NAME_BUILD_N_D_CUBOID + " : " + dimNum + "-Dimension");
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd, jobEngineConfig);
        appendExecCmdParameters(cmd, "cubename", getCubeName());
        appendExecCmdParameters(cmd, "segmentname", segment.getName());
        appendExecCmdParameters(cmd, "input", cuboidOutputTempPath[totalRowkeyColumnCount - dimNum - 1]);
        appendExecCmdParameters(cmd, "output", cuboidOutputTempPath[totalRowkeyColumnCount - dimNum]);
        appendExecCmdParameters(cmd, "jobname", "Kylin_ND-Cuboid_Builder_" + getCubeName() + "_Step");
        appendExecCmdParameters(cmd, "level", "" + (totalRowkeyColumnCount - dimNum));

        ndCuboidStep.setMapReduceParams(cmd.toString());
        ndCuboidStep.setMapReduceJobClass(NDCuboidJob.class);
        return ndCuboidStep;
    }

    private MapReduceExecutable createRangeRowkeyDistributionStep(String inputPath) {
        MapReduceExecutable rowkeyDistributionStep = new MapReduceExecutable();
        rowkeyDistributionStep.setName(ExecutableConstants.STEP_NAME_GET_CUBOID_KEY_DISTRIBUTION);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd, jobEngineConfig);
        appendExecCmdParameters(cmd, "input", inputPath);
        appendExecCmdParameters(cmd, "output", getRowkeyDistributionOutputPath());
        appendExecCmdParameters(cmd, "cubename", getCubeName());
        appendExecCmdParameters(cmd, "jobname", "Kylin_Region_Splits_Calculator_" + getCubeName() + "_Step");

        rowkeyDistributionStep.setMapReduceParams(cmd.toString());
        rowkeyDistributionStep.setMapReduceJobClass(RangeKeyDistributionJob.class);
        return rowkeyDistributionStep;
    }

    private HadoopShellExecutable createCreateHTableStep() {
        HadoopShellExecutable createHtableStep = new HadoopShellExecutable();
        createHtableStep.setName(ExecutableConstants.STEP_NAME_CREATE_HBASE_TABLE);
        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "cubename", getCubeName());
        appendExecCmdParameters(cmd, "input", getRowkeyDistributionOutputPath() + "/part-r-00000");
        appendExecCmdParameters(cmd, "htablename", getHTableName());

        createHtableStep.setJobParams(cmd.toString());
        createHtableStep.setJobClass(CreateHTableJob.class);

        return createHtableStep;
    }

    private MapReduceExecutable createConvertCuboidToHfileStep(String inputPath, String jobId) {
        MapReduceExecutable createHFilesStep = new MapReduceExecutable();
        createHFilesStep.setName(ExecutableConstants.STEP_NAME_CONVERT_CUBOID_TO_HFILE);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd, jobEngineConfig);
        appendExecCmdParameters(cmd, "cubename", getCubeName());
        appendExecCmdParameters(cmd, "input", inputPath);
        appendExecCmdParameters(cmd, "output", getHFilePath(jobId));
        appendExecCmdParameters(cmd, "htablename", getHTableName());
        appendExecCmdParameters(cmd, "jobname", "Kylin_HFile_Generator_" + getCubeName() + "_Step");

        createHFilesStep.setMapReduceParams(cmd.toString());
        createHFilesStep.setMapReduceJobClass(CubeHFileJob.class);

        return createHFilesStep;
    }

    private HadoopShellExecutable createBulkLoadStep(String jobId) {
        HadoopShellExecutable bulkLoadStep = new HadoopShellExecutable();
        bulkLoadStep.setName(ExecutableConstants.STEP_NAME_BULK_LOAD_HFILE);

        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "input", getHFilePath(jobId));
        appendExecCmdParameters(cmd, "htablename", getHTableName());
        appendExecCmdParameters(cmd, "cubename", getCubeName());

        bulkLoadStep.setJobParams(cmd.toString());
        bulkLoadStep.setJobClass(BulkLoadJob.class);

        return bulkLoadStep;

    }

    private UpdateCubeInfoAfterBuildStep createUpdateCubeInfoStep(String createFlatTableStepId, String baseCuboidStepId, String convertToHFileStepId, String jobId) {
        final UpdateCubeInfoAfterBuildStep updateCubeInfoStep = new UpdateCubeInfoAfterBuildStep();
        updateCubeInfoStep.setName(ExecutableConstants.STEP_NAME_UPDATE_CUBE_INFO);
        updateCubeInfoStep.setCubeName(getCubeName());
        updateCubeInfoStep.setSegmentId(segment.getUuid());
        updateCubeInfoStep.setCreateFlatTableStepId(createFlatTableStepId);
        updateCubeInfoStep.setBaseCuboidStepId(baseCuboidStepId);
        updateCubeInfoStep.setConvertToHFileStepId(convertToHFileStepId);
        updateCubeInfoStep.setCubingJobId(jobId);
        return updateCubeInfoStep;
    }

    private MapReduceExecutable createMergeCuboidDataStep(String inputPath, String outputPath) {
        MapReduceExecutable mergeCuboidDataStep = new MapReduceExecutable();
        mergeCuboidDataStep.setName(ExecutableConstants.STEP_NAME_MERGE_CUBOID);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd, jobEngineConfig);
        appendExecCmdParameters(cmd, "cubename", getCubeName());
        appendExecCmdParameters(cmd, "segmentname", segment.getName());
        appendExecCmdParameters(cmd, "input", inputPath);
        appendExecCmdParameters(cmd, "output", outputPath);
        appendExecCmdParameters(cmd, "jobname", "Kylin_Merge_Cuboid_" + getCubeName() + "_Step");

        mergeCuboidDataStep.setMapReduceParams(cmd.toString());
        mergeCuboidDataStep.setMapReduceJobClass(MergeCuboidJob.class);
        return mergeCuboidDataStep;
    }

    private UpdateCubeInfoAfterMergeStep createUpdateCubeInfoAfterMergeStep(List<String> mergingSegmentIds, String convertToHFileStepId, String jobId) {
        UpdateCubeInfoAfterMergeStep result = new UpdateCubeInfoAfterMergeStep();
        result.setName(ExecutableConstants.STEP_NAME_UPDATE_CUBE_INFO);
        result.setCubeName(getCubeName());
        result.setSegmentId(segment.getUuid());
        result.setMergingSegmentIds(mergingSegmentIds);
        result.setConvertToHFileStepId(convertToHFileStepId);
        result.setCubingJobId(jobId);
        return result;
    }

}
