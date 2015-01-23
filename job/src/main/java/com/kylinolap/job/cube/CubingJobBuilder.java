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
import com.kylinolap.job.execution.AbstractExecutable;

/**
 * Created by qianzhou on 12/25/14.
 */
public final class CubingJobBuilder extends AbstractJobBuilder {

    public CubingJobBuilder(JobEngineConfig engineConfig) {
        super(engineConfig);
    }

    public CubingJob buildJob(CubeSegment seg) {
        checkPreconditions(seg);
        CubingJob result = initialJob(seg, "BUILD");

        final int groupRowkeyColumnsCount = seg.getCubeDesc().getRowkey().getNCuboidBuildLevels();
        final int totalRowkeyColumnsCount = seg.getCubeDesc().getRowkey().getRowKeyColumns().length;

        final String jobId = result.getId();
        final CubeJoinedFlatTableDesc intermediateTableDesc = new CubeJoinedFlatTableDesc(seg.getCubeDesc(), seg);
        final String intermediateHiveTableName = getIntermediateHiveTableName(intermediateTableDesc, jobId);
        final String intermediateHiveTableLocation = getIntermediateHiveTableLocation(intermediateTableDesc, jobId);
        final String factDistinctColumnsPath = getFactDistinctColumnsPath(seg, jobId);
        final String cuboidRootPath = getJobWorkingDir(jobId) + "/" + seg.getCubeInstance().getName() + "/cuboid/";
        final String cuboidPath = cuboidRootPath + "*";
        final String[] cuboidOutputTempPath = getCuboidOutputPaths(cuboidRootPath, totalRowkeyColumnsCount, groupRowkeyColumnsCount);

        final AbstractExecutable intermediateHiveTableStep = createIntermediateHiveTableStep(intermediateTableDesc, jobId);
        result.addTask(intermediateHiveTableStep);

        result.addTask(createFactDistinctColumnsStep(seg, intermediateHiveTableName, jobId));

        result.addTask(createBuildDictionaryStep(seg, factDistinctColumnsPath));

        // base cuboid step
        final MapReduceExecutable baseCuboidStep = createBaseCuboidStep(seg, intermediateHiveTableLocation, cuboidOutputTempPath);
        result.addTask(baseCuboidStep);

        // n dim cuboid steps
        for (int i = 1; i <= groupRowkeyColumnsCount; i++) {
            int dimNum = totalRowkeyColumnsCount - i;
            result.addTask(createNDimensionCuboidStep(seg, cuboidOutputTempPath, dimNum, totalRowkeyColumnsCount));
        }

        result.addTask(createRangeRowkeyDistributionStep(seg, cuboidPath));
        // create htable step
        result.addTask(createCreateHTableStep(seg));
        // generate hfiles step
        final MapReduceExecutable convertCuboidToHfileStep = createConvertCuboidToHfileStep(seg, cuboidPath, jobId);
        result.addTask(convertCuboidToHfileStep);
        // bulk load step
        result.addTask(createBulkLoadStep(seg, jobId));

        result.addTask(createUpdateCubeInfoStep(seg, intermediateHiveTableStep.getId(), baseCuboidStep.getId(), convertCuboidToHfileStep.getId(), jobId));

        return result;
    }

    public CubingJob mergeJob(CubeSegment seg) {
        checkPreconditions(seg);
        CubingJob result = initialJob(seg, "MERGE");
        final String jobId = result.getId();
        List<CubeSegment> mergingSegments = seg.getCubeInstance().getMergingSegments(seg);
        Preconditions.checkState(mergingSegments != null && mergingSegments.size() > 1, "there should be more than 2 segments to merge");

        final List<String> mergingSegmentIds = Lists.transform(mergingSegments, new Function<CubeSegment, String>() {
            @Nullable
            @Override
            public String apply(CubeSegment input) {
                return input.getUuid();
            }
        });

        String[] cuboidPaths = new String[mergingSegments.size()];
        for (int i = 0; i < mergingSegments.size(); i++) {
            cuboidPaths[i] = getPathToMerge(mergingSegments.get(i));
        }
        final String formattedPath = StringUtils.join(cuboidPaths, ",");
        final String mergedCuboidPath = getJobWorkingDir(jobId) + "/" + seg.getCubeInstance().getName() + "/cuboid";

        result.addTask(createMergeDictionaryStep(seg, mergingSegmentIds));

        result.addTask(createMergeCuboidDataStep(seg, formattedPath, mergedCuboidPath));

        // get output distribution step
        result.addTask(createRangeRowkeyDistributionStep(seg, mergedCuboidPath));

        // create htable step
        result.addTask(createCreateHTableStep(seg));

        // generate hfiles step
        final MapReduceExecutable convertCuboidToHfileStep = createConvertCuboidToHfileStep(seg, mergedCuboidPath, jobId);
        result.addTask(convertCuboidToHfileStep);

        // bulk load step
        result.addTask(createBulkLoadStep(seg, jobId));

        result.addTask(createUpdateCubeInfoAfterMergeStep(seg, mergingSegmentIds, convertCuboidToHfileStep.getId(), jobId));

        return result;
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

    private void checkPreconditions(CubeSegment seg) {
        Preconditions.checkNotNull(seg, "segment cannot be null");
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
            String jobConf = engineConfig.getHadoopJobConfFilePath(seg.getCubeDesc().getCapacity());
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

    private UpdateCubeInfoAfterBuildStep createUpdateCubeInfoStep(CubeSegment seg, String createFlatTableStepId, String baseCuboidStepId, String convertToHFileStepId, String jobId) {
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
