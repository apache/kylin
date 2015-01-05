package com.kylinolap.job2.cube;

import com.kylinolap.cube.CubeSegment;
import com.kylinolap.job.JoinedFlatTable;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.hadoop.cube.*;
import com.kylinolap.job.hadoop.dict.CreateDictionaryJob;
import com.kylinolap.job.hadoop.hbase.BulkLoadJob;
import com.kylinolap.job.hadoop.hbase.CreateHTableJob;
import com.kylinolap.job.hadoop.hive.CubeJoinedFlatTableDesc;
import com.kylinolap.job2.common.HadoopShellExecutable;
import com.kylinolap.job2.common.MapReduceExecutable;
import com.kylinolap.job2.common.ShellExecutable;

import java.io.IOException;

/**
 * Created by qianzhou on 12/25/14.
 */
public final class BuildCubeJobBuilder {

    private static final String JOB_WORKING_DIR_PREFIX = "kylin-";

    private final JobEngineConfig jobEngineConfig;
    private final CubeSegment segment;

    private BuildCubeJobBuilder(JobEngineConfig engineCfg, CubeSegment segment) {
        this.jobEngineConfig = engineCfg;
        this.segment = segment;
    }

    public static BuildCubeJobBuilder newBuilder(JobEngineConfig engineCfg, CubeSegment segment) {
        return new BuildCubeJobBuilder(engineCfg, segment);
    }

    public BuildCubeJob build() {
        final int groupRowkeyColumnsCount = segment.getCubeDesc().getRowkey().getNCuboidBuildLevels();
        final int totalRowkeyColumnsCount = segment.getCubeDesc().getRowkey().getRowKeyColumns().length;

        BuildCubeJob result = new BuildCubeJob();
        final String jobId = result.getId();
        final CubeJoinedFlatTableDesc intermediateTableDesc = new CubeJoinedFlatTableDesc(segment.getCubeDesc(), this.segment);
        final String intermediateHiveTableName = getIntermediateHiveTableName(intermediateTableDesc, jobId);
        final String factDistinctColumnsPath = getFactDistinctColumnsPath(jobId);
        final String cuboidRootPath = getJobWorkingDir(jobId) + "/" + getCubeName() + "/cuboid/";
        final String cuboidPath = cuboidRootPath + "*";
        final String[] cuboidOutputTempPath = getCuboidOutputPaths(cuboidRootPath, totalRowkeyColumnsCount, groupRowkeyColumnsCount);

        final ShellExecutable intermediateHiveTableStep = createIntermediateHiveTableStep(intermediateTableDesc, jobId);
        result.addTask(intermediateHiveTableStep);

        result.addTask(createFactDistinctColumnsStep(intermediateHiveTableName, jobId));

        result.addTask(createBuildDictionaryStep(factDistinctColumnsPath));

        // base cuboid step
        final MapReduceExecutable baseCuboidStep = createBaseCuboidStep(intermediateHiveTableName, cuboidOutputTempPath);
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

        result.addTask(createUpdateCubeInfoStep(intermediateHiveTableStep.getId(), baseCuboidStep.getId(), convertCuboidToHfileStep.getId()));

        return result;
    }

    private String getJobWorkingDir(String jobUuid) {
        return jobEngineConfig.getHdfsWorkingDirectory() + "/" + JOB_WORKING_DIR_PREFIX + jobUuid;
    }

    private String getCubeName() {
        return segment.getCubeInstance().getName();
    }

    private String getSegmentName() {
        return segment.getName();
    }

    private String getRowkeyDistributionOutputPath() {
        return jobEngineConfig.getHdfsWorkingDirectory() + "/" + getCubeName() + "/rowkey_stats";
    }

    private void appendMapReduceParameters(StringBuilder builder, JobEngineConfig engineConfig) {
        try {
            String jobConf = engineConfig.getHadoopJobConfFilePath(segment.getCubeDesc().getCapacity());
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

    private StringBuilder appendExecCmdParameters(StringBuilder cmd, String paraName, String paraValue) {
        return cmd.append(" -").append(paraName).append(" ").append(paraValue);
    }

    private String getIntermediateHiveTableName(CubeJoinedFlatTableDesc intermediateTableDesc, String jobUuid) {
        return JoinedFlatTable.getTableDir(intermediateTableDesc, getJobWorkingDir(jobUuid), jobUuid);
    }

    private String getFactDistinctColumnsPath(String jobUuid) {
        return getJobWorkingDir(jobUuid) + "/" + getCubeName() + "/fact_distinct_columns";
    }

    private String getHTableName() {
        return segment.getStorageLocationIdentifier();
    }

    private String getHFilePath(String jobId) {
        return getJobWorkingDir(jobId) + "/" + getCubeName() + "/hfile/";
    }

    private ShellExecutable createIntermediateHiveTableStep(CubeJoinedFlatTableDesc intermediateTableDesc, String jobId) {
        try {
            ShellExecutable result = new ShellExecutable();
            result.setName(JobConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);
            String dropTableHql = JoinedFlatTable.generateDropTableStatement(intermediateTableDesc, jobId);
            String createTableHql = JoinedFlatTable.generateCreateTableStatement(intermediateTableDesc, getJobWorkingDir(jobId), jobId);
            String insertDataHql = JoinedFlatTable.generateInsertDataStatement(intermediateTableDesc, jobId, this.jobEngineConfig);


            StringBuilder buf = new StringBuilder();
            buf.append("hive -e \"");
            buf.append(dropTableHql + "\n");
            buf.append(createTableHql + "\n");
            buf.append(insertDataHql + "\n");
            buf.append("\"");

            result.setCmd(buf.toString());
            return result;
        } catch (IOException e) {
            throw new RuntimeException("fail to create job", e);
        }
    }

    private MapReduceExecutable createFactDistinctColumnsStep(String intermediateHiveTableName, String jobId) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName(JobConstants.STEP_NAME_FACT_DISTINCT_COLUMNS);
        result.setMapReduceJobClass(FactDistinctColumnsJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, jobEngineConfig);
        appendExecCmdParameters(cmd, "cubename", segment.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "input", intermediateHiveTableName);
        appendExecCmdParameters(cmd, "output", getFactDistinctColumnsPath(jobId));
        appendExecCmdParameters(cmd, "jobname", "Kylin_Fact_Distinct_Columns_" + getCubeName() + "_Step");

        result.setMapReduceParams(cmd.toString());
        return result;
    }

    private HadoopShellExecutable createBuildDictionaryStep(String factDistinctColumnsPath) {
        // base cuboid job
        HadoopShellExecutable buildDictionaryStep = new HadoopShellExecutable();
        buildDictionaryStep.setName(JobConstants.STEP_NAME_BUILD_DICTIONARY);
        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "cubename", getCubeName());
        appendExecCmdParameters(cmd, "segmentname", segment.getName());
        appendExecCmdParameters(cmd, "input", factDistinctColumnsPath);

        buildDictionaryStep.setMapReduceParams(cmd.toString());
        buildDictionaryStep.setJobClass(CreateDictionaryJob.class);
        return buildDictionaryStep;
    }

    private MapReduceExecutable createBaseCuboidStep(String intermediateHiveTableName, String[] cuboidOutputTempPath) {
        // base cuboid job
        MapReduceExecutable baseCuboidStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, jobEngineConfig);

        baseCuboidStep.setName(JobConstants.STEP_NAME_BUILD_BASE_CUBOID);

        appendExecCmdParameters(cmd, "cubename", getCubeName());
        appendExecCmdParameters(cmd, "segmentname", getSegmentName());
        appendExecCmdParameters(cmd, "input", intermediateHiveTableName);
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

        ndCuboidStep.setName(JobConstants.STEP_NAME_BUILD_N_D_CUBOID + " : " + dimNum + "-Dimension");
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd, jobEngineConfig);
        appendExecCmdParameters(cmd, "cubename", getCubeName());
        appendExecCmdParameters(cmd, "segmentname", getSegmentName());
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
        rowkeyDistributionStep.setName(JobConstants.STEP_NAME_GET_CUBOID_KEY_DISTRIBUTION);
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
        createHtableStep.setName(JobConstants.STEP_NAME_CREATE_HBASE_TABLE);
        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "cubename", getCubeName());
        appendExecCmdParameters(cmd, "input", getRowkeyDistributionOutputPath() + "/part-r-00000");
        appendExecCmdParameters(cmd, "htablename", getHTableName());

        createHtableStep.setMapReduceParams(cmd.toString());
        createHtableStep.setJobClass(CreateHTableJob.class);

        return createHtableStep;
    }

    private MapReduceExecutable createConvertCuboidToHfileStep(String inputPath, String jobId) {
        MapReduceExecutable createHFilesStep = new MapReduceExecutable();
        createHFilesStep.setName(JobConstants.STEP_NAME_CONVERT_CUBOID_TO_HFILE);
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
        bulkLoadStep.setName(JobConstants.STEP_NAME_BULK_LOAD_HFILE);

        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "input", getHFilePath(jobId));
        appendExecCmdParameters(cmd, "htablename", getHTableName());
        appendExecCmdParameters(cmd, "cubename", getCubeName());

        bulkLoadStep.setMapReduceParams(cmd.toString());
        bulkLoadStep.setJobClass(BulkLoadJob.class);

        return bulkLoadStep;

    }

    private UpdateCubeInfoExecutable createUpdateCubeInfoStep(String createFlatTableStepId, String baseCuboidStepId, String convertToHFileStepId) {
        final UpdateCubeInfoExecutable executable = new UpdateCubeInfoExecutable();
        executable.setCubeName(getCubeName());
        executable.setSegmentId(segment.getUuid());
        executable.setCreateFlatTableStepId(createFlatTableStepId);
        executable.setBaseCuboidStepId(baseCuboidStepId);
        executable.setConvertToHFileStepId(convertToHFileStepId);
        return executable;
    }


}
