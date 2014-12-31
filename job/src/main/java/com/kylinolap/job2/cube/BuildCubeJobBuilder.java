package com.kylinolap.job2.cube;

import com.kylinolap.cube.CubeSegment;
import com.kylinolap.job.JoinedFlatTable;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.hadoop.cube.FactDistinctColumnsJob;
import com.kylinolap.job.hadoop.dict.CreateDictionaryJob;
import com.kylinolap.job.hadoop.hive.CubeJoinedFlatTableDesc;
import com.kylinolap.job2.common.HadoopShellExecutable;
import com.kylinolap.job2.common.MapReduceExecutable;
import com.kylinolap.job2.common.ShellExecutable;
import org.apache.commons.lang3.StringUtils;

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
        BuildCubeJob result = new BuildCubeJob();
        final CubeJoinedFlatTableDesc intermediateTableDesc = new CubeJoinedFlatTableDesc(segment.getCubeDesc(), this.segment);
        final ShellExecutable intermediateHiveTableStep = createIntermediateHiveTableStep(intermediateTableDesc);
        final String intermediateHiveTableName = getIntermediateHiveTableName(intermediateTableDesc, intermediateHiveTableStep.getId());
        result.addTask(intermediateHiveTableStep);

        final MapReduceExecutable factDistinctColumnsStep = createFactDistinctColumnsStep(intermediateHiveTableName);
        result.addTask(factDistinctColumnsStep);
        final String factDistinctColumnsPath = getFactDistinctColumnsPath(factDistinctColumnsStep.getId());

        final HadoopShellExecutable buildDictionaryStep = createBuildDictionaryStep(factDistinctColumnsPath);
        result.addTask(buildDictionaryStep);

        return result;
    }

    private String getJobWorkingDir(String jobUuid) {
        return jobEngineConfig.getHdfsWorkingDirectory() + "/" + JOB_WORKING_DIR_PREFIX + jobUuid;
    }

    private String getCubeName() {
        return segment.getCubeInstance().getName();
    }

    private StringBuilder appendMapReduceParameters(JobEngineConfig engineConfig, StringBuilder builder) {
        try {
            String jobConf = engineConfig.getHadoopJobConfFilePath(segment.getCubeDesc().getCapacity());
            if (StringUtils.isBlank(jobConf) == false) {
                builder.append(" -conf ").append(jobConf);
            }
            return builder;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private StringBuilder appendExecCmdParameters(StringBuilder cmd, String paraName, String paraValue) {
        return cmd.append(" -").append(paraName).append(" ").append(paraValue);
    }

    private String getIntermediateHiveTableName(CubeJoinedFlatTableDesc intermediateTableDesc, String jobUuid) {
        return JoinedFlatTable.getTableDir(intermediateTableDesc, getJobWorkingDir(jobUuid), jobUuid);
    }

    private ShellExecutable createIntermediateHiveTableStep(CubeJoinedFlatTableDesc intermediateTableDesc) {
        try {
            ShellExecutable result = new ShellExecutable();
            result.setName(JobConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);
            String jobUUID = result.getId();
            String dropTableHql = JoinedFlatTable.generateDropTableStatement(intermediateTableDesc, jobUUID);
            String createTableHql = JoinedFlatTable.generateCreateTableStatement(intermediateTableDesc, getJobWorkingDir(jobUUID), jobUUID);
            String insertDataHql = JoinedFlatTable.generateInsertDataStatement(intermediateTableDesc, jobUUID, this.jobEngineConfig);


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

    private String getFactDistinctColumnsPath(String jobUuid) {
        return getJobWorkingDir(jobUuid) + "/" + getCubeName() + "/fact_distinct_columns";
    }

    private MapReduceExecutable createFactDistinctColumnsStep(String intermediateHiveTableName) {
        MapReduceExecutable result = new MapReduceExecutable();
        result.setName(JobConstants.STEP_NAME_FACT_DISTINCT_COLUMNS);
        result.setMapReduceJobClass(FactDistinctColumnsJob.class);
        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(jobEngineConfig, cmd);
        appendExecCmdParameters(cmd, "cubename", segment.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "input", intermediateHiveTableName);
        appendExecCmdParameters(cmd, "output", getFactDistinctColumnsPath(result.getId()));
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

}
