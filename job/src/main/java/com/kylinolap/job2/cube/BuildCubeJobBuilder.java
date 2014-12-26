package com.kylinolap.job2.cube;

import com.kylinolap.cube.CubeSegment;
import com.kylinolap.job.JoinedFlatTable;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.hadoop.hive.JoinedFlatTableDesc;
import com.kylinolap.job2.common.CommonJob;
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

    private String getJobWorkingDir(String jobUuid) {
        return jobEngineConfig.getHdfsWorkingDirectory() + "/" + JOB_WORKING_DIR_PREFIX + jobUuid;
    }

    public static BuildCubeJobBuilder newBuilder(JobEngineConfig engineCfg, CubeSegment segment) {
        return new BuildCubeJobBuilder(engineCfg, segment);
    }

    public CommonJob build() {
        CommonJob result = new CommonJob();
        result.addTask(createIntermediateHiveTableStep());
        return result;
    }

    private ShellExecutable createIntermediateHiveTableStep() {
        try {
            ShellExecutable result = new ShellExecutable();
            result.setName(JobConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);
            String jobUUID = result.getId();
            JoinedFlatTableDesc intermediateTableDesc = new JoinedFlatTableDesc(segment.getCubeDesc(), this.segment);
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
}
