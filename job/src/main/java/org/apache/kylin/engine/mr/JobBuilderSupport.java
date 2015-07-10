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

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.common.HadoopShellExecutable;
import org.apache.kylin.job.common.MapReduceExecutable;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.hadoop.cube.CubeHFileJob;
import org.apache.kylin.job.hadoop.cube.RangeKeyDistributionJob;
import org.apache.kylin.job.hadoop.hbase.BulkLoadJob;
import org.apache.kylin.job.hadoop.hbase.CreateHTableJob;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;

import com.google.common.base.Preconditions;

/**
 * Hold reusable methods for real builders.
 */
abstract public class JobBuilderSupport {

    protected final JobEngineConfig config;
    protected final CubeSegment seg;
    protected final String submitter;

    protected JobBuilderSupport(CubeSegment seg, String submitter) {
        Preconditions.checkNotNull(seg, "segment cannot be null");
        this.config = new JobEngineConfig(KylinConfig.getInstanceFromEnv());
        this.seg = seg;
        this.submitter = submitter;
    }
    
    protected AbstractExecutable createFlatHiveTableStep(IJoinedFlatTableDesc flatTableDesc, String jobId) {
        return createFlatHiveTableStep(config, flatTableDesc, jobId);
    }
    
    protected MapReduceExecutable createRangeRowkeyDistributionStep(String inputPath, String jobId) {
        MapReduceExecutable rowkeyDistributionStep = new MapReduceExecutable();
        rowkeyDistributionStep.setName(ExecutableConstants.STEP_NAME_GET_CUBOID_KEY_DISTRIBUTION);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd, seg);
        appendExecCmdParameters(cmd, "input", inputPath);
        appendExecCmdParameters(cmd, "output", getRowkeyDistributionOutputPath(jobId));
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "jobname", "Kylin_Region_Splits_Calculator_" + seg.getCubeInstance().getName() + "_Step");

        rowkeyDistributionStep.setMapReduceParams(cmd.toString());
        rowkeyDistributionStep.setMapReduceJobClass(RangeKeyDistributionJob.class);
        return rowkeyDistributionStep;
    }

    protected HadoopShellExecutable createCreateHTableStep(String jobId) {
        HadoopShellExecutable createHtableStep = new HadoopShellExecutable();
        createHtableStep.setName(ExecutableConstants.STEP_NAME_CREATE_HBASE_TABLE);
        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "segmentname", seg.getName());
        appendExecCmdParameters(cmd, "input", getRowkeyDistributionOutputPath(jobId) + "/part-r-00000");
        appendExecCmdParameters(cmd, "htablename", seg.getStorageLocationIdentifier());
        appendExecCmdParameters(cmd, "statisticsenabled", String.valueOf(config.isInMemCubing()));

        createHtableStep.setJobParams(cmd.toString());
        createHtableStep.setJobClass(CreateHTableJob.class);

        return createHtableStep;
    }

    protected MapReduceExecutable createConvertCuboidToHfileStep(String inputPath, String jobId) {
        MapReduceExecutable createHFilesStep = new MapReduceExecutable();
        createHFilesStep.setName(ExecutableConstants.STEP_NAME_CONVERT_CUBOID_TO_HFILE);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd, seg);
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "input", inputPath);
        appendExecCmdParameters(cmd, "output", getHFilePath(jobId));
        appendExecCmdParameters(cmd, "htablename", seg.getStorageLocationIdentifier());
        appendExecCmdParameters(cmd, "jobname", "Kylin_HFile_Generator_" + seg.getCubeInstance().getName() + "_Step");

        createHFilesStep.setMapReduceParams(cmd.toString());
        createHFilesStep.setMapReduceJobClass(CubeHFileJob.class);
        createHFilesStep.setCounterSaveAs(",," + CubingJob.CUBE_SIZE_BYTES);

        return createHFilesStep;
    }

    protected HadoopShellExecutable createBulkLoadStep(String jobId) {
        HadoopShellExecutable bulkLoadStep = new HadoopShellExecutable();
        bulkLoadStep.setName(ExecutableConstants.STEP_NAME_BULK_LOAD_HFILE);

        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "input", getHFilePath(jobId));
        appendExecCmdParameters(cmd, "htablename", seg.getStorageLocationIdentifier());
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());

        bulkLoadStep.setJobParams(cmd.toString());
        bulkLoadStep.setJobClass(BulkLoadJob.class);

        return bulkLoadStep;
    }

    protected GarbageCollectionStep createGarbageCollectionStep(List<String> oldHtables, String interimTable) {
        GarbageCollectionStep result = new GarbageCollectionStep();
        result.setName(ExecutableConstants.STEP_NAME_GARBAGE_COLLECTION);
        result.setOldHTables(oldHtables);
        result.setOldHiveTable(interimTable);
        return result;
    }

    protected String getJobWorkingDir(String jobId) {
        return getJobWorkingDir(config, jobId);
    }
    
    protected String getCuboidRootPath(String jobId) {
        return getJobWorkingDir(jobId) + "/" + seg.getCubeInstance().getName() + "/cuboid/";
    }
    
    protected String getCuboidRootPath(CubeSegment seg) {
        return getCuboidRootPath(seg.getLastBuildJobID());
    }
    
    protected void appendMapReduceParameters(StringBuilder buf, CubeSegment seg) {
        try {
            String jobConf = config.getHadoopJobConfFilePath(seg.getCubeDesc().getModel().getCapacity());
            if (jobConf != null && jobConf.length() > 0) {
                buf.append(" -conf ").append(jobConf);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected String getFactDistinctColumnsPath(String jobId) {
        return getJobWorkingDir(jobId) + "/" + seg.getCubeInstance().getName() + "/fact_distinct_columns";
    }


    protected String getStatisticsPath(String jobId) {
        return getJobWorkingDir(jobId) + "/" + seg.getCubeInstance().getName() + "/statistics";
    }


    protected String getHFilePath(String jobId) {
        return getJobWorkingDir(jobId) + "/" + seg.getCubeInstance().getName() + "/hfile/";
    }

    protected String getRowkeyDistributionOutputPath(String jobId) {
        return getJobWorkingDir(jobId) + "/" + seg.getCubeInstance().getName() + "/rowkey_stats";
    }

    // ============================================================================
    // static methods also shared by IIJobBuilder
    // ----------------------------------------------------------------------------

    public static AbstractExecutable createFlatHiveTableStep(JobEngineConfig conf, IJoinedFlatTableDesc flatTableDesc, String jobId) {

        final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatTableDesc, jobId);
        final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatTableDesc, getJobWorkingDir(conf, jobId), jobId);
        String insertDataHqls;
        try {
            insertDataHqls = JoinedFlatTable.generateInsertDataStatement(flatTableDesc, jobId, conf);
        } catch (IOException e1) {
            e1.printStackTrace();
            throw new RuntimeException("Failed to generate insert data SQL for intermediate table.");
        }

        ShellExecutable step = new ShellExecutable();
        StringBuffer buf = new StringBuffer();
        buf.append("hive -e \"");
        buf.append(dropTableHql + "\n");
        buf.append(createTableHql + "\n");
        buf.append(insertDataHqls + "\n");
        buf.append("\"");

        step.setCmd(buf.toString());
        step.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);

        return step;
    }

    public static String getJobWorkingDir(JobEngineConfig conf, String jobId) {
        return conf.getHdfsWorkingDirectory() + "/" + "kylin-" + jobId;
    }

    public static StringBuilder appendExecCmdParameters(StringBuilder buf, String paraName, String paraValue) {
        return buf.append(" -").append(paraName).append(" ").append(paraValue);
    }

}
