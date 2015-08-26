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

package org.apache.kylin.job;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.hadoop.hive.IJoinedFlatTableDesc;

public abstract class AbstractJobBuilder {

    protected static final String JOB_WORKING_DIR_PREFIX = "kylin-";

    protected JobEngineConfig engineConfig;
    protected String submitter;

    public AbstractJobBuilder(JobEngineConfig engineConfig) {
        this.engineConfig = engineConfig;
    }

    public AbstractJobBuilder setSubmitter(String submitter) {
        this.submitter = submitter;
        return this;
    }

    public String getSubmitter() {
        return submitter;
    }

    protected StringBuilder appendExecCmdParameters(StringBuilder cmd, String paraName, String paraValue) {
        return cmd.append(" -").append(paraName).append(" ").append(paraValue);
    }

    // return in full-qualified name, that is "dbname.tablename"
    protected String getIntermediateHiveTableName(IJoinedFlatTableDesc intermediateTableDesc, String jobUuid) {
        return engineConfig.getConfig().getHiveDatabaseForIntermediateTable() + "." + intermediateTableDesc.getTableName(jobUuid);
    }

    protected String getIntermediateHiveTableLocation(IJoinedFlatTableDesc intermediateTableDesc, String jobUUID) {
        return getJobWorkingDir(jobUUID) + "/" + intermediateTableDesc.getTableName(jobUUID);
    }

    protected AbstractExecutable createIntermediateHiveTableStep(IJoinedFlatTableDesc intermediateTableDesc, String jobId) {

        final String useDatabaseHql = "USE " + engineConfig.getConfig().getHiveDatabaseForIntermediateTable() + ";";
        final String setClusterHql = "-hiveconf " + FileSystem.FS_DEFAULT_NAME_KEY + "=\"" + HadoopUtil.getCurrentConfiguration().get(FileSystem.FS_DEFAULT_NAME_KEY) + "\"";
        final String dropTableHql = JoinedFlatTable.generateDropTableStatement(intermediateTableDesc, jobId);
        final String createTableHql = JoinedFlatTable.generateCreateTableStatement(intermediateTableDesc, getJobWorkingDir(jobId), jobId);
        String insertDataHqls;
        try {
            insertDataHqls = JoinedFlatTable.generateInsertDataStatement(intermediateTableDesc, jobId, this.engineConfig);
        } catch (IOException e1) {
            e1.printStackTrace();
            throw new RuntimeException("Failed to generate insert data SQL for intermediate table.");
        }

        ShellExecutable step = new ShellExecutable();
        StringBuffer buf = new StringBuffer();
        buf.append("hive ");
        buf.append(setClusterHql);
        buf.append(" -e \"");
        buf.append(useDatabaseHql + "\n");
        buf.append(dropTableHql + "\n");
        buf.append(createTableHql + "\n");
        buf.append(insertDataHqls + "\n");
        buf.append("\"");

        step.setCmd(buf.toString());
        step.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);

        return step;
    }

    protected String getJobWorkingDir(String uuid) {
        return engineConfig.getHdfsWorkingDirectory() + "/" + JOB_WORKING_DIR_PREFIX + uuid;
    }
}
