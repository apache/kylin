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

import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.job.execution.AbstractExecutable;

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

    protected String getIntermediateHiveTableName(IJoinedFlatTableDesc intermediateTableDesc, String jobUuid) {
        return intermediateTableDesc.getTableName(jobUuid);
    }

    protected String getIntermediateHiveTableLocation(IJoinedFlatTableDesc intermediateTableDesc, String jobUUID) {
        return getJobWorkingDir(jobUUID) + "/" + intermediateTableDesc.getTableName(jobUUID);
    }

    protected String getJobWorkingDir(String uuid) {
        return engineConfig.getHdfsWorkingDirectory() + "/" + JOB_WORKING_DIR_PREFIX + uuid;
    }
}
