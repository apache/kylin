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

package org.apache.kylin.job.cmd;

import org.apache.kylin.job.constant.JobStepStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xjiang
 * 
 */
public class ShellCmdOutput extends BaseCommandOutput implements ICommandOutput {

    protected static final Logger log = LoggerFactory.getLogger(ShellCmdOutput.class);

    protected StringBuilder output;
    protected int exitCode;
    protected JobStepStatusEnum status;

    public ShellCmdOutput() {
        init();
    }

    private void init() {
        output = new StringBuilder();
        exitCode = -1;
        status = JobStepStatusEnum.NEW;
    }

    @Override
    public JobStepStatusEnum getStatus() {
        return status;
    }

    @Override
    public void setStatus(JobStepStatusEnum s) {
        this.status = s;
    }

    @Override
    public String getOutput() {
        return output.toString();
    }

    @Override
    public void appendOutput(String message) {
        output.append(message).append(System.getProperty("line.separator"));
        log.debug(message);
    }

    @Override
    public int getExitCode() {
        return exitCode;
    }

    @Override
    public void setExitCode(int code) {
        exitCode = code;
    }

    @Override
    public void reset() {
        init();
    }

}
