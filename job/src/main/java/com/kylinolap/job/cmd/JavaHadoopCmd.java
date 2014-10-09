/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.job.cmd;

import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.job.constant.JobStepStatusEnum;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.exception.JobException;
import com.kylinolap.job.hadoop.AbstractHadoopJob;

/**
 * @author xduo
 *
 */
public class JavaHadoopCmd implements IJobCommand {
    protected static final Logger log = LoggerFactory.getLogger(JavaHadoopCmd.class);

    private final String executeCommand;
    private final ICommandOutput output;
    private final AbstractHadoopJob job;

    public JavaHadoopCmd(String executeCommand, String jobInstanceID, int jobStepID,
            JobEngineConfig engineConfig, AbstractHadoopJob job, boolean isAsync) {
        super();
        this.executeCommand = executeCommand;
        this.job = job;
        this.output = new JavaHadoopCmdOutput(jobInstanceID, jobStepID, engineConfig, job, isAsync);
    }

    @Override
    public ICommandOutput execute() throws JobException {
        output.appendOutput("Start to execute command: \n" + this.executeCommand);
        String[] args = executeCommand.trim().split("\\s+");

        try {
            output.setStatus(JobStepStatusEnum.RUNNING);
            int exitCode = ToolRunner.run(job, args);
            output.setExitCode(exitCode);
        } catch (Exception e) {
            output.appendOutput(e.getLocalizedMessage());
            output.setExitCode(-1);
        }

        output.appendOutput("Command execute return code " + output.getExitCode());

        if (output.getExitCode() != 0) {
            output.setStatus(JobStepStatusEnum.ERROR);
        }

        return output;
    }

    @Override
    public void cancel() throws JobException {
        job.kill();
    }
}
