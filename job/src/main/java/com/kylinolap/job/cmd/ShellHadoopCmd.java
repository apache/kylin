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

import com.kylinolap.job.JobDAO;
import com.kylinolap.job.JobInstance;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.exception.JobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author xjiang
 * 
 */
public class ShellHadoopCmd extends ShellCmd {
    private static Logger log = LoggerFactory.getLogger(ShellHadoopCmd.class);

    private final String jobInstanceID;
    private final int jobStepID;
    private final JobEngineConfig engineConfig;

    public ShellHadoopCmd(String executeCmd, String host, String user, String password, boolean async, String instanceID, int stepID, JobEngineConfig engineConfig) {
        super(executeCmd, new ShellHadoopCmdOutput(instanceID, stepID, engineConfig), host, user, password, async);
        this.jobInstanceID = instanceID;
        this.jobStepID = stepID;
        this.engineConfig = engineConfig;
    }

    @Override
    public void cancel() throws JobException {
        JobDAO jobDAO = JobDAO.getInstance(engineConfig.getConfig());
        JobInstance jobInstance = null;
        try {
            jobInstance = jobDAO.getJob(jobInstanceID);
            String mrJobId = jobInstance.getSteps().get(jobStepID).getInfo(JobInstance.MR_JOB_ID);
            log.debug("kill MR job " + mrJobId);
            executeCommand("hadoop job -kill " + mrJobId);
        } catch (IOException e) {
            throw new JobException(e);
        }
    }
}
