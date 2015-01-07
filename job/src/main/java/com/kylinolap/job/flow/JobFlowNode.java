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

package com.kylinolap.job.flow;

import java.io.IOException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.job.JobDAO;
import com.kylinolap.job.JobInstance;
import com.kylinolap.job.JobInstance.JobStep;
import com.kylinolap.job.cmd.ICommandOutput;
import com.kylinolap.job.cmd.IJobCommand;
import com.kylinolap.job.cmd.JobCommandFactory;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.constant.JobStepStatusEnum;
import com.kylinolap.job.engine.JobEngine;
import com.kylinolap.job.engine.JobEngineConfig;

/**
 * @author xduo
 * 
 */
public class JobFlowNode implements InterruptableJob {

    protected static final Logger log = LoggerFactory.getLogger(JobFlowNode.class);

    protected JobDetail currentJobDetail;
    protected IJobCommand jobCmd;

    /*
     * (non-Javadoc)
     * 
     * @see org.quartz.Job#execute(org.quartz.JobExecutionContext)
     */
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        this.currentJobDetail = context.getJobDetail();
        JobDataMap data = this.currentJobDetail.getJobDataMap();
        JobFlow jobFlow = (JobFlow) data.get(JobConstants.PROP_JOB_FLOW);
        JobEngineConfig engineConfig = jobFlow.getJobengineConfig();
        String jobInstanceID = data.getString(JobConstants.PROP_JOBINSTANCE_UUID);
        int jobStepID = data.getInt(JobConstants.PROP_JOBSTEP_SEQ_ID);
        String command = data.getString(JobConstants.PROP_COMMAND);
        KylinConfig config = engineConfig.getConfig();

        try {
            JobInstance jobInstance = updateJobStep(jobInstanceID, jobStepID, config, JobStepStatusEnum.RUNNING, System.currentTimeMillis(), null, null);

            jobCmd = JobCommandFactory.getJobCommand(command, jobInstance, jobStepID, engineConfig);
            data.put(JobConstants.PROP_JOB_CMD_EXECUTOR, jobCmd);
            context.getScheduler().addJob(this.currentJobDetail, true, true);

            ICommandOutput output = jobCmd.execute();

            if (data.getBoolean(JobConstants.PROP_JOB_KILLED)) {
                return;
            }

            int exitCode = output.getExitCode();
            updateJobStep(jobInstanceID, jobStepID, config, output.getStatus(), null, System.currentTimeMillis(), output.getOutput());
            context.setResult(exitCode);

            log.info("Job status for " + context.getJobDetail().getKey() + " has been updated.");
            log.info("cmd:" + command);
            log.info("output:" + output.getOutput());
            log.info("exitCode:" + exitCode);

        } catch (Throwable t) {
            handleException(jobInstanceID, jobStepID, config, t);
        }
    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
    }

    protected JobInstance updateJobStep(String jobInstanceUuid, int jobInstanceStepSeqId, KylinConfig config, JobStepStatusEnum newStatus, Long execStartTime, Long execEndTime, String output) throws IOException {
        // set step status to running
        JobInstance jobInstance = JobDAO.getInstance(config).getJob(jobInstanceUuid);
        JobStep currentStep = null;

        try {
            currentStep = jobInstance.getSteps().get(jobInstanceStepSeqId);
            JobStepStatusEnum currentStatus = currentStep.getStatus();
            boolean hasChange = false;

            if (null != execStartTime) {
                hasChange = true;
                currentStep.setExecStartTime(execStartTime);
            }
            if (null != execEndTime) {
                hasChange = true;
                currentStep.setExecEndTime(execEndTime);
            }
            if (null != output) {
                hasChange = true;
                // currentStep.setCmdOutput(output);
                JobDAO.getInstance(config).saveJobOutput(currentStep, output);
            }
            if (JobStepStatusEnum.WAITING == currentStatus && (JobStepStatusEnum.RUNNING == newStatus || JobStepStatusEnum.FINISHED == newStatus)) {
                hasChange = true;
                currentStep.setExecWaitTime((System.currentTimeMillis() - currentStep.getExecStartTime()) / 1000);
            }
            if (null != newStatus) {
                hasChange = true;
                currentStep.setStatus(newStatus);
            }

            if (hasChange) {
                JobDAO.getInstance(config).updateJobInstance(jobInstance);
            }
        } catch (IOException e) {
            log.error(e.getLocalizedMessage(), e);
        }

        if (null != execEndTime) {
            JobEngine.JOB_DURATION.put(JobInstance.getStepIdentity(jobInstance, currentStep) + " - " + String.valueOf(currentStep.getExecStartTime()), (double) (currentStep.getExecEndTime() - currentStep.getExecStartTime()) / 1000);
        }

        return jobInstance;
    }

    protected void handleException(String jobInstanceUuid, int jobInstanceStepSeqId, KylinConfig config, Throwable t) {
        log.error(t.getLocalizedMessage(), t);
        String exceptionMsg = "Failed with Exception:" + ExceptionUtils.getFullStackTrace(t);
        try {
            JobDAO dao = JobDAO.getInstance(config);
            JobInstance jobInstance = dao.getJob(jobInstanceUuid);
            JobStep jobStep = jobInstance.getSteps().get(jobInstanceStepSeqId);
            jobStep.setStatus(JobStepStatusEnum.ERROR);
            jobStep.setExecEndTime(System.currentTimeMillis());
            dao.updateJobInstance(jobInstance);

            String output = dao.getJobOutput(jobInstanceUuid, jobInstanceStepSeqId).getOutput();
            output = output + "\n" + exceptionMsg;
            dao.saveJobOutput(jobInstanceUuid, jobInstanceStepSeqId, output);
        } catch (IOException e1) {
            log.error(e1.getLocalizedMessage(), e1);
        }
    }
}
