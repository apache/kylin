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

import java.util.HashSet;
import java.util.Set;

import org.quartz.DateBuilder;
import org.quartz.DateBuilder.IntervalUnit;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.job.JobDAO;
import com.kylinolap.job.JobInstance;
import com.kylinolap.job.JobInstance.JobStep;
import com.kylinolap.job.cmd.ICommandOutput;
import com.kylinolap.job.cmd.JobCommandFactory;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.constant.JobStepStatusEnum;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.exception.JobException;

/**
 * @author xduo
 *
 */
public class AsyncJobFlowNode extends JobFlowNode {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        this.currentJobDetail = context.getJobDetail();
        JobDataMap data = this.currentJobDetail.getJobDataMap();
        JobFlow jobFlow = (JobFlow) data.get(JobConstants.PROP_JOB_FLOW);
        JobEngineConfig engineConfig = jobFlow.getJobengineConfig();
        KylinConfig config = engineConfig.getConfig();
        String jobInstanceID = data.getString(JobConstants.PROP_JOBINSTANCE_UUID);
        int jobStepID = data.getInt(JobConstants.PROP_JOBSTEP_SEQ_ID);
        ICommandOutput output = (ICommandOutput) data.get(JobConstants.PROP_JOB_CMD_OUTPUT);

        try {
            if (data.getBoolean(JobConstants.PROP_JOB_KILLED)) {
                log.info(this.currentJobDetail.getKey() + " is killed");
                return;
            }

            if (output == null) {
                JobInstance jobInstance =
                        updateJobStep(jobInstanceID, jobStepID, config, JobStepStatusEnum.RUNNING,
                                System.currentTimeMillis(), null, null);

                String command = data.getString(JobConstants.PROP_COMMAND);
                jobCmd = JobCommandFactory.getJobCommand(command, jobInstance, jobStepID, engineConfig);
                output = jobCmd.execute();
                data.put(JobConstants.PROP_JOB_CMD_OUTPUT, output);
                data.put(JobConstants.PROP_JOB_CMD_EXECUTOR, jobCmd);
                context.getScheduler().addJob(this.currentJobDetail, true, true);

                JobStepStatusEnum stepStatus = output.getStatus();
                updateJobStep(jobInstanceID, jobStepID, config, stepStatus, null, stepStatus.isComplete()
                        ? System.currentTimeMillis() : null, output.getOutput());

                context.setResult(output.getExitCode());
                scheduleStatusChecker(context);
                log.debug("Start async job " + currentJobDetail.getKey());
            } else {
                JobInstance jobInstance = JobDAO.getInstance(engineConfig.getConfig()).getJob(jobInstanceID);
                JobStep jobStep = jobInstance.getSteps().get(jobStepID);

                log.debug("Start to check hadoop job status of " + currentJobDetail.getKey());
                JobStepStatusEnum stepStatus = output.getStatus();

                if ((System.currentTimeMillis() - jobStep.getExecStartTime()) / 1000 >= engineConfig
                        .getJobStepTimeout()) {
                    throw new JobException("Job step " + jobStep.getName() + " timeout.");
                }

                updateJobStep(jobInstance.getUuid(), jobStepID, config, stepStatus, null,
                        stepStatus.isComplete() ? System.currentTimeMillis() : null, output.getOutput());

                if (!stepStatus.isComplete()) {
                    scheduleStatusChecker(context);
                }

                context.setResult(0);
                log.debug("Status of async job " + currentJobDetail.getKey() + ":" + stepStatus);
            }
        } catch (Throwable t) {
            handleException(jobInstanceID, jobStepID, config, t);
        }

    }

    private void scheduleStatusChecker(JobExecutionContext context) throws SchedulerException {
        JobDataMap jobDataMap = this.currentJobDetail.getJobDataMap();
        JobFlow jobFlow = (JobFlow) jobDataMap.get(JobConstants.PROP_JOB_FLOW);
        JobEngineConfig engineConfig = jobFlow.getJobengineConfig();
        int interval = engineConfig.getAsyncJobCheckInterval();
        log.debug("Trigger a status check job in " + interval + " seconds for job "
                + currentJobDetail.getKey());

        Trigger trigger =
                TriggerBuilder.newTrigger().startAt(DateBuilder.futureDate(interval, IntervalUnit.SECOND))
                        .build();
        Set<Trigger> triggers = new HashSet<Trigger>();
        triggers.add(trigger);
        context.getScheduler().scheduleJob(currentJobDetail, triggers, true);
    }
}
