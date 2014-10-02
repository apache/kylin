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

import com.kylinolap.common.KylinConfig;
import com.kylinolap.job.JobInstance;
import com.kylinolap.job.JobInstance.JobStep;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.constant.JobStepCmdTypeEnum;
import com.kylinolap.job.engine.JobEngineConfig;
import org.apache.commons.lang3.StringUtils;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @author xduo
 */
public class JobFlow {

    private static Logger log = LoggerFactory.getLogger(JobFlow.class);

    private final List<JobDetail> flowNodes;
    private final JobInstance jobInstance;
    private final JobEngineConfig engineConfig;

    public JobFlow(JobInstance job, JobEngineConfig context) {
        this.engineConfig = context;
        this.jobInstance = job;
        // validate job instance
        List<JobStep> sortedSteps = jobInstance.getSteps();

        if (sortedSteps == null || sortedSteps.size() == 0) {
            throw new IllegalStateException("Steps of job " + jobInstance.getUuid() + " is null or empty!");
        }

        // sort the steps by step_sequenceID
        Collections.sort(sortedSteps);
        // find the 1st runnable job 
        int firstStepIndex = findFirstStep(sortedSteps);

        log.info("Job " + jobInstance.getUuid() + " will be started at step " + firstStepIndex
                + " (sequence number)");

        flowNodes = new LinkedList<JobDetail>();
        for (int i = firstStepIndex; i < sortedSteps.size(); i++) {
            JobDetail node = createJobFlowNode(jobInstance, i);
            flowNodes.add(node);
        }
    }

    public JobInstance getJobInstance() {
        return jobInstance;
    }

    public JobEngineConfig getJobengineConfig() {
        return engineConfig;
    }

    public JobDetail getFirst() {
        if (flowNodes.isEmpty()) {
            return null;
        }
        return flowNodes.get(0);
    }

    public JobDetail getNext(JobDetail jobFlowNode) {
        int targetIndex = -1;
        for (int index = 0; index < flowNodes.size(); index++) {
            if (flowNodes.get(index).equals(jobFlowNode)) {
                targetIndex = index;
            }
        }

        if (targetIndex != -1 && flowNodes.size() > targetIndex + 1) {
            return flowNodes.get(targetIndex + 1);
        }
        return null;
    }

    private int findFirstStep(List<JobStep> stepList) {
        int firstJobIndex = 0;
        for (int i = 0; i < stepList.size(); i++) {
            JobStep currentStep = stepList.get(i);
            if (currentStep.getStatus().isRunable() == false) {
                continue;
            } else {
                firstJobIndex = i;
                break;
            }
        }
        return firstJobIndex;
    }

    private JobDetail createJobFlowNode(final JobInstance jobInstance, final int stepSeqId) {
        JobStep step = jobInstance.getSteps().get(stepSeqId);

        if (jobInstance.getName() == null || step.getName() == null) {
            throw new IllegalArgumentException("JobInstance name or JobStep name cannot be null!");
        }

        // submit job the different groups based on isRunAsync property
        JobDetail jobFlowNode =
                JobBuilder
                        .newJob(step.isRunAsync() ? AsyncJobFlowNode.class : JobFlowNode.class)
                        .withIdentity(JobInstance.getStepIdentity(jobInstance, step),
                                JobConstants.CUBE_JOB_GROUP_NAME).storeDurably().build();

        // add job flow to node 
        jobFlowNode.getJobDataMap().put(JobConstants.PROP_JOB_FLOW, this);

        // add command to flow node 
        String execCmd =
                (step.getCmdType() == JobStepCmdTypeEnum.SHELL_CMD || step.getCmdType() == JobStepCmdTypeEnum.SHELL_CMD_HADOOP)
                        ? wrapExecCmd(jobInstance, step.getExecCmd(), String.valueOf(step.getSequenceID()))
                        : step.getExecCmd();
        jobFlowNode.getJobDataMap().put(JobConstants.PROP_COMMAND, execCmd);

        // add job instance and step sequenceID to flow node 
        jobFlowNode.getJobDataMap().put(JobConstants.PROP_JOBINSTANCE_UUID, jobInstance.getUuid());
        jobFlowNode.getJobDataMap().put(JobConstants.PROP_JOBSTEP_SEQ_ID, step.getSequenceID());

        // add async flag to flow node 
        jobFlowNode.getJobDataMap().put(JobConstants.PROP_JOB_ASYNC, step.isRunAsync());

        return jobFlowNode;
    }

    private String wrapExecCmd(JobInstance job, String cmd, String suffix) {
        if (StringUtils.isBlank(cmd))
            return cmd;

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String log = config.getKylinJobLogDir() + "/" + job.getUuid() + "_" + suffix + ".log";

        return "set -o pipefail; " + cmd + " 2>&1 | tee " + log;
    }
}
