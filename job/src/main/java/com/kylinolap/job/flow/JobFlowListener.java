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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.kylinolap.cube.CubeSegmentStatusEnum;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.MailService;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.exception.CubeIntegrityException;
import com.kylinolap.job.JobDAO;
import com.kylinolap.job.JobInstance;
import com.kylinolap.job.JobInstance.JobStep;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.constant.JobStatusEnum;
import com.kylinolap.job.constant.JobStepStatusEnum;
import com.kylinolap.job.engine.JobEngineConfig;

/**
 * Handle kylin job and cube change update.
 * 
 * @author George Song (ysong1), xduo
 * 
 */
public class JobFlowListener implements JobListener {

    private static Logger log = LoggerFactory.getLogger(JobFlowListener.class);

    private String name;

    public JobFlowListener(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Listener name cannot be null!");
        }
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
        log.info(context.getJobDetail().getKey() + " was executed.");
        JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
        JobFlow jobFlow = (JobFlow) jobDataMap.get(JobConstants.PROP_JOB_FLOW);
        JobEngineConfig engineConfig = jobFlow.getJobengineConfig();
        String jobUuid = jobDataMap.getString(JobConstants.PROP_JOBINSTANCE_UUID);
        int stepSeqID = jobDataMap.getInt(JobConstants.PROP_JOBSTEP_SEQ_ID);
        KylinConfig config = engineConfig.getConfig();

        JobInstance jobInstance = null;
        JobStep jobStep = null;
        try {
            jobInstance = JobDAO.getInstance(config).getJob(jobUuid);
            jobStep = jobInstance.getSteps().get(stepSeqID);
            CubeInstance cube = CubeManager.getInstance(config).getCube(jobInstance.getRelatedCube());

            log.info(context.getJobDetail().getKey() + " status: " + jobStep.getStatus());
            switch (jobStep.getStatus()) {
            case FINISHED:
                // Ensure we are using the latest metadata
                CubeManager.getInstance(config).loadCubeCache(cube);
                updateKylinJobOnSuccess(jobInstance, stepSeqID, engineConfig);
                updateCubeSegmentInfoOnSucceed(jobInstance, engineConfig);
                notifyUsers(jobInstance, engineConfig);
                scheduleNextJob(context, jobInstance);
                break;
            case ERROR:
                updateKylinJobStatus(jobInstance, stepSeqID, engineConfig);
                notifyUsers(jobInstance, engineConfig);
                break;
            case DISCARDED:
                // Ensure we are using the latest metadata
                CubeManager.getInstance(config).loadCubeCache(cube);
                updateCubeSegmentInfoOnDiscard(jobInstance, engineConfig);
                notifyUsers(jobInstance, engineConfig);
                break;
            default:
                break;
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            handleException(jobUuid, stepSeqID, config, e);
        } finally {
            if (null != jobInstance && jobInstance.getStatus().isComplete()) {
                try {
                    context.getScheduler().deleteJob(context.getJobDetail().getKey());
                    @SuppressWarnings("unchecked")
                    ConcurrentHashMap<String, JobFlow> jobFlows = (ConcurrentHashMap<String, JobFlow>) context.getScheduler().getContext().get(JobConstants.PROP_JOB_RUNTIME_FLOWS);
                    jobFlows.remove(JobInstance.getJobIdentity(jobInstance));
                } catch (SchedulerException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.quartz.JobListener#jobToBeExecuted(org.quartz.JobExecutionContext)
     */
    @Override
    public void jobToBeExecuted(JobExecutionContext context) {
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.quartz.JobListener#jobExecutionVetoed(org.quartz.JobExecutionContext)
     */
    @Override
    public void jobExecutionVetoed(JobExecutionContext context) {
    }

    /**
     * @param context
     * @param jobInstance
     */
    protected void scheduleNextJob(JobExecutionContext context, JobInstance jobInstance) {
        try {
            // schedule next job
            JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
            JobFlow jobFlow = (JobFlow) jobDataMap.get(JobConstants.PROP_JOB_FLOW);
            JobDetail nextJob = (JobDetail) jobFlow.getNext(context.getJobDetail());
            if (nextJob != null) {
                try {
                    Trigger trigger = TriggerBuilder.newTrigger().startNow().build();
                    log.debug("Job " + context.getJobDetail().getKey() + " will now chain to Job " + nextJob.getKey() + "");

                    context.getScheduler().scheduleJob(nextJob, trigger);

                } catch (SchedulerException se) {
                    log.error("Error encountered during chaining to Job " + nextJob.getKey() + "", se);
                }
            }

            context.getScheduler().deleteJob(context.getJobDetail().getKey());
        } catch (SchedulerException e) {
            log.error(e.getLocalizedMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * @param jobInstance
     * @param stepId
     */
    private void updateKylinJobStatus(JobInstance jobInstance, int stepId, JobEngineConfig engineConfig) {
        validate(jobInstance);
        List<JobStep> steps = jobInstance.getSteps();
        Collections.sort(steps);

        JobStep jobStep = jobInstance.getSteps().get(stepId);

        long duration = jobStep.getExecEndTime() - jobStep.getExecStartTime();
        jobInstance.setDuration(jobInstance.getDuration() + (duration > 0 ? duration : 0) / 1000);
        jobInstance.setMrWaiting(jobInstance.getMrWaiting() + jobStep.getExecWaitTime());

        try {
            JobDAO.getInstance(engineConfig.getConfig()).updateJobInstance(jobInstance);
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getLocalizedMessage(), e);
        }
    }

    private void updateKylinJobOnSuccess(JobInstance jobInstance, int stepId, JobEngineConfig engineConfig) {
        validate(jobInstance);
        List<JobStep> steps = jobInstance.getSteps();
        Collections.sort(steps);

        JobStep jobStep = jobInstance.getSteps().get(stepId);
        jobInstance.setExecStartTime(steps.get(0).getExecStartTime());

        long duration = jobStep.getExecEndTime() - jobStep.getExecStartTime();
        jobInstance.setDuration(jobInstance.getDuration() + (duration > 0 ? duration / 1000 : 0));
        jobInstance.setMrWaiting(jobInstance.getMrWaiting() + jobStep.getExecWaitTime());
        if (jobInstance.getStatus().equals(JobStatusEnum.FINISHED)) {
            jobInstance.setExecEndTime(steps.get(steps.size() - 1).getExecEndTime());
        }

        try {
            JobDAO.getInstance(engineConfig.getConfig()).updateJobInstance(jobInstance);
        } catch (IOException e) {
            e.printStackTrace();
            log.error(e.getLocalizedMessage(), e);
        }
    }

    private void updateCubeSegmentInfoOnDiscard(JobInstance jobInstance, JobEngineConfig engineConfig) throws IOException, CubeIntegrityException {
        CubeManager cubeMgr = CubeManager.getInstance(engineConfig.getConfig());
        CubeInstance cubeInstance = cubeMgr.getCube(jobInstance.getRelatedCube());
        cubeMgr.updateSegmentOnJobDiscard(cubeInstance, jobInstance.getRelatedSegment());
    }

    private void updateCubeSegmentInfoOnSucceed(JobInstance jobInstance, JobEngineConfig engineConfig) throws CubeIntegrityException, IOException {
        if (jobInstance.getStatus().equals(JobStatusEnum.FINISHED)) {
            validate(jobInstance);

            log.info("Updating cube segment " + jobInstance.getRelatedSegment() + " for cube " + jobInstance.getRelatedCube());

            long cubeSize = 0;
            JobStep convertToHFileStep = jobInstance.findStep(JobConstants.STEP_NAME_CONVERT_CUBOID_TO_HFILE);
            if (null != convertToHFileStep) {
                String cubeSizeString = convertToHFileStep.getInfo(JobInstance.HDFS_BYTES_WRITTEN);
                if (cubeSizeString == null || cubeSizeString.equals("")) {
                    log.warn("Can't get cube segment size from job " + jobInstance.getUuid());
                } else {
                    cubeSize = Long.parseLong(cubeSizeString) / 1024;
                }
            } else {
                log.info("No step with name '" + JobConstants.STEP_NAME_CONVERT_CUBOID_TO_HFILE + "' is found");
            }

            CubeManager cubeMgr = CubeManager.getInstance(engineConfig.getConfig());
            CubeInstance cubeInstance = cubeMgr.getCube(jobInstance.getRelatedCube());
            CubeSegment newSegment = cubeInstance.getSegmentById(jobInstance.getUuid());

            long sourceCount = 0;
            long sourceSize = 0;
            switch (jobInstance.getType()) {
            case BUILD:
                JobStep baseCuboidStep = jobInstance.findStep(JobConstants.STEP_NAME_BUILD_BASE_CUBOID);
                if (null != baseCuboidStep) {
                    String sourceRecordsCount = baseCuboidStep.getInfo(JobInstance.SOURCE_RECORDS_COUNT);
                    if (sourceRecordsCount == null || sourceRecordsCount.equals("")) {
                        log.warn("Can't get cube source record count from job " + jobInstance.getUuid());
                    } else {
                        sourceCount = Long.parseLong(sourceRecordsCount);
                    }
                } else {
                    log.info("No step with name '" + JobConstants.STEP_NAME_BUILD_BASE_CUBOID + "' is found");
                }

                JobStep createFlatTableStep = jobInstance.findStep(JobConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);
                if (null != createFlatTableStep) {
                    String sourceRecordsSize = createFlatTableStep.getInfo(JobInstance.SOURCE_RECORDS_SIZE);
                    if (sourceRecordsSize == null || sourceRecordsSize.equals("")) {
                        log.warn("Can't get cube source record from job " + jobInstance.getUuid());
                    } else {
                        sourceSize = Long.parseLong(sourceRecordsSize);
                    }
                } else {
                    log.info("No step with name '" + JobConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE + "' is found");
                }

                if (cubeInstance.needMergeImmediatelyAfterBuild(newSegment)) {
                    for (CubeSegment seg : cubeInstance.getSegment(CubeSegmentStatusEnum.READY)) {
                        sourceCount += seg.getSourceRecords();
                        sourceSize += seg.getSourceRecordsSize();
                    }
                }
                break;
            case MERGE:
                for (CubeSegment seg : cubeInstance.getMergingSegments()) {
                    sourceCount += seg.getSourceRecords();
                    sourceSize += seg.getSourceRecordsSize();
                }
                break;
            }

            cubeMgr.updateSegmentOnJobSucceed(cubeInstance, jobInstance.getType(), jobInstance.getRelatedSegment(), jobInstance.getUuid(), jobInstance.getExecEndTime(), cubeSize, sourceCount, sourceSize);
            log.info("Update cube segment succeed" + jobInstance.getRelatedSegment() + " for cube " + jobInstance.getRelatedCube());
        }
    }

    private void validate(JobInstance jobInstance) {
        List<JobStep> steps = jobInstance.getSteps();
        if (steps == null || steps.size() == 0) {
            throw new RuntimeException("Steps of job " + jobInstance.getUuid() + " is null or empty!");
        }
    }

    private void handleException(String jobInstanceUuid, int jobInstanceStepSeqId, KylinConfig config, Throwable t) {
        log.error(t.getLocalizedMessage(), t);
        String exceptionMsg = "Failed with Exception:" + ExceptionUtils.getFullStackTrace(t);
        try {
            JobInstance jobInstance = JobDAO.getInstance(config).getJob(jobInstanceUuid);
            jobInstance.getSteps().get(jobInstanceStepSeqId).setStatus(JobStepStatusEnum.ERROR);
            // String output =
            // jobInstance.getSteps().get(jobInstanceStepSeqId).getCmdOutput();
            // jobInstance.getSteps().get(jobInstanceStepSeqId).setCmdOutput(output
            // + "\n" + exceptionMsg);
            jobInstance.getSteps().get(jobInstanceStepSeqId).setExecEndTime(System.currentTimeMillis());
            JobDAO.getInstance(config).updateJobInstance(jobInstance);

            String output = JobDAO.getInstance(config).getJobOutput(jobInstanceUuid, jobInstanceStepSeqId).getOutput();
            output = output + "\n" + exceptionMsg;
            JobDAO.getInstance(config).saveJobOutput(jobInstanceUuid, jobInstanceStepSeqId, output);
        } catch (IOException e1) {
            log.error(e1.getLocalizedMessage(), e1);
        }
    }

    /**
     * @param jobInstance
     */
    protected void notifyUsers(JobInstance jobInstance, JobEngineConfig engineConfig) {
        KylinConfig config = engineConfig.getConfig();
        String cubeName = jobInstance.getRelatedCube();
        CubeInstance cubeInstance = CubeManager.getInstance(config).getCube(cubeName);
        String finalStatus = null;
        String content = JobConstants.NOTIFY_EMAIL_TEMPLATE;
        String logMsg = "";

        switch (jobInstance.getStatus()) {
        case FINISHED:
            finalStatus = "SUCCESS";
            break;
        case ERROR:
            for (JobStep step : jobInstance.getSteps()) {
                if (step.getStatus() == JobStepStatusEnum.ERROR) {
                    try {
                        logMsg = JobDAO.getInstance(config).getJobOutput(step).getOutput();
                    } catch (IOException e) {
                        log.error(e.getLocalizedMessage(), e);
                    }
                }
            }
            finalStatus = "FAILED";
            break;
        case DISCARDED:
            finalStatus = "DISCARDED";
        default:
            break;
        }

        if (null == finalStatus) {
            return;
        }

        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            content = content.replaceAll("\\$\\{job_engine\\}", inetAddress.getCanonicalHostName());
        } catch (UnknownHostException e) {
            log.error(e.getLocalizedMessage(), e);
        }

        content = content.replaceAll("\\$\\{job_name\\}", jobInstance.getName());
        content = content.replaceAll("\\$\\{result\\}", finalStatus);
        content = content.replaceAll("\\$\\{cube_name\\}", cubeName);
        content = content.replaceAll("\\$\\{start_time\\}", new Date(jobInstance.getExecStartTime()).toString());
        content = content.replaceAll("\\$\\{duration\\}", jobInstance.getDuration() / 60 + "mins");
        content = content.replaceAll("\\$\\{mr_waiting\\}", jobInstance.getMrWaiting() / 60 + "mins");
        content = content.replaceAll("\\$\\{last_update_time\\}", new Date(jobInstance.getLastModified()).toString());
        content = content.replaceAll("\\$\\{submitter\\}", jobInstance.getSubmitter());
        content = content.replaceAll("\\$\\{error_log\\}", logMsg);

        
        MailService mailService = new MailService();
        try {
            List<String> users = new ArrayList<String>();

            if (null != cubeInstance.getDescriptor().getNotifyList()) {
                users.addAll(cubeInstance.getDescriptor().getNotifyList());
            }

            if (null != engineConfig.getAdminDls()) {
                String[] adminDls = engineConfig.getAdminDls().split(",");

                for (String adminDl : adminDls) {
                    users.add(adminDl);
                }
            }

            log.info("prepare to send email to:"+users);
            
            log.info("job name:"+jobInstance.getName());
            
            log.info("submitter:"+jobInstance.getSubmitter());
            
            if (users.size() > 0) {
                log.info("notify list:"+users);
                mailService.sendMail(users, "["+ finalStatus + "] - [Kylin Cube Build Job]-" + cubeName, content);
                log.info("notified users:"+users);
            }
        } catch (IOException e) {
            log.error(e.getLocalizedMessage(), e);
        }

    }
}
