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

package com.kylinolap.job;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.cube.CubeBuildTypeEnum;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.CubeSegmentStatusEnum;
import com.kylinolap.cube.exception.CubeIntegrityException;
import com.kylinolap.cube.project.ProjectInstance;
import com.kylinolap.cube.project.ProjectManager;
import com.kylinolap.job.JobInstance.JobStep;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.constant.JobStatusEnum;
import com.kylinolap.job.constant.JobStepStatusEnum;
import com.kylinolap.job.engine.JobEngine;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.exception.InvalidJobInstanceException;
import com.kylinolap.job.exception.JobException;
import com.kylinolap.job.hadoop.hive.JoinedFlatTableDesc;
import com.kylinolap.metadata.model.cube.CubeDesc;

/**
 * @author xjiang, ysong1
 * 
 */
public class JobManager {

    private static Logger log = LoggerFactory.getLogger(JobManager.class);

    private final KylinConfig config;
    private final JobEngineConfig engineConfig;
    private final JobEngine jobEngine;
    private final JobDAO jobDAO;

    public JobManager(String engineID, JobEngineConfig engineCfg) throws JobException, UnknownHostException {
        this.engineConfig = engineCfg;
        this.config = engineConfig.getConfig();
        this.jobDAO = JobDAO.getInstance(config);

        // InetAddress ia = InetAddress.getLocalHost();
        // this.jobEngine =
        // Constant.getInstanceFromEnv(ia.getCanonicalHostName(), cfg);
        this.jobEngine = JobEngine.getInstance(engineID, engineCfg);
    }

    public JobInstance createJob(String cubeName, String segmentName, CubeBuildTypeEnum jobType) throws IOException {
        // build job instance
        JobInstance jobInstance = buildJobInstance(cubeName, segmentName, jobType);

        // create job steps based on job type
        JobInstanceBuilder stepBuilder = new JobInstanceBuilder(this.engineConfig);
        jobInstance.addSteps(stepBuilder.buildSteps(jobInstance));

        return jobInstance;
    }

    private JobInstance buildJobInstance(String cubeName, String segmentName, CubeBuildTypeEnum jobType) {
        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone(this.engineConfig.getTimeZone()));

        JobInstance jobInstance = new JobInstance();
        jobInstance.setUuid(UUID.randomUUID().toString());
        jobInstance.setType(jobType);
        jobInstance.setRelatedCube(cubeName);
        jobInstance.setRelatedSegment(segmentName);
        jobInstance.setName(cubeName + " - " + segmentName + " - " + jobType.toString() + " - " + format.format(new Date(System.currentTimeMillis())));

        return jobInstance;
    }

    public String submitJob(JobInstance job) throws IOException, InvalidJobInstanceException {
        if (hasDuplication(job) == false) {
            // submitted job status should always be PENDING
            // job.setStatus(JobStatusEnum.PENDING);
            jobDAO.updateJobInstance(job);
            return job.getUuid();
        } else {
            throw new InvalidJobInstanceException("Job " + job.getName() + " is duplicated!");
        }
    }

    public void resumeJob(String uuid) throws IOException, JobException {
        JobInstance jobInstance = jobDAO.getJob(uuid);
        log.info("Resuming job " + uuid);
        if (jobInstance.getStatus() != JobStatusEnum.ERROR) {
            throw new RuntimeException("Can't resume job with status " + jobInstance.getStatus().toString());
        }

        for (JobStep jobStep : jobInstance.getSteps()) {
            if (jobStep.getStatus() == JobStepStatusEnum.ERROR) {
                jobStep.setStatus(JobStepStatusEnum.PENDING);
                // jobStep.setCmdOutput("");
                jobStep.clearInfo();
                jobDAO.saveJobOutput(jobStep, "");
            }
        }
        jobDAO.updateJobInstance(jobInstance);
    }

    private boolean hasDuplication(JobInstance newJob) throws IOException {
        List<JobInstance> allJobs = listJobs(null, null);
        for (JobInstance job : allJobs) {
            if (job.getRelatedCube().equals(newJob.getRelatedCube()) && job.getRelatedSegment().equals(newJob.getRelatedSegment()) && job.getType().equals(newJob.getType()) && job.getStatus().equals(newJob.getStatus())) {
                return true;
            }
        }
        return false;
    }

    public void discardJob(String uuid) throws IOException, CubeIntegrityException, JobException {
        // check job status
        JobInstance jobInstance = jobDAO.getJob(uuid);
        CubeInstance cube = CubeManager.getInstance(config).getCube(jobInstance.getRelatedCube());

        switch (jobInstance.getStatus()) {
        case RUNNING:
            try {
                killRunningJob(jobInstance);
            } finally {
                CubeManager.getInstance(config).updateSegmentOnJobDiscard(cube, jobInstance.getRelatedSegment());
            }
            break;
        case ERROR:
            try {
                for (JobStep jobStep : jobInstance.getSteps()) {
                    if (jobStep.getStatus() != JobStepStatusEnum.FINISHED) {
                        jobStep.setStatus(JobStepStatusEnum.DISCARDED);
                    }
                }
                jobDAO.updateJobInstance(jobInstance);
            } finally {
                CubeManager.getInstance(config).updateSegmentOnJobDiscard(cube, jobInstance.getRelatedSegment());
            }
            break;
        default:
            throw new IllegalStateException("Invalid status to discard : " + jobInstance.getStatus());
        }
    }

    /**
     * @param uuid
     * @param jobInstance
     * @throws IOException
     * @throws JobException
     */
    private void killRunningJob(JobInstance jobInstance) throws IOException, JobException {
        // find the running step
        JobStep runningStep = jobInstance.getRunningStep();
        if (runningStep == null) {
            throw new IllegalStateException("There is no running step in job " + jobInstance.getUuid());
        }

        // update job to DISCARDED
        runningStep.setStatus(JobStepStatusEnum.DISCARDED);
        runningStep.setExecEndTime(System.currentTimeMillis());
        jobDAO.updateJobInstance(jobInstance);

        // cancel job in engine
        this.jobEngine.interruptJob(jobInstance, runningStep);
    }

    public List<JobInstance> listJobs(String cubeName, String projectName) throws IOException {
        List<JobInstance> jobs = jobDAO.listAllJobs(cubeName);

        if (null == projectName || null == ProjectManager.getInstance(config).getProject(projectName)) {
            return jobs;
        } else {
            List<JobInstance> filtedJobs = new ArrayList<JobInstance>();
            ProjectInstance project = ProjectManager.getInstance(config).getProject(projectName);
            for (JobInstance job : jobs) {
                if (project.getCubes().contains(job.getRelatedCube().toUpperCase())) {
                    filtedJobs.add(job);
                }
            }

            return filtedJobs;
        }
    }

    public JobInstance getJob(String uuid) throws IOException {
        return jobDAO.getJob(uuid);
    }

    public String getJobStepOutput(String jobUuid, int stepSequenceId) throws IOException {
        JobStepOutput output = jobDAO.getJobOutput(jobUuid, stepSequenceId);
        if (null == output) {
            return "";
        } else {
            return output.getOutput();
        }
    }

    public void deleteJob(String uuid) throws IOException {
        jobDAO.deleteJob(uuid);
    }

    public void deleteAllJobs() throws IOException {
        List<JobInstance> allJobs = listJobs(null, null);
        for (JobInstance job : allJobs) {
            jobDAO.deleteJob(job);
        }
    }

    public String previewFlatHiveQL(String cubeName, String segmentName) {
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        CubeDesc cubeDesc = cube.getDescriptor();
        CubeSegment cubeSegment = cube.getSegment(segmentName, CubeSegmentStatusEnum.READY);
        JoinedFlatTableDesc flatTableDesc = new JoinedFlatTableDesc(cubeDesc, cubeSegment);
        return JoinedFlatTable.generateSelectDataStatement(flatTableDesc);
    }

    public void startJobEngine() throws Exception {
        startJobEngine(JobConstants.DEFAULT_SCHEDULER_INTERVAL_SECONDS);
    }

    public void startJobEngine(int daemonJobIntervalInSeconds) throws Exception {
        jobDAO.updateRunningJobToError();
        jobEngine.start(daemonJobIntervalInSeconds);
    }

    public void stopJobEngine() throws JobException {
        jobEngine.stop();
    }

    // Job engine metrics related methods

    public int getNumberOfJobStepsExecuted() {
        return jobEngine.getNumberOfJobStepsExecuted();
    }

    public String getPrimaryEngineID() throws Exception {
        return jobEngine.getPrimaryEngineID();
    }

    public double getMinJobStepDuration() {
        return jobEngine.getMinJobStepDuration();
    }

    public double getMaxJobStepDuration() {
        return jobEngine.getMaxJobStepDuration();
    }

    /**
     * @param percentile
     *            (eg. 95 percentile)
     * @return the percentile value
     */
    public double getPercentileJobStepDuration(double percentile) {
        return jobEngine.getPercentileJobStepDuration(percentile);
    }

    /**
     * @return
     */
    public Integer getScheduledJobsSzie() {
        return jobEngine.getScheduledJobsSzie();
    }

    /**
     * @return
     */
    public int getEngineThreadPoolSize() {
        return jobEngine.getEngineThreadPoolSize();
    }

    /**
     * @return
     */
    public int getNumberOfIdleSlots() {
        return jobEngine.getNumberOfIdleSlots();
    }

    /**
     * @return
     */
    public int getNumberOfJobStepsRunning() {
        return jobEngine.getNumberOfJobStepsRunning();
    }
}
