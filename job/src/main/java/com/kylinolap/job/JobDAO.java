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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.JsonSerializer;
import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.persistence.Serializer;
import com.kylinolap.job.JobInstance.JobStep;
import com.kylinolap.job.constant.JobStatusEnum;
import com.kylinolap.job.constant.JobStepStatusEnum;
import com.kylinolap.metadata.MetadataManager;

/**
 * @author ysong1
 */
public class JobDAO {
    private static Logger log = LoggerFactory.getLogger(JobDAO.class);

    private static final Serializer<JobInstance> JOB_SERIALIZER = new JsonSerializer<JobInstance>(JobInstance.class);
    private static final Serializer<JobStepOutput> JOB_OUTPUT_SERIALIZER = new JsonSerializer<JobStepOutput>(JobStepOutput.class);

    private ResourceStore store;

    private static final Logger logger = LoggerFactory.getLogger(JobDAO.class);

    private static final ConcurrentHashMap<KylinConfig, JobDAO> CACHE = new ConcurrentHashMap<KylinConfig, JobDAO>();

    public static JobDAO getInstance(KylinConfig config) {
        JobDAO r = CACHE.get(config);
        if (r == null) {
            r = new JobDAO(config);
            CACHE.put(config, r);
            if (CACHE.size() > 1) {
                logger.warn("More than one singleton exist");
            }

        }
        return r;
    }

    private JobDAO(KylinConfig config) {
        log.info("Using metadata url: " + config);
        this.store = MetadataManager.getInstance(config).getStore();
    }

    public List<JobInstance> listAllJobs() throws IOException {
        ArrayList<String> jobResources = store.listResources(ResourceStore.JOB_PATH_ROOT);
        if (jobResources == null)
            return Collections.emptyList();

        ArrayList<JobInstance> result = new ArrayList<JobInstance>(jobResources.size());
        for (String path : jobResources) {
            result.add(readJobResource(path));
        }

        return result;
    }

    public List<JobInstance> listAllJobs(String cubeName) throws IOException {

        List<JobInstance> allJobs = listAllJobs();
        if (allJobs.size() == 0) {
            return Collections.emptyList();
        }

        if (null == cubeName || cubeName.trim().length() == 0) {
            return allJobs;
        }

        ArrayList<JobInstance> result = new ArrayList<JobInstance>();
        for (JobInstance job : allJobs) {
            if (job != null) {
                if (job.getRelatedCube().toLowerCase().contains(cubeName.toLowerCase())) {
                    result.add(job);
                }
            }
        }

        return result;
    }

    public List<JobInstance> listAllJobs(JobStatusEnum status) throws IOException {

        List<JobInstance> allJobs = listAllJobs();
        if (allJobs.size() == 0) {
            return Collections.emptyList();
        }

        ArrayList<JobInstance> result = new ArrayList<JobInstance>();
        for (JobInstance job : allJobs) {
            if (job != null) {
                if (job.getStatus().equals(status)) {
                    result.add(job);
                }
            }
        }

        return result;
    }

    public JobStepOutput getJobOutput(String jobUuid, int stepSequenceId) throws IOException {
        return readJobOutputResource(ResourceStore.JOB_OUTPUT_PATH_ROOT + "/" + JobStepOutput.nameOfOutput(jobUuid, stepSequenceId));
    }

    public JobStepOutput getJobOutput(JobStep jobStep) throws IOException {
        return getJobOutput(jobStep.getJobInstance().getUuid(), jobStep.getSequenceID());
    }

    public void saveJobOutput(String jobUuid, int stepSequenceId, String outputString) throws IOException {
        JobStepOutput output = this.getJobOutput(jobUuid, stepSequenceId);

        if (output == null) {
            output = new JobStepOutput();
            output.setName(JobStepOutput.nameOfOutput(jobUuid, stepSequenceId));
        }

        output.setOutput(outputString);
        writeJobOutputResource(pathOfJobOutput(output), output);
    }

    public void saveJobOutput(JobStep jobStep, String outputString) throws IOException {
        saveJobOutput(jobStep.getJobInstance().getUuid(), jobStep.getSequenceID(), outputString);
    }

    private void saveJob(JobInstance job) throws IOException {
        writeJobResource(pathOfJob(job), job);
    }

    public JobInstance getJob(String uuid) throws IOException {
        return readJobResource(ResourceStore.JOB_PATH_ROOT + "/" + uuid);
    }

    public void deleteJob(JobInstance job) throws IOException {
        store.deleteResource(pathOfJob(job));
    }

    public void deleteJob(String uuid) throws IOException {
        store.deleteResource(ResourceStore.JOB_PATH_ROOT + "/" + uuid);
    }

    public void updateJobInstance(JobInstance jobInstance) throws IOException {
        try {
            JobInstance updatedJob = getJob(jobInstance.getUuid());
            if (updatedJob == null) {
                saveJob(jobInstance);
                return;
            }

            updatedJob.setExecEndTime(jobInstance.getExecEndTime());
            updatedJob.setExecStartTime(jobInstance.getExecStartTime());
            updatedJob.setDuration(jobInstance.getDuration());
            updatedJob.setMrWaiting(jobInstance.getMrWaiting());
            updatedJob.setRelatedCube(jobInstance.getRelatedCube());
            updatedJob.setRelatedSegment(jobInstance.getRelatedSegment());
            updatedJob.setType(jobInstance.getType());

            updatedJob.clearSteps();
            updatedJob.addSteps(jobInstance.getSteps());

            saveJob(updatedJob);
        } catch (IOException e) {
            log.error(e.getLocalizedMessage(), e);
            throw e;
        }
    }

    public void updateRunningJobToError() throws IOException {
        List<JobInstance> runningJobs = listAllJobs(JobStatusEnum.RUNNING);
        for (JobInstance job : runningJobs) {
            // job.setStatus(JobStatusEnum.ERROR);

            // set the last running step to ERROR
            int lastRunningStepIndex = 0;
            for (int i = job.getSteps().size() - 1; i >= 0; i--) {
                JobStep currentStep = job.getSteps().get(i);
                if (currentStep.getStatus() != JobStepStatusEnum.RUNNING && currentStep.getStatus() != JobStepStatusEnum.WAITING) {
                    continue;
                } else {
                    lastRunningStepIndex = i;
                    break;
                }
            }

            job.getSteps().get(lastRunningStepIndex).setStatus(JobStepStatusEnum.ERROR);
            this.updateJobInstance(job);

            this.saveJobOutput(job.getUuid(), lastRunningStepIndex, "ERROR state set by job engine");
        }
    }

    private String pathOfJob(JobInstance job) {
        return ResourceStore.JOB_PATH_ROOT + "/" + job.getUuid();
    }

    private JobInstance readJobResource(String path) throws IOException {
        return store.getResource(path, JobInstance.class, JOB_SERIALIZER);
    }

    private void writeJobResource(String path, JobInstance job) throws IOException {
        store.putResource(path, job, JOB_SERIALIZER);
    }

    private String pathOfJobOutput(JobStepOutput output) {
        return ResourceStore.JOB_OUTPUT_PATH_ROOT + "/" + output.getName();
    }

    private JobStepOutput readJobOutputResource(String path) throws IOException {
        return store.getResource(path, JobStepOutput.class, JOB_OUTPUT_SERIALIZER);
    }

    private void writeJobOutputResource(String path, JobStepOutput output) throws IOException {
        store.putResource(path, output, JOB_OUTPUT_SERIALIZER);
    }
}
