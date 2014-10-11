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

package com.kylinolap.job.engine;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.util.StringSplitter;
import com.kylinolap.job.JobDAO;
import com.kylinolap.job.JobInstance;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.constant.JobStatusEnum;
import com.kylinolap.job.flow.JobFlow;

/**
 * @author ysong1, xduo
 * 
 */
public class JobFetcher implements Job {

    private static final Logger log = LoggerFactory.getLogger(JobFetcher.class);

    public static final int JOB_THRESHOLD = 10;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

        JobEngineConfig engineConfig = (JobEngineConfig) context.getJobDetail().getJobDataMap().get(JobConstants.PROP_ENGINE_CONTEXT);

        JobDAO jobDAO = JobDAO.getInstance(engineConfig.getConfig());

        try {
            // get all pending jobs
            log.debug("Using metadata url: " + engineConfig.getConfig());
            log.debug("Getting pending job list");
            List<JobInstance> pendingJobList = jobDAO.listAllJobs(JobStatusEnum.PENDING);

            log.debug("Pending job count is " + pendingJobList.size());
            int leftJobs = JOB_THRESHOLD;
            Random rand = new Random();
            int maxConcurrentJobCount = engineConfig.getMaxConcurrentJobLimit();

            for (JobInstance jobInstance : pendingJobList) {
                @SuppressWarnings("unchecked")
                ConcurrentHashMap<String, JobFlow> jobFlows = (ConcurrentHashMap<String, JobFlow>) context.getScheduler().getContext().get(JobConstants.PROP_JOB_RUNTIME_FLOWS);

                if (jobFlows.size() >= maxConcurrentJobCount) {
                    // If too many job instances in current job context, just
                    // wait.
                    break;
                }

                try {
                    // there should be only 1 job for a certain job running
                    boolean cubeHasRunningJob = false;
                    for (String s : jobFlows.keySet()) {
                        String[] tmp = StringSplitter.split(s, ".");
                        String cubename = tmp[0];
                        String jobid = tmp[1];
                        if (cubename.equals(jobInstance.getRelatedCube())) {
                            log.info("There is already a job of cube " + jobInstance.getRelatedCube() + " running, job uuid is " + jobid);
                            cubeHasRunningJob = true;
                            break;
                        }
                    }

                    if (cubeHasRunningJob == false && jobFlows.containsKey(JobInstance.getJobIdentity(jobInstance)) == false) {
                        // create job flow
                        JobFlow jobFlow = new JobFlow(jobInstance, engineConfig);
                        jobFlows.put(JobInstance.getJobIdentity(jobInstance), jobFlow);

                        // schedule the 1st step
                        Trigger trigger = TriggerBuilder.newTrigger().startNow().build();
                        JobDetail firstStep = jobFlow.getFirst();
                        context.getScheduler().scheduleJob(firstStep, trigger);

                        log.info("Job " + jobInstance.getUuid() + " has been scheduled with the first step " + firstStep.getKey().toString());
                    }
                } catch (Exception e) {
                    log.error("Failed to trigger the job detail", e);
                }

                if (--leftJobs < 0) {
                    log.info("Too many pending jobs!");
                    break;
                }
                long ms = Math.abs(rand.nextLong() % 10L);
                Thread.sleep(ms * 1000L);
            }
        } catch (Throwable t) {
            log.error(t.getMessage());
            throw new JobExecutionException(t);
        }
    }
}
