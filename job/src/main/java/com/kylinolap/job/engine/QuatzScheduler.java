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

import com.kylinolap.job.JobInstance;
import com.kylinolap.job.JobInstance.JobStep;
import com.kylinolap.job.cmd.IJobCommand;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.exception.JobException;
import com.kylinolap.job.flow.JobFlow;
import com.kylinolap.job.flow.JobFlowListener;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author xduo
 */
public class QuatzScheduler {

    private static Logger log = LoggerFactory.getLogger(QuatzScheduler.class);

    private Scheduler scheduler;
    private JobFlowListener globalJobListener;

    //    public static void scheduleJobFlow(Scheduler scheduler, JobFlow jobFlow) throws JobException {
    //        // schedule the 1st step
    //        Trigger trigger = TriggerBuilder.newTrigger().startNow().build();
    //        JobDetail firstStep = jobFlow.getFirst();
    //        try {
    //            scheduler.scheduleJob(firstStep, trigger);
    //        } catch (SchedulerException e) {
    //            throw new JobException(e);
    //        }
    //    }

    public QuatzScheduler() throws JobException {
        this.globalJobListener = new JobFlowListener(JobConstants.GLOBAL_LISTENER_NAME);
        StdSchedulerFactory sf = new StdSchedulerFactory();
        Properties schedulerProperties = new Properties();
        int numberOfProcessors = Runtime.getRuntime().availableProcessors();
        schedulerProperties.setProperty("org.quartz.threadPool.threadCount",
                String.valueOf(numberOfProcessors));
        schedulerProperties.setProperty("org.quartz.scheduler.skipUpdateCheck", "true");

        try {
            sf.initialize(schedulerProperties);
            this.scheduler = sf.getScheduler();
            this.scheduler.getListenerManager().addJobListener(this.globalJobListener,
                    GroupMatcher.jobGroupEquals(JobConstants.CUBE_JOB_GROUP_NAME));

            // cubename.jobUuid -> job flow
            this.scheduler.getContext().put(JobConstants.PROP_JOB_RUNTIME_FLOWS,
                    new ConcurrentHashMap<String, JobFlow>());

            // put the scheduler in standby mode first
            this.scheduler.standby();
        } catch (SchedulerException e) {
            throw new JobException(e);
        }
    }

    public void start() throws JobException {
        try {
            this.scheduler.start();
        } catch (SchedulerException e) {
            throw new JobException(e);
        }
    }

    public void scheduleFetcher(int intervalInSeconds, JobEngineConfig engineConfig) throws JobException {
        JobDetail job =
                JobBuilder
                        .newJob(JobFetcher.class)
                        .withIdentity(JobFetcher.class.getCanonicalName(), JobConstants.DAEMON_JOB_GROUP_NAME)
                        .build();
        job.getJobDataMap().put(JobConstants.PROP_ENGINE_CONTEXT, engineConfig);

        Trigger trigger =
                TriggerBuilder
                        .newTrigger()
                        .startNow()
                        .withSchedule(
                                SimpleScheduleBuilder.simpleSchedule()
                                        .withIntervalInSeconds(intervalInSeconds).repeatForever()
                        ).build();

        try {
            this.scheduler.scheduleJob(job, trigger);
        } catch (SchedulerException e) {
            throw new JobException(e);
        }
    }

    public boolean interrupt(JobInstance jobInstance, JobStep jobStep) throws JobException, IOException {
        JobKey jobKey =
                new JobKey(JobInstance.getStepIdentity(jobInstance, jobStep),
                        JobConstants.CUBE_JOB_GROUP_NAME);

        boolean res = false;
        try {
            JobDetail jobDetail = this.scheduler.getJobDetail(jobKey);

            IJobCommand iJobStepCmd =
                    (IJobCommand) jobDetail.getJobDataMap().get(JobConstants.PROP_JOB_CMD_EXECUTOR);
            if (null != iJobStepCmd) {
                iJobStepCmd.cancel();
            }

            jobDetail.getJobDataMap().put(JobConstants.PROP_JOB_KILLED, true);
            this.scheduler.addJob(jobDetail, true, true);

            @SuppressWarnings("unchecked")
            ConcurrentHashMap<String, JobFlow> jobFlows =
                    (ConcurrentHashMap<String, JobFlow>) this.scheduler.getContext().get(
                            JobConstants.PROP_JOB_RUNTIME_FLOWS);
            jobFlows.remove(JobInstance.getJobIdentity(jobInstance));
        } catch (UnableToInterruptJobException e) {
            log.error(e.getLocalizedMessage(), e);
            throw new JobException(e);
        } catch (SchedulerException e) {
            log.error(e.getLocalizedMessage(), e);
            throw new JobException(e);
        }
        return res;
    }

    public void stop() throws JobException {
        try {
            this.scheduler.standby();
        } catch (SchedulerException e) {
            throw new JobException(e);
        }
    }

    public Scheduler getScheduler() {
        return this.scheduler;
    }

    //// metrics

    public int getThreadPoolSize() {
        try {
            return scheduler.getMetaData().getThreadPoolSize();
        } catch (SchedulerException e) {
            log.error("Can't get scheduler metadata!", e);
            return 0;
        }
    }

    public int getRunningJobs() {
        try {
            return this.scheduler.getCurrentlyExecutingJobs().size();
        } catch (SchedulerException e) {
            log.error("Can't get scheduler metadata!", e);
            return 0;
        }
    }

    public int getIdleSlots() {
        try {
            return this.scheduler.getMetaData().getThreadPoolSize()
                    - this.scheduler.getCurrentlyExecutingJobs().size();
        } catch (SchedulerException e) {
            log.error("Can't get scheduler metadata!", e);
            return 0;
        }
    }

    public int getScheduledJobs() {
        int allTriggersCount = 0;
        try {
            for (String groupName : scheduler.getJobGroupNames()) {
                allTriggersCount += scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName)).size();
            }
        } catch (SchedulerException e) {
            log.error("Can't get scheduler metadata!", e);
        }
        return allTriggersCount;
    }

}
