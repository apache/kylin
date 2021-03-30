/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.job.impl.threadpool;

import java.util.Map;
import java.util.Set;

import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.annotations.VisibleForTesting;

public abstract class FetcherRunner implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(FetcherRunner.class);

    protected JobEngineConfig jobEngineConfig;
    protected DefaultContext context;
    protected JobExecutor jobExecutor;
    protected volatile boolean fetchFailed = false;
    protected Set<String> succeedJobs = Sets.newHashSet();//cache succeed jobid
    protected static int nRunning, nReady, nStopped, nOthers, nError, nDiscarded, nSUCCEED;

    public FetcherRunner(JobEngineConfig jobEngineConfig, DefaultContext context, JobExecutor jobExecutor) {
        this.jobEngineConfig = jobEngineConfig;
        this.context = context;
        this.jobExecutor = jobExecutor;
    }

    protected boolean isJobPoolFull() {
        Map<String, Executable> runningJobs = context.getRunningJobs();
        if (runningJobs.size() >= jobEngineConfig.getMaxConcurrentJobLimit()) {
            logger.warn("There are too many jobs running, Job Fetch will wait until next schedule time");
            return true;
        }

        return false;
    }

    protected void addToJobPool(AbstractExecutable executable, int priority) {
        String jobDesc = executable.toString();
        logger.info(jobDesc + " prepare to schedule and its priority is " + priority);
        try {
            context.addRunningJob(executable);
            jobExecutor.execute(executable);
            logger.info(jobDesc + " scheduled");
        } catch (Exception ex) {
            context.removeRunningJob(executable);
            logger.warn(jobDesc + " fail to schedule", ex);
        }
    }
    
    protected void jobStateCount(String id) {
        final Output outputDigest = getExecutableManager().getOutputDigest(id);
        // logger.debug("Job id:" + id + " not runnable");
        if (outputDigest.getState() == ExecutableState.SUCCEED) {
            succeedJobs.add(id);
            nSUCCEED++;
        } else if (outputDigest.getState() == ExecutableState.ERROR) {
            nError++;
        } else if (outputDigest.getState() == ExecutableState.DISCARDED) {
            nDiscarded++;
        } else if (outputDigest.getState() == ExecutableState.STOPPED) {
            nStopped++;
        } else {
            if (fetchFailed) {
                getExecutableManager().forceKillJob(id);
                nError++;
            } else {
                nOthers++;
            }
        }
    }

    @VisibleForTesting
    void setFetchFailed(boolean fetchFailed) {
        this.fetchFailed = fetchFailed;
    }

    ExecutableManager getExecutableManager() {
        return ExecutableManager.getInstance(jobEngineConfig.getConfig());
    }
}
