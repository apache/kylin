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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultFetcherRunner extends FetcherRunner {

    private static final Logger logger = LoggerFactory.getLogger(DefaultFetcherRunner.class);

    public DefaultFetcherRunner(JobEngineConfig jobEngineConfig, DefaultContext context, JobExecutor jobExecutor) {
        super(jobEngineConfig, context, jobExecutor);
    }

    @Override
    synchronized public void run() {
        try (SetThreadName ignored = new SetThreadName(//
                "FetcherRunner %s", System.identityHashCode(this))) {//
            // logger.debug("Job Fetcher is running...");
            Map<String, Executable> runningJobs = context.getRunningJobs();
            if (isJobPoolFull()) {
                return;
            }

            nRunning = 0;
            nReady = 0;
            nStopped = 0;
            nOthers = 0;
            nError = 0;
            nDiscarded = 0;
            nSUCCEED = 0;
            for (final String id : getExecutableManager().getAllJobIdsInCache()) {
                if (isJobPoolFull()) {
                    return;
                }
                if (runningJobs.containsKey(id)) {
                    // logger.debug("Job id:" + id + " is already running");
                    nRunning++;
                    continue;
                }
                if (succeedJobs.contains(id)) {
                    nSUCCEED++;
                    continue;
                }

                final Output outputDigest;
                try {
                    outputDigest = getExecutableManager().getOutputDigest(id);
                } catch (IllegalArgumentException e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("job " + id + " output digest is null.", e);
                    }
                    nOthers++;
                    continue;
                }
                if ((outputDigest.getState() != ExecutableState.READY)) {
                    // logger.debug("Job id:" + id + " not runnable");
                    jobStateCount(id);
                    continue;
                }

                final AbstractExecutable executable = getExecutableManager().getJob(id);
                if (executable == null) {
                    logger.info("job " + id + " get job is null, skip.");
                    nOthers++;
                    continue;
                }
                if (!executable.isReady()) {
                    nOthers++;
                    continue;
                }

                KylinConfig config = jobEngineConfig.getConfig();
                if (config.isSchedulerSafeMode()) {
                    String cubeName = executable.getCubeName();
                    String projectName = CubeManager.getInstance(config).getCube(cubeName).getProject();
                    if (!config.getSafeModeRunnableProjects().contains(projectName) &&
                            executable.getStartTime() == 0) {
                        logger.info("New job is pending for scheduler in safe mode. Project: {}, job: {}",
                                projectName, executable.getName());
                        continue;
                    }
                }

                nReady++;
                addToJobPool(executable, executable.getDefaultPriority());
            }

            fetchFailed = false;
            logger.info("Job Fetcher: " + nRunning + " should running, " + runningJobs.size() + " actual running, "
                    + nStopped + " stopped, " + nReady + " ready, " + nSUCCEED + " already succeed, " + nError
                    + " error, " + nDiscarded + " discarded, " + nOthers + " others");
        } catch (Throwable th) {
            fetchFailed = true; // this could happen when resource store is unavailable
            logger.warn("Job Fetcher caught a exception ", th);
        }
    }
}
