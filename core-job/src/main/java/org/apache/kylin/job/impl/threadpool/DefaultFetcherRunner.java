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

import org.apache.kylin.common.util.SetThreadName;
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

            int nRunning = 0, nReady = 0, nStopped = 0, nOthers = 0, nError = 0, nDiscarded = 0, nSUCCEED = 0;
            for (final String id : getExecutableManger().getAllJobIdsInCache()) {
                if (isJobPoolFull()) {
                    return;
                }
                if (runningJobs.containsKey(id)) {
                    // logger.debug("Job id:" + id + " is already running");
                    nRunning++;
                    continue;
                }

                final Output outputDigest = getExecutableManger().getOutputDigest(id);
                if ((outputDigest.getState() != ExecutableState.READY)) {
                    // logger.debug("Job id:" + id + " not runnable");
                    if (outputDigest.getState() == ExecutableState.SUCCEED) {
                        nSUCCEED++;
                    } else if (outputDigest.getState() == ExecutableState.ERROR) {
                        nError++;
                    } else if (outputDigest.getState() == ExecutableState.DISCARDED) {
                        nDiscarded++;
                    } else if (outputDigest.getState() == ExecutableState.STOPPED) {
                        nStopped++;
                    } else {
                        if (fetchFailed) {
                            getExecutableManger().forceKillJob(id);
                            nError++;
                        } else {
                            nOthers++;
                        }
                    }
                    continue;
                }

                final AbstractExecutable executable = getExecutableManger().getJob(id);
                if (!executable.isReady()) {
                    nOthers++;
                    continue;
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
