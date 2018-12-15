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

import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class PriorityFetcherRunner extends FetcherRunner {

    private static final Logger logger = LoggerFactory.getLogger(PriorityFetcherRunner.class);

    private volatile PriorityQueue<Pair<AbstractExecutable, Integer>> jobPriorityQueue = new PriorityQueue<>(1,
            new Comparator<Pair<AbstractExecutable, Integer>>() {
                @Override
                public int compare(Pair<AbstractExecutable, Integer> o1, Pair<AbstractExecutable, Integer> o2) {
                    return o1.getSecond() > o2.getSecond() ? -1 : 1;
                }
            });

    public PriorityFetcherRunner(JobEngineConfig jobEngineConfig, DefaultContext context, JobExecutor jobExecutor) {
        super(jobEngineConfig, context, jobExecutor);
    }

    @Override
    synchronized public void run() {
        try (SetThreadName ignored = new SetThreadName(//
                "PriorityFetcherRunner %s", System.identityHashCode(this))) {//
            // logger.debug("Job Fetcher is running...");

            // fetch job from jobPriorityQueue first to reduce chance to scan job list
            Map<String, Integer> leftJobPriorities = Maps.newHashMap();
            Pair<AbstractExecutable, Integer> executableWithPriority;
            while ((executableWithPriority = jobPriorityQueue.peek()) != null
                    // the priority of jobs in pendingJobPriorities should be above a threshold
                    && executableWithPriority.getSecond() >= jobEngineConfig.getFetchQueuePriorityBar()) {
                executableWithPriority = jobPriorityQueue.poll();
                AbstractExecutable executable = executableWithPriority.getFirst();
                int curPriority = executableWithPriority.getSecond();
                // the job should wait more than one time
                if (curPriority > executable.getDefaultPriority() + 1) {
                    addToJobPool(executable, curPriority);
                } else {
                    leftJobPriorities.put(executable.getId(), curPriority + 1);
                }
            }

            Map<String, Executable> runningJobs = context.getRunningJobs();
            if (isJobPoolFull()) {
                return;
            }

            while ((executableWithPriority = jobPriorityQueue.poll()) != null) {
                leftJobPriorities.put(executableWithPriority.getFirst().getId(),
                        executableWithPriority.getSecond() + 1);
            }

            int nRunning = 0, nReady = 0, nStopped = 0, nOthers = 0, nError = 0, nDiscarded = 0, nSUCCEED = 0;
            for (final String id : getExecutableManger().getAllJobIdsInCache()) {
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

                AbstractExecutable executable = getExecutableManger().getJob(id);
                if (!executable.isReady()) {
                    nOthers++;
                    continue;
                }

                nReady++;
                Integer priority = leftJobPriorities.get(id);
                if (priority == null) {
                    priority = executable.getDefaultPriority();
                }
                jobPriorityQueue.add(new Pair<>(executable, priority));
            }

            while ((executableWithPriority = jobPriorityQueue.poll()) != null && !isJobPoolFull()) {
                addToJobPool(executableWithPriority.getFirst(), executableWithPriority.getSecond());
            }

            fetchFailed = false;
            logger.info("Priority Job Fetcher: " + nRunning + " running, " + runningJobs.size() + " actual running, "
                    + nStopped + " stopped, " + nReady + " ready, " + jobPriorityQueue.size() + " waiting, " //
                    + nSUCCEED + " already succeed, " + nError + " error, " + nDiscarded + " discarded, " + nOthers
                    + " others");
        } catch (Throwable th) {
            fetchFailed = true; // this could happen when resource store is unavailable
            logger.warn("Priority Job Fetcher caught a exception " + th);
        }
    }
}
