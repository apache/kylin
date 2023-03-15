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
package org.apache.kylin.job.runners;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.JobStoppedNonVoluntarilyException;
import org.apache.kylin.job.exception.JobStoppedVoluntarilyException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.common.logging.SetLogCategory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;
import lombok.var;

public class JobRunner extends AbstractDefaultSchedulerRunner {
    private static final Logger logger = LoggerFactory.getLogger(JobRunner.class);

    private final AbstractExecutable executable;

    private final FetcherRunner fetcherRunner;

    public JobRunner(NDefaultScheduler nDefaultScheduler, AbstractExecutable executable, FetcherRunner fetcherRunner) {
        super(nDefaultScheduler);
        this.fetcherRunner = fetcherRunner;
        this.executable = executable;
    }

    @Override
    protected void doRun() {
        //only the first 8 chars of the job uuid
        val jobIdSimple = executable.getId().substring(0, 8);
        try (SetThreadName ignored = new SetThreadName("JobWorker(project:%s,jobid:%s)", project, jobIdSimple);
                SetLogCategory logCategory = new SetLogCategory("schedule")) {
            executable.execute(context);
            // trigger the next step asap
            fetcherRunner.scheduleNext();
        } catch (JobStoppedVoluntarilyException | JobStoppedNonVoluntarilyException e) {
            logger.info("Job quits either voluntarily or non-voluntarily,job: {}", jobIdSimple, e);
        } catch (ExecuteException e) {
            logger.error("ExecuteException occurred while job: " + executable.getId(), e);
        } catch (Exception e) {
            logger.error("unknown error execute job: " + executable.getId(), e);
        } finally {
            context.removeRunningJob(executable);
            val config = KylinConfig.getInstanceFromEnv();
            var usingMemory = 0;
            if (!config.getDeployMode().equals("cluster")) {
                usingMemory = executable.computeStepDriverMemory();
            }
            logger.info("Before job:{} global memory release {}", jobIdSimple,
                    NDefaultScheduler.getMemoryRemaining().availablePermits());
            NDefaultScheduler.getMemoryRemaining().release(usingMemory);
            logger.info("After job:{} global memory release {}", jobIdSimple,
                    NDefaultScheduler.getMemoryRemaining().availablePermits());
        }
    }
}
