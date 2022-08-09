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

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class JobCheckRunner extends AbstractDefaultSchedulerRunner {

    private static final Logger logger = LoggerFactory.getLogger(JobCheckRunner.class);

    public JobCheckRunner(NDefaultScheduler nDefaultScheduler) {
        super(nDefaultScheduler);
    }

    private boolean checkTimeoutIfNeeded(String jobId, Long startTime) {
        Integer timeOutMinute = KylinConfig.getInstanceFromEnv().getSchedulerJobTimeOutMinute();
        if (timeOutMinute == 0) {
            return false;
        }
        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        if (executableManager.getJob(jobId).getStatus().isFinalState()) {
            return false;
        }
        long duration = System.currentTimeMillis() - startTime;
        long durationMins = Math.toIntExact(duration / (60 * 1000));
        return durationMins >= timeOutMinute;
    }

    private boolean discardTimeoutJob(String jobId, Long startTime) {
        try {
            if (checkTimeoutIfNeeded(jobId, startTime)) {
                return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    if (checkTimeoutIfNeeded(jobId, startTime)) {
                        logger.error("project {} job {} running timeout.", project, jobId);
                        NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).errorJob(jobId);
                        return true;
                    }
                    return false;
                }, project, UnitOfWork.DEFAULT_MAX_RETRY, context.getEpochId(), jobId);
            }
        } catch (Exception e) {
            logger.warn("[UNEXPECTED_THINGS_HAPPENED] project " + project + " job " + jobId
                    + " should be timeout but discard failed", e);
        }
        return false;
    }

    @Override
    protected void doRun() {

        logger.info("start check project {} job pool.", project);

        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        Map<String, Executable> runningJobs = context.getRunningJobs();
        Map<String, Long> runningJobInfos = context.getRunningJobInfos();
        for (final String id : executableManager.getJobs()) {
            if (runningJobs.containsKey(id)) {
                discardTimeoutJob(id, runningJobInfos.get(id));
                stopJobIfSQLReached(id);
            }
        }

    }
}
