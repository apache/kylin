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
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.SneakyThrows;

public abstract class AbstractDefaultSchedulerRunner implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(AbstractDefaultSchedulerRunner.class);
    protected final NDefaultScheduler nDefaultScheduler;

    protected final ExecutableContext context;

    protected final String project;

    public AbstractDefaultSchedulerRunner(final NDefaultScheduler nDefaultScheduler) {
        this.nDefaultScheduler = nDefaultScheduler;
        this.context = nDefaultScheduler.getContext();
        this.project = nDefaultScheduler.getProject();
    }

    @SneakyThrows
    private boolean checkEpochIdFailed() {
        //check failed if isInterrupted
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }

        if (!KylinConfig.getInstanceFromEnv().isUTEnv()
                && !EpochManager.getInstance().checkEpochId(context.getEpochId(), project)) {
            nDefaultScheduler.forceShutdown();
            return true;
        }
        return false;
    }

    // stop job if Storage Quota Limit reached
    protected void stopJobIfSQLReached(String jobId) {
        if (context.isReachQuotaLimit()) {
            try {
                EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    if (context.isReachQuotaLimit()) {
                        NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).pauseJob(jobId);
                        logger.info("Job {} paused due to no available storage quota.", jobId);
                        logger.info("Please clean up low-efficient storage in time, "
                                + "increase the low-efficient storage threshold, "
                                + "or notify the administrator to increase the storage quota for this project.");
                    }
                    return null;
                }, project, UnitOfWork.DEFAULT_MAX_RETRY, context.getEpochId(), jobId);
            } catch (Exception e) {
                logger.warn("[UNEXPECTED_THINGS_HAPPENED] project {} job {} failed to pause", project, jobId, e);
            }
        }
    }

    @Override
    public void run() {
        try (SetLogCategory ignored = new SetLogCategory("schedule")) {
            if (checkEpochIdFailed()) {
                return;
            }
            doRun();
        }
    }

    protected abstract void doRun();
}
