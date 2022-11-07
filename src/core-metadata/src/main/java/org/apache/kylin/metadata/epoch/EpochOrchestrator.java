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

package org.apache.kylin.metadata.epoch;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.persistence.transaction.AuditLogReplayWorker;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.AddressUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.guava20.shaded.common.eventbus.Subscribe;
import lombok.Synchronized;

/**
 */
public class EpochOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(EpochOrchestrator.class);

    private final EpochManager epochMgr;

    private ScheduledExecutorService checkerPool;

    private volatile boolean isCheckerRunning = true;

    private static final String OWNER_IDENTITY;

    static {
        OWNER_IDENTITY = AddressUtil.getLocalInstance() + "|" + System.currentTimeMillis();
    }

    public static String getOwnerIdentity() {
        return OWNER_IDENTITY;
    }

    public EpochOrchestrator(KylinConfig kylinConfig) {
        epochMgr = EpochManager.getInstance();
        String serverMode = kylinConfig.getServerMode();
        if (!kylinConfig.isJobNode()) {
            logger.info("server mode: {},  no need to run EventOrchestrator", serverMode);
            return;
        }

        long pollSecond = kylinConfig.getEpochCheckerIntervalSecond();
        logger.info("Try to update epoch every {} seconds", pollSecond);
        logger.info("renew executor work size is :{}", kylinConfig.getRenewEpochWorkerPoolSize());

        checkerPool = Executors.newScheduledThreadPool(2, new NamedThreadFactory("EpochChecker"));
        checkerPool.scheduleWithFixedDelay(new EpochChecker(), 1, pollSecond, TimeUnit.SECONDS);
        checkerPool.scheduleAtFixedRate(new EpochRenewer(), pollSecond, pollSecond, TimeUnit.SECONDS);

        EventBusFactory.getInstance().register(new ReloadMetadataListener(), true);
    }

    public void shutdown() {
        logger.info("Shutting down EpochOrchestrator ....");
        if (checkerPool != null)
            ExecutorServiceUtil.shutdownGracefully(checkerPool, 60);
    }

    public void forceShutdown() {
        logger.info("Shutting down EpochOrchestrator ....");
        if (checkerPool != null)
            ExecutorServiceUtil.forceShutdown(checkerPool);
    }

    @Synchronized
    void updateCheckerStatus(boolean isRunning) {
        logger.info("Change epoch checker status from {} to {}", isCheckerRunning, isRunning);
        isCheckerRunning = isRunning;
    }

    class EpochChecker implements Runnable {

        @Override
        public synchronized void run() {
            try {
                if (!isCheckerRunning) {
                    return;
                }
                epochMgr.getEpochUpdateManager().tryUpdateAllEpochs();
            } catch (Exception e) {
                logger.error("Failed to update epochs");
            }
        }
    }

    class EpochRenewer implements Runnable {

        private final AtomicBoolean raceCheck = new AtomicBoolean(false);

        @Override
        public void run() {
            try {
                if (!isCheckerRunning || !raceCheck.compareAndSet(false, true)) {
                    return;
                }
                epochMgr.getEpochUpdateManager().tryRenewOwnedEpochs();
            } catch (Exception e) {
                logger.error("Failed to renew epochs", e);
            } finally {
                raceCheck.compareAndSet(true, false);
            }
        }
    }

    class ReloadMetadataListener {

        @Subscribe
        public void onStart(AuditLogReplayWorker.StartReloadEvent start) {
            updateCheckerStatus(false);
            epochMgr.releaseOwnedEpochs();
        }

        @Subscribe
        public void onEnd(AuditLogReplayWorker.EndReloadEvent end) {
            updateCheckerStatus(true);
            epochMgr.updateAllEpochs();
        }
    }

}
