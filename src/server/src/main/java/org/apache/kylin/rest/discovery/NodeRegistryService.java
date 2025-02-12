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

package org.apache.kylin.rest.discovery;

import java.util.TimeZone;
import java.util.concurrent.locks.Lock;

import javax.annotation.PreDestroy;

import org.apache.commons.lang.StringUtils;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.system.NodeRegistryManager;
import org.apache.kylin.rest.aspect.Transaction;
import org.apache.kylin.rest.util.SpringContext;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@ConditionalOnNodeRegistryJdbcEnabled
@Service
@Slf4j
public class NodeRegistryService {

    private ThreadPoolTaskScheduler scheduler;

    @EventListener(ApplicationReadyEvent.class)
    public void initOnApplicationReady(ApplicationReadyEvent event) {
        log.info("NodeRegistryService initOnApplicationReady");
        SpringContext.getBean(NodeRegistryService.class).tryCreateNodeRegistry();

        // Since the spring scheduler annotation is skipped on query mode according to SchedulerEnhancer,
        // an independent task scheduler is used.
        // See org.apache.kylin.rest.aspect.SchedulerEnhancer for more details.
        scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("NodeRegistryScheduler-");
        scheduler.initialize();
        SpringContext.getBean(NodeRegistryService.class).manuallyScheduleTasks();
    }

    public void manuallyScheduleTasks() {
        if (scheduler == null) {
            throw new IllegalStateException("scheduler not initialized");
        }
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String renewCron = config.getServerNodeRegistryJdbcRenewCron();
        String checkCron = config.getServerNodeRegistryJdbcCheckCron();
        scheduler.schedule(() -> SpringContext.getBean(NodeRegistryService.class).scheduleRenew(),
                new CronTrigger(renewCron, TimeZone.getDefault()));
        scheduler.schedule(() -> SpringContext.getBean(NodeRegistryService.class).scheduleCheck(),
                new CronTrigger(checkCron, TimeZone.getDefault()));
    }

    @PreDestroy
    public void preDestroy() {
        log.info("NodeRegistryService preDestroy");
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(UnitOfWorkParams.builder().unitName(UnitOfWork.GLOBAL_UNIT)
                .projectId(StringUtils.EMPTY).readonly(false).maxRetry(1).processor(() -> {
                    try {
                        NodeRegistryManager manager = NodeRegistryManager.getInstance(KylinConfig.getInstanceFromEnv());
                        manager.cleanup();
                        return null;
                    } catch (Throwable throwable) {
                        throw new TransactionException(throwable);
                    }
                }).build());
    }

    public void scheduleRenew() {
        log.debug("Start renew registry");
        SpringContext.getBean(NodeRegistryService.class).renew();
        log.debug("End renew registry");
    }

    public void scheduleCheck() {
        Lock lock = KylinConfig.getInstanceFromEnv().getDistributedLockFactory()
                .lockForCurrentProcess("node-registry-check-lock");
        if (lock.tryLock()) {
            try {
                log.debug("Start check registry");
                SpringContext.getBean(NodeRegistryService.class).check();
                sleep(KylinConfig.getInstanceFromEnv().getServerNodeRegistryJdbcCheckIdleTime());
            } catch (Exception e) {
                log.warn("Check registry failed", e);
            } finally {
                lock.unlock();
                log.debug("End check registry");
            }
        }
    }

    @Transaction
    public void tryCreateNodeRegistry() {
        NodeRegistryManager manager = NodeRegistryManager.getInstance(KylinConfig.getInstanceFromEnv());
        try {
            manager.createNodeRegistryIfNotExists();
        } catch (PersistenceException e) {
            log.warn("SYSTEM/node_registry is most likely existing");
        }
    }

    @Transaction
    public void renew() {
        NodeRegistryManager manager = NodeRegistryManager.getInstance(KylinConfig.getInstanceFromEnv());
        manager.renew();
    }

    @Transaction(retry = 1)
    public void check() {
        NodeRegistryManager manager = NodeRegistryManager.getInstance(KylinConfig.getInstanceFromEnv());
        manager.checkAndClean();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            log.warn("Thread sleeping was interrupted");
            Thread.currentThread().interrupt();
        }
    }
}
