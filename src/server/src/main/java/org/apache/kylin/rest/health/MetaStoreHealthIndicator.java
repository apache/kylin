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
package org.apache.kylin.rest.health;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.rest.config.initialize.AfterMetadataReadyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;

@Component
public class MetaStoreHealthIndicator implements HealthIndicator, ApplicationListener<AfterMetadataReadyEvent> {
    private static final Logger logger = LoggerFactory.getLogger(MetaStoreHealthIndicator.class);

    private static final String UNIT_NAME = "_health";
    private static final String HEALTH_ROOT_PATH = "/" + UNIT_NAME;
    private static final String UUID_PATH = "/UUID";
    private static final int MAX_RETRY = 3;
    private static final ScheduledExecutorService META_STORE_HEALTH_EXECUTOR = Executors.newScheduledThreadPool(1,
            new NamedThreadFactory("MetaStoreHealthChecker"));
    private volatile boolean isHealth = false;

    private final int warningResponseMs;
    private final int errorResponseMs;

    public MetaStoreHealthIndicator() {
        KapConfig wrappedConfig = KapConfig.wrap(KylinConfig.getInstanceFromEnv());
        this.warningResponseMs = wrappedConfig.getMetaStoreHealthWarningResponseMs();
        this.errorResponseMs = wrappedConfig.getMetaStoreHealthErrorResponseMs();
    }

    private void checkTime(long start, String operation) throws InterruptedException {
        // in case canary was timeout
        if (Thread.interrupted())
            throw new InterruptedException();

        long response = System.currentTimeMillis() - start;
        logger.trace("{} took {} ms", operation, response);

        if (response > errorResponseMs) {
            throw new IllegalStateException("check time is time out");
        } else if (response > warningResponseMs) {
            logger.warn("found warning, {} took {} ms", operation, response);
        }
    }

    @Override
    public void onApplicationEvent(AfterMetadataReadyEvent event) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        META_STORE_HEALTH_EXECUTOR.scheduleWithFixedDelay(this::healthCheck, 0, config.getMetadataCheckDuration(),
                TimeUnit.MILLISECONDS);
    }

    public void healthCheck() {
        Health ret;
        try {
            if (KylinConfig.getInstanceFromEnv().isJobNode()) {
                ret = allNodeCheck();
            } else {
                ret = queryNodeCheck();
            }
        } catch (Exception e) {
            logger.error("Failed to check the metastore health", e);
            isHealth = false;
            return;
        }

        if (Objects.isNull(ret)) {
            isHealth = false;
            return;
        }

        isHealth = true;
    }

    @VisibleForTesting
    public Health allNodeCheck() {
        return UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.<Health> builder().skipAuditLog(true)
                .unitName(UNIT_NAME).maxRetry(MAX_RETRY).processor(() -> {
                    ResourceStore store;
                    try {
                        store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to get meta store", e);
                    }

                    String uuid = RandomUtil.randomUUIDStr();
                    String resourcePath = HEALTH_ROOT_PATH + "/" + uuid;
                    long start;
                    String op;

                    // test write
                    op = "Writing metadata (40 bytes)";
                    logger.trace(op);
                    start = System.currentTimeMillis();
                    try {
                        store.checkAndPutResource(resourcePath, new StringEntity(uuid), StringEntity.serializer);
                        checkTime(start, op);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to write metadata", e);
                    }

                    // test read
                    op = "Reading metadata (40 bytes)";
                    logger.trace(op);
                    start = System.currentTimeMillis();
                    try {
                        StringEntity value = store.getResource(resourcePath, StringEntity.serializer);
                        checkTime(start, op);
                        if (!new StringEntity(uuid).equals(value)) {
                            throw new RuntimeException("Metadata store failed to read a newly created resource.");
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to read metadata", e);
                    }

                    // test delete
                    op = "Deleting metadata (40 bytes)";
                    logger.trace(op);
                    start = System.currentTimeMillis();
                    try {
                        store.deleteResource(resourcePath);
                        checkTime(start, op);
                    } catch (Exception e) {
                        logger.error("Failed to delete metadata", e);
                        throw new RuntimeException("Failed to delete metadata", e);
                    }

                    return Health.up().build();
                }).build());
    }

    private Health queryNodeCheck() {
        return UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.<Health> builder().skipAuditLog(true).readonly(true)
                .unitName(UNIT_NAME).maxRetry(MAX_RETRY).processor(() -> {
                    ResourceStore store;
                    try {
                        store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to get meta store", e);
                    }

                    long start;
                    String op;

                    // test read
                    op = "Reading metadata /UUID";
                    logger.trace(op);
                    start = System.currentTimeMillis();
                    try {
                        StringEntity value = store.getResource(UUID_PATH, StringEntity.serializer);
                        checkTime(start, op);
                        if (Objects.isNull(value)) {
                            throw new RuntimeException("Metadata store failed to read a resource.");
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to read metadata", e);
                    }

                    return Health.up().build();
                }).build());
    }

    @Override
    public Health health() {
        return isHealth ? Health.up().build() : Health.down().build();
    }
}
