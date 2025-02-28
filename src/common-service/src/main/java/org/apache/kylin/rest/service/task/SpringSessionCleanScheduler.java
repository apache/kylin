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

package org.apache.kylin.rest.service.task;

import static org.apache.kylin.job.factory.JobFactoryConstant.SPRING_SESSION_CLEAN_EXPIRED_JOB_FACTORY;

import javax.annotation.PostConstruct;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.factory.JobFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@ConditionalOnProperty(name = "spring.session.store-type", havingValue = "JDBC")
@Slf4j
public class SpringSessionCleanScheduler {

    @PostConstruct
    public void init() {
        JobFactory.register(SPRING_SESSION_CLEAN_EXPIRED_JOB_FACTORY, new SpringSessionCleanExpiredJob.Factory());
    }

    @Scheduled(cron = "${spring.session.jdbc.cleanup-cron-task:0 */30 * * * *}")
    public void submitJob() {
        ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), ResourceStore.GLOBAL_PROJECT)
                .checkAndSubmitCronJob(SPRING_SESSION_CLEAN_EXPIRED_JOB_FACTORY,
                        JobTypeEnum.SPRING_SESSION_CLEAN_EXPIRED);
        log.debug("submit job to clean spring session");
    }

}
