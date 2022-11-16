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
package org.apache.kylin.rest;

import java.util.concurrent.Executors;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.util.LoadCounter;
import org.apache.kylin.rest.config.initialize.SparderStartEvent;
import org.apache.kylin.rest.monitor.SparkContextCanary;
import org.apache.spark.scheduler.SparkUIZombieJobCleaner;
import org.apache.spark.sql.SparderEnv;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@AutoConfigureOrder
public class SparderConfiguration {

    @Component
    class SparderAsyncListener implements ApplicationListener<SparderStartEvent.AsyncEvent> {
        @Override
        @Async
        public void onApplicationEvent(SparderStartEvent.AsyncEvent event) {
            init();
        }
    }

    @Component
    class SparderSyncListener implements ApplicationListener<SparderStartEvent.SyncEvent> {
        @Override
        public void onApplicationEvent(SparderStartEvent.SyncEvent event) {
            init();
        }
    }

    public void init() {
        SparderEnv.init();
        if (KylinConfig.getInstanceFromEnv().isCleanSparkUIZombieJob()) {
            SparkUIZombieJobCleaner.regularClean();
        }
        if (System.getProperty("spark.local", "false").equals("true")) {
            log.debug("spark.local=true");
            return;
        }

        // monitor Spark
        if (KapConfig.getInstanceFromEnv().getSparkCanaryEnable()) {
            val service = Executors.newSingleThreadScheduledExecutor();
            SparkContextCanary.getInstance().init(service);
            LoadCounter.getInstance().init(service);
        }
    }

}
