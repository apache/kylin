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

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.rest.config.initialize.AfterMetadataReadyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;

@Component
public class FileSystemHealthIndicator implements HealthIndicator, ApplicationListener<AfterMetadataReadyEvent> {
    private static final Logger logger = LoggerFactory.getLogger(FileSystemHealthIndicator.class);
    private static final ScheduledExecutorService FILE_SYSTEM_HEALTH_EXECUTOR = Executors.newScheduledThreadPool(1,
            new NamedThreadFactory("FileSystemHealthChecker"));
    private volatile boolean isHealth = false;

    @Override
    public void onApplicationEvent(AfterMetadataReadyEvent event) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        FILE_SYSTEM_HEALTH_EXECUTOR.scheduleWithFixedDelay(this::healthCheck, 0, config.getMetadataCheckDuration(),
                TimeUnit.MILLISECONDS);
    }

    public void healthCheck() {
        try {
            checkFileSystem();
            isHealth = true;
            return;
        } catch (IOException e) {
            logger.error("File System is closed, try to clean cache.", e);
        }

        // verify again
        try {
            FileSystem.closeAll();
            checkFileSystem();
            isHealth = true;
            return;
        } catch (IOException e) {
            logger.error("File System is closed AND DID NOT RECOVER", e);
        }
        isHealth = false;
    }

    @VisibleForTesting
    public void checkFileSystem() throws IOException {
        FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
        fileSystem.exists(new Path("/"));
    }

    @Override
    public Health health() {
        return isHealth ? Health.up().build() : Health.down().build();
    }
}
