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

package org.apache.kylin.rest.config;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.common.metrics.MetricsController;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.config.initialize.MetricsRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class MetricsConfig {
    @Autowired
    ClusterManager clusterManager;

    private static final ScheduledExecutorService METRICS_SCHEDULED_EXECUTOR = Executors.newScheduledThreadPool(2,
            new NamedThreadFactory("MetricsChecker"));

    private static final Set<String> allControlledProjects = Collections.synchronizedSet(new HashSet<>());

    @EventListener(ApplicationReadyEvent.class)
    public void registerMetrics() {
        if (!KapConfig.getInstanceFromEnv().isMonitorEnabled()) {
            return;
        }

        MetricsController.init(KapConfig.wrap(KylinConfig.getInstanceFromEnv()));

        String host = clusterManager.getLocalServer();

        log.info("Register global metrics...");
        MetricsRegistry.registerGlobalMetrics(KylinConfig.getInstanceFromEnv(), host);

        log.info("Register host metrics...");
        MetricsRegistry.registerHostMetrics(host);
        if (KylinConfig.getInstanceFromEnv().isPrometheusMetricsEnabled()) {
            log.info("Register prometheus global metrics... ");
            MetricsRegistry.registerGlobalPrometheusMetrics();
        }

        METRICS_SCHEDULED_EXECUTOR.scheduleAtFixedRate(() -> {
            Set<String> allProjects = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).listAllProjects()
                    .stream().map(ProjectInstance::getName).collect(Collectors.toSet());

            MetricsRegistry.refreshProjectLongRunningJobs(KylinConfig.getInstanceFromEnv(), allProjects);
            Sets.SetView<String> newProjects = Sets.difference(allProjects, allControlledProjects);
            for (String newProject : newProjects) {
                log.info("Register project metrics for {}", newProject);
                MetricsRegistry.registerProjectMetrics(KylinConfig.getInstanceFromEnv(), newProject, host);
                MetricsRegistry.registerProjectPrometheusMetrics(KylinConfig.getInstanceFromEnv(), newProject);
            }

            Sets.SetView<String> outDatedProjects = Sets.difference(allControlledProjects, allProjects);

            for (String outDatedProject : outDatedProjects) {
                log.info("Remove project metrics for {}", outDatedProject);
                MetricsGroup.removeProjectMetrics(outDatedProject);
                MetricsRegistry.removeProjectFromStorageSizeMap(outDatedProject);
            }

            allControlledProjects.clear();
            allControlledProjects.addAll(allProjects);

        }, 0, 1, TimeUnit.MINUTES);

        METRICS_SCHEDULED_EXECUTOR.scheduleAtFixedRate(MetricsRegistry::refreshTotalStorageSize,
                0, 10, TimeUnit.MINUTES);

        METRICS_SCHEDULED_EXECUTOR.execute(() -> MetricsController.startReporters(KapConfig.wrap(KylinConfig.getInstanceFromEnv())));
    }
}
