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

import java.util.Date;
import java.util.List;

import org.apache.commons.jnet.Installer;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.constant.Constant;
import org.apache.kylin.common.hystrix.NCircuitBreaker;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.metadata.EpochStore;
import org.apache.kylin.common.persistence.metadata.JdbcAuditLogStore;
import org.apache.kylin.common.persistence.transaction.EventListenerRegistry;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.HostInfoFetcher;
import org.apache.kylin.engine.spark.ExecutableUtils;
import org.apache.kylin.metadata.epoch.EpochOrchestrator;
import org.apache.kylin.metadata.project.NProjectLoader;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.streaming.JdbcStreamingJobStatsStore;
import org.apache.kylin.rest.config.initialize.AclTCRListener;
import org.apache.kylin.rest.config.initialize.AfterMetadataReadyEvent;
import org.apache.kylin.rest.config.initialize.CacheCleanListener;
import org.apache.kylin.rest.config.initialize.EpochChangedListener;
import org.apache.kylin.rest.config.initialize.JobSchedulerListener;
import org.apache.kylin.rest.config.initialize.ModelBrokenListener;
import org.apache.kylin.rest.config.initialize.ProcessStatusListener;
import org.apache.kylin.rest.config.initialize.QueryMetricsListener;
import org.apache.kylin.rest.config.initialize.SparderStartEvent;
import org.apache.kylin.rest.config.initialize.TableSchemaChangeListener;
import org.apache.kylin.rest.service.CommonQueryCacheSupporter;
import org.apache.kylin.rest.util.JStackDumpTask;
import org.apache.kylin.streaming.jobs.StreamingJobListener;
import org.apache.kylin.tool.daemon.KapGuardianHATask;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationPreparedEvent;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.TaskScheduler;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@Order(1)
public class AppInitializer {

    @Autowired
    TaskScheduler taskScheduler;

    @Autowired(required = false)
    CommonQueryCacheSupporter queryCacheManager;

    @Autowired(required = false)
    EpochChangedListener epochChangedListener;

    @Autowired(required = false)
    HostInfoFetcher hostInfoFetcher;

    JdbcStreamingJobStatsStore streamingJobStatsStore;

    @EventListener(ApplicationPreparedEvent.class)
    public void init(ApplicationPreparedEvent event) throws Exception {
        val kylinConfig = KylinConfig.getInstanceFromEnv();

        NCircuitBreaker.start(KapConfig.wrap(kylinConfig));

        boolean isJob = kylinConfig.isJobNode();

        if (isJob) {
            // restore from metadata, should not delete
            val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
            resourceStore.setChecker((e) -> {
                String instance = e.getInstance();
                String localIdentify = EpochOrchestrator.getOwnerIdentity().split("\\|")[0];
                return localIdentify.equalsIgnoreCase(instance);
            });
            streamingJobStatsStore = new JdbcStreamingJobStatsStore(kylinConfig);

            // register scheduler listener
            EventBusFactory.getInstance().register(new JobSchedulerListener(), false);
            EventBusFactory.getInstance().register(new ModelBrokenListener(), false);
            EventBusFactory.getInstance().register(epochChangedListener, false);
            //            EventBusFactory.getInstance().registerBroadcast(broadcastListener);
            EventBusFactory.getInstance().register(new ProcessStatusListener(), true);
            EventBusFactory.getInstance().register(new StreamingJobListener(), true);

            ExecutableUtils.initJobFactory();
        } else {
            val auditLogStore = new JdbcAuditLogStore(kylinConfig);
            val epochStore = EpochStore.getEpochStore(kylinConfig);
            kylinConfig.setQueryHistoryUrl(kylinConfig.getQueryHistoryUrl().toString());
            kylinConfig.setStreamingStatsUrl(kylinConfig.getStreamingStatsUrl().toString());
            if (kylinConfig.getMetadataStoreType().equals("hdfs")) {
                kylinConfig.setProperty("kylin.metadata.url", kylinConfig.getMetadataUrlPrefix() + "@hdfs");
            }
            val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
            resourceStore.getMetadataStore().setAuditLogStore(auditLogStore);
            resourceStore.catchup();
            resourceStore.getMetadataStore().setEpochStore(epochStore);
        }

        kylinConfig.getDistributedLockFactory().initialize();
        warmUpSystemCache();
        event.getApplicationContext().publishEvent(new AfterMetadataReadyEvent(event.getApplicationContext()));

        if (kylinConfig.isQueryNode()) {
            if (kylinConfig.isSparderAsync()) {
                event.getApplicationContext()
                        .publishEvent(new SparderStartEvent.AsyncEvent(event.getApplicationContext()));
            } else {
                event.getApplicationContext()
                        .publishEvent(new SparderStartEvent.SyncEvent(event.getApplicationContext()));
            }
        }
        // register acl update listener
        EventListenerRegistry.getInstance(kylinConfig).register(new AclTCRListener(queryCacheManager), "acl");
        // register schema change listener
        EventListenerRegistry.getInstance(kylinConfig).register(new TableSchemaChangeListener(queryCacheManager),
                "table");
        // register for clean cache when delete
        EventListenerRegistry.getInstance(kylinConfig).register(new CacheCleanListener(), "cacheInManager");

        EventBusFactory.getInstance().register(new QueryMetricsListener(), false);

        postInit();
    }

    private void warmUpSystemCache() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        List<ProjectInstance> prjInstances = NProjectManager.getInstance(kylinConfig).listAllProjects();
        prjInstances.forEach(prjInstance -> {
            NProjectLoader.updateCache(prjInstance.getName());
            NProjectLoader.removeCache();
        });
    }

    @EventListener(ApplicationReadyEvent.class)
    public void afterReady(ApplicationReadyEvent event) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        setFsUrlStreamHandlerFactory();
        if (kylinConfig.isJobNode()) {
            new EpochOrchestrator(kylinConfig);
        }
        if (kylinConfig.getJStackDumpTaskEnabled()) {
            taskScheduler.scheduleAtFixedRate(new JStackDumpTask(),
                    kylinConfig.getJStackDumpTaskPeriod() * Constant.MINUTE);
        }
        if (kylinConfig.isGuardianEnabled() && kylinConfig.isGuardianHAEnabled()) {
            log.info("Guardian Process ha is enabled, start check scheduler");
            taskScheduler.scheduleAtFixedRate(new KapGuardianHATask(),
                    new Date(System.currentTimeMillis() + kylinConfig.getGuardianHACheckInitDelay() * Constant.SECOND),
                    kylinConfig.getGuardianHACheckInterval() * Constant.SECOND);
        }
    }

    private void postInit() {
        AddressUtil.setHostInfoFetcher(hostInfoFetcher);
    }

    private static void setFsUrlStreamHandlerFactory() {
        try {
            Installer.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        } catch (Exception e) {
            log.warn("set Fs URL stream handler factory failed", e);
        }
    }
}
