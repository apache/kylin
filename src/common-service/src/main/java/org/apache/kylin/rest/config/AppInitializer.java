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

import javax.annotation.PostConstruct;

import org.apache.commons.jnet.Installer;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.constant.Constant;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.hystrix.NCircuitBreaker;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.lock.TransactionDeadLockHandler;
import org.apache.kylin.common.persistence.metadata.EpochStore;
import org.apache.kylin.common.persistence.metadata.JdbcAuditLogStore;
import org.apache.kylin.common.persistence.transaction.EventListenerRegistry;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.scheduler.ProjectSerialEventBus;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.HostInfoFetcher;
import org.apache.kylin.engine.spark.filter.QueryFiltersCollector;
import org.apache.kylin.engine.spark.utils.SparkJobFactoryUtils;
import org.apache.kylin.metadata.model.util.ComputedColumnUtil;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectLoader;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.streaming.JdbcStreamingJobStatsStore;
import org.apache.kylin.query.util.ComputedColumnRewriter;
import org.apache.kylin.rest.config.initialize.AclTCRListener;
import org.apache.kylin.rest.config.initialize.AfterMetadataReadyEvent;
import org.apache.kylin.rest.config.initialize.CacheCleanListener;
import org.apache.kylin.rest.config.initialize.JobSchedulerListener;
import org.apache.kylin.rest.config.initialize.ModelBrokenListener;
import org.apache.kylin.rest.config.initialize.ProcessStatusListener;
import org.apache.kylin.rest.config.initialize.QueryMetricsListener;
import org.apache.kylin.rest.config.initialize.SparderStartEvent;
import org.apache.kylin.rest.config.initialize.TableSchemaChangeListener;
import org.apache.kylin.rest.config.initialize.UserAclListener;
import org.apache.kylin.rest.service.CommonQueryCacheSupporter;
import org.apache.kylin.rest.service.task.QueryHistoryMetaUpdateScheduler;
import org.apache.kylin.rest.util.GCLogUploadTask;
import org.apache.kylin.rest.util.InitResourceGroupUtils;
import org.apache.kylin.rest.util.JStackDumpTask;
import org.apache.kylin.rest.util.QueryHistoryOffsetUtil;
import org.apache.kylin.streaming.jobs.StreamingJobListener;
import org.apache.kylin.streaming.jobs.scheduler.StreamingScheduler;
import org.apache.kylin.tool.daemon.KapGuardianHATask;
import org.apache.kylin.tool.garbage.CleanTaskExecutorService;
import org.apache.kylin.tool.garbage.PriorityExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.security.web.FilterChainProxy;
import org.springframework.security.web.firewall.DefaultHttpFirewall;
import org.springframework.security.web.firewall.HttpFirewall;
import org.springframework.session.web.http.CookieSerializer;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@Order(1)
@Profile("!test")
public class AppInitializer {

    @Autowired
    TaskScheduler taskScheduler;

    @Autowired(required = false)
    CommonQueryCacheSupporter queryCacheManager;

    @Autowired(required = false)
    HostInfoFetcher hostInfoFetcher;

    @Autowired
    ApplicationContext context;

    // https://github.com/spring-projects/spring-boot/issues/35240
    // since spring-boot-autoconfigure:2.7.12, the commit will
    // prevent early initialization of SessionRepository beans.
    // But we want the cookieSerializer to be initialized before init,
    // so we autowired it here.
    @Autowired
    CookieSerializer cookieSerializer;

    JdbcStreamingJobStatsStore streamingJobStatsStore;

    @PostConstruct
    public void init() throws Exception {

        val kylinConfig = KylinConfig.getInstanceFromEnv();
        NCircuitBreaker.start(KapConfig.wrap(kylinConfig));

        boolean isJob = kylinConfig.isJobNode();
        boolean isDataLoading = kylinConfig.isDataLoadingNode();
        boolean isMetadata = kylinConfig.isMetadataNode();
        boolean isQueryOnly = kylinConfig.isQueryNodeOnly();
        boolean isResource = kylinConfig.isResource();

        // set kylin.metadata.distributed-lock.jdbc.url
        // before kylin.metadata.url is changed
        kylinConfig.setJDBCDistributedLockURL(kylinConfig.getJDBCDistributedLockURL().toString());

        if (!isQueryOnly) {
            // restore from metadata, should not delete
            val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).createMetaStoreUuidIfNotExist();
                return true;
            }, ResourceStore.GLOBAL_PROJECT);
            if (!isResource) {
                resourceStore.catchup();
            }
            InitResourceGroupUtils.initResourceGroup();
            if (isJob || isDataLoading) {
                // register scheduler listener
                EventBusFactory.getInstance().register(new JobSchedulerListener(), false);
                if (kylinConfig.isStreamingConfigEnabled())
                    streamingJobStatsStore = new JdbcStreamingJobStatsStore(kylinConfig);
                // register scheduler listener
                EventBusFactory.getInstance().register(new StreamingJobListener(), true);
            }
            if (isJob || isMetadata) {
                EventBusFactory.getInstance().register(new ModelBrokenListener(), false);
            }

            SparkJobFactoryUtils.initJobFactory();
            ComputedColumnUtil.setEXTRACTOR(ComputedColumnRewriter::extractCcRexNode);
            TransactionDeadLockHandler.getInstance().start();
        } else {
            val auditLogStore = new JdbcAuditLogStore(kylinConfig);
            val epochStore = EpochStore.getEpochStore(kylinConfig);
            kylinConfig.setQueryHistoryUrl(kylinConfig.getQueryHistoryUrl().toString());
            kylinConfig.setStreamingStatsUrl(kylinConfig.getStreamingStatsUrl().toString());
            kylinConfig.setJdbcShareStateUrl(kylinConfig.getJdbcShareStateUrl().toString());
            if (kylinConfig.getMetadataStoreType().equals("hdfs")) {
                // cache db metadata url before switch to hdfs
                kylinConfig.setCoreMetadataDBUrl();
                kylinConfig.setProperty("kylin.metadata.url", kylinConfig.getMetadataUrlPrefix() + "@hdfs");
            }
            val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
            resourceStore.getMetadataStore().setAuditLogStore(auditLogStore);
            resourceStore.catchup();
            resourceStore.getMetadataStore().setEpochStore(epochStore);
        }

        kylinConfig.getDistributedLockFactory().initialize();
        if (!isResource) {
            warmUpSystemCache();
            cacheCcRexNode();
        }
        context.publishEvent(new AfterMetadataReadyEvent(context));

        if (kylinConfig.isQueryNode()) {
            if (kylinConfig.isSparderAsync()) {
                context.publishEvent(new SparderStartEvent.AsyncEvent(context));
            } else {
                context.publishEvent(new SparderStartEvent.SyncEvent(context));
            }
            // register acl update listener
            EventListenerRegistry.getInstance(kylinConfig).register(new AclTCRListener(queryCacheManager), "acl");
            // register schema change listener, for clean query cache
            EventListenerRegistry.getInstance(kylinConfig).register(new TableSchemaChangeListener(queryCacheManager),
                    "table");
            EventBusFactory.getInstance().register(new QueryMetricsListener(), false);
            if (kylinConfig.isBloomCollectFilterEnabled()) {
                QueryFiltersCollector.initScheduler();
            }
        }
        EventBusFactory.getInstance().register(ProcessStatusListener.getInstance(), true);
        // register for clean cache when delete
        EventListenerRegistry.getInstance(kylinConfig).register(new CacheCleanListener(), "cacheInManager");

        EventBusFactory.getInstance().register(new UserAclListener(), true);

        if (kylinConfig.isAllowNonAsciiCharInUrl()) {
            // Note: DefaultHttpFirewall vs StrictHttpFirewall
            // In order to allow Chinese chars on URL like "/{cubeName}/segments",
            // we have to use DefaultHttpFirewall.
            // If later we have to use StrictHttpFirewall,
            // then StrictHttpFirewall.rejectNonPrintableAsciiCharactersInFieldName()
            // must be overridden to allow Chinese chars on URL.
            FilterChainProxy filterChainProxy = context.getBean(FilterChainProxy.class);
            filterChainProxy.setFirewall(this.getHttpFirewall());
        }

        postInit();

        log.info("Kylin initialization completed.");
        log.info("KylinConfig in env, ID is {}", kylinConfig.hashCode());
        log.info("KylinConfig in env, metadata is {}", kylinConfig.getMetadataUrl());
        log.info("KylinConfig in env, working dir is {}", kylinConfig.getHdfsWorkingDirectory());

        // Init global static instances
        CleanTaskExecutorService.getInstance().bindWorkingPool(() -> PriorityExecutor
                .newWorkingThreadPool("clean-storages-pool", kylinConfig.getStorageCleanTaskConcurrency()));
    }

    private void warmUpSystemCache() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        List<ProjectInstance> prjInstances = NProjectManager.getInstance(kylinConfig).listAllProjects();
        prjInstances.forEach(prjInstance -> {
            NProjectLoader.updateCache(prjInstance.getName());
            NProjectLoader.removeCache();
        });
        log.info("The system cache is warmed up.");
    }

    private void cacheCcRexNode() {
        Thread thread = new Thread(() -> {
            log.info("cache ComputedColumn RexNode cache.");
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            List<ProjectInstance> projects = NProjectManager.getInstance(kylinConfig).listAllProjects();
            projects.forEach(prjInstance -> ComputedColumnRewriter.cacheCcRexNode(kylinConfig, prjInstance.getName()));
        });
        thread.setName("CacheComputedColumnRexNodeThread");
        thread.start();
    }

    private void resetProjectOffsetId() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        List<ProjectInstance> prjInstances = NProjectManager.getInstance(kylinConfig).listAllProjects();
        prjInstances.forEach(project -> QueryHistoryOffsetUtil.resetOffsetId(project.getName()));
    }

    @EventListener(ApplicationReadyEvent.class)
    public void afterReady(ApplicationReadyEvent ignoredEvent) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        setFsUrlStreamHandlerFactory();
        if (kylinConfig.isJobNode() || kylinConfig.isMetadataNode()) {
            resetProjectOffsetId();
        }
        if (kylinConfig.getJStackDumpTaskEnabled()) {
            taskScheduler.scheduleAtFixedRate(new JStackDumpTask(),
                    kylinConfig.getJStackDumpTaskPeriod() * Constant.MINUTE);
        }
        if (kylinConfig.isUploadGCLogToWorkingDirEnabled()) {
            taskScheduler.scheduleAtFixedRate(new GCLogUploadTask(),
                    kylinConfig.getGCLogUploadTaskPeriod() * Constant.MINUTE);
        }
        if (kylinConfig.isGuardianEnabled() && kylinConfig.isGuardianHAEnabled()) {
            log.info("Guardian Process ha is enabled, start check scheduler");
            taskScheduler.scheduleAtFixedRate(new KapGuardianHATask(),
                    new Date(System.currentTimeMillis() + kylinConfig.getGuardianHACheckInitDelay() * Constant.SECOND),
                    kylinConfig.getGuardianHACheckInterval() * Constant.SECOND);
        }

        taskScheduler.scheduleAtFixedRate(new ProjectSerialEventBus.TimingDispatcher(),
                ProjectSerialEventBus.TimingDispatcher.INTERVAL);

        initSchedule();

    }

    private void initSchedule() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (kylinConfig.isJobNode() || kylinConfig.isDataLoadingNode()) {
            StreamingScheduler ss = StreamingScheduler.getInstance();
            ss.init();
            if (!ss.getHasStarted().get()) {
                throw new KylinRuntimeException("Streaming Scheduler has not been started");
            }
        }

        if (kylinConfig.getQueryHistoryAccelerateInterval() > 0) {
            QueryHistoryMetaUpdateScheduler qhMetaUpdateScheduler = QueryHistoryMetaUpdateScheduler.getInstance();
            qhMetaUpdateScheduler.init();
            if (!qhMetaUpdateScheduler.hasStarted()) {
                throw new KylinRuntimeException("Query history accelerate scheduler has not been started");
            }
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

    @Bean
    public HttpFirewall getHttpFirewall() {
        return new DefaultHttpFirewall();
    }
}
