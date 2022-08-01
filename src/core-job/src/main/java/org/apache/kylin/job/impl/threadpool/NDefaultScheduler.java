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

package org.apache.kylin.job.impl.threadpool;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.RandomUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.job.Scheduler;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.runners.FetcherRunner;
import org.apache.kylin.job.runners.JobCheckRunner;
import org.apache.kylin.job.runners.LicenseCapacityCheckRunner;
import org.apache.kylin.job.runners.QuotaStorageCheckRunner;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.SystemInfoCollector;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.val;

/**
 */
public class NDefaultScheduler implements Scheduler<AbstractExecutable> {
    private static final Logger logger = LoggerFactory.getLogger(NDefaultScheduler.class);

    @Getter
    private String project;
    private ScheduledExecutorService fetcherPool;
    private ExecutorService jobPool;
    @Getter
    private ExecutableContext context;
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private AtomicBoolean hasStarted = new AtomicBoolean(false);
    @Getter
    private JobEngineConfig jobEngineConfig;
    @Getter
    private static volatile Semaphore memoryRemaining = new Semaphore(Integer.MAX_VALUE);
    private long epochId = UnitOfWork.DEFAULT_EPOCH_ID;
    private static final Map<String, NDefaultScheduler> INSTANCE_MAP = Maps.newConcurrentMap();

    public NDefaultScheduler() {
    }

    public NDefaultScheduler(String project) {
        Preconditions.checkNotNull(project);
        this.project = project;

        if (INSTANCE_MAP.containsKey(project))
            throw new IllegalStateException(
                    "DefaultScheduler for project " + project + " has been initiated. Use getInstance() instead.");

        logger.debug("New NDefaultScheduler created by project '{}': {}", project,
                System.identityHashCode(NDefaultScheduler.this));
    }

    public static synchronized NDefaultScheduler getInstance(String project) {
        return INSTANCE_MAP.computeIfAbsent(project, NDefaultScheduler::new);
    }

    public void fetchJobsImmediately() {
        fetcherPool.schedule(new FetcherRunner(this, jobPool, fetcherPool), 1, TimeUnit.SECONDS);
    }

    public static List<NDefaultScheduler> listAllSchedulers() {
        return Lists.newArrayList(INSTANCE_MAP.values());
    }

    public static synchronized void destroyInstance() {

        for (Map.Entry<String, NDefaultScheduler> entry : INSTANCE_MAP.entrySet()) {
            entry.getValue().shutdown();
        }
        INSTANCE_MAP.clear();
    }

    public static synchronized void shutdownByProject(String project) {
        val instance = getInstanceByProject(project);
        if (instance != null) {
            INSTANCE_MAP.remove(project);
            instance.forceShutdown();
        }
    }

    public static synchronized NDefaultScheduler getInstanceByProject(String project) {
        return INSTANCE_MAP.get(project);
    }

    @Override
    public synchronized void init(JobEngineConfig jobEngineConfig) {

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        if (!config.isUTEnv()) {
            this.epochId = EpochManager.getInstance().getEpochId(project);
        }

        String serverMode = jobEngineConfig.getServerMode();
        if (!config.isJobNode()) {
            logger.info("server mode: {}, no need to run job scheduler", serverMode);
            return;
        }
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing Job Engine ....");

        if (!initialized.compareAndSet(false, true)) {
            return;
        }

        this.jobEngineConfig = jobEngineConfig;

        //load all executable, set them to a consistent status
        fetcherPool = Executors.newScheduledThreadPool(1,
                new NamedThreadFactory("FetchJobWorker(project:" + project + ")"));
        int corePoolSize = jobEngineConfig.getMaxConcurrentJobLimit();
        if (config.getAutoSetConcurrentJob()) {
            val availableMemoryRate = config.getMaxLocalConsumptionRatio();
            synchronized (NDefaultScheduler.class) {
                if (Integer.MAX_VALUE == memoryRemaining.availablePermits()) {
                    memoryRemaining = new Semaphore(
                            (int) (SystemInfoCollector.getAvailableMemoryInfo() * availableMemoryRate));
                }
            }

            logger.info("Scheduler memory remaining: {}", memoryRemaining.availablePermits());
        }
        jobPool = new ThreadPoolExecutor(corePoolSize, corePoolSize, Long.MAX_VALUE, TimeUnit.DAYS,
                new SynchronousQueue<>(), new NamedThreadFactory("RunJobWorker(project:" + project + ")"));
        context = new ExecutableContext(Maps.newConcurrentMap(), Maps.newConcurrentMap(), jobEngineConfig.getConfig(),
                epochId);

        val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        executableManager.resumeAllRunningJobs();

        int pollSecond = jobEngineConfig.getPollIntervalSecond();
        logger.info("Fetching jobs every {} seconds", pollSecond);
        val fetcher = new FetcherRunner(this, jobPool, fetcherPool);

        if (config.isCheckQuotaStorageEnabled()) {
            fetcherPool.scheduleWithFixedDelay(new QuotaStorageCheckRunner(this), RandomUtils.nextInt(0, pollSecond),
                    pollSecond, TimeUnit.SECONDS);
        }

        fetcherPool.scheduleWithFixedDelay(new JobCheckRunner(this), RandomUtils.nextInt(0, pollSecond), pollSecond,
                TimeUnit.SECONDS);
        fetcherPool.scheduleWithFixedDelay(new LicenseCapacityCheckRunner(this), RandomUtils.nextInt(0, pollSecond),
                pollSecond, TimeUnit.SECONDS);
        fetcherPool.scheduleWithFixedDelay(fetcher, RandomUtils.nextInt(0, pollSecond), pollSecond, TimeUnit.SECONDS);
        hasStarted.set(true);
    }

    @SneakyThrows
    @Override
    public void shutdown() {
        if (Thread.currentThread().isInterrupted()) {
            logger.warn("shutdown->current thread is interrupted,{}", Thread.currentThread().getName());
            throw new InterruptedException();
        }

        logger.info("Shutting down DefaultScheduler for project {} ....", project);
        releaseResources();
        if (null != fetcherPool) {
            ExecutorServiceUtil.shutdownGracefully(fetcherPool, 60);
        }
        if (null != jobPool) {
            ExecutorServiceUtil.shutdownGracefully(jobPool, 60);
        }
    }

    @SneakyThrows
    public void forceShutdown() {
        if (Thread.currentThread().isInterrupted()) {
            logger.warn("shutdownNow->current thread is interrupted,{}", Thread.currentThread().getName());
            throw new InterruptedException();
        }

        logger.info("Force to shut down DefaultScheduler for project {} ....", project);
        releaseResources();
        ExecutorServiceUtil.forceShutdown(fetcherPool);
        ExecutorServiceUtil.forceShutdown(jobPool);
    }

    private void releaseResources() {
        initialized.set(false);
        hasStarted.set(false);
    }

    @Override
    public boolean hasStarted() {
        return hasStarted.get();
    }

    public static double currentAvailableMem() {
        return 1.0 * memoryRemaining.availablePermits();
    }

}
