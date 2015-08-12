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

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.kylin.common.lock.JobLock;
import org.apache.kylin.job.Scheduler;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.manager.ExecutableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * Created by qianzhou on 12/15/14.
 */
public class DefaultScheduler implements Scheduler<AbstractExecutable>, ConnectionStateListener {

    private static final String ZOOKEEPER_LOCK_PATH = "/kylin/job_engine/lock";

    private ExecutableManager executableManager;
    private FetcherRunner fetcher;
    private ScheduledExecutorService fetcherPool;
    private ExecutorService jobPool;
    private DefaultContext context;

    private Logger logger = LoggerFactory.getLogger(DefaultScheduler.class);
    private volatile boolean initialized = false;
    private volatile boolean hasStarted = false;
    private CuratorFramework zkClient;
    private JobEngineConfig jobEngineConfig;
    private InterProcessMutex sharedLock;

    private static final DefaultScheduler INSTANCE = new DefaultScheduler();

    private DefaultScheduler() {
    }

    private class FetcherRunner implements Runnable {

        @Override
        synchronized public void run() {
            try {
                // logger.debug("Job Fetcher is running...");
                Map<String, Executable> runningJobs = context.getRunningJobs();
                if (runningJobs.size() >= jobEngineConfig.getMaxConcurrentJobLimit()) {
                    logger.warn("There are too many jobs running, Job Fetch will wait until next schedule time");
                    return;
                }

                int nRunning = 0, nReady = 0, nOthers = 0;
                for (final String id : executableManager.getAllJobIds()) {
                    if (runningJobs.containsKey(id)) {
                        // logger.debug("Job id:" + id + " is already running");
                        nRunning++;
                        continue;
                    }
                    final Output output = executableManager.getOutput(id);
                    if ((output.getState() != ExecutableState.READY)) {
                        // logger.debug("Job id:" + id + " not runnable");
                        nOthers++;
                        continue;
                    }
                    nReady++;
                    AbstractExecutable executable = executableManager.getJob(id);
                    String jobDesc = executable.toString();
                    logger.info(jobDesc + " prepare to schedule");
                    try {
                        context.addRunningJob(executable);
                        jobPool.execute(new JobRunner(executable));
                        logger.info(jobDesc + " scheduled");
                    } catch (Exception ex) {
                        context.removeRunningJob(executable);
                        logger.warn(jobDesc + " fail to schedule", ex);
                    }
                }
                logger.info("Job Fetcher: " + nRunning + " running, " + runningJobs.size() + " actual running, " + nReady + " ready, " + nOthers + " others");
            } catch (Exception e) {
                logger.warn("Job Fetcher caught a exception " + e);
            }
        }
    }

    private class JobRunner implements Runnable {

        private final AbstractExecutable executable;

        public JobRunner(AbstractExecutable executable) {
            this.executable = executable;
        }

        @Override
        public void run() {
            try {
                executable.execute(context);
                // trigger the next step asap
                fetcherPool.schedule(fetcher, 0, TimeUnit.SECONDS);
            } catch (ExecuteException e) {
                logger.error("ExecuteException job:" + executable.getId(), e);
            } catch (Exception e) {
                logger.error("unknown error execute job:" + executable.getId(), e);
            } finally {
                context.removeRunningJob(executable);
            }
        }
    }

    public static DefaultScheduler getInstance() {
        return INSTANCE;
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if ((newState == ConnectionState.SUSPENDED) || (newState == ConnectionState.LOST)) {
            try {
                shutdown();
            } catch (SchedulerException e) {
                throw new RuntimeException("failed to shutdown scheduler", e);
            }
        }
    }

    @Override
    public synchronized void init(JobEngineConfig jobEngineConfig, final JobLock jobLock) throws SchedulerException {
        if (!initialized) {
            initialized = true;
        } else {
            return;
        }

        this.jobEngineConfig = jobEngineConfig;
        jobLock.lock();

        executableManager = ExecutableManager.getInstance(jobEngineConfig.getConfig());
        //load all executable, set them to a consistent status
        fetcherPool = Executors.newScheduledThreadPool(1);
        int corePoolSize = jobEngineConfig.getMaxConcurrentJobLimit();
        jobPool = new ThreadPoolExecutor(corePoolSize, corePoolSize, Long.MAX_VALUE, TimeUnit.DAYS, new SynchronousQueue<Runnable>());
        context = new DefaultContext(Maps.<String, Executable> newConcurrentMap(), jobEngineConfig.getConfig());

        for (AbstractExecutable executable : executableManager.getAllExecutables()) {
            if (executable.getStatus() == ExecutableState.READY) {
                executableManager.updateJobOutput(executable.getId(), ExecutableState.ERROR, null, "scheduler initializing work to reset job to ERROR status");
            }
        }
        executableManager.updateAllRunningJobsToError();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.debug("Closing zk connection");
                try {
                    shutdown();
                    jobLock.unlock();
                } catch (SchedulerException e) {
                    logger.error("error shutdown scheduler", e);
                }
            }
        });

        fetcher = new FetcherRunner();
        fetcherPool.scheduleAtFixedRate(fetcher, 10, ExecutableConstants.DEFAULT_SCHEDULER_INTERVAL_SECONDS, TimeUnit.SECONDS);
        hasStarted = true;
    }

    @Override
    public void shutdown() throws SchedulerException {
        fetcherPool.shutdown();
        jobPool.shutdown();
    }

    @Override
    public boolean stop(AbstractExecutable executable) throws SchedulerException {
        if (hasStarted) {
            return true;
        } else {
            //TODO should try to stop this executable
            return true;
        }
    }

    public boolean hasStarted() {
        return this.hasStarted;
    }

}
