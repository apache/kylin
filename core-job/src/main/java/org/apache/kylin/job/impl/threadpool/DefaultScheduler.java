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
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.job.Scheduler;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.lock.JobLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 */
public class DefaultScheduler implements Scheduler<AbstractExecutable>, ConnectionStateListener {

    private JobLock jobLock;
    private ExecutableManager executableManager;
    private FetcherRunner fetcher;
    private ScheduledExecutorService fetcherPool;
    private ExecutorService jobPool;
    private DefaultContext context;

    private static final Logger logger = LoggerFactory.getLogger(DefaultScheduler.class);
    private volatile boolean initialized = false;
    private volatile boolean hasStarted = false;
    private JobEngineConfig jobEngineConfig;

    private static DefaultScheduler INSTANCE = null;

    public DefaultScheduler() {
        if (INSTANCE != null) {
            throw new IllegalStateException("DefaultScheduler has been initiated.");
        }
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

                int nRunning = 0, nReady = 0, nStopped = 0, nOthers = 0, nError = 0, nDiscarded = 0, nSUCCEED = 0;
                for (final String id : executableManager.getAllJobIds()) {
                    if (runningJobs.containsKey(id)) {
                        // logger.debug("Job id:" + id + " is already running");
                        nRunning++;
                        continue;
                    }
                    final Output output = executableManager.getOutput(id);
                    if ((output.getState() != ExecutableState.READY)) {
                        // logger.debug("Job id:" + id + " not runnable");
                        if (output.getState() == ExecutableState.DISCARDED) {
                            nDiscarded++;
                        } else if (output.getState() == ExecutableState.ERROR) {
                            nError++;
                        } else if (output.getState() == ExecutableState.SUCCEED) {
                            nSUCCEED++;
                        } else if (output.getState() == ExecutableState.STOPPED) {
                            nStopped++;
                        } else {
                            nOthers++;
                        }
                        continue;
                    }
                    nReady++;
                    AbstractExecutable executable = null;
                    String jobDesc = null;
                    try {
                        executable = executableManager.getJob(id);
                        jobDesc = executable.toString();
                        logger.info(jobDesc + " prepare to schedule");
                        context.addRunningJob(executable);
                        jobPool.execute(new JobRunner(executable));
                        logger.info(jobDesc + " scheduled");
                    } catch (Exception ex) {
                        if (executable != null)
                            context.removeRunningJob(executable);
                        logger.warn(jobDesc + " fail to schedule", ex);
                    }
                }
                logger.info("Job Fetcher: " + nRunning + " should running, " + runningJobs.size() + " actual running, " + nStopped + " stopped, " + nReady + " ready, " + nSUCCEED + " already succeed, " + nError + " error, " + nDiscarded + " discarded, " + nOthers + " others");
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
            try (SetThreadName ignored = new SetThreadName("Job %s", executable.getId())) {
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

    public synchronized static DefaultScheduler createInstance() {
        destroyInstance();
        INSTANCE = new DefaultScheduler();
        return INSTANCE;
    }

    public synchronized static void destroyInstance() {
        DefaultScheduler tmp = INSTANCE;
        INSTANCE = null;
        if (tmp != null) {
            try {
                tmp.shutdown();
            } catch (SchedulerException e) {
                logger.error("error stop DefaultScheduler", e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public synchronized void init(JobEngineConfig jobEngineConfig, final JobLock jobLock) throws SchedulerException {
        this.jobLock = jobLock;
        
        String serverMode = jobEngineConfig.getConfig().getServerMode();
        if (!("job".equals(serverMode.toLowerCase()) || "all".equals(serverMode.toLowerCase()))) {
            logger.info("server mode: " + serverMode + ", no need to run job scheduler");
            return;
        }
        logger.info("Initializing Job Engine ....");

        if (!initialized) {
            initialized = true;
        } else {
            return;
        }

        this.jobEngineConfig = jobEngineConfig;

        if (jobLock.lock() == false) {
            throw new IllegalStateException("Cannot start job scheduler due to lack of job lock");
        }

        executableManager = ExecutableManager.getInstance(jobEngineConfig.getConfig());
        //load all executable, set them to a consistent status
        fetcherPool = Executors.newScheduledThreadPool(1);
        int corePoolSize = jobEngineConfig.getMaxConcurrentJobLimit();
        jobPool = new ThreadPoolExecutor(corePoolSize, corePoolSize, Long.MAX_VALUE, TimeUnit.DAYS, new SynchronousQueue<Runnable>());
        context = new DefaultContext(Maps.<String, Executable> newConcurrentMap(), jobEngineConfig.getConfig());

        executableManager.resumeAllRunningJobs();

        fetcher = new FetcherRunner();
        fetcherPool.scheduleAtFixedRate(fetcher, 10, ExecutableConstants.DEFAULT_SCHEDULER_INTERVAL_SECONDS, TimeUnit.SECONDS);
        hasStarted = true;
    }

    @Override
    public void shutdown() throws SchedulerException {
        logger.info("Shutingdown Job Engine ....");
        jobLock.unlock();
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

    @Override
    public boolean hasStarted() {
        return this.hasStarted;
    }

}
