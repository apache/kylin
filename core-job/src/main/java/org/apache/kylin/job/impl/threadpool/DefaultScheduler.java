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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.util.ServerMode;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.job.Scheduler;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.lock.JobLock;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class DefaultScheduler implements Scheduler<AbstractExecutable> {

    private static DefaultScheduler INSTANCE;

    public synchronized static DefaultScheduler getInstance() {
        if (INSTANCE == null) {
            INSTANCE = createInstance();
        }
        return INSTANCE;
    }

    public static synchronized DefaultScheduler createInstance() {
        destroyInstance();
        INSTANCE = new DefaultScheduler();
        return INSTANCE;
    }

    public static synchronized void destroyInstance() {
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

    // ============================================================================

    private JobLock jobLock;
    private FetcherRunner fetcher;
    private ScheduledExecutorService fetcherPool;
    private ExecutorService jobPool;
    private DefaultContext context;

    private static final Logger logger = LoggerFactory.getLogger(DefaultScheduler.class);
    private volatile boolean initialized = false;
    private volatile boolean hasStarted = false;
    private JobEngineConfig jobEngineConfig;

    public DefaultScheduler() {
        if (INSTANCE != null) {
            throw new IllegalStateException("DefaultScheduler has been initiated.");
        }
    }

    public ExecutableManager getExecutableManager() {
        return ExecutableManager.getInstance(jobEngineConfig.getConfig());
    }

    public FetcherRunner getFetcherRunner() {
        return fetcher;
    }

    private class JobRunner implements Runnable {

        private final AbstractExecutable executable;

        public JobRunner(AbstractExecutable executable) {
            this.executable = executable;
        }

        @Override
        public void run() {
            try (SetThreadName ignored = new SetThreadName("Scheduler %s Job %s",
                    System.identityHashCode(DefaultScheduler.this), executable.getId())) {
                executable.execute(context);
            } catch (ExecuteException e) {
                logger.error("ExecuteException job:" + executable.getId(), e);
            } catch (Exception e) {
                if (AbstractExecutable.isMetaDataPersistException(e, 5)) {
                    // Job fail due to PersistException
                    ExecutableManager.getInstance(jobEngineConfig.getConfig())
                            .forceKillJobWithRetry(executable.getId());
                }
                logger.error("unknown error execute job:" + executable.getId(), e);
            } finally {
                context.removeRunningJob(executable);
            }

            // trigger the next step asap
            fetcherPool.schedule(fetcher, 0, TimeUnit.SECONDS);
        }
    }

    @Override
    public synchronized void init(JobEngineConfig jobEngineConfig, JobLock lock) throws SchedulerException {
        jobLock = lock;

        if (!ServerMode.SERVER_MODE.canServeJobBuild()) {
            logger.info(
                    "server mode: " + jobEngineConfig.getConfig().getServerMode() + ", no need to run job scheduler");
            return;
        }
        logger.info("Initializing Job Engine ....");

        if (!initialized) {
            initialized = true;
        } else {
            return;
        }

        this.jobEngineConfig = jobEngineConfig;

        if (jobLock.lockJobEngine() == false) {
            throw new IllegalStateException("Cannot start job scheduler due to lack of job lock");
        }

        //load all executable, set them to a consistent status
        fetcherPool = Executors.newScheduledThreadPool(1);
        int corePoolSize = jobEngineConfig.getMaxConcurrentJobLimit();
        jobPool = new ThreadPoolExecutor(corePoolSize, corePoolSize, Long.MAX_VALUE, TimeUnit.DAYS,
                new SynchronousQueue<Runnable>());
        context = new DefaultContext(Maps.<String, Executable> newConcurrentMap(), jobEngineConfig.getConfig());

        logger.info("Starting resume all running jobs.");
        ExecutableManager executableManager = getExecutableManager();
        executableManager.resumeAllRunningJobs();
        logger.info("Finishing resume all running jobs.");

        int pollSecond = jobEngineConfig.getPollIntervalSecond();

        logger.info("Fetching jobs every {} seconds", pollSecond);
        JobExecutor jobExecutor = new JobExecutor() {
            @Override
            public void execute(AbstractExecutable executable) {
                jobPool.execute(new JobRunner(executable));
            }
        };
        fetcher = jobEngineConfig.getJobPriorityConsidered()
                ? new PriorityFetcherRunner(jobEngineConfig, context, jobExecutor)
                : new DefaultFetcherRunner(jobEngineConfig, context, jobExecutor);
        logger.info("Creating fetcher pool instance:" + System.identityHashCode(fetcher));
        fetcherPool.scheduleAtFixedRate(fetcher, pollSecond / 10, pollSecond, TimeUnit.SECONDS);
        hasStarted = true;
    }

    @Override
    public void shutdown() throws SchedulerException {
        logger.info("Shutting down DefaultScheduler ....");
        jobLock.unlockJobEngine();
        initialized = false;
        hasStarted = false;
        try {
            fetcherPool.shutdownNow();//interrupt
            fetcherPool.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            //ignore it
            logger.warn("InterruptedException is caught when shutting down job fetcher.", e);
        }
        try {
            jobPool.shutdownNow();//interrupt
            jobPool.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            //ignore it
            logger.warn("InterruptedException is caught when shutting down job pool.", e);
        }
    }

    @Override
    public boolean hasStarted() {
        return this.hasStarted;
    }

}
