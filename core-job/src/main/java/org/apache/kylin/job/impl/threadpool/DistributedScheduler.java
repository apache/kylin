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

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.common.util.ServerMode;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.common.util.ToolUtil;
import org.apache.kylin.job.Scheduler;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.lock.JobLock;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * schedule the cubing jobs when several job server running with the same metadata.
 *
 * to enable the distributed job server, you need to set and update three configs in the kylin.properties:
 *  1. kylin.job.scheduler.default=2
 *  2. add all the job servers and query servers to the kylin.server.cluster-servers
 */
public class DistributedScheduler implements Scheduler<AbstractExecutable> {
    public static final String ZOOKEEPER_LOCK_PATH = "/job_engine/lock"; // note ZookeeperDistributedLock will ensure zk path prefix: /${kylin.env.zookeeper-base-path}/metadata
    private static final Logger logger = LoggerFactory.getLogger(DistributedScheduler.class);
    //keep all running job
    private final Set<String> jobWithLocks = new CopyOnWriteArraySet<>();
    private ExecutableManager executableManager;

    // ============================================================================
    private FetcherRunner fetcher;
    private ScheduledExecutorService fetcherPool;
    private ExecutorService watchPool;
    private ExecutorService jobPool;
    private DefaultContext context;
    private DistributedLock jobLock;
    private Closeable lockWatch;
    private volatile boolean initialized = false;
    private volatile boolean hasStarted = false;
    private JobEngineConfig jobEngineConfig;
    private String serverName;

    public static DistributedScheduler getInstance(KylinConfig config) {
        return config.getManager(DistributedScheduler.class);
    }

    // called by reflection
    static DistributedScheduler newInstance(KylinConfig config) throws IOException {
        return new DistributedScheduler();
    }

    public static String getLockPath(String pathName) {
        return dropDoubleSlash(ZOOKEEPER_LOCK_PATH + "/" + pathName);
    }

    private static String getWatchPath() {
        return dropDoubleSlash(ZOOKEEPER_LOCK_PATH);
    }

    public static String dropDoubleSlash(String path) {
        for (int n = Integer.MAX_VALUE; n > path.length();) {
            n = path.length();
            path = path.replace("//", "/");
        }
        return path;
    }

    @Override
    public synchronized void init(JobEngineConfig jobEngineConfig, JobLock jobLock) throws SchedulerException {
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
        this.jobLock = (DistributedLock) jobLock;
        this.serverName = this.jobLock.getClient(); // the lock's client string contains node name of this server

        executableManager = ExecutableManager.getInstance(jobEngineConfig.getConfig());
        //load all executable, set them to a consistent status
        fetcherPool = Executors.newScheduledThreadPool(1);

        //watch the zookeeper node change, so that when one job server is down, other job servers can take over.
        watchPool = Executors.newFixedThreadPool(1);
        WatcherProcessImpl watcherProcess = new WatcherProcessImpl(this.serverName);
        lockWatch = this.jobLock.watchLocks(getWatchPath(), watchPool, watcherProcess);

        int corePoolSize = jobEngineConfig.getMaxConcurrentJobLimit();
        jobPool = new ThreadPoolExecutor(corePoolSize, corePoolSize, Long.MAX_VALUE, TimeUnit.DAYS,
                new SynchronousQueue<Runnable>());
        context = new DefaultContext(Maps.<String, Executable> newConcurrentMap(), jobEngineConfig.getConfig());

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
        fetcherPool.scheduleAtFixedRate(fetcher, pollSecond / 10, pollSecond, TimeUnit.SECONDS);
        hasStarted = true;

        resumeAllRunningJobs();
    }

    private void resumeAllRunningJobs() {
        for (final String id : executableManager.getAllJobIds()) {
            final Output output = executableManager.getOutput(id);
            AbstractExecutable executable = executableManager.getJob(id);
            if (output.getState() == ExecutableState.RUNNING && executable instanceof DefaultChainedExecutable) {
                try {
                    if (!jobLock.isLocked(getLockPath(executable.getId()))) {
                        executableManager.resumeRunningJobForce(executable.getId());
                        fetcherPool.schedule(fetcher, 0, TimeUnit.SECONDS);
                    }
                } catch (Exception e) {
                    logger.error("resume the job " + id + " fail in server: " + serverName, e);
                }
            }
        }
    }

    @Override
    public void shutdown() throws SchedulerException {
        logger.info("Will shut down Job Engine ....");

        try {
            lockWatch.close();
        } catch (IOException e) {
            throw new SchedulerException(e);
        }

        releaseAllLocks();
        logger.info("The all locks has released");

        fetcherPool.shutdown();
        logger.info("The fetcherPool has down");

        jobPool.shutdown();
        logger.info("The jobPoll has down");
    }

    private void releaseAllLocks() {
        for (String jobId : jobWithLocks) {
            jobLock.unlock(getLockPath(jobId));
        }
    }

    @Override
    public boolean hasStarted() {
        return this.hasStarted;
    }

    private class JobRunner implements Runnable {

        private final AbstractExecutable executable;

        public JobRunner(AbstractExecutable executable) {
            this.executable = executable;
        }

        @Override
        public void run() {
            try (SetThreadName ignored = new SetThreadName("Scheduler %s Job %s",
                    System.identityHashCode(DistributedScheduler.this), executable.getId())) {

                boolean isAssigned = true;
                if (!StringUtils.isEmpty(executable.getCubeName())) {
                    KylinConfig config = executable.getCubeSpecificConfig();
                    isAssigned = config.isOnAssignedServer(ToolUtil.getHostName(),
                            ToolUtil.getFirstIPV4NonLoopBackAddress().getHostAddress());
                    logger.debug("cube = " + executable.getCubeName() + "; jobId=" + executable.getId()
                            + (isAssigned ? " is " : " is not ") + "assigned on this server : "
                            + ToolUtil.getHostName());
                }

                if (isAssigned && jobLock.lock(getLockPath(executable.getId()))) {
                    logger.info(executable.toString() + " scheduled in server: " + serverName);

                    context.addRunningJob(executable);
                    jobWithLocks.add(executable.getId());
                    executable.execute(context);
                }
            } catch (ExecuteException e) {
                logger.error("ExecuteException job:" + executable.getId() + " in server: " + serverName, e);
            } catch (Exception e) {
                logger.error("unknown error execute job:" + executable.getId() + " in server: " + serverName, e);
            } finally {
                context.removeRunningJob(executable);
                releaseJobLock(executable);
                // trigger the next step asap
                fetcherPool.schedule(fetcher, 0, TimeUnit.SECONDS);
            }
        }

        //release job lock when job state is ready or running and the job server keep the cube lock.
        private void releaseJobLock(AbstractExecutable executable) {
            if (executable instanceof DefaultChainedExecutable) {
                ExecutableState state = executable.getStatus();

                if (state != ExecutableState.READY && state != ExecutableState.RUNNING) {
                    if (jobWithLocks.contains(executable.getId())) {
                        logger.info(
                                executable.toString() + " will release the lock for the job: " + executable.getId());
                        jobLock.unlock(getLockPath(executable.getId()));
                        jobWithLocks.remove(executable.getId());
                    }
                }
            }
        }
    }

    //when the job lock released but the related job still running, resume the job.
    private class WatcherProcessImpl implements DistributedLock.Watcher {
        private String serverName;

        public WatcherProcessImpl(String serverName) {
            this.serverName = serverName;
        }

        @Override
        public void onUnlock(String path, String nodeData) {
            String[] paths = StringUtil.split(path, "/");
            String jobId = paths[paths.length - 1];

            // Sync execute cache in case broadcast not available
            try {
                executableManager.syncDigestsOfJob(jobId);
            } catch (PersistentException e) {
                logger.error("Failed to sync cache of job: " + jobId + ", at server: " + serverName);
            }

            final Output output = executableManager.getOutput(jobId);
            if (output.getState() == ExecutableState.RUNNING) {
                AbstractExecutable executable = executableManager.getJob(jobId);
                if (executable instanceof DefaultChainedExecutable && !nodeData.equalsIgnoreCase(serverName)) {
                    try {
                        logger.warn(nodeData + " has released the lock for: " + jobId
                                + " but the job still running. so " + serverName + " resume the job");
                        if (!jobLock.isLocked(getLockPath(jobId))) {
                            executableManager.resumeRunningJobForce(executable.getId());
                            fetcherPool.schedule(fetcher, 0, TimeUnit.SECONDS);
                        }
                    } catch (Exception e) {
                        logger.error("resume the job but fail in server: " + serverName, e);
                    }
                }
            }
        }

        @Override
        public void onLock(String lockPath, String client) {
        }
    }
}
