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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.job.Scheduler;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.lock.JobLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * schedule the cubing jobs when several job server running with the same metadata.
 *
 * to enable the distributed job server, you need to set and update three configs in the kylin.properties:
 *  1. kylin.job.scheduler.default=2
 *  2. kylin.job.lock=org.apache.kylin.storage.hbase.util.ZookeeperJobLock
 *  3. add all the job servers and query servers to the kylin.server.cluster-servers
 */
public class DistributedScheduler implements Scheduler<AbstractExecutable>, ConnectionStateListener {
    private static final Logger logger = LoggerFactory.getLogger(DistributedScheduler.class);
    
    private final static String SEGMENT_ID = "segmentId";
    public static final String ZOOKEEPER_LOCK_PATH = "/job_engine/lock"; // note ZookeeperDistributedLock will ensure zk path prefix: /${kylin.env.zookeeper-base-path}/metadata

    public static DistributedScheduler getInstance(KylinConfig config) {
        return config.getManager(DistributedScheduler.class);
    }

    // called by reflection
    static DistributedScheduler newInstance(KylinConfig config) throws IOException {
        return new DistributedScheduler();
    }

    // ============================================================================
    
    private ExecutableManager executableManager;
    private FetcherRunner fetcher;
    private ScheduledExecutorService fetcherPool;
    private ExecutorService watchPool;
    private ExecutorService jobPool;
    private DefaultContext context;
    private DistributedLock jobLock;
    private Closeable lockWatch;

    //keep all segments having running job
    private final Set<String> segmentWithLocks = new CopyOnWriteArraySet<>();
    private volatile boolean initialized = false;
    private volatile boolean hasStarted = false;
    private JobEngineConfig jobEngineConfig;
    private String serverName;

    private class FetcherRunner implements Runnable {
        @Override
        synchronized public void run() {
            try {
                Map<String, Executable> runningJobs = context.getRunningJobs();
                if (runningJobs.size() >= jobEngineConfig.getMaxConcurrentJobLimit()) {
                    logger.warn("There are too many jobs running, Job Fetch will wait until next schedule time");
                    return;
                }

                int nRunning = 0, nOtherRunning = 0, nReady = 0, nOthers = 0;
                for (final String id : executableManager.getAllJobIds()) {
                    if (runningJobs.containsKey(id)) {
                        nRunning++;
                        continue;
                    }

                    final Output output = executableManager.getOutput(id);

                    if ((output.getState() != ExecutableState.READY)) {
                        if (output.getState() == ExecutableState.RUNNING) {
                            nOtherRunning++;
                        } else {
                            nOthers++;
                        }
                        continue;
                    }

                    nReady++;
                    final AbstractExecutable executable = executableManager.getJob(id);
                    try {
                        jobPool.execute(new JobRunner(executable));
                    } catch (Exception ex) {
                        logger.warn(executable.toString() + " fail to schedule in server: " + serverName, ex);
                    }
                }
                logger.info("Job Fetcher: " + nRunning + " should running, " + runningJobs.size() + " actual running, "
                        + nOtherRunning + " running in other server, " + nReady + " ready, " + nOthers + " others");
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
            try (SetThreadName ignored = new SetThreadName("Scheduler %s Job %s",
                    System.identityHashCode(DistributedScheduler.this), executable.getId())) {
                String segmentId = executable.getParam(SEGMENT_ID);
                if (jobLock.lock(getLockPath(segmentId))) {
                    logger.info(executable.toString() + " scheduled in server: " + serverName);

                    context.addRunningJob(executable);
                    segmentWithLocks.add(segmentId);
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
                String segmentId = executable.getParam(SEGMENT_ID);
                ExecutableState state = executable.getStatus();

                if (state != ExecutableState.READY && state != ExecutableState.RUNNING) {
                    if (segmentWithLocks.contains(segmentId)) {
                        logger.info(executable.toString() + " will release the lock for the segment: " + segmentId);
                        jobLock.unlock(getLockPath(segmentId));
                        segmentWithLocks.remove(segmentId);
                    }
                }
            }
        }
    }

    //when the segment lock released but the segment related job still running, resume the job.
    private class WatcherProcessImpl implements DistributedLock.Watcher {
        private String serverName;

        public WatcherProcessImpl(String serverName) {
            this.serverName = serverName;
        }

        @Override
        public void onUnlock(String path, String nodeData) {
            String[] paths = path.split("/");
            String segmentId = paths[paths.length - 1];

            for (final String id : executableManager.getAllJobIds()) {
                final Output output = executableManager.getOutput(id);
                if (output.getState() == ExecutableState.RUNNING) {
                    AbstractExecutable executable = executableManager.getJob(id);
                    if (executable instanceof DefaultChainedExecutable
                            && executable.getParams().get(SEGMENT_ID).equalsIgnoreCase(segmentId)
                            && !nodeData.equalsIgnoreCase(serverName)) {
                        try {
                            logger.warn(nodeData + " has released the lock for: " + segmentId
                                    + " but the job still running. so " + serverName + " resume the job");
                            if (!jobLock.isLocked(getLockPath(segmentId))) {
                                executableManager.resumeRunningJobForce(executable.getId());
                                fetcherPool.schedule(fetcher, 0, TimeUnit.SECONDS);
                                break;
                            }
                        } catch (Exception e) {
                            logger.error("resume the job but fail in server: " + serverName, e);
                        }
                    }
                }
            }
        }

        @Override
        public void onLock(String lockPath, String client) {
        }
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
    public synchronized void init(JobEngineConfig jobEngineConfig, JobLock jobLock) throws SchedulerException {
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
        fetcher = new FetcherRunner();
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
                    if (!jobLock.isLocked(getLockPath(executable.getParam(SEGMENT_ID)))) {
                        executableManager.resumeRunningJobForce(executable.getId());
                        fetcherPool.schedule(fetcher, 0, TimeUnit.SECONDS);
                    }
                } catch (Exception e) {
                    logger.error("resume the job " + id + " fail in server: " + serverName, e);
                }
            }
        }
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
        for (String segmentId : segmentWithLocks) {
            jobLock.unlock(getLockPath(segmentId));
        }
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
