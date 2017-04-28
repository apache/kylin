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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.job.Scheduler;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.Output;
import org.apache.kylin.job.lock.DistributedJobLock;
import org.apache.kylin.job.lock.JobLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * schedule the cubing jobs when several job server running with the same metadata.
 *
 * to enable the distributed job server, you need to set and update three configs in the kylin.properties:
 *  1. kylin.job.scheduler.default=2
 *  2. kylin.job.lock=org.apache.kylin.storage.hbase.util.ZookeeperDistributedJobLock
 *  3. add all the job servers and query servers to the kylin.server.cluster-servers
 */
public class DistributedScheduler implements Scheduler<AbstractExecutable>, ConnectionStateListener {
    private ExecutableManager executableManager;
    private FetcherRunner fetcher;
    private ScheduledExecutorService fetcherPool;
    private ExecutorService watchPool;
    private ExecutorService jobPool;
    private DefaultContext context;
    private DistributedJobLock jobLock;

    private static final Logger logger = LoggerFactory.getLogger(DistributedScheduler.class);
    private static final ConcurrentMap<KylinConfig, DistributedScheduler> CACHE = new ConcurrentHashMap<KylinConfig, DistributedScheduler>();
    //keep all segments having running job
    private final Set<String> segmentWithLocks = new CopyOnWriteArraySet<>();
    private volatile boolean initialized = false;
    private volatile boolean hasStarted = false;
    private JobEngineConfig jobEngineConfig;

    private final static String SEGMENT_ID = "segmentId";

    //only for it test
    public static DistributedScheduler getInstance(KylinConfig config) {
        DistributedScheduler r = CACHE.get(config);
        if (r == null) {
            synchronized (DistributedScheduler.class) {
                r = CACHE.get(config);
                if (r == null) {
                    r = new DistributedScheduler();
                    CACHE.put(config, r);
                    if (CACHE.size() > 1) {
                        logger.warn("More than one singleton exist");
                    }
                }
            }
        }
        return r;
    }

    public static void clearCache() {
        CACHE.clear();
    }

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
                logger.info("Job Fetcher: " + nRunning + " should running, " + runningJobs.size() + " actual running, " + nOtherRunning + " running in other server, " + nReady + " ready, " + nOthers + " others");
            } catch (Exception e) {
                logger.warn("Job Fetcher caught a exception " + e);
            }
        }
    }

    private String serverName = getServerName();

    private String getServerName() {
        String serverName = null;
        try {
            serverName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.error("fail to get the serverName");
        }
        return serverName;
    }

    //only for it test
    public void setServerName(String serverName) {
        this.serverName = serverName;
        logger.info("serverName update to:" + this.serverName);
    }

    private class JobRunner implements Runnable {

        private final AbstractExecutable executable;

        public JobRunner(AbstractExecutable executable) {
            this.executable = executable;
        }

        @Override
        public void run() {
            try (SetThreadName ignored = new SetThreadName("Job %s", executable.getId())) {
                String segmentId = executable.getParam(SEGMENT_ID);
                if (jobLock.lockWithName(segmentId, serverName)) {
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
                        jobLock.unlockWithName(segmentId);
                        segmentWithLocks.remove(segmentId);
                    }
                }
            }
        }
    }

    //when the segment lock released but the segment related job still running, resume the job.
    private class DoWatchImpl implements org.apache.kylin.job.lock.DistributedJobLock.DoWatchLock {
        private String serverName;

        public DoWatchImpl(String serverName) {
            this.serverName = serverName;
        }

        @Override
        public void doWatch(String path, String nodeData) {
            String[] paths = path.split("/");
            String segmentId = paths[paths.length - 1];

            for (final String id : executableManager.getAllJobIds()) {
                final Output output = executableManager.getOutput(id);
                if (output.getState() == ExecutableState.RUNNING) {
                    AbstractExecutable executable = executableManager.getJob(id);
                    if (executable instanceof DefaultChainedExecutable && executable.getParams().get(SEGMENT_ID).equalsIgnoreCase(segmentId) && !nodeData.equalsIgnoreCase(serverName)) {
                        try {
                            logger.warn(nodeData + " has released the lock for: " + segmentId + " but the job still running. so " + serverName + " resume the job");
                            if (!jobLock.isHasLocked(segmentId)) {
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
        this.jobLock = (DistributedJobLock) jobLock;

        executableManager = ExecutableManager.getInstance(jobEngineConfig.getConfig());
        //load all executable, set them to a consistent status
        fetcherPool = Executors.newScheduledThreadPool(1);

        //watch the zookeeper node change, so that when one job server is down, other job servers can take over.
        watchPool = Executors.newFixedThreadPool(1);
        DoWatchImpl doWatchImpl = new DoWatchImpl(this.serverName);
        this.jobLock.watchLock(watchPool, doWatchImpl);

        int corePoolSize = jobEngineConfig.getMaxConcurrentJobLimit();
        jobPool = new ThreadPoolExecutor(corePoolSize, corePoolSize, Long.MAX_VALUE, TimeUnit.DAYS, new SynchronousQueue<Runnable>());
        context = new DefaultContext(Maps.<String, Executable> newConcurrentMap(), jobEngineConfig.getConfig());

        resumeAllRunningJobs();

        fetcher = new FetcherRunner();
        fetcherPool.scheduleAtFixedRate(fetcher, 10, ExecutableConstants.DEFAULT_SCHEDULER_INTERVAL_SECONDS, TimeUnit.SECONDS);
        hasStarted = true;
    }

    private void resumeAllRunningJobs() {
        for (final String id : executableManager.getAllJobIds()) {
            final Output output = executableManager.getOutput(id);
            AbstractExecutable executable = executableManager.getJob(id);
            if (output.getState() == ExecutableState.RUNNING && executable instanceof DefaultChainedExecutable) {
                try {
                    if (!jobLock.isHasLocked(executable.getParam(SEGMENT_ID))) {
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

        releaseAllLocks();
        logger.info("The all locks has released");

        watchPool.shutdown();
        logger.info("The watchPool has down");

        fetcherPool.shutdown();
        logger.info("The fetcherPool has down");

        jobPool.shutdown();
        logger.info("The jobPoll has down");
    }

    private void releaseAllLocks() {
        for (String segmentId : segmentWithLocks) {
            jobLock.unlockWithName(segmentId);
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
