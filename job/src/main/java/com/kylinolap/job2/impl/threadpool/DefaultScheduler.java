package com.kylinolap.job2.impl.threadpool;

import com.google.common.collect.Maps;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job2.Scheduler;
import com.kylinolap.job2.constants.ExecutableConstants;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.exception.LockException;
import com.kylinolap.job2.exception.SchedulerException;
import com.kylinolap.job2.execution.Executable;
import com.kylinolap.job2.execution.ExecutableState;
import com.kylinolap.job2.service.ExecutableManager;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by qianzhou on 12/15/14.
 */
public class DefaultScheduler implements Scheduler<AbstractExecutable>, ConnectionStateListener {

    private static final String ZOOKEEPER_LOCK_PATH = "/kylin/job_engine/lock";


    private ExecutableManager executableManager;
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
    private static final String SCHEDULER_RESET_STATUS_HINT = "scheduler initializing work to reset job to ERROR status";


    private DefaultScheduler() {}

    private class FetcherRunner implements Runnable {

        @Override
        public void run() {
            logger.info("Job Fetcher is running...");
            Map<String, Executable> runningJobs = context.getRunningJobs();
            if (runningJobs.size() >= jobEngineConfig.getMaxConcurrentJobLimit()) {
                logger.warn("There are too many jobs running, Job Fetch will wait until next schedule time");
                return;
            }
            for (final AbstractExecutable executable : executableManager.getAllExecutables()) {
                final String id = executable.getId();
                String jobDesc = executable.toString();
                if (runningJobs.containsKey(id)) {
                    logger.info(jobDesc + " is already running");
                    continue;
                }
                if (!executable.isRunnable()) {
                    logger.info(jobDesc + " not runnable");
                    continue;
                }
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
            logger.info("Job Fetcher finish running");
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
            } catch (ExecuteException e) {
                logger.error("ExecuteException job:" + executable.getId(), e);
            } catch (Exception e) {
                logger.error("unknown error execute job:" + executable.getId(), e);
            } finally {
                context.removeRunningJob(executable);
            }
        }
    }

    private void releaseLock() {
        try {
            if (zkClient.getState().equals(CuratorFrameworkState.STARTED)) {
                // client.setData().forPath(ZOOKEEPER_LOCK_PATH, null);
                if (zkClient.checkExists().forPath(schedulerId()) != null) {
                    zkClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(schedulerId());
                }
            }
        } catch (Exception e) {
            logger.error("error release lock:" + schedulerId());
            throw new RuntimeException(e);
        }
    }

    private String schedulerId() {
        return ZOOKEEPER_LOCK_PATH + "/" + jobEngineConfig.getConfig().getMetadataUrlPrefix();
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
    public synchronized void init(JobEngineConfig jobEngineConfig) throws SchedulerException {
        if (!initialized) {
            initialized = true;
        } else {
            return;
        }
        this.jobEngineConfig = jobEngineConfig;
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.zkClient = CuratorFrameworkFactory.newClient(jobEngineConfig.getZookeeperString(), retryPolicy);
        this.zkClient.start();
        this.sharedLock = new InterProcessMutex(zkClient, schedulerId());
        boolean hasLock = false;
        try {
            hasLock = sharedLock.acquire(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.warn("error acquire lock", e);
        }
        if (!hasLock) {
            logger.warn("fail to acquire lock, scheduler has not been started");
            zkClient.close();
            return;
        }
        executableManager = ExecutableManager.getInstance(jobEngineConfig.getConfig());
        //load all executable, set them to a consistent status
        fetcherPool = Executors.newScheduledThreadPool(1);
        int corePoolSize = jobEngineConfig.getMaxConcurrentJobLimit();
        jobPool = new ThreadPoolExecutor(corePoolSize, corePoolSize, Long.MAX_VALUE, TimeUnit.DAYS, new SynchronousQueue<Runnable>());
        context = new DefaultContext(Maps.<String, Executable>newConcurrentMap(), jobEngineConfig.getConfig());


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
            } catch (SchedulerException e) {
                logger.error("error shutdown scheduler", e);
            }
            }
        });

        fetcherPool.scheduleAtFixedRate(new FetcherRunner(), 10, ExecutableConstants.DEFAULT_SCHEDULER_INTERVAL_SECONDS, TimeUnit.SECONDS);
        hasStarted = true;
    }

    @Override
    public void shutdown() throws SchedulerException {
        fetcherPool.shutdown();
        jobPool.shutdown();
        releaseLock();
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
