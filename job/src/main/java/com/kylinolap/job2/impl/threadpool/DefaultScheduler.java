package com.kylinolap.job2.impl.threadpool;

import com.google.common.collect.Maps;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job2.Scheduler;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.exception.LockException;
import com.kylinolap.job2.exception.SchedulerException;
import com.kylinolap.job2.execution.Executable;
import com.kylinolap.job2.execution.ExecutableStatus;
import com.kylinolap.job2.service.DefaultJobService;
import org.apache.commons.math3.analysis.function.Abs;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by qianzhou on 12/15/14.
 */
public class DefaultScheduler implements Scheduler<AbstractExecutable>, ConnectionStateListener {

    private static final String ZOOKEEPER_LOCK_PATH = "/kylin/job_engine/lock";


    private DefaultJobService jobService;
    private ScheduledExecutorService fetcherPool;
    private ExecutorService jobPool;
    private DefaultContext context;

    private Logger logger = LoggerFactory.getLogger(DefaultScheduler.class);
    private boolean initialized = false;
    private CuratorFramework zkClient;
    private JobEngineConfig jobEngineConfig;

    private static final DefaultScheduler INSTANCE = new DefaultScheduler();

    private DefaultScheduler() {}

    private class FetcherRunner implements Runnable {

        @Override
        public void run() {
            for (final AbstractExecutable executable : jobService.getAllExecutables()) {
                boolean hasLock = false;
                try {
                    hasLock = acquireJobLock(executable, 1);
                } catch (LockException e) {
                    logger.error("error acquire job lock, id:" + executable.getId(), e);
                }
                logger.info("acquire job lock:" + executable.getId() + " status:" + (hasLock ? "succeed" : "failed"));
                if (hasLock) {
                    try {
                        logger.info("start to run job id:" + executable.getId());
                        context.addRunningJob(executable);
                        jobPool.execute(new JobRunner(executable));
                    } finally {
                        try {
                            logger.info("finish running job id:" + executable.getId());
                            releaseJobLock(executable.getId());
                        } catch (LockException ex) {
                            logger.error("error release job lock, id:" + executable.getId(), ex);
                        }
                    }
                }
                resetStatus(executable);
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
            } catch (ExecuteException e) {
                logger.error("ExecuteException job:" + executable.getId(), e);
            } catch (Exception e) {
                logger.error("unknown error execute job:" + executable.getId(), e);
            } finally {
                context.removeRunningJob(executable);
            }
        }
    }

    private void resetStatus(Executable executable) {
        if (!context.getRunningJobs().containsKey(executable.getId()) && executable.getStatus() == ExecutableStatus.RUNNING) {
            logger.warn("job:" + executable.getId() + " status should not be:" + ExecutableStatus.RUNNING + ", reset it to ERROR");
            jobService.updateJobStatus(executable.getId(), ExecutableStatus.ERROR, "job fetcher has detected the status in inconsistent status, and reset it to ERROR");
        }
    }

    private boolean acquireJobLock(Executable executable, long timeoutSeconds) throws LockException {
        Map<String, Executable> runningJobs = context.getRunningJobs();
        if (runningJobs.size() >= jobEngineConfig.getMaxConcurrentJobLimit()) {
            return false;
        }
        if (runningJobs.containsKey(executable.getId())) {
            return false;
        }
        if (!executable.isRunnable()) {
            return false;
        }
        return true;
    }

    private void releaseJobLock(String jobId) throws LockException {

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
            releaseLock();
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
        jobService = DefaultJobService.getInstance(jobEngineConfig.getConfig());
        //load all executable, set them to a consistent status
        fetcherPool = Executors.newScheduledThreadPool(1);
        int corePoolSize = jobEngineConfig.getMaxConcurrentJobLimit();
        jobPool = new ThreadPoolExecutor(corePoolSize, corePoolSize, Long.MAX_VALUE, TimeUnit.DAYS, new SynchronousQueue<Runnable>());
        context = new DefaultContext(Maps.<String, Executable>newConcurrentMap());

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.zkClient = CuratorFrameworkFactory.newClient(jobEngineConfig.getZookeeperString(), retryPolicy);
        this.zkClient.start();

        for (AbstractExecutable executable : jobService.getAllExecutables()) {
            if (executable.getStatus() == ExecutableStatus.RUNNING) {
                jobService.updateJobStatus(executable.getId(), ExecutableStatus.READY, null);
            }
        }

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

        fetcherPool.scheduleAtFixedRate(new FetcherRunner(), 10, JobConstants.DEFAULT_SCHEDULER_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void shutdown() throws SchedulerException {
        fetcherPool.shutdown();
        jobPool.shutdown();
        if (zkClient.getState().equals(CuratorFrameworkState.STARTED)) {
            try {
                if (zkClient.checkExists().forPath(schedulerId()) != null) {
                    zkClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(schedulerId());
                }
            } catch (Exception e) {
                logger.error("error delete scheduler", e);
                throw new SchedulerException(e);
            }
        }

    }


    @Override
    public boolean submit(AbstractExecutable executable) throws SchedulerException {
        jobService.addJob(executable);
        return true;
    }

    @Override
    public boolean stop(AbstractExecutable executable) throws SchedulerException {
        return true;
    }

}
