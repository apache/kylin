package com.kylinolap.job2.impl.threadpool;

import com.google.common.collect.Maps;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job2.Scheduler;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.exception.LockException;
import com.kylinolap.job2.exception.SchedulerException;
import com.kylinolap.job2.execution.Executable;
import com.kylinolap.job2.service.DefaultJobService;
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

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        //TODO
    }

    private class FetcherRunner implements Runnable {

        @Override
        public void run() {
            List<AbstractExecutable> allExecutables = jobService.getAllExecutables();
            for (final AbstractExecutable executable : allExecutables) {
                if (executable.isRunnable() && !context.getRunningJobs().containsKey(executable.getId())) {
                    boolean hasLock = false;
                    try {
                        hasLock = acquireJobLock(executable.getId(), 1);
                        logger.info("acquire job lock:" + executable.getId() + " status:" + (hasLock ? "succeed" : "failed"));
                        if (hasLock) {
                            logger.info("start to run job id:" + executable.getId());
                            jobPool.execute(new JobRunner(executable));
                        }
                    } catch (LockException e) {
                        logger.error("error acquire job lock, id:" + executable.getId(), e);
                    } finally {
                        try {
                            if (hasLock) {
                                logger.info("finish running job id:" + executable.getId());
                                releaseJobLock(executable.getId());
                            }
                        } catch (LockException ex) {
                            logger.error("error release job lock, id:" + executable.getId(), ex);
                        }
                    }
                }
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
                context.addRunningJob(executable);
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

    private boolean acquireJobLock(String jobId, long timeoutSeconds) throws LockException {
        return !context.getRunningJobs().containsKey(jobId);
    }

    private void releaseJobLock(String jobId) throws LockException {

    }

    private String schedulerId() throws UnknownHostException {
        return ZOOKEEPER_LOCK_PATH + "/" + InetAddress.getLocalHost().getCanonicalHostName();
    }

    public static DefaultScheduler getInstance() {
        return INSTANCE;
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
