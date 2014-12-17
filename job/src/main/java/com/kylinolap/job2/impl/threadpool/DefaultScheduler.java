package com.kylinolap.job2.impl.threadpool;

import com.google.common.collect.Maps;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job2.Scheduler;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.exception.LockException;
import com.kylinolap.job2.exception.SchedularException;
import com.kylinolap.job2.execution.Executable;
import com.kylinolap.job2.service.DefaultJobService;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
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
public class DefaultScheduler implements Scheduler {

    private static final String ZOOKEEPER_LOCK_PATH = "/kylin/job_engine/lock";


    private DefaultJobService jobService;
    private ScheduledExecutorService fetcherPool;
    private ExecutorService jobPool;
    private DefaultContext context;

    private Logger logger = LoggerFactory.getLogger(DefaultScheduler.class);
    private boolean initialized = false;
    private CuratorFramework zkClient;
    private JobEngineConfig jobEngineConfig;

    private static final DefaultScheduler defaultScheduler = new DefaultScheduler();

    private DefaultScheduler() {}

    private class FetcherRunner implements Runnable {

        @Override
        public void run() {
            List<AbstractExecutable> allExecutables = jobService.getAllExecutables();
            for (final AbstractExecutable executable : allExecutables) {
                if (executable.isRunnable() && !context.getRunningJobs().containsKey(executable.getId())) {
                    boolean hasLock = false;
                    try {
                        hasLock = acquireJobLock(executable.getId(), 1);
                        jobPool.execute(new JobRunner(executable));
                    } catch (LockException e) {
                        logger.error("error acquire job lock, id:" + executable.getId(), e);
                    } finally {
                        try {
                            if (hasLock) {
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
            if (context.getRunningJobs().containsKey(executable.getId())) {
                logger.warn("job:" + executable.getId() + " is already running");
                return;
            }
            try {
                executable.execute(context);
            } catch (ExecuteException e) {
                logger.error("ExecuteException job:" + executable.getId(), e);
            } catch (Exception e) {
                logger.error("unknown error execute job:" + executable.getId(), e);
            } finally {
            }
        }
    }

    private boolean acquireJobLock(String jobId, long timeoutSeconds) throws LockException {
        return true;
    }

    private void releaseJobLock(String jobId) throws LockException {

    }

    private String schedulerId() throws UnknownHostException {
        return ZOOKEEPER_LOCK_PATH + "/" + InetAddress.getLocalHost().getCanonicalHostName();
    }

    @Override
    public synchronized void init(JobEngineConfig jobEngineConfig) throws SchedularException {
        if (!initialized) {
            initialized = true;
        } else {
            throw new UnsupportedOperationException("cannot init this instance twice");
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
            } catch (SchedularException e) {
                logger.error("error shutdown scheduler", e);
            }
            }
        });

        fetcherPool.scheduleAtFixedRate(new FetcherRunner(), 0, JobConstants.DEFAULT_SCHEDULER_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void shutdown() throws SchedularException {
        fetcherPool.shutdown();
        jobPool.shutdown();
        if (zkClient.getState().equals(CuratorFrameworkState.STARTED)) {
            try {
                if (zkClient.checkExists().forPath(schedulerId()) != null) {
                    zkClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(schedulerId());
                }
            } catch (Exception e) {
                logger.error("error delete scheduler", e);
                throw new SchedularException(e);
            }
        }
    }


    @Override
    public boolean submit(Executable executable) throws SchedularException {
        //to persistent
        return true;
    }

    @Override
    public boolean stop(Executable executable) throws SchedularException {
        //update persistent
        return true;
    }

}
