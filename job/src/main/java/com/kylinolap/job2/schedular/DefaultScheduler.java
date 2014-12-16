package com.kylinolap.job2.schedular;

import com.google.common.collect.Maps;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.exception.SchedularException;
import com.kylinolap.job2.execution.Executable;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;
import com.kylinolap.job2.impl.threadpool.DefaultContext;
import com.kylinolap.job2.service.DefaultJobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by qianzhou on 12/15/14.
 */
public class DefaultScheduler implements Scheduler {


    private DefaultJobService jobService;
    private ScheduledExecutorService fetcherPool;
    private ExecutorService jobPool;
    private DefaultContext context;

    private Logger logger = LoggerFactory.getLogger(DefaultScheduler.class);
    private boolean initialized = false;

    @Override
    public synchronized void init(JobEngineConfig jobEngineConfig) throws SchedularException {
        if (!initialized) {
            initialized = true;
        } else {
            throw new UnsupportedOperationException("cannot init this instance twice");
        }
        jobService = DefaultJobService.getInstance(jobEngineConfig.getConfig());
        //load all executable, set them to a consistent status
        fetcherPool = Executors.newScheduledThreadPool(1);
        int corePoolSize = Runtime.getRuntime().availableProcessors();
        jobPool = new ThreadPoolExecutor(corePoolSize, corePoolSize, Long.MAX_VALUE, TimeUnit.DAYS, new SynchronousQueue<Runnable>());

        context = new DefaultContext(Maps.<String, Executable>newConcurrentMap());

        fetcherPool.scheduleAtFixedRate(new FetcherRunner(), 0, JobConstants.DEFAULT_SCHEDULER_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    private class FetcherRunner implements Runnable {

        @Override
        public void run() {
            List<AbstractExecutable> allExecutables = jobService.getAllExecutables();
            for (final AbstractExecutable executable : allExecutables) {
                if (executable.isRunnable() && !context.getRunningJobs().containsKey(executable.getId())) {
                    jobPool.execute(new JobRunner(executable));
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
                context.addRunningJob(executable);
                executable.execute(context);
            } catch (ExecuteException e) {
                e.printStackTrace();
            } finally {
                context.removeRunningJob(executable);
            }
        }
    }

    @Override
    public void shutdown() throws SchedularException {
        fetcherPool.shutdown();
        jobPool.shutdown();
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

    @Override
    public List<Executable> getAllExecutables() {
        return Collections.emptyList();
    }
}
