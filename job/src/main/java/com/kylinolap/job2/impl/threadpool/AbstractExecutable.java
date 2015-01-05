package com.kylinolap.job2.impl.threadpool;

import com.google.common.base.Preconditions;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.job2.dao.JobPO;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.execution.*;
import com.kylinolap.job2.service.ExecutableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * Created by qianzhou on 12/16/14.
 */
public abstract class AbstractExecutable implements Executable, Idempotent {

    private JobPO job;
    protected static final Logger logger = LoggerFactory.getLogger(AbstractExecutable.class);

    protected static ExecutableManager jobService = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv());

    public AbstractExecutable() {
        String uuid = UUID.randomUUID().toString();
        this.job = new JobPO();
        this.job.setType(this.getClass().getName());
        this.job.setUuid(uuid);

    }

    protected AbstractExecutable(JobPO job) {
        Preconditions.checkArgument(job != null, "job cannot be null");
        Preconditions.checkArgument(job.getId() != null, "job id cannot be null");
        this.job = job;
    }

    protected void onExecuteStart(ExecutableContext executableContext) {
        jobService.updateJobStatus(getId(), ExecutableState.RUNNING);
    }

    protected void onExecuteFinished(ExecuteResult result, ExecutableContext executableContext) {
        if (result.succeed()) {
            jobService.updateJobStatus(getId(), ExecutableState.SUCCEED, result.output());
        } else if (result.state() == ExecuteResult.State.STOPPED) {
            jobService.updateJobStatus(getId(), ExecutableState.STOPPED, result.output());
        } else {
            jobService.updateJobStatus(getId(), ExecutableState.ERROR, result.output());
        }
    }

    protected void onExecuteError(Throwable exception, ExecutableContext executableContext) {
        jobService.updateJobStatus(getId(), ExecutableState.ERROR, exception.getLocalizedMessage());
    }

    @Override
    public final ExecuteResult execute(ExecutableContext executableContext) throws ExecuteException {
        Preconditions.checkArgument(executableContext instanceof DefaultContext);
        ExecuteResult result;
        try {
            onExecuteStart(executableContext);
            result = doWork(executableContext);
        } catch (Throwable e) {
            onExecuteError(e, executableContext);
            throw new ExecuteException(e);
        }
        onExecuteFinished(result, executableContext);
        return result;
    }

    protected abstract ExecuteResult doWork(ExecutableContext context) throws ExecuteException;

    @Override
    public void cleanup() throws ExecuteException {

    }

    @Override
    public boolean isRunnable() {
        return this.getStatus() == ExecutableState.READY;
    }

    @Override
    public String getName() {
        return job.getName();
    }

    public void setName(String name) {
        job.setName(name);
    }

    @Override
    public final String getId() {
        return job.getId();
    }

    @Override
    public final ExecutableState getStatus() {
        return jobService.getJobStatus(this.getId());
    }

    @Override
    public final Map<String, String> getParams() {
        return Collections.unmodifiableMap(job.getParams());
    }

    public final String getParam(String key) {
        return job.getParams().get(key);
    }

    public final void setParam(String key, String value) {
        job.getParams().put(key, value);
    }

    @Override
    public String getOutput() {
        return jobService.getJobOutput(getId());
    }

    public JobPO getJobPO() {
        return job;
    }

    /*
    * stop is triggered by JobService, the Scheduler is not awake of that, so
    *
    * */
    protected final boolean isStopped() {
        return getStatus() == ExecutableState.STOPPED;
    }
}
