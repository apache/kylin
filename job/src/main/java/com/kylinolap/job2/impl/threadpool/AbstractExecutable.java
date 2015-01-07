package com.kylinolap.job2.impl.threadpool;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
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

    private static final String SUBMITTER = "submitter";
    private static final String START_TIME = "startTime";
    private static final String END_TIME = "endTime";

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
        Map<String, String> info = Maps.newHashMap();
        info.put(START_TIME, Long.toString(System.currentTimeMillis()));
        jobService.updateJobOutput(getId(), ExecutableState.RUNNING, info, null);
    }

    protected void onExecuteFinished(ExecuteResult result, ExecutableContext executableContext) {
        jobService.addJobInfo(getId(), END_TIME, Long.toString(System.currentTimeMillis()));
        if (result.succeed()) {
            jobService.updateJobOutput(getId(), ExecutableState.SUCCEED, null, result.output());
        } else if (result.state() == ExecuteResult.State.STOPPED) {
            jobService.updateJobOutput(getId(), ExecutableState.STOPPED, null, result.output());
        } else {
            jobService.updateJobOutput(getId(), ExecutableState.ERROR, null, result.output());
        }
    }

    protected void onExecuteError(Throwable exception, ExecutableContext executableContext) {
        jobService.addJobInfo(getId(), END_TIME, Long.toString(System.currentTimeMillis()));
        jobService.updateJobOutput(getId(), ExecutableState.ERROR, null, exception.getLocalizedMessage());
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
        return jobService.getOutput(this.getId()).getState();
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

    public final long getLastModified() {
        return jobService.getOutput(getId()).getLastModified();
    }

    public final void setSubmitter(String submitter) {
        setParam(SUBMITTER, submitter);
    }

    public final String getSubmitter() {
        return getParam(SUBMITTER);
    }

    @Override
    public final Output getOutput() {
        return jobService.getOutput(getId());
    }

    protected long getExtraInfoAsLong(String key, long defaultValue) {
        final String str = jobService.getOutput(getId()).getExtra().get(key);
        if (str != null) {
            return Long.parseLong(str);
        } else {
            return defaultValue;
        }
    }

    protected final void addExtraInfo(String key, String value) {
        jobService.addJobInfo(getId(), key, value);
    }

    public final long getStartTime() {
        return getExtraInfoAsLong(START_TIME, 0L);
    }

    public final long getEndTime() {
        return getExtraInfoAsLong(END_TIME, 0L);
    }

    public JobPO getJobPO() {
        return job;
    }

    /*
    * stop is triggered by JobService, the Scheduler is not awake of that, so
    *
    * */
    protected final boolean isStopped() {
        final ExecutableState status = getStatus();
        return status == ExecutableState.STOPPED || status == ExecutableState.DISCARDED;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("id", getId()).add("name", getName()).add("state", getStatus()).toString();
    }
}
