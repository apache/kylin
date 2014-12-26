package com.kylinolap.job2.impl.threadpool;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.job2.dao.JobOutputPO;
import com.kylinolap.job2.dao.JobPO;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.execution.*;
import com.kylinolap.job2.service.DefaultJobService;
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
    private JobOutputPO jobOutput;
    protected static final Logger logger = LoggerFactory.getLogger(AbstractExecutable.class);

    private static DefaultJobService jobService = DefaultJobService.getInstance(KylinConfig.getInstanceFromEnv());

    public AbstractExecutable() {
        String uuid = UUID.randomUUID().toString();
        this.job = new JobPO();
        this.job.setType(this.getClass().getName());
        this.job.setUuid(uuid);

        this.jobOutput = new JobOutputPO();
        this.jobOutput.setUuid(uuid);
        this.jobOutput.setStatus(ExecutableStatus.READY.toString());
    }

    protected AbstractExecutable(JobPO job, JobOutputPO jobOutput) {
        Preconditions.checkArgument(job != null, "job cannot be null");
        Preconditions.checkArgument(jobOutput != null, "jobOutput cannot be null");
        Preconditions.checkArgument(job.getId() != null, "job id cannot be null");
        Preconditions.checkArgument(jobOutput.getId() != null, "jobOutput id cannot be null");
        Preconditions.checkArgument(job.getId().equalsIgnoreCase(jobOutput.getId()), "job id should be equals");
        this.job = job;
        this.jobOutput = jobOutput;
    }

    protected void onExecuteStart(ExecutableContext executableContext) {
        jobService.updateJobStatus(this, ExecutableStatus.RUNNING);
    }

    protected void onExecuteSucceed(ExecuteResult result, ExecutableContext executableContext) {
        if (result.succeed()) {
            jobService.updateJobStatus(this, ExecutableStatus.SUCCEED, result.output());
        } else {
            jobService.updateJobStatus(this, ExecutableStatus.ERROR, result.output());
        }
    }

    protected void onExecuteError(Throwable exception, ExecutableContext executableContext) {
        jobService.updateJobStatus(this, ExecutableStatus.ERROR, exception.getLocalizedMessage());
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
        onExecuteSucceed(result, executableContext);
        return result;
    }

    protected abstract ExecuteResult doWork(ExecutableContext context) throws ExecuteException;

    @Override
    public void stop() throws ExecuteException {

    }

    @Override
    public void cleanup() throws ExecuteException {

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
    public final ExecutableStatus getStatus() {
        return ExecutableStatus.valueOf(jobOutput.getStatus());
    }

    public final void setStatus(ExecutableStatus status) {
        jobOutput.setStatus(status.toString());
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

    public void setOutput(String output) {
        this.jobOutput.setContent(output);
    }

    @Override
    public String getOutput() {
        return jobOutput.getContent();
    }

    public JobPO getJobPO() {
        return job;
    }

    public JobOutputPO getJobOutput() {
        return jobOutput;
    }
}
