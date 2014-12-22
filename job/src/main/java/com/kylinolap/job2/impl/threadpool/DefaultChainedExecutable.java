package com.kylinolap.job2.impl.threadpool;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.execution.*;
import com.kylinolap.job2.service.DefaultJobService;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by qianzhou on 12/16/14.
 */
public class DefaultChainedExecutable extends AbstractExecutable implements ChainedExecutable {

    private final List<AbstractExecutable> subTasks = new ArrayList<AbstractExecutable>();

    private final DefaultJobService jobService = DefaultJobService.getInstance(KylinConfig.getInstanceFromEnv());

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        List<AbstractExecutable> executables = getExecutables();
        final int size = executables.size();
        for (int i = 0; i < size; ++i) {
            AbstractExecutable subTask = executables.get(i);
            if (subTask.isRunnable()) {
                return subTask.execute(context);
            }
        }
        return new ExecuteResult(true, null);
    }

    @Override
    protected void onExecuteStart(ExecutableContext executableContext) {
        this.setStatus(ExecutableStatus.RUNNING);
        jobService.updateJobStatus(this);
    }

    @Override
    protected void onExecuteError(Throwable exception, ExecutableContext executableContext) {
        this.setStatus(ExecutableStatus.ERROR);
        jobService.updateJobStatus(this);
    }

    @Override
    protected void onExecuteSucceed(ExecuteResult result, ExecutableContext executableContext) {
        if (result.succeed()) {
            List<AbstractExecutable> jobs = getExecutables();
            AbstractExecutable lastJob = jobs.get(jobs.size() - 1);
            if (lastJob.isRunnable()) {
                this.setStatus(ExecutableStatus.READY);
                jobService.updateJobStatus(this);
            } else if (lastJob.getStatus() == ExecutableStatus.SUCCEED) {
                this.setStatus(ExecutableStatus.SUCCEED);
                jobService.updateJobStatus(this);
            } else {

            }
        } else {
            jobService.updateJobStatus(getId(), ExecutableStatus.ERROR, null);
        }
    }

    @Override
    public boolean isRunnable() {
        return getStatus() == ExecutableStatus.READY;
    }

    public void addTask(AbstractExecutable executable) {
        subTasks.add(executable);
    }

    @Override
    public List<AbstractExecutable> getExecutables() {
        return subTasks;
    }
}
