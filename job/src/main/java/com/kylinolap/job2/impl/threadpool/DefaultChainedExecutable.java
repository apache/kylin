package com.kylinolap.job2.impl.threadpool;

import com.google.common.collect.Lists;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.job2.dao.JobOutputPO;
import com.kylinolap.job2.dao.JobPO;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.execution.*;
import com.kylinolap.job2.service.DefaultJobService;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by qianzhou on 12/16/14.
 */
public class DefaultChainedExecutable extends AbstractExecutable implements ChainedExecutable {

    private final List<AbstractExecutable> subTasks = Lists.newArrayList();

    private final DefaultJobService jobService = DefaultJobService.getInstance(KylinConfig.getInstanceFromEnv());

    public DefaultChainedExecutable(){
        super();
    }

    public DefaultChainedExecutable(JobPO job, JobOutputPO jobOutput) {
        super(job, jobOutput);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        List<? extends Executable> executables = getTasks();
        final int size = executables.size();
        for (int i = 0; i < size; ++i) {
            Executable subTask = executables.get(i);
            if (subTask.isRunnable()) {
                return subTask.execute(context);
            }
        }
        return new ExecuteResult(ExecuteResult.State.SUCCEED, null);
    }

    @Override
    protected void onExecuteStart(ExecutableContext executableContext) {
        jobService.updateJobStatus(this, ExecutableStatus.RUNNING);
    }

    @Override
    protected void onExecuteError(Throwable exception, ExecutableContext executableContext) {
        jobService.updateJobStatus(this, ExecutableStatus.ERROR);
    }

    @Override
    protected void onExecuteSucceed(ExecuteResult result, ExecutableContext executableContext) {
        if (result.succeed()) {
            List<? extends Executable> jobs = getTasks();
            Executable lastJob = jobs.get(jobs.size() - 1);
            if (lastJob.isRunnable()) {
                jobService.updateJobStatus(this, ExecutableStatus.READY);
            } else if (lastJob.getStatus() == ExecutableStatus.SUCCEED) {
                jobService.updateJobStatus(this, ExecutableStatus.SUCCEED);
            } else {

            }
        } else if (result.state() == ExecuteResult.State.STOPPED) {
            jobService.updateJobStatus(this, ExecutableStatus.STOPPED, null);
        } else {
            jobService.updateJobStatus(this, ExecutableStatus.ERROR, null);
        }
    }

    @Override
    public boolean isRunnable() {
        return getStatus() == ExecutableStatus.READY;
    }

    @Override
    public List<AbstractExecutable> getTasks() {
        return subTasks;
    }

    public void addTask(AbstractExecutable executable) {
        this.subTasks.add(executable);
    }
}
