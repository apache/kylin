package com.kylinolap.job2.impl.threadpool;

import com.google.common.collect.Lists;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.job2.dao.JobPO;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.execution.*;
import com.kylinolap.job2.service.ExecutableManager;

import java.util.List;

/**
 * Created by qianzhou on 12/16/14.
 */
public class DefaultChainedExecutable extends AbstractExecutable implements ChainedExecutable {

    private final List<AbstractExecutable> subTasks = Lists.newArrayList();

    protected final ExecutableManager jobService = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv());

    public DefaultChainedExecutable(){
        super();
    }

    public DefaultChainedExecutable(JobPO job) {
        super(job);
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
    protected void onExecuteFinished(ExecuteResult result, ExecutableContext executableContext) {
        if (result.succeed()) {
            List<? extends Executable> jobs = getTasks();
            boolean allSucceed = true;
            boolean hasError = false;
            for (Executable task: jobs) {
                final ExecutableState status = task.getStatus();
                if (status == ExecutableState.ERROR) {
                    hasError = true;
                }
                if (status != ExecutableState.SUCCEED) {
                    allSucceed = false;
                }
            }
            if (allSucceed) {
                jobService.updateJobOutput(getId(), ExecutableState.SUCCEED, null, null);
            } else if (hasError) {
                jobService.updateJobOutput(getId(), ExecutableState.ERROR, null, null);
            } else {
                jobService.updateJobOutput(getId(), ExecutableState.READY, null, null);
            }
        } else if (result.state() == ExecuteResult.State.STOPPED) {
            if (getStatus() == ExecutableState.STOPPED) {
                //
            } else {
                jobService.updateJobOutput(getId(), ExecutableState.ERROR, null, null);
            }
        } else {
            jobService.updateJobOutput(getId(), ExecutableState.ERROR, null, null);
        }
    }

    @Override
    public List<AbstractExecutable> getTasks() {
        return subTasks;
    }

    public final AbstractExecutable getTaskByName(String name) {
        for (AbstractExecutable task : subTasks) {
            if (task.getName() != null && task.getName().equalsIgnoreCase(name)) {
                return task;
            }
        }
        return null;
    }

    public void addTask(AbstractExecutable executable) {
        this.subTasks.add(executable);
    }
}
