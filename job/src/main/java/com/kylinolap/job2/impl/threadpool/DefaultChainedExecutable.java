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
                ExecuteResult result = subTask.execute(context);
                if (result.succeed()) {
                    this.setStatus(ExecutableStatus.READY);
                    jobService.updateJobStatus(getId(), ExecutableStatus.READY, null);
                } else {
                    jobService.updateJobStatus(getId(), ExecutableStatus.ERROR, null);
                }
            }
        }
        jobService.updateJobStatus(getId(), ExecutableStatus.SUCCEED, null);
        return new ExecuteResult(true, null);
    }

    @Override
    public boolean isRunnable() {
        for (Executable subTask: getExecutables()) {
            if (subTask.isRunnable()) {
                return true;
            }
        }
        return false;
    }

    public void addTask(AbstractExecutable executable) {
        subTasks.add(executable);
    }

    @Override
    public List<AbstractExecutable> getExecutables() {
        return subTasks;
    }
}
