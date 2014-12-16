package com.kylinolap.job2.impl.threadpool;

import com.kylinolap.common.util.Array;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.execution.ChainedExecutable;
import com.kylinolap.job2.execution.Executable;
import com.kylinolap.job2.execution.ExecutableContext;
import com.kylinolap.job2.execution.ExecuteResult;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by qianzhou on 12/16/14.
 */
public class DefaultChainedExecutable extends AbstractExecutable implements ChainedExecutable {

    private final List<AbstractExecutable> subTasks = new ArrayList<AbstractExecutable>();

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        for (Executable subTask: getExecutables()) {
            if (subTask.isRunnable()) {
                return subTask.execute(context);
            }
        }
        throw new ExecuteException("this job:" + getId() + " is not Runnable");
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
