package com.kylinolap.job2;

import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.execution.ExecutableContext;
import com.kylinolap.job2.execution.ExecuteResult;
import com.kylinolap.job2.execution.ExecutableStatus;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;

import java.util.UUID;

/**
 * Created by qianzhou on 12/16/14.
 */
public class TestExecutable extends AbstractExecutable {

    public TestExecutable() {
        this.setId(UUID.randomUUID().toString());
        this.setStatus(ExecutableStatus.READY);
    }
    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new ExecuteException(e);
        }
        return new ExecuteResult(true, "success");
    }

    @Override
    public boolean isRunnable() {
        return getStatus() == ExecutableStatus.READY;
    }
}
