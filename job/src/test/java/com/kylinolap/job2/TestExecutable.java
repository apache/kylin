package com.kylinolap.job2;

import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.execution.ExecutableContext;
import com.kylinolap.job2.execution.ExecuteResult;
import com.kylinolap.job2.execution.ExecuteStatus;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;

import java.util.UUID;

/**
 * Created by qianzhou on 12/16/14.
 */
public class TestExecutable extends AbstractExecutable {

    public TestExecutable() {
        this.setId(UUID.randomUUID().toString());
        this.setAsync(false);
        this.setStatus(ExecuteStatus.NEW);
    }
    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new ExecuteException(e);
        }
        return new ExecuteResult() {
            @Override
            public int statusCode() {
                return 0;
            }

            @Override
            public String output() {
                return "success";
            }
        };
    }

    @Override
    public boolean isRunnable() {
        return getStatus() == ExecuteStatus.NEW || getStatus() == ExecuteStatus.STOPPED || getStatus() == ExecuteStatus.PENDING;
    }
}
