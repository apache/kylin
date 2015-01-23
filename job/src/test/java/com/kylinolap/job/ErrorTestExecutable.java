package com.kylinolap.job;

import com.kylinolap.job.dao.ExecutablePO;
import com.kylinolap.job.exception.ExecuteException;
import com.kylinolap.job.execution.ExecutableContext;
import com.kylinolap.job.execution.ExecuteResult;

/**
 * Created by qianzhou on 12/22/14.
 */
public class ErrorTestExecutable extends BaseTestExecutable {

    public ErrorTestExecutable() {
    }

    public ErrorTestExecutable(ExecutablePO job) {
        super(job);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        throw new RuntimeException("test error");
    }
}
