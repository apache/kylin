package com.kylinolap.job;

import com.kylinolap.job.dao.JobPO;
import com.kylinolap.job.exception.ExecuteException;
import com.kylinolap.job.execution.ExecutableContext;
import com.kylinolap.job.execution.ExecutableState;
import com.kylinolap.job.execution.ExecuteResult;

/**
 * Created by qianzhou on 12/31/14.
 */
public class SelfStopExecutable extends BaseTestExecutable {

    public SelfStopExecutable() {
    }

    public SelfStopExecutable(JobPO job) {
        super(job);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
        }
        if (isDiscarded()) {
            return new ExecuteResult(ExecuteResult.State.STOPPED, "stopped");
        } else {
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        }
    }

}
