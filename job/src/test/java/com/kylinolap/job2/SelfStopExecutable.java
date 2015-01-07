package com.kylinolap.job2;

import com.kylinolap.job2.dao.JobPO;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.execution.ExecutableContext;
import com.kylinolap.job2.execution.ExecutableState;
import com.kylinolap.job2.execution.ExecuteResult;

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
        if (isStopped()) {
            return new ExecuteResult(ExecuteResult.State.STOPPED, "stopped");
        } else {
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        }
    }

    private boolean isStopped() {
        final ExecutableState status = jobService.getOutput(getId()).getState();
        return status == ExecutableState.STOPPED || status == ExecutableState.DISCARDED;
    }

}
