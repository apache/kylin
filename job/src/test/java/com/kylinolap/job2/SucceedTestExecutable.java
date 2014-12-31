package com.kylinolap.job2;

import com.kylinolap.job2.dao.JobOutputPO;
import com.kylinolap.job2.dao.JobPO;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.execution.ExecutableContext;
import com.kylinolap.job2.execution.ExecuteResult;

/**
 * Created by qianzhou on 12/22/14.
 */
public class SucceedTestExecutable extends BaseTestExecutable {

    public SucceedTestExecutable() {
    }

    public SucceedTestExecutable(JobPO job, JobOutputPO jobOutput) {
        super(job, jobOutput);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
    }
}
