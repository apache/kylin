package org.apache.kylin.job;

import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;

/**
 * Created by qianzhou on 12/22/14.
 */
public class FailedTestExecutable extends BaseTestExecutable {

    public FailedTestExecutable() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        return new ExecuteResult(ExecuteResult.State.FAILED, "failed");
    }
}
