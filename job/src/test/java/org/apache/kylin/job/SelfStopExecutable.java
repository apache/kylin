package org.apache.kylin.job;

import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;

/**
 * Created by qianzhou on 12/31/14.
 */
public class SelfStopExecutable extends BaseTestExecutable {

    public SelfStopExecutable() {
        super();
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
